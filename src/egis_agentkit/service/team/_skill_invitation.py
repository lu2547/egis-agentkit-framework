"""在个人助理会话锁内登记明确的技能包安装委托。"""

import asyncio
import json
import re
from pathlib import PurePosixPath

from agentscope.message import Msg

from egis_agentkit.client.nacos._settings import TeamInviteConfig
from egis_agentkit.core.skill._assistant import SKILL_ASSISTANT_ID
from egis_agentkit.errors import Forbidden, NotFound
from egis_agentkit.service.agent._agent_manager import get_definition_for_execution
from egis_agentkit.service.team._collaboration import (
    collaboration_allowed,
    collaborators,
)
from egis_agentkit.storage.model import new_id
from egis_agentkit.storage.model._definition import build_definition_reference

_ATTACHMENTS = "\n\n已上传的项目资料（工作空间根目录）：\n"
_INSTALL = r"(?:(?:请|麻烦)\s*)?(?:(?:帮我|给我)\s*)?(?:安装|装|install)\s*(?:一下\s*)?"
_ATTACHED_REQUEST = re.compile(
    _INSTALL
    + r"(?:(?:这个|那个|刚上传的|附件里的|the\s+attached)\s*)?"
    + r"(?:技能包|技能|skill(?:\s+package)?|zip|压缩包)?",
    re.IGNORECASE,
)
_NAMED_REQUEST = re.compile(_INSTALL + r"(.+?\.zip)", re.IGNORECASE)

def _requested_package(message: Msg) -> str | None:
    """只识别明确的安装命令，目标来自随消息发出的附件或用户点名。"""
    command, _, references = (message.get_text_content() or "").partition(_ATTACHMENTS)
    command = command.strip().rstrip("。！!").strip()
    attached = {
        line.removeprefix("- ")
        for line in references.splitlines()
        if line.startswith("- ") and line.lower().endswith(".zip")
    }
    named = _NAMED_REQUEST.fullmatch(command)
    if named:
        target = named[1].strip()
        if target in attached or not attached:
            return target
        matches = [path for path in attached if PurePosixPath(path).name == target]
        return matches[0] if len(matches) == 1 else None
    if len(attached) == 1 and _ATTACHED_REQUEST.fullmatch(command):
        return next(iter(attached))
    return None


async def _find_member(user, scope, binding, permissions):
    members = collaborators(binding)
    for member in members:
        if member["definition_id"] == SKILL_ASSISTANT_ID:
            return member
    limits = (
        scope.configuration.team.invite if scope.configuration else TeamInviteConfig()
    )
    if len(members) >= limits.max_members:
        return None
    try:
        definition = await get_definition_for_execution(
            permissions, user, "agent", SKILL_ASSISTANT_ID, "system"
        )
    except (Forbidden, NotFound):
        return None
    return {
        **build_definition_reference(definition),
        "display_name": definition["agent_display_name"],
        "avatar": definition["payload"].get("avatar"),
        "native_agent_id": new_id(),
        "status": "pending",
    }


async def _is_uploaded_file(files, user_id, project_id, path):
    parent = PurePosixPath(path).parent.as_posix()
    try:
        entries = await asyncio.to_thread(
            files.list, user_id, project_id, "uploads", parent
        )
    except (Forbidden, NotFound):
        return False
    return any(item["path"] == path and not item["is_directory"] for item in entries)


async def stage_skill_invitation(user, scope, state, inputs, permissions, files):
    """校验本轮请求和专家授权，将单次委托加入当前会话的邀请日志。"""
    if not collaboration_allowed(scope.session_record, scope.root_definition):
        return
    messages = [inputs] if isinstance(inputs, Msg) else inputs
    if not isinstance(messages, list) or len(messages) != 1:
        return
    message = messages[0]
    if not isinstance(message, Msg) or message.role != "user":
        return
    path = _requested_package(message)
    if path is None or not await _is_uploaded_file(
        files, user.user_id, scope.project_id, path
    ):
        return

    binding = state.middle_context["egis_agentkit"]
    journal = binding.get("collaboration", {})
    requests = journal.get("requests", [])
    request_id = f"skill-install:{message.id}"
    if any(
        request["request_id"] == request_id
        or (
            not request.get("completed")
            and any(
                expert["definition_id"] == SKILL_ASSISTANT_ID
                for expert in request["experts"]
            )
        )
        for request in requests
    ):
        return
    member = await _find_member(user, scope, binding, permissions)
    if member is None:
        return
    journal = binding.setdefault("collaboration", {"requests": [], "members": []})
    if member not in journal["members"]:
        journal["members"].append(member)
    journal["requests"].append(
        {
            "request_id": request_id,
            "experts": [
                {
                    key: member[key]
                    for key in (
                        "definition_id",
                        "resource_view_type",
                        "resource_version",
                    )
                }
            ],
            "completed": False,
            "prompt": (
                "用户明确要求安装当前项目上传区中的技能包。"
                "请用 InstallSkillPackage 工具校验并安装，不要运行包中的脚本。"
                f"工具参数 path 为 {json.dumps(path, ensure_ascii=False)}。"
                "安装结果请通过 TeamSay 向个人助理汇报。"
            ),
        }
    )
