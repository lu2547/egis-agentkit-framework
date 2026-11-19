"""仅在 NAS 主机运行的内部文件服务。"""

import json
import logging
import os
from hmac import compare_digest
from pathlib import Path
from types import SimpleNamespace

from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.requests import ClientDisconnect, Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from egis_agentkit.client._logging import exception_origin
from egis_agentkit.client.nacos import AgentKitConfig
from egis_agentkit.client.nas_files import decode_wire, encode_wire
from egis_agentkit.core.agent._personal_definition import DOCUMENT_FILES
from egis_agentkit.errors import AgentKitError, Conflict, Forbidden, InvalidRequest
from egis_agentkit.storage._files import FileStorage
from egis_agentkit.storage.project._files import ProjectFileStorage
from egis_agentkit.storage.skill._upload import SkillUploadStorage
from egis_agentkit.storage.skill._version import SkillVersionStorage

logger = logging.getLogger(__name__)

_OPERATIONS = {
    "workspace": {
        "initialize_user_workspace",
        "read_user_texts",
        "write_user_texts",
    },
    "project": {
        "initialize_project",
        "read",
        "write",
        "update_file",
        "list",
        "deliver",
        "scan",
        "context_inputs",
        "discover_deliveries",
        "remove",
        "remove_project",
        "write_probe_file",
        "read_probe_file",
        "remove_probe_file",
    },
    "skill_version": {"publish", "read", "read_runtime", "read_avatar", "write_avatar"},
    "skill_upload": {"write", "read", "remove"},
}
_PREFERENCE_FILES = frozenset(DOCUMENT_FILES.values())
_REQUEST_OVERHEAD_BYTES = 64 * 1024
_PROBE_BYTES = 1024


def _request_limit(snapshot, scope, operation):
    """为 Base64、JSON 字段以及来自 ZIP 或扫描记录的路径留出容量。"""
    project, skill = snapshot.project, snapshot.skill.archive
    binary = extra = 0
    if (scope, operation) == ("workspace", "write_user_texts"):
        extra = 6 * len(_PREFERENCE_FILES) * snapshot.agent.personal.documents.max_bytes
    elif scope == "project" and operation in {"write", "update_file"}:
        binary = project.upload.max_bytes
    elif (scope, operation) == ("project", "discover_deliveries"):
        entries = max(project.scan.max_entries, project.delivery.max_entries)
        extra = 2 * entries * (6 * project.scan.max_path_bytes + 32)
    elif (scope, operation) == ("skill_upload", "write"):
        binary = skill.max_bytes
    elif (scope, operation) == ("skill_version", "publish"):
        binary = skill.expanded_bytes_limit
        extra = 6 * skill.max_bytes
    return _REQUEST_OVERHEAD_BYTES + 4 * ((binary + 2) // 3) + extra


def _bounded_limit(value, ceiling):
    if type(value) is not int or value <= 0:
        raise InvalidRequest("文件大小限额必须为正整数")
    return min(value, ceiling)


def _validate_arguments(snapshot, scope, operation, arguments):
    if not isinstance(arguments, dict) or arguments.keys() & {"limits", "settings"}:
        raise InvalidRequest("无效的文件操作参数")
    if "content" in arguments and not isinstance(arguments["content"], bytes):
        raise InvalidRequest("文件内容必须为字节数据")
    for name in arguments.keys() & {"recursive", "return_metadata", "overwrite"}:
        value = arguments[name]
        if type(value) is not bool and (name != "overwrite" or value is not None):
            raise InvalidRequest("无效的文件操作选项")

    if scope == "workspace" and operation in {"read_user_texts", "write_user_texts"}:
        limit = _bounded_limit(
            arguments.get("max_bytes"), snapshot.agent.personal.documents.max_bytes
        )
        if arguments.get("directory") != "preferences":
            raise Forbidden("只允许访问个人偏好目录")
        if operation == "read_user_texts":
            filenames = arguments.get("filenames")
            if not isinstance(filenames, (list, tuple)) or len(filenames) > len(
                _PREFERENCE_FILES
            ):
                raise InvalidRequest("无效的偏好文档列表")
        else:
            documents = arguments.get("documents")
            if not isinstance(documents, dict) or any(
                not isinstance(value, str) for value in documents.values()
            ):
                raise InvalidRequest("无效的偏好文档")
            filenames = documents
            if any(len(value.encode("utf-8")) > limit for value in documents.values()):
                raise Conflict("偏好文档超过大小限制")
        if any(
            not isinstance(name, str) or name not in _PREFERENCE_FILES
            for name in filenames
        ):
            raise Forbidden("只允许访问受管偏好文档")
        arguments["max_bytes"] = limit

    if scope == "skill_version" and operation in {"publish", "read", "read_runtime"}:
        arguments["max_bytes"] = _bounded_limit(
            arguments.get("max_bytes"), snapshot.skill.archive.expanded_bytes_limit
        )
        if operation == "publish":
            files = arguments.get("files")
            if not isinstance(files, dict) or any(
                not isinstance(name, str) or not isinstance(content, bytes)
                for name, content in files.items()
            ):
                raise InvalidRequest("无效的技能文件")

    if (scope, operation) == ("project", "read_probe_file"):
        arguments["size"] = _bounded_limit(arguments.get("size"), _PROBE_BYTES)
    if (scope, operation) == ("project", "write_probe_file"):
        if len(arguments.get("content", b"")) > _PROBE_BYTES:
            raise Conflict("存储探针超过大小限制")


def _execute(snapshot, storage, scope, operation, body):
    try:
        arguments = decode_wire(json.loads(body))
        _validate_arguments(snapshot, scope, operation, arguments)
        result = getattr(storage, operation)(**arguments)
        return JSONResponse(encode_wire(result))
    except AgentKitError as exc:
        system = exc.status_code >= 500
        logger.log(
            logging.ERROR if system else logging.WARNING,
            "NAS file operation failed" if system else "NAS file operation rejected",
            extra={
                "component": f"{type(storage).__name__}.{operation}",
                "context": {
                    "category": "system" if system else "business",
                    "scope": scope,
                    "error_type": type(exc).__name__,
                    "error_description": str(exc),
                    "origin": exception_origin(exc),
                },
            },
            exc_info=exc if system else None,
        )
        return _error(exc.status_code, type(exc).__name__, str(exc))
    except (TypeError, ValueError, KeyError, OverflowError, RecursionError):
        return _error(422, "InvalidRequest", "无效的文件操作参数")
    except Exception:
        logger.exception(
            "NAS file operation failed",
            extra={
                "component": "_execute",
                "context": {"category": "system", "scope": scope, "operation": operation},
            },
        )
        return _error(503, "Unavailable", "工作区文件服务暂时不可用")


def _error(status, code, detail, *, headers=None):
    return JSONResponse({"error": code, "detail": detail}, status, headers=headers)


def create_nas_file_app(snapshot: AgentKitConfig) -> Starlette:
    """按部署快照组装文件入口，复用 NAS 上的本地存储校验。"""
    files = snapshot.workspace.files
    token = os.environ.get(files.token_env, "")
    if not token or not token.isascii() or any(ch.isspace() for ch in token):
        raise ValueError(f"文件服务凭据 {files.token_env} 缺失或格式无效")
    authorization = f"Bearer {token}".encode("ascii")

    workspace = snapshot.workspace.model_copy(
        update={"files": files.model_copy(update={"win": False})}
    )
    config = SimpleNamespace(
        snapshot=snapshot.model_copy(update={"workspace": workspace})
    )
    root = Path(workspace.runtime.root)
    storage = {
        "workspace": FileStorage(root, config_provider=config),
        "project": ProjectFileStorage(root, config_provider=config),
        "skill_version": SkillVersionStorage(root, config_provider=config),
        "skill_upload": SkillUploadStorage.from_config(root, config),
    }

    async def execute(request: Request):
        supplied = request.headers.get("Authorization", "").encode("latin-1")
        if not compare_digest(supplied, authorization):
            return _error(
                401,
                "Unauthorized",
                "文件服务鉴权失败",
                headers={"WWW-Authenticate": "Bearer"},
            )
        scope, operation = (
            request.path_params["scope"],
            request.path_params["operation"],
        )
        if operation not in _OPERATIONS.get(scope, ()):
            return _error(404, "NotFound", "文件操作不存在")
        media_type = request.headers.get("Content-Type", "").partition(";")[0]
        if media_type.strip().lower() != "application/json":
            return _error(415, "InvalidRequest", "文件操作需要 JSON 请求")

        maximum = _request_limit(snapshot, scope, operation)
        body = bytearray()
        try:
            async for chunk in request.stream():
                if len(body) + len(chunk) > maximum:
                    return _error(413, "Conflict", "文件服务请求超过大小限制")
                body.extend(chunk)
        except ClientDisconnect:
            return _error(400, "InvalidRequest", "文件服务请求已中断")
        return await run_in_threadpool(
            _execute, snapshot, storage[scope], scope, operation, body
        )

    return Starlette(
        routes=[Route("/v1/nas/{scope}/{operation}", execute, methods=["POST"])]
    )
