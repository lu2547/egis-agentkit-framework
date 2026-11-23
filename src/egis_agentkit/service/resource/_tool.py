"""注册系统默认及专家专属工具，校验执行范围与调用参数。"""

from __future__ import annotations

import hashlib
import json
from functools import partial
from types import SimpleNamespace

from agentscope import __version__ as AGENTSCOPE_VERSION
from agentscope.app._manager._background_task_manager import ToolStop
from agentscope.app._manager._scheduler._tools import (
    ScheduleCreate,
    ScheduleDelete,
    ScheduleList,
    ScheduleView,
)
from agentscope.app._tool import (
    AgentCreate,
    AgentInvite,
    SubmitHandover,
    SubmitVerdict,
    TeamCreate,
    TeamDelete,
    TeamSay,
)
from agentscope.app._tool._agent_invite import _AgentInviteParams
from agentscope.middleware._longterm_memory._reme._tools import _MemorySearchTool
from agentscope.tool import (
    Bash,
    Edit,
    Glob,
    Grep,
    PowerShell,
    Read,
    TaskCreate,
    TaskGet,
    TaskList,
    TaskUpdate,
    Write,
)
from agentscope.tool._builtin._meta import ResetTools
from agentscope.tool._builtin._skill import SkillViewer
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

from egis_agentkit.client.nacos import NacosConfigProvider
from egis_agentkit.core.agent._invite_tool import ExpertInvite
from egis_agentkit.core.session._context import get_verified_chat_token
from egis_agentkit.errors import AgentKitError, Conflict, Forbidden, NotFound
from egis_agentkit.service.access._permission import is_system_default_tool
from egis_agentkit.storage.model import (
    Asset,
)
from egis_agentkit.tool._internet_search_tool import InternetSearch
from egis_agentkit.tool._knowledgebase_tool import KnowledgeBaseSearch
from egis_agentkit.tool._schedule_tools import SCHEDULE_TOOLS, SCHEDULE_TOOL_NAMES

from ._registry import _LocalRegistry


def _entrypoint(cls):
    if isinstance(cls, partial):
        cls = cls.func
    return f"{cls.__module__}:{cls.__qualname__}"


class FunctionToolRegistry(_LocalRegistry):
    """登记平台默认工具和随专家执行的部署工具。"""

    def __init__(self, resources):
        super().__init__(resources.storage.assets, "tool")
        self.resources = resources
        self.catalog = {}
        self.native = {}
        self.deployed = {}
        self.optional = set()
        self.schedule_service = None
        for cls in (Bash, PowerShell, Read, Write, Edit, Glob, Grep):
            self._native(cls, "workspace")
        for cls in (TaskCreate, TaskList, TaskGet, TaskUpdate, ToolStop, SkillViewer):
            self._native(cls, "toolkit")
        for cls in (SubmitHandover, SubmitVerdict, ScheduleList, ScheduleView):
            self._native(cls, "toolkit")
        self._native(KnowledgeBaseSearch, "optional", version="1.1.0")
        self._native(InternetSearch, "optional")
        for cls in SCHEDULE_TOOLS:
            self._native(cls, "optional", display_name=cls.label)
        self._native(TeamSay, "toolkit", exposure="team")
        for cls in (
            TeamCreate,
            AgentCreate,
            TeamDelete,
            ScheduleCreate,
            ScheduleDelete,
        ):
            self._native(cls, "toolkit", exposure="internal")
        self._native(
            AgentInvite,
            "toolkit",
            exposure="internal",
            schema=_AgentInviteParams.model_json_schema(),
        )
        self._native(
            ResetTools,
            "toolkit",
            schema=ResetTools(groups=[], response_template=None).input_schema,
        )
        # ReMe's default limit is configured per middleware instance.
        memory_tool = _MemorySearchTool(
            SimpleNamespace(_parameters=SimpleNamespace(top_k=5))
        )
        self._native(_MemorySearchTool, "middleware", schema=memory_tool.input_schema)
        self._add(
            "CompressContext",
            AGENTSCOPE_VERSION,
            "CompressContext",
            "toolkit",
            "Compress the active model context.",
            {"type": "object", "properties": {}},
            "agentscope.agent._agent:Agent._compress_context_tool",
        )

    def bind_default_configuration(self) -> None:
        config = self.resources.config
        if isinstance(config, NacosConfigProvider):
            config.bind_platform_tool_ids(
                {
                    resource_id
                    for (resource_id, _), asset in self.catalog.items()
                    if is_system_default_tool(asset.model_dump())
                }
            )

    def is_default_tool(self, asset, scope=None) -> bool:
        if asset["resource_id"] in (ScheduleList.name, ScheduleView.name):
            return False
        if not is_system_default_tool(asset):
            return False
        registered = self.catalog.get((asset["resource_id"], asset["resource_version"]))
        if registered is None or registered.checksum_sha256 != asset["checksum_sha256"]:
            return False
        configuration = getattr(scope, "configuration", None)
        if configuration is None and self.resources.config is not None:
            configuration = self.resources.config.snapshot
        if configuration is None:
            return True
        allowed_ids = configuration.tool.defaults.registered_ids
        return allowed_ids is None or asset["resource_id"] in allowed_ids

    def is_optional_tool(self, asset) -> bool:
        key = asset["resource_id"], asset["resource_version"]
        return (
            asset["resource_type"] == "tool"
            and key in self.optional
            and asset["checksum_sha256"] == self.catalog[key].checksum_sha256
        )

    def optional_tool_available(self, asset, scope=None) -> bool:
        if not self.is_optional_tool(asset):
            return False
        configuration = getattr(scope, "configuration", None)
        if configuration is None and self.resources.config is not None:
            configuration = self.resources.config.snapshot
        if asset["resource_id"] in SCHEDULE_TOOL_NAMES:
            return self.schedule_service is not None and (
                scope is None or scope.session_record.origin.type == "user"
            )
        if asset["resource_id"] == KnowledgeBaseSearch.name:
            return (
                configuration is not None
                and configuration.tool.knowledge_base.query_url is not None
            )
        return True

    def _add(
        self,
        rid,
        version,
        name,
        source,
        description,
        schema,
        entrypoint,
        exposure="model",
        dependency_only=False,
        display_name=None,
    ):
        self.resources._identity(rid, version)
        if not isinstance(schema, dict):
            raise ValueError("Function Tool input schema must be an object")
        Draft202012Validator.check_schema(schema)
        payload = {
            "tool_name": name,
            "display_name": display_name or name,
            "source": source,
            "description": description,
            "input_schema": schema,
            "exposure": exposure,
        }
        if dependency_only:
            payload["dependency_only"] = True
        checksum = hashlib.sha256(
            json.dumps([payload, entrypoint], sort_keys=True).encode()
        ).hexdigest()
        self.catalog[(rid, version)] = Asset(
            resource_type="tool",
            resource_id=rid,
            resource_version=version,
            owner_type="system",
            owner_id="system",
            entrypoint=entrypoint,
            payload=payload,
            checksum_sha256=checksum,
        )

    def _native(
        self, cls, source, *, exposure="model", schema=None, version=None,
        display_name=None,
    ):
        # SDK 的工具说明和参数随 SDK 发版；独立部署的业务工具保留自己的版本。
        version = version or (
            AGENTSCOPE_VERSION if cls.__module__.startswith("agentscope.") else "1.0.0"
        )
        description = getattr(cls, "description", None)
        if cls is Read:
            description = Read().description
        if not isinstance(description, str):
            description = (cls.__doc__ or cls.name).strip().split("\n", 1)[0]
        self._add(
            cls.name,
            version,
            cls.name,
            source,
            description,
            cls.input_schema if schema is None else schema,
            _entrypoint(cls),
            exposure,
            display_name=display_name,
        )
        self.native[cls] = (cls.name, version)
        if source == "optional":
            self.optional.add((cls.name, version))

    def deploy(
        self,
        rid,
        version,
        factory,
        *,
        description,
        input_schema,
        name=None,
        dependency_only=False,
    ):
        key = rid, version
        if key in self.catalog:
            raise ValueError("Function Tool version already registered")
        self._add(
            rid,
            version,
            name or rid,
            "deployed",
            description,
            input_schema,
            _entrypoint(factory),
            dependency_only=dependency_only,
        )
        self.deployed[key] = factory

    async def seed(self):
        async with self.resources.storage.definition_write() as write:
            await write.assets.lock_resources("tool", [rid for rid, _ in self.catalog])
            existing = {
                (row["resource_id"], row["resource_version"]): row
                for row in await write.assets.get_many("tool", self.catalog)
            }
            missing, enable = [], []
            for key, asset in self.catalog.items():
                row = existing.get(key)
                if row is None:
                    missing.append(asset)
                elif row["checksum_sha256"] != asset.checksum_sha256:
                    raise Conflict(
                        f"Function Tool {key[0]} {key[1]} changed without a new version"
                    )
                elif not row["is_enabled"] and is_system_default_tool(row):
                    enable.append(key)
            await write.assets.insert_assets(missing, actor="system")
            await write.assets.enable_versions("tool", enable, actor="system")

    def is_agent_tool(self, asset):
        """判断工具版本是否已启用且在当前进程部署。"""
        key = asset["resource_id"], asset["resource_version"]
        return (
            asset["resource_type"] == "tool"
            and asset["is_enabled"]
            and key in self.deployed
            and asset["checksum_sha256"] == self.catalog[key].checksum_sha256
        )

    async def resolve(self, user, scope):
        """装配已开放的平台工具及当前专家绑定的工具。"""
        resources = self.resources
        tools = []
        configuration = getattr(scope, "configuration", None)
        settings = (
            configuration.tool.knowledge_base if configuration is not None else None
        )
        selections = (
            await resources.personal.list_resources(user, "tool")
            if scope.definition["agent_type"] == "personal"
            else {}
        )
        optional_keys = [
            key for key in self.optional
            if selections.get(key[0], {}).get("is_enabled")
        ]
        knowledge_key = self.native[KnowledgeBaseSearch]
        keys = (
            [
                (ref["resource_id"], ref["resource_version"])
                for ref in scope.definition["payload"].get("bindings", [])
                if ref["resource_type"] == "tool"
                and (ref["resource_id"], ref["resource_version"]) in self.deployed
            ]
            if scope.definition["agent_type"] == "expert"
            else []
        )
        requested = [*keys, *optional_keys]
        if not requested:
            return tools
        rows = await self.assets.get_many("tool", requested)
        assets = {(row["resource_id"], row["resource_version"]): row for row in rows}
        snapshot = await resources._read_snapshot(
            user, rows, scope=scope if keys else None
        )
        user = snapshot.user
        if knowledge_key in optional_keys:
            asset = assets.get(knowledge_key)
            if asset is None:
                raise NotFound()
            if await resources._usable(user, asset, scope, snapshot=snapshot):
                access_token = get_verified_chat_token(
                    scope.user_id, scope.agent_id, scope.session_id
                )
                tools.append(KnowledgeBaseSearch(settings, access_token=access_token))
        internet_key = self.native[InternetSearch]
        if internet_key in optional_keys:
            asset = assets.get(internet_key)
            if asset is None:
                raise NotFound()
            if await resources._usable(user, asset, scope, snapshot=snapshot):
                tools.append(InternetSearch())
        for cls in SCHEDULE_TOOLS:
            key = self.native[cls]
            if key not in optional_keys:
                continue
            asset = assets.get(key)
            if asset is None:
                raise NotFound()
            if await resources._usable(user, asset, scope, snapshot=snapshot):
                tools.append(cls(self.schedule_service, user, scope.project_id))
        for key in keys:
            asset = assets.get(key)
            if asset is None:
                raise NotFound()
            if not await resources._usable(user, asset, scope, snapshot=snapshot):
                continue
            tool = self.deployed[key](scope)
            expected = self.catalog[key].payload
            if (
                tool.name != expected["tool_name"]
                or tool.input_schema != expected["input_schema"]
            ):
                raise Conflict(
                    "Function Tool implementation differs from its registered schema"
                )
            tool._egis_tool_registration = (self, key, scope.session_id)
            tools.append(tool)
        return tools

    def _tool_key(self, scope, agent, tool):
        managed_invite = type(tool) is ExpertInvite
        if managed_invite and scope.definition.get("agent_type") != "personal":
            raise Forbidden("只有个人助理可使用自动邀请")
        key = self.native.get(AgentInvite if managed_invite else type(tool))
        if tool is getattr(agent, "_compression_tool", None):
            key = ("CompressContext", AGENTSCOPE_VERSION)
        registered = getattr(tool, "_egis_tool_registration", None)
        if (
            registered is not None
            and registered[0] is self
            and registered[2] == scope.session_id
        ):
            key = registered[1]
        if key not in self.catalog:
            raise Forbidden("工具未在平台注册")
        return key

    async def authorize_many(self, user, scope, tools):
        """Display-time authorization reads the exact builtin asset set once."""
        selected = []
        for tool in tools:
            try:
                key = self._tool_key(scope, None, tool)
            except AgentKitError:
                continue
            selected.append((key, tool))
        assets = {
            (row["resource_id"], row["resource_version"]): row
            for row in await self.assets.get_many("tool", [key for key, _ in selected])
        }
        result = []
        for key, tool in selected:
            try:
                asset = assets.get(key)
                if asset is None:
                    continue
                await self._authorize_loaded(user, scope, tool, key, asset)
            except AgentKitError:
                continue
            result.append(tool)
        return result

    async def authorize(self, user, scope, agent, tool, parameters=None):
        """调用前校验登记来源、当前专家绑定及工具参数。"""
        key = self._tool_key(scope, agent, tool)
        try:
            asset = await self.resources._asset("tool", *key)
        except NotFound:
            raise Forbidden("工具未在平台注册") from None
        return await self._authorize_loaded(user, scope, tool, key, asset, parameters)

    async def _authorize_loaded(self, user, scope, tool, key, asset, parameters=None):
        expected = self.catalog[key]
        exposure = (
            "team" if type(tool) is ExpertInvite else expected.payload["exposure"]
        )
        if (
            asset["checksum_sha256"] != expected.checksum_sha256
            or exposure not in ("model", "team")
            or tool.name != expected.payload["tool_name"]
        ):
            raise Forbidden("工具来源或暴露范围无效")
        if exposure == "team":
            if not asset["is_enabled"] or not user.is_enabled:
                raise Forbidden("专家团消息工具已停用")
        else:
            await self.resources._require(user, asset, scope)
        if parameters is not None:
            try:
                Draft202012Validator(tool.input_schema).validate(parameters)
            except ValidationError:
                raise Forbidden("工具参数不符合已登记的 Schema") from None
        return True
