"""约定 Agent 定义的创建能力，并复用原生记录装配和版本写入。

核心能力
- build 创建首版，由类型决定创建方式。
- make_record 使用定义构造 AgentScope 的运行身份。
- 可编辑定义复用同一套写入、更新及停用流程。
"""

from abc import ABC, abstractmethod
from typing import Literal

from agentscope.app.storage import AgentChatConfig, AgentData, AgentRecord, InviteConfig

from egis_agentkit.errors import Forbidden, NotFound
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.storage.model import Definition, User


class AgentDefinitionBase(ABC):
    """个人助理、专家、主理人和远程专家共用的定义能力。"""

    agent_type: str

    def __init__(self, storage, policies=None):
        self.storage = storage
        self.repository = storage.definitions
        self.permissions = PermissionService(storage, policies)

    @abstractmethod
    async def build(self, user: User, data: Definition | None = None) -> dict:
        """按具体类型的规则创建首版定义。"""

    @staticmethod
    def make_record(
        user_id: str,
        definition: dict,
        *,
        source: Literal["user", "team"] = "user",
        invite_description: str | None = None,
        record_id: str | None = None,
    ) -> AgentRecord:
        """从业务定义生成原生身份，提示词在运行时按定义装配。"""
        return AgentRecord(
            user_id=user_id,
            source=source,
            data=AgentData(
                name=definition["agent_name"],
                system_prompt="",
                chat_config=AgentChatConfig(
                    invite_config=InviteConfig(
                        invitable=invite_description is not None,
                        invite_description=invite_description,
                    )
                ),
            ),
            **({"id": record_id} if record_id is not None else {}),
        )


class EditableAgentDefinition(AgentDefinitionBase):
    """可编辑定义的模板方法，类型差异由写入授权和数据模型表达。"""

    async def build(self, user: User, data: Definition | None = None) -> dict:
        """校验写入资格并创建首版。"""
        if data is None:
            raise ValueError("创建 Agent 需要定义数据")
        return await self._write(user, data, creating=True)

    async def save(self, user: User, data: Definition) -> dict:
        """校验原归属、版本和绑定，保存下一版。"""
        return await self._write(user, data, creating=False)

    def _authorize_write(self, user: User, data: Definition) -> None:
        self.permissions.require_definition_write(
            user, owner_type=data.owner_type, owner_id=data.owner_id
        )
        if data.agent_type != self.agent_type:
            raise Forbidden("不能通过此定义维护其他类型的 Agent")

    async def _write(self, user: User, data: Definition, *, creating: bool) -> dict:
        self._authorize_write(user, data)
        async with self.storage.definition_write() as write:
            # 更新只允许修改配置。归属和类型从锁定的定义读取。
            if not creating:
                previous = await write.definitions.get_definition_for_update(data.id)
                identity = ("owner_type", "owner_id", "definition_type", "agent_type")
                if any(previous[field] != getattr(data, field) for field in identity):
                    raise Forbidden("不能通过保存改变资源归属或类型")

            # 绑定授权、定义版本和所有者授权在同一个事务内提交。
            await PermissionService(
                write, self.permissions.policies
            ).require_agent_bindings(user, data)
            values = data.model_dump(mode="json")
            if creating:
                saved = await write.definitions.insert_definition(
                    values, actor=user.user_id
                )
            else:
                saved = await write.definitions.update_definition(
                    data.id, values, actor=user.user_id
                )
            await GrantService(write).record_definition_owner(saved, actor=user.user_id)
            return saved

    def _authorize_disable(self, user: User, row: dict) -> None:
        self.permissions.require_definition_write(
            user, owner_type=row["owner_type"], owner_id=row["owner_id"]
        )

    async def disable(self, user: User, identifier: str) -> None:
        """停用指定类型的定义并回收它发出的授权。"""
        async with self.storage.definition_write() as write:
            row = await write.definitions.locked_definition(identifier)
            if row is None or row["definition_type"] != "agent":
                raise NotFound()
            if row["agent_type"] != self.agent_type:
                raise Forbidden("不能通过此定义停用其他类型的 Agent")
            self._authorize_disable(user, row)
            await write.definitions.disable_definition(identifier, actor=user.user_id)
            await GrantService(write).retire_definition_grants(
                identifier, actor=user.user_id, previous=row
            )
