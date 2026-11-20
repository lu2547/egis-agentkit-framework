"""保存单次会话执行上下文，供读取覆盖及能力装配共用。

核心能力
- SessionExecutionContext 描述本次执行的身份、固定定义及项目目录。
- session_execution_context / session_setup_error 在会话锁内绑定并在退出时复原。
- build_agent_record 复用 Core 定义规则装配原生记录，不读取 Service 或数据库。
"""

from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import PurePosixPath

from agentscope.app.storage import AgentRecord, SessionRecord
from pydantic import SecretStr

from egis_agentkit.client.nacos._settings import AccessConfig, AgentKitConfig
from egis_agentkit.core.agent._definition_base import AgentDefinitionBase
from egis_agentkit.core.agent._personal_definition import PersonalDefinition
from egis_agentkit.errors import AgentKitError, Forbidden


@dataclass(frozen=True)
class SessionExecutionContext:
    """锁持有期间可用的会话、定义与工作区。"""

    user_id: str
    agent_id: str
    session_id: str
    definition: dict
    root_definition: dict
    binding: dict
    session_record: SessionRecord
    workdir: PurePosixPath
    artifacts_dir: PurePosixPath
    project_id: str = ""
    policies: AccessConfig | None = None
    collaborator_definitions: dict = field(default_factory=dict)
    member_definitions: dict = field(default_factory=dict)
    configuration: AgentKitConfig | None = None
    preferences: dict[str, str] | None = None


session_execution_context: ContextVar[SessionExecutionContext | None] = ContextVar(
    "egis_execution_scope", default=None
)


@dataclass(frozen=True, slots=True)
class VerifiedChatAuthorization:
    """已验签且通过会话访问校验的聊天请求凭据。"""

    user_id: str
    agent_id: str
    session_id: str
    access_token: SecretStr


verified_chat_authorization: ContextVar[VerifiedChatAuthorization | None] = ContextVar(
    "egis_verified_chat_authorization", default=None
)


def get_verified_chat_token(
    user_id: str, agent_id: str, session_id: str
) -> SecretStr | None:
    authorization = verified_chat_authorization.get()
    if authorization and (
        authorization.user_id,
        authorization.agent_id,
        authorization.session_id,
    ) == (user_id, agent_id, session_id):
        return authorization.access_token
    return None


session_setup_error: ContextVar[AgentKitError | None] = ContextVar(
    "egis_execution_setup_error", default=None
)

stream_subscription_ready: ContextVar[object | None] = ContextVar(
    "egis_stream_subscription_ready", default=None
)


def build_agent_record(
    record: AgentRecord, scope: SessionExecutionContext
) -> AgentRecord:
    definition = scope.definition
    personal = definition["agent_type"] == "personal"
    builder = PersonalDefinition if personal else AgentDefinitionBase
    return builder.build_agent(
        record.user_id,
        definition,
        record=record,
        preferences=getattr(scope, "preferences", None),
    )


def get_session_execution_context(
    user_id: str, agent_id: str, session_id: str
) -> SessionExecutionContext:
    scope = session_execution_context.get()
    if scope is None or (scope.user_id, scope.agent_id, scope.session_id) != (
        user_id,
        agent_id,
        session_id,
    ):
        raise Forbidden("Agent 装配缺少固定执行上下文")
    return scope
