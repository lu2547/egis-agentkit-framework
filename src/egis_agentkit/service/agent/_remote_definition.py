"""登记通过远程协议执行的专家定义。

核心能力
- 管理员登记、更新、停用远程专家。
- 远端协议、地址和凭据引用通过配置模型校验。
"""

from egis_agentkit.backend.nacos._settings import RemoteConfig
from egis_agentkit.service._user import require_admin
from egis_agentkit.storage.model import Definition, User
from egis_agentkit.storage.model._remote_agent import RemoteAgentConfig

from ._definition_base import EditableAgentDefinition
from ._remote import resolve_connection


class RemoteDefinition(EditableAgentDefinition):
    """只有管理员可以管理远程连接及其生命周期。"""

    agent_type = "remote"

    def __init__(self, storage, policies=None, *, config=None):
        super().__init__(storage, policies)
        self.config = config

    def _authorize_write(self, user: User, data: Definition) -> None:
        require_admin(user)
        super()._authorize_write(user, data)
        settings = (
            self.config.snapshot.agent.remote
            if self.config is not None
            else RemoteConfig()
        )
        resolve_connection(
            RemoteAgentConfig.model_validate(data.payload["remote_config"]), settings
        )

    def _authorize_disable(self, user: User, row: dict) -> None:
        require_admin(user)
