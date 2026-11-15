"""Agent 定义版本中的提示、展示信息、绑定和远程连接配置。"""

from pydantic import Field

from .._base import Model
from .._resource import ResourceRef
from ._remote_config import RemoteAgentConfig


class AgentPayload(Model):
    description: str = Field(default="", max_length=2000)
    category: str = Field(default="办公协同", max_length=64)
    tags: list[str] = Field(default_factory=list, max_length=20)
    avatar: str = Field(default="spark", max_length=64)
    system_prompt: str = Field(
        default="你是一个认真、可靠的企业工作助理。", max_length=64000
    )
    starter_prompts: list[str] = Field(default_factory=list, max_length=10)
    model_key: str | None = None
    bindings: list[ResourceRef] = Field(default_factory=list, max_length=100)
    remote_config: RemoteAgentConfig | None = None
