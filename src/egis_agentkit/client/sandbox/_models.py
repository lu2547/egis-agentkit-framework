"""沙箱通信使用的中立数据，不包含厂商 SDK 类型或数据库模型。"""

from dataclasses import dataclass


@dataclass(frozen=True)
class SandboxMount:
    """描述挂载意图，由具体 Client 转换成提供方请求。"""

    name: str
    source: str
    target: str
    read_only: bool = False


@dataclass(frozen=True)
class SandboxInstance:
    """实例查询结果；归属规则由 Core 校验。"""

    id: str
    state: str
    metadata: dict[str, str]
