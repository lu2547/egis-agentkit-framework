"""定义可替换的沙箱通信契约，不约定公司 SDK、HTTP 路径或认证方式。

核心能力
- 实例发现、创建、连接、续期和关闭；Client 对象只持有一个执行连接。
- 项目清理使用独立 Client 查询、销毁实例，不复用正在执行的连接。
- 命令和文件通信交给 Core Backend 使用；close 只释放通信句柄。
"""

from typing import Protocol

from agentscope.tool import ExecResult

from egis_agentkit.errors import NotFound

from ._models import SandboxInstance, SandboxMount


class SandboxInstanceNotFound(NotFound):
    """只表示提供方明确确认实例失效，允许 Manager 安全重建。"""

    def __init__(self):
        super().__init__("沙箱实例不存在或已失效")


class SandboxClient(Protocol):
    """公司 Client 可直接实现本协议，不要求继承厂商类。"""

    @property
    def instance_id(self) -> str | None:
        """返回当前连接实例的 ID；未连接或已关闭时返回 None。"""

    def mount_fingerprint(self, mounts: list[SandboxMount]) -> str:
        """按实际挂载语义生成稳定摘要，不包含认证信息。"""

    async def find_instance(self, binding_id: str) -> SandboxInstance | None:
        """完整查询可恢复实例；没有匹配项返回 None，查询失败不能冒充未找到。"""

    async def create_instance(
        self, metadata: dict[str, str], mounts: list[SandboxMount]
    ) -> None:
        """创建并持有执行连接；metadata 必须随实例持久保存。"""

    async def connect_instance(self, instance: SandboxInstance) -> None:
        """连接或恢复 Core 已验证归属的实例，不自行更换身份。"""

    async def renew_instance(self, timeout_seconds: float) -> None:
        """延长有效期；只有明确确认实例消失才抛 SandboxInstanceNotFound。"""

    async def close_instance(self) -> None:
        """暂停当前实例并释放连接，保留文件；实例已不存在时视为已关闭。"""

    async def list_instances(self, metadata: dict[str, str]) -> list[SandboxInstance]:
        """返回全部匹配实例，包含暂停实例；由 Core 再校验归属。"""

    async def destroy_instance(self, identifier: str) -> None:
        """实际销毁指定实例；调用者已完成归属校验。"""

    async def exec_command(
        self, command: list[str], *, cwd: str, timeout: float | None = None
    ) -> ExecResult:
        """保留 argv 参数边界，返回退出码与标准输出、错误字节。"""

    async def read_file(self, path: str) -> bytes:
        """读取原始字节；文件缺失使用 FileNotFoundError，不代表实例消失。"""

    async def write_file(self, path: str, content: bytes) -> None:
        """写入完整字节内容，不解码或转换文本。"""

    async def close(self) -> None:
        """释放本进程通信句柄；不等同于销毁实例，允许重复调用。"""
