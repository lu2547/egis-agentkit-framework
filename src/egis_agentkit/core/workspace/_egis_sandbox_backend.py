"""实现企业沙箱 Backend，让原生工具在注入的 Client 上执行。

核心能力
- exec_shell：绑定默认工作目录，保留 argv 参数边界和明确的执行结果。
- read_file / write_file：提供二进制安全的文件 IO，写入前检查目录创建结果。
- 文件枚举、流式读取等派生能力复用原生 BackendBase，不依赖厂商 SDK。
"""

from agentscope.tool import BackendBase, ExecResult

from egis_agentkit.client.sandbox import SandboxClient


class EgisSandboxBackend(BackendBase):
    """Workspace 的核心执行后端，持有实例 Client 和沙箱内部工作目录。"""

    def __init__(self, client: SandboxClient, workdir: str):
        self.client, self.workdir = client, workdir

    async def exec_shell(
        self,
        command: list[str],
        *,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> ExecResult:
        """不拼接 shell 字符串；具体通信编码由 Client 完成。"""
        if not command:
            raise ValueError("执行命令不能为空")
        return await self.client.exec_command(
            command, cwd=cwd or self.workdir, timeout=timeout
        )

    async def read_file(self, path: str) -> bytes:
        """保持原始字节和缺失文件错误，交给原生工具决定如何呈现。"""
        return await self.client.read_file(path)

    async def write_file(self, path: str, data: bytes) -> None:
        """先确认目标目录可用，目录创建失败时不继续写入。"""
        parent = self.dirname(path)
        if parent:
            result = await self.exec_shell(["mkdir", "-p", parent])
            if not result.ok():
                raise OSError("无法创建沙箱文件目录")
        await self.client.write_file(path, data)
