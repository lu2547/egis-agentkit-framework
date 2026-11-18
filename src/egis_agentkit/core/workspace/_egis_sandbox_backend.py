"""实现企业沙箱 Backend，让原生工具在注入的 Client 上执行。

核心能力
- exec_shell：绑定默认工作目录，保留 argv 参数边界和明确的执行结果。
- read_file / write_file：提供二进制安全的文件 IO，写入前检查目录创建结果。
- 文件枚举、流式读取等派生能力复用原生 BackendBase，不依赖厂商 SDK。
"""

from pathlib import PurePosixPath

from agentscope.tool import BackendBase, ExecResult

from egis_agentkit.client.sandbox import SandboxClient
from egis_agentkit.errors import Forbidden


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

    async def resolve_path(self, path: str) -> PurePosixPath:
        """Resolve the tool's path in the sandbox, including uncreated targets."""
        error = "无法在沙箱中验证文件路径"
        if not isinstance(path, str) or "\0" in path:
            raise Forbidden(error)
        result = await self.exec_shell(["realpath", "-m", "--", path])
        if not result.ok():
            raise Forbidden(error)
        try:
            resolved = result.stdout.decode("utf-8").removesuffix("\n")
        except UnicodeDecodeError as exc:
            raise Forbidden(error) from exc
        candidate = PurePosixPath(resolved)
        if (
            candidate.anchor != "/"
            or ".." in candidate.parts
            or any(char in resolved for char in "\0\r\n")
        ):
            raise Forbidden(error)
        return candidate

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
