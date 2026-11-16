"""实现 ToolMiddlewareBase，在原生 Read 处理前拦截不支持的二进制文件。

核心能力
- on_tool_call：按配置进行有界采样，拒绝不支持的输入和含空字符的输出。
- _is_binary：按已验证的控制字节占比阈值判定，不更改模型调用流程。
"""

from contextlib import aclosing
from pathlib import PurePosixPath

from agentscope.message import TextBlock, ToolResultState
from agentscope.tool import BackendBase, ToolChunk, ToolMiddlewareBase

from egis_agentkit.client.nacos._settings import WorkspaceReadGuardConfig


class ReadToolGuardMiddleware(ToolMiddlewareBase):
    MESSAGE = "Read 不支持解析此类二进制文件。"
    OUTPUT_MESSAGE = "Read 无法将包含空字符的内容作为文本返回。"
    # Native Read routes these formats to its own media handling, including
    # model capability checks. Do not mistake their binary bytes for text.
    NATIVE_MEDIA = frozenset(
        {
            ".pdf",
            ".png",
            ".jpg",
            ".jpeg",
            ".gif",
            ".webp",
            ".bmp",
            ".tiff",
            ".tif",
            ".ico",
        }
    )
    BINARY_EXTENSIONS = frozenset(
        {
            ".zip",
            ".tar",
            ".gz",
            ".7z",
            ".rar",
            ".bz2",
            ".xz",
            ".doc",
            ".docx",
            ".xls",
            ".xlsx",
            ".ppt",
            ".pptx",
            ".docm",
            ".xlsm",
            ".pptm",
            ".odt",
            ".ods",
            ".odp",
            ".exe",
            ".dll",
            ".so",
            ".dylib",
            ".class",
            ".jar",
            ".war",
            ".bin",
            ".dat",
            ".obj",
            ".o",
            ".a",
            ".lib",
            ".wasm",
            ".pyc",
            ".pyo",
            ".mp3",
            ".wav",
            ".mp4",
            ".mov",
            ".sqlite",
            ".sqlite3",
            ".db",
        }
    )

    def __init__(
        self, backend: BackendBase, *, settings: WorkspaceReadGuardConfig | None = None
    ):
        self.backend = backend
        self.settings = settings if settings is not None else WorkspaceReadGuardConfig()

    @classmethod
    def _error(cls, message=None):
        return ToolChunk(
            content=[TextBlock(text=message or cls.MESSAGE)],
            state=ToolResultState.ERROR,
        )

    def _is_binary(self, sample: bytes) -> bool:
        if b"\0" in sample:
            return True
        controls = sum(byte < 9 or 13 < byte < 32 for byte in sample)
        return (
            bool(sample)
            and controls / len(sample) > self.settings.control_byte_ratio_threshold
        )

    async def on_tool_call(self, tool, input_kwargs, next_handler):
        path = input_kwargs["file_path"]
        extension = PurePosixPath(self.backend.basename(path)).suffix.lower()
        if extension not in self.NATIVE_MEDIA and self.backend.isabs(path):
            try:
                # Keep native missing-file and directory errors intact. This
                # middleware runs only after AgentScope's permission checks.
                if await self.backend.file_exists(
                    path
                ) and not await self.backend.is_dir(path):
                    if extension in self.BINARY_EXTENSIONS:
                        yield self._error()
                        return
                    async with aclosing(
                        self.backend.read_stream(
                            path, chunk_size=self.settings.sample_bytes
                        )
                    ) as stream:
                        sample = await anext(stream, b"")
                    if self._is_binary(sample):
                        yield self._error()
                        return
            except OSError:
                # Read already converts filesystem failures into tool results.
                pass
        async for chunk in next_handler(**input_kwargs):
            # Sampling is intentionally bounded. If a later window contains a
            # NUL, stop it before native streaming publishes invalid PG JSONB.
            if any(
                isinstance(block, TextBlock) and "\0" in block.text
                for block in chunk.content
            ):
                yield self._error(self.OUTPUT_MESSAGE)
                return
            yield chunk
