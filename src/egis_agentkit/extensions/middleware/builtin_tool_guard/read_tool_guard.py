"""Reject unsupported binary input before native Read decodes it as text."""

from contextlib import aclosing
from pathlib import PurePosixPath

from agentscope.message import TextBlock, ToolResultState
from agentscope.tool import BackendBase, ToolChunk, ToolMiddlewareBase


class ReadToolGuardMiddleware(ToolMiddlewareBase):
    MESSAGE = (
        "Read 工具不支持此类二进制文件读取，请先转成 Markdown，或尝试使用其他工具。"
    )
    OUTPUT_MESSAGE = "Read 工具读取结果包含空字符，无法作为文本返回。请先转成 Markdown，或尝试使用其他工具。"
    SAMPLE_BYTES = 4096
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

    def __init__(self, backend: BackendBase):
        self.backend = backend

    @classmethod
    def _error(cls, message=None):
        return ToolChunk(
            content=[TextBlock(text=message or cls.MESSAGE)],
            state=ToolResultState.ERROR,
        )

    @staticmethod
    def _is_binary(sample: bytes) -> bool:
        if b"\0" in sample:
            return True
        controls = sum(byte < 9 or 13 < byte < 32 for byte in sample)
        return bool(sample) and controls / len(sample) > 0.3

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
                        self.backend.read_stream(path, chunk_size=self.SAMPLE_BYTES)
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
