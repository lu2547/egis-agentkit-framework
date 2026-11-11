"""生成 Office 文档的 PDF 预览。

核心能力
- 限制输入、输出、并发请求和执行时间。
- 在临时目录运行转换，回收超时或被取消的进程。
"""

import asyncio
import os
import shutil
import signal
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path

from egis_agentkit.backend.nacos._settings import WorkspacePreviewConfig
from egis_agentkit.errors import AgentKitError


class OfficeToPdfConverter:
    EXTENSIONS = {
        ".doc",
        ".docx",
        ".ppt",
        ".pptx",
        ".xls",
        ".xlsx",
        ".odt",
        ".odp",
        ".ods",
    }

    def __init__(
        self, binary=None, *, timeout=None, max_bytes=None, config_provider=None
    ):
        self.binary = binary or shutil.which("soffice") or shutil.which("libreoffice")
        self.config = config_provider
        overrides = {
            key: value
            for key, value in {
                "timeout_seconds": timeout,
                "max_bytes": max_bytes,
            }.items()
            if value is not None
        }
        self.defaults = WorkspacePreviewConfig(**overrides)
        self._slots = asyncio.Condition()
        self._active = 0
        self._pending = 0

    @asynccontextmanager
    async def _slot(self, limits):
        """按该请求的配置快照限制排队和执行中的转换。"""
        if self._pending >= limits.max_pending:
            raise AgentKitError("页面预览服务繁忙，请稍后重试", 429)
        self._pending += 1
        try:
            async with self._slots:
                await self._slots.wait_for(lambda: self._active < limits.max_concurrent)
                self._active += 1
            try:
                yield
            finally:
                async with self._slots:
                    self._active -= 1
                    self._slots.notify_all()
        finally:
            self._pending -= 1

    async def convert(self, content: bytes, filename: str) -> bytes:
        """在受限进程中将 Office 文档转换成 PDF。"""
        limits = (
            self.config.snapshot.workspace.preview if self.config else self.defaults
        )
        suffix = Path(filename).suffix.lower()
        if suffix not in self.EXTENSIONS:
            raise AgentKitError("此格式无需 Office 页面转换", 415)
        if len(content) > limits.max_bytes:
            raise AgentKitError("文件超过页面预览大小限制", 413)
        if not self.binary:
            raise AgentKitError("页面预览服务未安装 LibreOffice，请联系管理员", 503)
        async with self._slot(limits):
            with tempfile.TemporaryDirectory(prefix="agentkit-preview-") as directory:
                root = Path(directory)
                source = root / ("document" + suffix)
                source.write_bytes(content)
                profile = root / "profile"
                (profile / "user").mkdir(parents=True)
                (profile / "user" / "registrymodifications.xcu").write_text(
                    '<?xml version="1.0" encoding="UTF-8"?>'
                    '<oor:items xmlns:oor="http://openoffice.org/2001/registry">'
                    '<item oor:path="/org.openoffice.Office.Common/Security/Scripting">'
                    '<prop oor:name="MacroSecurityLevel" oor:op="fuse"><value>3</value></prop>'
                    "</item></oor:items>",
                    encoding="utf-8",
                )
                # 转换子进程只继承运行所需的环境变量。
                environment = {
                    "PATH": os.environ.get("PATH", ""),
                    "HOME": directory,
                    "TMPDIR": directory,
                    "LANG": "en_US.UTF-8",
                }
                process = await asyncio.create_subprocess_exec(
                    self.binary,
                    f"-env:UserInstallation={profile.as_uri()}",
                    "--headless",
                    "--nologo",
                    "--nodefault",
                    "--norestore",
                    "--convert-to",
                    "pdf",
                    "--outdir",
                    directory,
                    str(source),
                    env=environment,
                    start_new_session=os.name == "posix",
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                try:
                    await asyncio.wait_for(process.wait(), limits.timeout_seconds)
                except (TimeoutError, asyncio.CancelledError) as exc:
                    if process.returncode is None:
                        try:
                            if os.name == "posix":
                                os.killpg(process.pid, signal.SIGKILL)
                            else:
                                process.kill()
                        except ProcessLookupError:
                            pass
                    await process.wait()
                    if isinstance(exc, asyncio.CancelledError):
                        raise
                    raise AgentKitError("页面转换超时，请稍后重试", 504) from None
                target = root / "document.pdf"
                if process.returncode or not target.is_file():
                    raise AgentKitError("此文件无法转换，请确认文件未损坏或加密", 422)
                if target.stat().st_size > limits.max_bytes:
                    raise AgentKitError("转换结果超过预览大小限制", 413)
                result = target.read_bytes()
                if not result.startswith(b"%PDF-"):
                    raise AgentKitError("页面转换未产生有效 PDF", 422)
                return result
