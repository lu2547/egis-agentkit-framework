"""Convert authorized Office bytes to PDF in a disposable, credential-free process."""

import asyncio
import os
import shutil
import signal
import tempfile
from pathlib import Path

from ..errors import AgentKitError


class DocumentPreviewService:
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

    def __init__(self, binary=None, *, timeout=45, max_bytes=20 * 1024 * 1024):
        self.binary = binary or shutil.which("soffice") or shutil.which("libreoffice")
        self.timeout, self.max_bytes = timeout, max_bytes
        self._slots = asyncio.Semaphore(2)
        self._pending = 0

    async def convert(self, content: bytes, filename: str) -> bytes:
        suffix = Path(filename).suffix.lower()
        if suffix not in self.EXTENSIONS:
            raise AgentKitError("此格式无需 Office 页面转换", 415)
        if len(content) > self.max_bytes:
            raise AgentKitError("文件超过页面预览大小限制", 413)
        if not self.binary:
            raise AgentKitError("页面预览服务未安装 LibreOffice，请联系管理员", 503)
        if self._pending >= 4:
            raise AgentKitError("页面预览服务繁忙，请稍后重试", 429)
        self._pending += 1
        try:
            async with self._slots:
                with tempfile.TemporaryDirectory(
                    prefix="agentkit-preview-"
                ) as directory:
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
                    # Never inherit model keys, tokens or database credentials.
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
                        await asyncio.wait_for(process.wait(), self.timeout)
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
                        raise AgentKitError(
                            "此文件无法转换，请确认文件未损坏或加密", 422
                        )
                    if target.stat().st_size > self.max_bytes:
                        raise AgentKitError("转换结果超过预览大小限制", 413)
                    result = target.read_bytes()
                    if not result.startswith(b"%PDF-"):
                        raise AgentKitError("页面转换未产生有效 PDF", 422)
                    return result
        finally:
            self._pending -= 1
