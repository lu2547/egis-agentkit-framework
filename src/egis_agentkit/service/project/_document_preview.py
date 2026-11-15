"""生成 Office 文档的 PDF 预览。

核心能力
- 限制输入、输出、并发请求和执行时间。
- 在临时目录运行转换，回收超时或被取消的进程。
"""

import asyncio
import hashlib
import json
import logging
import os
import shutil
import signal
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, tostring

from egis_agentkit.client.nacos._settings import ProjectPreviewConfig
from egis_agentkit.errors import AgentKitError
from egis_agentkit.storage.project._preview_cache import PreviewCacheStorage


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

    def __init__(
        self,
        binary=None,
        *,
        timeout=None,
        max_bytes=None,
        config_provider=None,
        cache_storage=None,
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
        self.defaults = ProjectPreviewConfig(**overrides)
        self._slots = asyncio.Condition()
        self._active = 0
        self._pending = 0
        self.cache = cache_storage or PreviewCacheStorage()
        self._inflight = {}
        self._requests = 0

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

    def get_config(self) -> ProjectPreviewConfig:
        """读取该次预览使用的已验证配置，统一转换和引用限制来源。"""
        return self.config.snapshot.project.preview if self.config else self.defaults

    @staticmethod
    def _prepare_font_environment(
        root: Path, limits: ProjectPreviewConfig
    ) -> dict[str, str]:
        """为隔离的转换进程显式装配字体，不依赖临时 HOME 推导用户字体目录。"""
        if not limits.font_directories and not limits.font_config_file:
            return {}
        document = Element("fontconfig")
        if limits.font_config_file:
            SubElement(
                document, "include", {"ignore_missing": "yes"}
            ).text = limits.font_config_file
        for directory in limits.font_directories:
            SubElement(document, "dir").text = directory
        SubElement(document, "cachedir").text = str(root / "font-cache")
        target = root / "fonts.conf"
        target.write_bytes(tostring(document, encoding="utf-8", xml_declaration=True))
        return {"FONTCONFIG_FILE": str(target)}

    async def convert(
        self, content: bytes, filename: str, *, page: int | None = None
    ) -> bytes:
        """在受限进程中将 Office 文档转换成 PDF。"""
        limits = self.get_config()
        if self._requests >= limits.max_pending:
            raise AgentKitError("页面预览服务繁忙，请稍后重试", 429)
        self._requests += 1
        try:
            return await self._get_preview(content, filename, page, limits)
        finally:
            self._requests -= 1

    async def _get_preview(self, content, filename, page, limits):
        suffix = Path(filename).suffix.lower()
        if suffix not in self.EXTENSIONS:
            raise AgentKitError("此格式无需 Office 页面转换", 415)
        if len(content) > limits.max_bytes:
            raise AgentKitError("文件超过页面预览大小限制", 413)
        if not self.binary:
            raise AgentKitError("页面预览服务未安装 LibreOffice，请联系管理员", 503)
        if page is not None and (page < 1 or suffix not in {".ppt", ".pptx", ".odp"}):
            raise AgentKitError("单页预览只支持有效的演示文稿页码", 422)
        try:
            binary_signature = (
                str(Path(self.binary).resolve()),
                Path(self.binary).stat().st_mtime_ns,
            )
        except OSError:
            binary_signature = (self.binary, None)
        key = hashlib.sha256(
            json.dumps(
                {
                    "digest": hashlib.sha256(content).hexdigest(),
                    "suffix": suffix,
                    "page": page,
                    "binary": binary_signature,
                    "fonts": limits.font_directories,
                    "fontConfig": limits.font_config_file,
                    "renderer": limits.cache.renderer_revision,
                    "maxBytes": limits.max_bytes,
                    "timeout": limits.timeout_seconds,
                },
                sort_keys=True,
            ).encode()
        ).hexdigest()
        entry = self._inflight.get(key)
        if entry is None:
            entry = {
                "task": asyncio.create_task(
                    self._get_or_convert(key, content, suffix, page, limits)
                ),
                "readers": 0,
            }
            self._inflight[key] = entry
        entry["readers"] += 1
        try:
            return await asyncio.shield(entry["task"])
        finally:
            entry["readers"] -= 1
            if not entry["readers"]:
                if not entry["task"].done():
                    entry["task"].cancel()
                if self._inflight.get(key) is entry:
                    self._inflight.pop(key, None)

    async def _get_or_convert(self, key, content, suffix, page, limits):
        async def read():
            if not limits.cache.is_enabled:
                return None
            try:
                return await asyncio.to_thread(
                    self.cache.get_preview, key, limits.cache, limits.max_bytes
                )
            except OSError:
                logging.getLogger(__name__).warning(
                    "PDF cache read unavailable", exc_info=True
                )
                return None

        cached = await read()
        if cached is not None:
            return cached
        async with self._slot(limits):
            async with self.cache.guard(key):
                cached = await read()
                if cached is not None:
                    return cached
                result = await self._convert(content, suffix, page, limits)
                if limits.cache.is_enabled:
                    try:
                        await asyncio.to_thread(
                            self.cache.save_preview, key, result, limits.cache
                        )
                    except OSError:
                        logging.getLogger(__name__).warning(
                            "PDF cache write unavailable", exc_info=True
                        )
                return result

    async def _convert(self, content, suffix, page, limits):
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
            environment.update(self._prepare_font_environment(root, limits))
            export_options = {
                "ExportHiddenSlides": {"type": "boolean", "value": "true"}
            }
            if page is not None:
                export_options["PageRange"] = {"type": "string", "value": str(page)}
            process = await asyncio.create_subprocess_exec(
                self.binary,
                f"-env:UserInstallation={profile.as_uri()}",
                "--headless",
                "--nologo",
                "--nodefault",
                "--norestore",
                "--convert-to",
                (
                    "pdf:impress_pdf_Export:" + json.dumps(export_options)
                    if suffix in {".ppt", ".pptx", ".odp"}
                    else "pdf"
                ),
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
