"""管理项目 PPTX 页面，组合页面能力、项目访问校验和文件保存。

核心能力
- get_presentation 返回当前文件摘要、页面目录及界面限制。
- update_slides 校验已读版本，执行页面操作并通过文件服务保存。
- get_options 为旧 PPT 预览提供同一套目录配置。
"""

import asyncio
import hashlib
from pathlib import PurePosixPath
from typing import Any

from egis_agentkit.client.nacos._settings import ProjectPresentationConfig
from egis_agentkit.core.project._presentation import Presentation, SlideOperation
from egis_agentkit.errors import Conflict, InvalidRequest
from egis_agentkit.service.project._file_manager import ProjectFileManager
from egis_agentkit.storage.model import User


class ProjectPresentationManager:
    """只管理演示文稿页面；文件 IO、项目锁和变更流水复用项目文件服务。"""

    def __init__(self, files: ProjectFileManager):
        self.files = files

    def _limits(self) -> ProjectPresentationConfig:
        return (
            self.files.config.snapshot.project.presentation
            if self.files.config
            else ProjectPresentationConfig()
        )

    async def get_options(self, user: User, project_id: str) -> dict[str, Any]:
        async with self.files.projects.guard(user, project_id):
            return self._limits().model_dump(by_alias=True)

    async def _read(self, user: User, project_id: str, area: str, path: str) -> bytes:
        if PurePosixPath(path).suffix.lower() != ".pptx":
            raise InvalidRequest("页面编辑支持 PPTX 格式，请先将旧 PPT 另存为 PPTX")
        content, _ = await self.files.read_project_file(user, project_id, area, path)
        return content

    async def get_presentation(
        self, user: User, project_id: str, area: str, path: str
    ) -> dict[str, Any]:
        """按项目读取文件及稳定页面目录，不从转换后的 PDF 反推页面身份。"""
        content = await self._read(user, project_id, area, path)
        limits = self._limits()

        def describe():
            deck = Presentation(content, limits)
            return {
                "checksum": hashlib.sha256(content).hexdigest(),
                "slides": deck.list_slides(),
                "options": limits.model_dump(by_alias=True),
            }

        return await asyncio.to_thread(describe)

    async def update_slides(
        self,
        user: User,
        project_id: str,
        area: str,
        path: str,
        operation: SlideOperation,
        slide_id: str,
        checksum: str,
        target_index: int | None = None,
    ) -> dict[str, Any]:
        """读取已知版本，修改页面，保存时再次校验摘要，拒绝并发覆盖。"""
        # 1. 读取用户所属项目的真实文件，先拒绝缺失和过期的读取版本。
        content = await self._read(user, project_id, area, path)
        if not checksum:
            raise InvalidRequest("页面操作必须携带打开文件时的摘要")
        if hashlib.sha256(content).hexdigest() != checksum:
            raise Conflict("演示文稿已更新，请重新打开后再操作")
        limits = self._limits()

        # 2. Core 只修改包内页面结构，不接触数据库连接和文件路径。
        def apply_change():
            deck = Presentation(content, limits)
            updated, active = deck.update_slides(operation, slide_id, target_index)
            return updated, active, deck.list_slides()

        updated, active, slides = await asyncio.to_thread(apply_change)
        # 3. 文件服务在项目锁内二次检查摘要并保存，页面修改记录为普通文件变更。
        saved = await self.files.update_project_file(
            user, project_id, area, path, updated, checksum
        )
        return {
            "checksum": saved["checksum"],
            "slides": slides,
            "active_slide_id": active,
        }
