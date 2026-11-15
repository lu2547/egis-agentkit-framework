"""管理技能版本登记与 NAS 归档读取，内容规则复用 Core。"""

import asyncio
import re

import yaml

from ._registry import _LocalRegistry


class LocalSkillRegistry(_LocalRegistry):
    """先把版本包完整保存到共享文件目录，再登记包含定位元数据的版本。"""

    def __init__(self, assets, archives):
        super().__init__(assets, "skill")
        self.archives = archives

    async def register(self, asset, *, actor, archive=None):
        """NAS 写入失败时不写版本；数据库失败时不发布未登记的文件。"""
        if archive is None:
            raise ValueError("Skill archive is required")
        location = await asyncio.to_thread(self.archives.write_archive, archive)
        saved = asset.model_copy(update={"payload": {**asset.payload, **location}})
        return await super().register(saved, actor=actor)

    async def archive(self, asset):
        """读取当前版本定位的共享包，不提供数据库二进制回退。"""
        return await asyncio.to_thread(self.archives.read_archive, asset["payload"])


def _skill_front(markdown: str) -> dict:
    try:
        match = re.match(r"\A---\r?\n(.*?)\r?\n---(?:\r?\n|$)", markdown, re.S)
        front = yaml.safe_load(match.group(1)) if match else {}
        if not isinstance(front, dict):
            raise ValueError("Invalid Skill front matter")
        return front
    except yaml.YAMLError:
        raise ValueError("Invalid Skill front matter") from None
