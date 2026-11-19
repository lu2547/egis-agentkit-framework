"""管理技能版本登记与文件读取，内容规则复用 Core。"""

import asyncio
import re

import yaml

from egis_agentkit.errors import BadRequest

from ._registry import _LocalRegistry


class LocalSkillRegistry(_LocalRegistry):
    """Use the asset registration and the shared version directory together."""

    def __init__(self, assets, versions):
        super().__init__(assets, "skill")
        self.versions = versions

    async def find_current(self, skill_id):
        """读取技能的所有者版本和稳定归属。"""
        return await self.assets.find_asset(self.kind, skill_id)

    async def publish_files(self, asset, files, *, max_bytes):
        await asyncio.to_thread(
            self.versions.publish, asset, files, max_bytes=max_bytes
        )

    async def read(self, asset, *, max_bytes):
        return await asyncio.to_thread(self.versions.read, asset, max_bytes=max_bytes)


def _skill_front(markdown: str) -> dict:
    try:
        match = re.match(r"\A---\r?\n(.*?)\r?\n---(?:\r?\n|$)", markdown, re.S)
        front = yaml.safe_load(match.group(1)) if match else {}
        if not isinstance(front, dict):
            raise BadRequest("Invalid Skill front matter")
        return front
    except yaml.YAMLError:
        raise BadRequest("Invalid Skill front matter") from None


def decode_skill_markdown(content: bytes) -> str:
    try:
        return content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise BadRequest("Skill Markdown must be UTF-8") from exc
