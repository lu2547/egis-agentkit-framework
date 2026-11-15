"""统一访问资源的版本记录。"""

from __future__ import annotations


class _LocalRegistry:
    def __init__(self, assets, kind):
        self.assets, self.kind = assets, kind

    async def list(self, *, resource_ids=None):
        return await self.assets.list_versions(self.kind, resource_ids=resource_ids)

    async def get_version(self, resource_id, version):
        return await self.assets.get(self.kind, resource_id, version)

    async def register(self, asset, *, actor, archive=None):
        await self.assets.insert_asset(asset, actor=actor)
        return asset
