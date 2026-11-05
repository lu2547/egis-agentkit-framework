from pathlib import Path
from urllib.parse import unquote, urlparse

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert

from ..errors import Conflict, NotFound
from ..models import utcnow
from ..storage import tables as t
from .users import require_admin


class PackageService:
    def __init__(self, storage, loader):
        self.storage, self.loader = storage, loader

    async def register(
        self, user, path, *, checksum=None, entrypoint=None, owner_id="egis-demo"
    ):
        require_admin(user)
        asset = await self.loader.load(
            path, expected_sha256=checksum, entrypoint=entrypoint
        )
        asset.owner_id = owner_id
        asset.download_url = Path(path).resolve().as_uri()
        async with self.storage.transaction() as conn:
            await conn.execute(
                insert(t.assets)
                .values(
                    **asset.model_dump(),
                    created_by=user.user_id,
                    updated_by=user.user_id,
                )
                .on_conflict_do_nothing()
            )
            prior = (
                (
                    await conn.execute(
                        select(t.assets)
                        .where(
                            t.assets.c.resource_type == "assembly",
                            t.assets.c.resource_id == asset.resource_id,
                            t.assets.c.resource_version == asset.resource_version,
                        )
                        .with_for_update()
                    )
                )
                .mappings()
                .one()
            )
            if not prior["is_enabled"]:
                raise Conflict("已停用的包版本不能重新注册")
            if (prior["checksum_sha256"], prior["entrypoint"]) != (
                asset.checksum_sha256,
                asset.entrypoint,
            ):
                raise Conflict("同一包版本的内容不可覆盖")
            return dict(prior)

    async def list(self, user):
        require_admin(user)
        states = {
            (r["assembly_ref"], r["assembly_version"]): r for r in self.loader.status()
        }
        async with self.storage.engine.connect() as conn:
            rows = [
                dict(r)
                for r in (
                    await conn.execute(
                        select(t.assets).where(t.assets.c.resource_type == "assembly")
                    )
                ).mappings()
            ]
        return [
            {
                **row,
                "is_loaded": (row["resource_id"], row["resource_version"]) in states,
                "load_state": states.get(
                    (row["resource_id"], row["resource_version"]), {}
                ),
            }
            for row in rows
        ]

    async def disable(self, user, resource_id, version):
        require_admin(user)
        async with self.storage.transaction() as conn:
            result = await conn.execute(
                update(t.assets)
                .where(
                    t.assets.c.resource_type == "assembly",
                    t.assets.c.resource_id == resource_id,
                    t.assets.c.resource_version == version,
                )
                .values(is_enabled=False, updated_by=user.user_id, updated_at=utcnow())
            )
            if not result.rowcount:
                raise NotFound()
        try:
            self.loader.disable(resource_id, version)
        except KeyError:
            pass  # Another node may have registered a package this node never loaded.

    @staticmethod
    def _path(row):
        location = row["download_url"] or row["payload"].get("package_path")
        if not location:
            raise Conflict("发布包缺少持久化位置")
        return Path(unquote(urlparse(location).path))

    async def resolve(self, resource_id, version):
        async with self.storage.engine.connect() as conn:
            row = (
                (
                    await conn.execute(
                        select(t.assets).where(
                            t.assets.c.resource_type == "assembly",
                            t.assets.c.resource_id == resource_id,
                            t.assets.c.resource_version == version,
                            t.assets.c.is_enabled,
                        )
                    )
                )
                .mappings()
                .first()
            )
        if row is None:
            raise NotFound("发布包不存在或已停用")
        if not row["checksum_sha256"] or not row["entrypoint"]:
            raise Conflict("发布包缺少固定的摘要或入口")
        state = next(
            (
                entry
                for entry in self.loader.status()
                if (entry["assembly_ref"], entry["assembly_version"])
                == (resource_id, version)
            ),
            None,
        )
        if state is not None:
            if (state["checksum_sha256"], state["entrypoint"]) != (
                row["checksum_sha256"],
                row["entrypoint"],
            ):
                raise Conflict("本地发布包与注册版本不匹配")
        else:
            try:
                loaded = await self.loader.load(
                    self._path(row),
                    expected_sha256=row["checksum_sha256"],
                    entrypoint=row["entrypoint"],
                )
            except (ValueError, OSError):
                raise Conflict("发布包文件不可用或完整性校验失败") from None
            if (loaded.resource_id, loaded.resource_version) != (resource_id, version):
                raise Conflict("发布包标识与注册版本不匹配")
            # The import may have taken long enough for another node to disable it.
            async with self.storage.engine.connect() as conn:
                current = (
                    (
                        await conn.execute(
                            select(
                                t.assets.c.is_enabled,
                                t.assets.c.checksum_sha256,
                                t.assets.c.entrypoint,
                            ).where(
                                t.assets.c.resource_type == "assembly",
                                t.assets.c.resource_id == resource_id,
                                t.assets.c.resource_version == version,
                            )
                        )
                    )
                    .mappings()
                    .first()
                )
            if current is None or not current["is_enabled"]:
                raise NotFound("发布包不存在或已停用")
            if (current["checksum_sha256"], current["entrypoint"]) != (
                row["checksum_sha256"],
                row["entrypoint"],
            ):
                raise Conflict("发布包注册信息已变化")
        try:
            return self.loader.get(resource_id, version)
        except KeyError:
            raise Conflict("本地发布包已停用") from None

    async def restore_loaded(self):
        async with self.storage.engine.connect() as conn:
            rows = (
                await conn.execute(
                    select(t.assets.c.resource_id, t.assets.c.resource_version).where(
                        t.assets.c.resource_type == "assembly", t.assets.c.is_enabled
                    )
                )
            ).all()
        for resource_id, version in rows:
            await self.resolve(resource_id, version)
