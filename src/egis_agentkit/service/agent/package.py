from pathlib import Path
from typing import Protocol
from urllib.parse import unquote, urlparse

from egis_agentkit.errors import Conflict, NotFound
from egis_agentkit.models import Asset, User
from egis_agentkit.packages.contracts import Assembly, PackageLoadState
from egis_agentkit.service.user.service import require_admin
from egis_agentkit.storage import AgentKitStorage
from egis_agentkit.storage.package import PackageStorage


class AssemblyLoader(Protocol):
    """Operations needed to register, inspect and resolve assembly releases."""

    async def load(
        self,
        package_path: Path,
        *,
        expected_sha256: str | None = None,
        entrypoint: str | None = None,
    ) -> Asset: ...

    def get(self, assembly_ref: str, assembly_version: str) -> Assembly: ...

    def disable(self, assembly_ref: str, assembly_version: str) -> None: ...

    def status(self) -> list[PackageLoadState]: ...


class PackageService:
    def __init__(self, storage: AgentKitStorage, loader: AssemblyLoader):
        self.storage, self.loader = storage, loader
        self.releases = PackageStorage(storage.engine)

    async def register(
        self,
        user: User,
        path: Path | str,
        *,
        checksum: str | None = None,
        entrypoint: str | None = None,
        owner_id: str = "egis-demo",
    ) -> dict:
        require_admin(user)
        path = Path(path)
        asset = await self.loader.load(
            path, expected_sha256=checksum, entrypoint=entrypoint
        )
        asset.owner_id = owner_id
        asset.download_url = path.resolve().as_uri()
        async with self.storage.transaction() as conn:
            prior = await self.releases.register_release(
                asset, actor=user.user_id, conn=conn
            )
            if not prior["is_enabled"]:
                raise Conflict("已停用的包版本不能重新注册")
            if (prior["checksum_sha256"], prior["entrypoint"]) != (
                asset.checksum_sha256,
                asset.entrypoint,
            ):
                raise Conflict("同一包版本的内容不可覆盖")
            return prior

    async def list(self, user: User) -> list[dict]:
        require_admin(user)
        states = {
            (r["assembly_ref"], r["assembly_version"]): r for r in self.loader.status()
        }
        rows = await self.releases.list_releases()
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

    async def disable(self, user: User, resource_id: str, version: str) -> None:
        require_admin(user)
        async with self.storage.transaction() as conn:
            disabled = await self.releases.disable_release(
                resource_id, version, actor=user.user_id, conn=conn
            )
            if not disabled:
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

    async def resolve(self, resource_id: str, version: str) -> Assembly:
        assembly, _ = await self.resolve_for_run(resource_id, version)
        return assembly

    async def resolve_for_run(
        self, resource_id: str, version: str
    ) -> tuple[Assembly, int]:
        """Resolve the exact release and its callback contract together."""
        row = await self.releases.get_release(resource_id, version)
        if row is None or not row["is_enabled"]:
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
            current = await self.releases.get_release(resource_id, version)
            if current is None or not current["is_enabled"]:
                raise NotFound("发布包不存在或已停用")
            if (current["checksum_sha256"], current["entrypoint"]) != (
                row["checksum_sha256"],
                row["entrypoint"],
            ):
                raise Conflict("发布包注册信息已变化")
        try:
            return (
                self.loader.get(resource_id, version),
                row["payload"].get("framework_api_version", 1),
            )
        except KeyError:
            raise Conflict("本地发布包已停用") from None

    async def restore_loaded(self) -> None:
        for resource_id, version in await self.releases.enabled_release_keys():
            await self.resolve(resource_id, version)
