"""Compose model catalog, selection and provider resolution."""

from .catalog import ModelCatalog
from .provider import ModelProvider
from .selection import ModelSelection


class ModelService:
    def __init__(
        self,
        storage,
        config_provider,
        *,
        catalog: ModelCatalog | None = None,
        provider: ModelProvider | None = None,
        selection: ModelSelection | None = None,
    ):
        self.storage, self.config = storage, config_provider
        self.catalog = catalog if catalog is not None else ModelCatalog()
        self.provider = provider if provider is not None else ModelProvider(storage)
        self.selection = selection if selection is not None else ModelSelection()

    def list(self):
        return self.catalog.list(self.config.snapshot)

    async def resolve(self, user, model_key=None, *, is_override=False):
        snapshot = self.config.snapshot
        key = self.selection.select(snapshot, model_key, is_override=is_override)
        spec = self.catalog.get(snapshot, key)
        return await self.provider.resolve(
            user, spec, config_version=snapshot.config_version
        )
