from types import SimpleNamespace

import pytest
from agentscope.app.storage import AsyncSQLAlchemyStorage, ChatModelConfig
from agentscope.credential import CredentialFactory

from egis_agentkit.config.snapshot import ConfigSnapshot, ModelSpec
from egis_agentkit.errors import Forbidden, NotFound
from egis_agentkit.service.model import ModelService


API_KEY_ENV = "MODEL_COMPONENTS_API_KEY"
ALICE = SimpleNamespace(user_id="alice")
BOB = SimpleNamespace(user_id="bob")


def model_spec(key="a", **changes):
    return ModelSpec.model_validate(
        {
            "key": key,
            "display_name": f"模型 {key.upper()}",
            "model": f"model-{key}",
            "api_key_env": API_KEY_ENV,
            "base_url": "https://models.example.test/v1",
            "parameters": {"temperature": 0.2, "max_tokens": 128},
            **changes,
        }
    )


def snapshot(*, version=1, default_key="a", specs=None, **policies):
    return ConfigSnapshot.model_validate(
        {
            "config_version": version,
            "runtime": {
                "config_version": version,
                "workspace_root": "/tmp/model-components",
                "release_roots": [],
                "auth": {
                    "mode": "demo",
                    "issuer": "test",
                    "audience": "test",
                    "algorithm": "HS256",
                },
            },
            "models": {
                "config_version": version,
                "default_key": default_key,
                "models": specs if specs is not None else [model_spec(), model_spec("b")],
            },
            "policies": {"config_version": version, **policies},
        }
    )


@pytest.fixture
async def native_storage():
    async with AsyncSQLAlchemyStorage("sqlite+aiosqlite:///:memory:") as storage:
        yield storage


@pytest.fixture(autouse=True)
def model_key(monkeypatch):
    monkeypatch.setenv(API_KEY_ENV, "first-test-key")


def test_list_refreshes_catalog_and_policy_without_changing_default():
    config = SimpleNamespace(snapshot=snapshot())
    service = ModelService(None, config)
    assert service.list() == {
        "items": [
            {"key": "a", "display_name": "模型 A"},
            {"key": "b", "display_name": "模型 B"},
        ],
        "default_key": "a",
    }

    config.snapshot = snapshot(
        version=2,
        default_key="b",
        specs=[model_spec(display_name="新版 A"), model_spec("b")],
        allowed_model_keys=["a"],
    )
    assert service.list() == {
        "items": [{"key": "a", "display_name": "新版 A"}],
        "default_key": "b",
    }


async def test_resolve_uses_one_snapshot_per_call_and_observes_refresh(native_storage):
    class RefreshingConfig:
        reads = 0

        @property
        def snapshot(self):
            self.reads += 1
            if self.reads == 1:
                return snapshot(allowed_model_keys=["a"])
            return snapshot(
                version=2, default_key="b", allowed_model_keys=["b"]
            )

    config = RefreshingConfig()
    service = ModelService(native_storage, config)
    old = await service.resolve(ALICE)
    assert config.reads == 1
    assert old.model == "model-a"
    assert old.credential_id == "7fc32743080956852c9c21f8321fa74e"

    refreshed = await service.resolve(ALICE)
    assert config.reads == 2
    assert refreshed.model == "model-b"
    assert refreshed.credential_id != old.credential_id


async def test_credentials_are_stable_upserted_and_isolated_by_user(native_storage):
    service = ModelService(native_storage, SimpleNamespace(snapshot=snapshot()))
    first = await service.resolve(ALICE)
    repeated = await service.resolve(ALICE, "a")
    other = await service.resolve(BOB)
    assert isinstance(first, ChatModelConfig)
    assert first.credential_id == repeated.credential_id == "7fc32743080956852c9c21f8321fa74e"
    assert other.credential_id != first.credential_id
    assert len(await native_storage.list_credentials("alice")) == 1
    assert len(await native_storage.list_credentials("bob")) == 1
    assert await native_storage.get_credential("bob", first.credential_id) is None
    record = await native_storage.get_credential("alice", first.credential_id)
    credential = CredentialFactory.from_dict(record.data)
    assert credential.id == first.credential_id
    assert credential.name == "模型 A"
    assert credential.api_key.get_secret_value() == "first-test-key"
    assert credential.base_url == "https://models.example.test/v1"
    assert first.type == "openai_credential"
    assert first.parameters == {"temperature": 0.2, "max_tokens": 128}


@pytest.mark.parametrize("change", ["key", "version", "endpoint", "parameters", "model"])
async def test_rotation_keeps_pending_confirmation_credentials(
    native_storage, monkeypatch, change
):
    config = SimpleNamespace(snapshot=snapshot())
    service = ModelService(native_storage, config)
    pending = await service.resolve(ALICE)
    before = await native_storage.get_credential("alice", pending.credential_id)
    old_data = before.data.copy()

    changes = {}
    if change == "key":
        monkeypatch.setenv(API_KEY_ENV, "rotated-test-key")
    elif change == "version":
        config.snapshot = snapshot(version=2)
    else:
        changes = {
            "endpoint": {"base_url": "https://new-models.example.test/v1"},
            "parameters": {"parameters": {"temperature": 0.9}},
            "model": {"model": "new-model"},
        }[change]
        config.snapshot = snapshot(specs=[model_spec(**changes)])

    current = await service.resolve(ALICE)
    assert current.credential_id != pending.credential_id
    assert (await service.resolve(ALICE)).credential_id == current.credential_id
    assert len(await native_storage.list_credentials("alice")) == 2
    assert (await native_storage.get_credential("alice", pending.credential_id)).data == old_data
    record = await native_storage.get_credential("alice", current.credential_id)
    assert record.data["api_key"] == (
        "rotated-test-key" if change == "key" else "first-test-key"
    )
    assert record.data["base_url"] == changes.get(
        "base_url", "https://models.example.test/v1"
    )
    assert current.model == changes.get("model", "model-a")
    assert current.parameters == changes.get(
        "parameters", {"temperature": 0.2, "max_tokens": 128}
    )
    assert pending.model == "model-a"
    assert pending.parameters == {"temperature": 0.2, "max_tokens": 128}


@pytest.mark.parametrize("value", [None, ""])
async def test_missing_credentials_fail_without_writing(native_storage, monkeypatch, value):
    if value is None:
        monkeypatch.delenv(API_KEY_ENV)
    else:
        monkeypatch.setenv(API_KEY_ENV, value)
    service = ModelService(native_storage, SimpleNamespace(snapshot=snapshot()))
    with pytest.raises(Forbidden, match="模型服务凭证尚未配置"):
        await service.resolve(ALICE)
    assert await native_storage.list_credentials("alice") == []


@pytest.mark.parametrize(
    ("policies", "key", "is_override", "error", "message"),
    [
        ({"allow_model_override": False}, "b", True, Forbidden, "不允许覆盖"),
        ({"allow_model_override": False}, None, True, Forbidden, "不允许覆盖"),
        ({"allow_model_override": False}, "missing", True, Forbidden, "不允许覆盖"),
        ({"allowed_model_keys": ["a"]}, "b", False, Forbidden, "未开放此模型"),
        ({"allowed_model_keys": ["b"]}, None, False, Forbidden, "未开放此模型"),
        ({"allowed_model_keys": ["a"]}, "missing", False, Forbidden, "未开放此模型"),
        ({}, "missing", False, NotFound, "模型配置不存在"),
    ],
)
async def test_selection_rejects_before_credential_resolution(
    native_storage, monkeypatch, policies, key, is_override, error, message
):
    monkeypatch.delenv(API_KEY_ENV)
    service = ModelService(native_storage, SimpleNamespace(snapshot=snapshot(**policies)))
    with pytest.raises(error, match=message):
        await service.resolve(ALICE, key, is_override=is_override)
    assert await native_storage.list_credentials("alice") == []


@pytest.mark.parametrize(
    ("policies", "key", "is_override", "expected"),
    [
        ({"allow_model_override": False}, None, False, "model-a"),
        ({"allow_model_override": False}, "b", False, "model-b"),
        ({}, "b", True, "model-b"),
        ({}, "", False, "model-a"),
        ({"allowed_model_keys": ["b"]}, "b", True, "model-b"),
    ],
)
async def test_selection_preserves_default_and_explicit_key_rules(
    native_storage, policies, key, is_override, expected
):
    service = ModelService(native_storage, SimpleNamespace(snapshot=snapshot(**policies)))
    result = await service.resolve(ALICE, key, is_override=is_override)
    assert result.model == expected


@pytest.mark.parametrize(
    ("model_class", "expected"),
    [
        ("OpenAIChatModel", "openai_chat"),
        ("OpenAIResponseModel", "openai_response"),
        ("openai_chat", "openai_chat"),
        ("custom_chat", "custom_chat"),
    ],
)
async def test_model_class_aliases_preserve_native_and_custom_names(
    native_storage, model_class, expected
):
    config = SimpleNamespace(snapshot=snapshot(specs=[model_spec(model_class=model_class)]))
    result = await ModelService(native_storage, config).resolve(ALICE)
    assert result.model_class == expected


async def test_explicit_provider_receives_selected_spec_and_snapshot_version(monkeypatch):
    monkeypatch.delenv(API_KEY_ENV)

    class ExplicitProvider:
        async def resolve(self, user, spec, config_version):
            return ChatModelConfig(
                type=spec.credential_type,
                credential_id=f"{user.user_id}-{config_version}",
                model=spec.model,
                model_class="custom_chat",
                parameters=spec.parameters,
            )

    config = SimpleNamespace(snapshot=snapshot(version=3, default_key="b"))
    service = ModelService(None, config, provider=ExplicitProvider())
    result = await service.resolve(ALICE)
    assert result.credential_id == "alice-3"
    assert result.model == "model-b"
    assert result.model_class == "custom_chat"
    assert result.parameters == {"temperature": 0.2, "max_tokens": 128}


async def test_exported_components_can_be_explicitly_composed(native_storage):
    from egis_agentkit.service.model import ModelCatalog, ModelProvider, ModelSelection

    class NamedCatalog(ModelCatalog):
        def get(self, snapshot, key):
            return super().get(snapshot, key).model_copy(update={"model": "catalog-model"})

        def list(self, snapshot):
            result = super().list(snapshot)
            result["items"] = result["items"][:1]
            return result

    class ChooseB(ModelSelection):
        def select(self, snapshot, model_key=None, *, is_override=False):
            return super().select(snapshot, model_key or "b", is_override=is_override)

    service = ModelService(
        None,
        SimpleNamespace(snapshot=snapshot()),
        catalog=NamedCatalog(),
        provider=ModelProvider(native_storage),
        selection=ChooseB(),
    )
    assert service.list()["items"] == [{"key": "a", "display_name": "模型 A"}]
    result = await service.resolve(ALICE)
    assert result.model == "catalog-model"
    record = await native_storage.get_credential("alice", result.credential_id)
    assert record.data["name"] == "模型 B"
    assert record.data["base_url"] == "https://models.example.test/v1"


async def test_provider_resolves_without_catalog_or_config_provider(native_storage):
    from egis_agentkit.service.model import ModelProvider

    provider = ModelProvider(native_storage)
    result = await provider.resolve(ALICE, model_spec(base_url=None), config_version=1)
    assert result.model == "model-a"
    assert result.model_class == "openai_chat"
    assert result.parameters == {"temperature": 0.2, "max_tokens": 128}
    record = await native_storage.get_credential("alice", result.credential_id)
    assert record.data["base_url"] is None
    assert record.data["api_key"] == "first-test-key"
