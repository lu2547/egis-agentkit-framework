"""验证数据库取密、Tracing 和日志配置的边界及配置错误脱敏。"""

import pytest
import yaml
from pydantic import ValidationError
from test_nacos_config import documents

from egis_agentkit.client.nacos import NacosError, _settings, load_agentkit_config


def test_credentials_are_secrets_and_modes_are_mutually_exclusive():
    key = _settings.PostgresConnectionConfig(password="private-password")
    assert key.password.get_secret_value() == "private-password"
    assert "private-password" not in repr(key)
    cyberark = dict(
        endpoint="https://vault.internal/AIMWebService/api/Accounts",
        app_id="agentkit",
        safe="database",
        object_name="postgres",
    )
    remote = _settings.PostgresConnectionConfig(mode="cyberark", cyberark=cyberark)
    assert remote.cyberark.folder == "Root"
    for data in (
        {"mode": "cyberark"},
        {"mode": "cyberark", "password": "secret", "cyberark": cyberark},
        {"mode": "key", "cyberark": cyberark},
    ):
        with pytest.raises(ValidationError):
            _settings.PostgresConnectionConfig.model_validate(data)
    with pytest.raises(ValidationError) as exc:
        _settings.PostgresConnectionConfig(mode="cyberark", password="private-password")
    assert "private-password" not in str(exc.value)


@pytest.mark.parametrize(
    "data",
    [
        {"sampleRatio": -0.1},
        {"sampleRatio": 1.1},
        {"maxQueueSize": 1, "maxExportBatchSize": 2},
        {"endpoint": "http://user:secret@collector/v1/traces"},
        {"endpoint": "http://collector/v1/traces?token=secret"},
    ],
)
def test_tracing_rejects_invalid_export_configuration(data):
    with pytest.raises(ValidationError):
        _settings.TracingConfig.model_validate(data)


def test_nacos_config_supports_observability_and_scrubs_secret_validation_errors():
    doc = documents()["agentkit.yaml"]
    doc["agentkit"].update(logging={"level": "DEBUG", "format": "json"})
    doc["agentkit"]["client"] = {
        "postgres": {"connection": {"password": "private-password"}},
        "tracing": {"enabled": True, "headers": {"authorization": "private-header"}},
    }
    config = load_agentkit_config(yaml.safe_dump(doc))
    assert config.logging.level == "DEBUG"
    assert config.client.tracing.enabled
    assert "private-header" not in repr(config)
    doc["agentkit"]["client"]["postgres"]["connection"]["port"] = "private-password"
    with pytest.raises(NacosError) as exc:
        load_agentkit_config(yaml.safe_dump(doc))
    assert "private-password" not in str(exc.value)
    assert "port" in str(exc.value)
