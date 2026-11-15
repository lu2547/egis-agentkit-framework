"""验证外部系统连接统一从 client 接入，配置与代码使用同一能力名称。"""

import importlib
from pathlib import Path


def test_external_connections_are_available_from_client():
    postgres = importlib.import_module("egis_agentkit.client._postgres")
    nacos = importlib.import_module("egis_agentkit.client.nacos")
    kafka = importlib.import_module("egis_agentkit.client._kafka")
    assert callable(postgres.create_postgres_engine)
    assert callable(nacos.NacosClient)
    assert callable(kafka.KafkaTransport)
    source = Path(postgres.__file__).parents[1]
    assert not (source / "backend").exists()


def test_client_configuration_has_no_backend_alias():
    import yaml

    from egis_agentkit.client.nacos import load_agentkit_config

    document = Path("examples/nacos/agentkit.yaml").read_text()
    config = load_agentkit_config(document)
    assert config.client.postgres.pool.size > 0
    assert config.client.nacos.refresh.interval_seconds > 0
    assert "backend" not in yaml.safe_load(document)["agentkit"]
