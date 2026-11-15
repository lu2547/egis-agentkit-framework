"""Read deployment settings for connecting to Nacos."""

import os

from egis_agentkit.client.nacos._client import NacosBootstrap


def nacos_bootstrap() -> NacosBootstrap:
    return NacosBootstrap(
        server_url=os.environ.get("NACOS_SERVER_URL", "http://127.0.0.1:8848"),
        console_url=os.environ.get("NACOS_CONSOLE_URL") or None,
        namespace_id=os.environ.get("NACOS_NAMESPACE_ID", "public"),
        group=os.environ.get("NACOS_GROUP", "EGIS_AGENTKIT"),
        identity_key=os.environ.get("NACOS_IDENTITY_KEY") or None,
        identity_value=os.environ.get("NACOS_IDENTITY_VALUE") or None,
        access_token=os.environ.get("NACOS_ACCESS_TOKEN") or None,
    )
