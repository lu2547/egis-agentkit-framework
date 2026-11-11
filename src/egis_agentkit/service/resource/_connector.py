"""管理本地连接器配置与受权的 MCP 客户端。"""

from __future__ import annotations

import hashlib
import json
from urllib.parse import urlsplit

from agentscope.mcp import MCPClient

from egis_agentkit.errors import Conflict, NotFound

from ._registry import _LocalRegistry


class LocalMCPRegistry(_LocalRegistry):
    def __init__(self, assets):
        super().__init__(assets, "mcp")

    def config(self, asset):
        config = asset["payload"].get("config")
        if config is None:
            raise NotFound("MCP 版本配置不存在")
        return config


def mcp_config_checksum(config: dict) -> str:
    """Canonical identity for a credential-free HTTP MCP transport and URL."""
    if not isinstance(config, dict) or config.get("transport") != "streamable_http":
        raise ValueError("Only streamable HTTP MCP configuration is supported")
    try:
        parsed = urlsplit(config["url"])
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.port == 0
        ):
            raise ValueError
        host = parsed.hostname.lower()
        if ":" in host:
            host = "[" + host + "]"
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        canonical = {
            "transport": "streamable_http",
            "url": f"{parsed.scheme}://{host}:{port}{parsed.path or '/'}",
        }
        if config.get("auth_type", "none") != "none":
            canonical.update(
                {key: value for key, value in config.items() if key not in canonical}
            )
    except (KeyError, ValueError, TypeError, AttributeError):
        raise ValueError("Invalid MCP endpoint") from None
    return hashlib.sha256(json.dumps(canonical, sort_keys=True).encode()).hexdigest()


def _verify_mcp_card(asset, card):
    try:
        config = card.config_template
        actual = mcp_config_checksum(
            {
                **asset["payload"].get("config", {}),
                "transport": "streamable_http",
                "url": config.url,
            }
        )
    except (ValueError, TypeError, AttributeError):
        raise Conflict("MCP configuration is not a pinned HTTP endpoint") from None
    if not asset["checksum_sha256"] or actual != asset["checksum_sha256"]:
        raise Conflict("MCP source checksum changed or is missing")


class AuthorizedMCPClient(MCPClient):
    async def get_tool(self, name):
        tool = await super().get_tool(name)
        tool._egis_mcp_client = self
        return tool
