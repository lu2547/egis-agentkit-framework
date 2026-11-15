"""校验连接器客户端配置及资源中固定的端点身份。"""

import hashlib
import json
from urllib.parse import urlsplit

from egis_agentkit.errors import Conflict


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
