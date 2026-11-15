"""按照 Java OpenAPI 第 11.1 节生成 HMAC，请求体使用实际发送的原始字节。"""

import hashlib
import hmac


def sign_request(
    secret: str,
    method: str,
    path: str,
    query: str,
    timestamp: str,
    nonce: str,
    body: bytes,
) -> str:
    """PATH 由调用方传入 /openapi/v1/...，不包含 servlet 或网关前缀。"""
    canonical = "\n".join(
        (
            method.upper(),
            path,
            query,
            timestamp,
            nonce,
            hashlib.sha256(body).hexdigest(),
        )
    )
    return hmac.new(secret.encode(), canonical.encode(), hashlib.sha256).hexdigest()
