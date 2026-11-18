"""签发测试令牌，供 Framework 校验链路验证。"""

import json
import time
from functools import lru_cache

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa

TEST_IDENTITY = {
    "issuer": "urn:egis:agentkit:test",
    "audience": "egis-agentkit",
    "jwks_url": "http://identity.invalid/.well-known/jwks.json",
}
TEST_KEY_ID = "test-issuer"


@lru_cache(maxsize=1)
def _key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _jwks(_client):
    key = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(_key().public_key()))
    return {"keys": [{**key, "kid": TEST_KEY_ID, "use": "sig", "alg": "RS256"}]}


def configure_test_issuer(monkeypatch):
    monkeypatch.setattr(jwt.PyJWKClient, "fetch_data", _jwks)


def signed_token(user_id, **overrides):
    now = int(time.time())
    claims = {
        "sub": user_id,
        "name": user_id,
        "iss": TEST_IDENTITY["issuer"],
        "aud": TEST_IDENTITY["audience"],
        "iat": now,
        "exp": now + 300,
        **overrides,
    }
    return jwt.encode(claims, _key(), algorithm="RS256", headers={"kid": TEST_KEY_ID})
