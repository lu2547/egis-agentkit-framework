import time

import jwt
import pytest

from egis_agentkit.auth import JWTIdentity
from egis_agentkit.config import AuthConfig
from egis_agentkit.errors import Forbidden


def test_auth_verifies_issuer_audience_expiry_and_ignores_role_claim(monkeypatch):
    monkeypatch.setenv("TEST_JWT_KEY", "test-only-key-with-at-least-thirty-two-bytes")
    auth = JWTIdentity(
        AuthConfig(
            mode="demo",
            issuer="egis-test",
            audience="egis",
            algorithm="HS256",
            signing_secret_env="TEST_JWT_KEY",
        )
    )
    token = auth.issue("alice", "张三")
    claims = auth.verify(token)
    assert claims["sub"] == "alice"
    with pytest.raises(Forbidden):
        auth.verify(
            jwt.encode(
                {
                    "sub": "alice",
                    "iss": "wrong",
                    "aud": "egis",
                    "exp": int(time.time()) + 300,
                    "iat": int(time.time()),
                },
                "test-only-key-with-at-least-thirty-two-bytes",
                algorithm="HS256",
            )
        )
    with pytest.raises(Forbidden):
        auth.verify(token + "modified")


def test_production_rejects_symmetric_demo_signer(monkeypatch):
    monkeypatch.setenv("TEST_JWT_KEY", "test-only-key-with-at-least-thirty-two-bytes")
    with pytest.raises(ValueError):
        JWTIdentity(
            AuthConfig(
                mode="jwt",
                issuer="enterprise",
                audience="egis",
                algorithm="HS256",
                signing_secret_env="TEST_JWT_KEY",
            )
        )
