import time

import pytest
from identity_helpers import TEST_IDENTITY, configure_test_issuer, signed_token
from jwt import PyJWKClientConnectionError
from pydantic import ValidationError

from egis_agentkit.client.identity import JWTIdentity
from egis_agentkit.client.nacos import AuthConfig
from egis_agentkit.errors import AgentKitError, Forbidden


def test_auth_verifies_signature_issuer_audience_and_expiry(monkeypatch):
    configure_test_issuer(monkeypatch)
    auth = JWTIdentity(AuthConfig(**TEST_IDENTITY))
    token = signed_token("alice")
    assert auth.verify(token)["sub"] == "alice"
    for invalid in (
        signed_token("alice", iss="wrong"),
        signed_token("alice", aud="wrong"),
        signed_token("alice", exp=int(time.time()) - 30),
        token + "modified",
    ):
        with pytest.raises(Forbidden):
            auth.verify(invalid)


def test_identity_provider_outage_is_retriable_server_failure(monkeypatch):
    auth = JWTIdentity(AuthConfig(**TEST_IDENTITY))

    def unavailable(_):
        raise PyJWKClientConnectionError("upstream-sensitive-url")

    monkeypatch.setattr(auth.jwks, "get_signing_key_from_jwt", unavailable)
    with pytest.raises(AgentKitError) as error:
        auth.verify(signed_token("alice"))
    assert error.value.status_code == 503
    assert isinstance(error.value.__cause__, PyJWKClientConnectionError)
    assert "upstream-sensitive-url" not in str(error.value)


@pytest.mark.parametrize("override", [{"algorithm": "HS256"}, {"jwks_url": None}])
def test_identity_requires_rs256_and_jwks(override):
    with pytest.raises(ValidationError):
        AuthConfig(**(TEST_IDENTITY | override))
