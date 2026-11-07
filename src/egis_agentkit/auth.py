"""Verify identity only. Roles and groups always come from PostgreSQL."""

import os
import time
from functools import cached_property

import jwt

from egis_agentkit.errors import Forbidden


class JWTIdentity:
    def __init__(self, config):
        self.config = config
        if config.mode == "jwt" and (
            config.algorithm != "RS256" or not config.jwks_url
        ):
            raise ValueError("企业认证必须配置 RS256 JWKS、issuer 和 audience")
        if not config.issuer or not config.audience:
            raise ValueError("认证 issuer/audience 不能为空")
        if config.mode == "demo":
            if (
                config.algorithm != "HS256"
                or len(os.environ.get(config.signing_secret_env, "")) < 32
            ):
                raise ValueError("Demo 必须显式配置至少 32 字节的签名密钥")

    @cached_property
    def jwks(self):
        return jwt.PyJWKClient(
            self.config.jwks_url, cache_keys=True, lifespan=300, timeout=10
        )

    def verify(self, token: str) -> dict:
        if not token or len(token) > 16384:
            raise Forbidden("认证失败")
        try:
            if self.config.mode == "demo":
                key = os.environ[self.config.signing_secret_env]
            else:
                key = self.jwks.get_signing_key_from_jwt(token).key
            claims = jwt.decode(
                token,
                key,
                algorithms=[self.config.algorithm],
                issuer=self.config.issuer,
                audience=self.config.audience,
                leeway=10,
                options={"require": ["exp", "iat", "iss", "aud", "sub"]},
            )
            if not isinstance(claims["sub"], str) or not claims["sub"]:
                raise ValueError
            return claims
        except (jwt.PyJWTError, ValueError, KeyError, OSError):
            raise Forbidden("认证无效或已过期") from None

    def issue(self, user_id: str, username: str) -> str:
        if self.config.mode != "demo":
            raise Forbidden("企业登录令牌必须由公司身份服务签发")
        now = int(time.time())
        return jwt.encode(
            {
                "sub": user_id,
                "name": username,
                "iss": self.config.issuer,
                "aud": self.config.audience,
                "iat": now,
                "exp": now + 3600,
            },
            os.environ[self.config.signing_secret_env],
            algorithm="HS256",
        )
