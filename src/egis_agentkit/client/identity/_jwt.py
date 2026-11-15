"""验证 App 登录身份，角色和组织组由平台用户记录提供。"""

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
                or len(os.environ.get(config.signing_secret_env, "").encode())
                < config.jwt_min_signing_secret_length
            ):
                raise ValueError("Demo 必须显式配置符合最小长度的签名密钥")

    @cached_property
    def jwks(self):
        return jwt.PyJWKClient(
            self.config.jwks_url,
            cache_keys=True,
            lifespan=self.config.jwks_cache_seconds,
            timeout=self.config.jwks_timeout_seconds,
        )

    def verify(self, token: str) -> dict:
        if not token or len(token) > self.config.jwt_max_token_length:
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
                leeway=self.config.jwt_clock_skew_seconds,
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
                "exp": now + self.config.jwt_lifetime_seconds,
            },
            os.environ[self.config.signing_secret_env],
            algorithm="HS256",
        )
