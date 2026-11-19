"""验证 App 登录身份，角色和组织组由平台用户记录提供。"""

from functools import cached_property

import jwt

from egis_agentkit.errors import AgentKitError, Forbidden


class JWTIdentity:
    def __init__(self, config):
        self.config = config

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
        except (jwt.PyJWKClientConnectionError, OSError) as exc:
            raise AgentKitError("认证服务暂时不可用，请稍后重试", 503) from exc
        except (jwt.PyJWTError, ValueError, KeyError):
            raise Forbidden("认证无效或已过期") from None
