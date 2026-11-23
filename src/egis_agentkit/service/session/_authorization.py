"""把已验证的会话凭据保留到原生队列执行；不进入消息、模型上下文或事件流。"""

import json
import math
import time

from pydantic import SecretStr

from egis_agentkit.core.session._context import VerifiedChatAuthorization


class ChatAuthorizationStore:
    """通过共享总线的私有 registry 传递凭据，最迟在 JWT 过期时失效。"""

    def __init__(self, bus):
        self.bus = bus

    @staticmethod
    def namespace(session_id):
        return f"egis:chat_authorization:{session_id}"

    async def remember(self, authorization):
        if authorization.expires_at is None:
            return
        remaining = authorization.expires_at - time.time()
        if remaining <= 0:
            return
        await self.bus.registry_set(
            self.namespace(authorization.session_id),
            "verified",
            json.dumps({
                "user_id": authorization.user_id,
                "agent_id": authorization.agent_id,
                "session_id": authorization.session_id,
                "access_token": authorization.access_token.get_secret_value(),
                "expires_at": authorization.expires_at,
            }),
            ttl_secs=math.ceil(remaining),
        )

    async def load(self, user_id, agent_id, session_id):
        raw = await self.bus.registry_get(self.namespace(session_id), "verified")
        if raw is None:
            return None
        value = json.loads(raw)
        if (
            (value["user_id"], value["agent_id"], value["session_id"])
            != (user_id, agent_id, session_id)
            or value["expires_at"] <= time.time()
        ):
            return None
        return VerifiedChatAuthorization(
            user_id, agent_id, session_id, SecretStr(value["access_token"]),
            expires_at=value["expires_at"],
        )

    async def discard(self, session_id):
        await self.bus.registry_del(self.namespace(session_id), "verified")
