"""管理外部系统上传 Skill 的身份及凭证。"""

import re
from hashlib import sha256
from hmac import compare_digest
from secrets import token_urlsafe
from uuid import uuid4

from egis_agentkit.errors import Forbidden, InvalidRequest, NotFound
from egis_agentkit.storage.model import User

from ._user import VIRTUAL_USER_PREFIX, require_admin

VIRTUAL_TOKEN_PREFIX = "ak-skill."
CREDENTIAL_RANDOM_BYTES = 32
CREDENTIAL_PATTERN = re.compile(r"ak-skill\.([a-f0-9]{32})\.[A-Za-z0-9_-]{43}")
UPLOAD_PATH = "/manager/skills/imports"
STATUS_PATH = re.compile(r"/manager/skills/imports/[a-f0-9]{32}(/retry)?\Z")


class VirtualUserService:
    """管理员管理账号，外部系统通过 Bearer 凭证获得受限上传身份。"""

    def __init__(self, storage):
        self.storage = storage

    @staticmethod
    def allows_request(method, path):
        match = STATUS_PATH.fullmatch(path)
        if match:
            return method == ("POST" if match.group(1) else "GET")
        return path == UPLOAD_PATH and method in ("GET", "POST")

    @staticmethod
    def _new_token(user_id):
        suffix = user_id.removeprefix(VIRTUAL_USER_PREFIX)
        return (
            f"{VIRTUAL_TOKEN_PREFIX}{suffix}.{token_urlsafe(CREDENTIAL_RANDOM_BYTES)}"
        )

    async def create(self, admin, username):
        require_admin(admin)
        username = username.strip()
        if not username:
            raise InvalidRequest("请输入对接系统名称")
        user_id = VIRTUAL_USER_PREFIX + uuid4().hex
        token = self._new_token(user_id)
        await self.storage.create(
            user_id=user_id,
            username=username,
            digest=sha256(token.encode()).hexdigest(),
            actor=admin.user_id,
        )
        return {
            "user_id": user_id,
            "username": username,
            "is_enabled": True,
            "token": token,
        }

    async def list(self, admin):
        require_admin(admin)
        return await self.storage.list()

    async def set_enabled(self, admin, user_id, enabled):
        require_admin(admin)
        row = await self.storage.update(
            user_id, actor=admin.user_id, is_enabled=enabled
        )
        if row is None:
            raise NotFound("虚拟用户不存在")
        return row

    async def rotate(self, admin, user_id):
        require_admin(admin)
        token = self._new_token(user_id)
        row = await self.storage.update(
            user_id,
            actor=admin.user_id,
            credential_sha256=sha256(token.encode()).hexdigest(),
        )
        if row is None:
            raise NotFound("虚拟用户不存在")
        return {**row, "token": token}

    async def authenticate(self, token):
        match = CREDENTIAL_PATTERN.fullmatch(token)
        if match is None:
            raise Forbidden("虚拟用户凭证无效")
        user_id = VIRTUAL_USER_PREFIX + match.group(1)
        row = await self.storage.get(user_id)
        if (
            row is None
            or not row["is_enabled"]
            or not compare_digest(
                row["credential_sha256"], sha256(token.encode()).hexdigest()
            )
        ):
            raise Forbidden("虚拟用户凭证无效")
        return User(user_id=user_id, username=row["username"])

    async def get(self, user_id):
        row = await self.storage.get(user_id)
        if row is None or not row["is_enabled"]:
            raise Forbidden("虚拟用户已停用")
        return User(user_id=user_id, username=row["username"])
