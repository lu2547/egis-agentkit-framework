"""传输 Linux 文件服务的请求，保留存储调用的路径、字节及错误语义。"""

import os
from base64 import b64decode, b64encode
from datetime import datetime
from pathlib import PurePath, PurePosixPath

import httpx

from egis_agentkit.client.nacos._settings import WorkspaceFilesConfig
from egis_agentkit.errors import (
    AgentKitError,
    Conflict,
    Forbidden,
    InvalidRequest,
    NotFound,
    UploadNameConflict,
)

_TYPE = "$nas"
_ERRORS = {
    error.__name__: error for error in (Conflict, Forbidden, InvalidRequest, NotFound)
}


def encode_wire(value):
    """递归编码文件存储接口中 JSON 无法直接表达的值。"""
    if isinstance(value, bytes):
        return {_TYPE: "bytes", "value": b64encode(value).decode("ascii")}
    if isinstance(value, PurePath):
        return {_TYPE: "path", "value": value.as_posix()}
    if isinstance(value, datetime):
        return {_TYPE: "datetime", "value": value.isoformat()}
    if isinstance(value, tuple):
        return {_TYPE: "tuple", "value": [encode_wire(item) for item in value]}
    if isinstance(value, list):
        return [encode_wire(item) for item in value]
    if isinstance(value, dict):
        if _TYPE in value or any(not isinstance(key, str) for key in value):
            return {
                _TYPE: "mapping",
                "value": [
                    [encode_wire(key), encode_wire(item)] for key, item in value.items()
                ],
            }
        return {key: encode_wire(item) for key, item in value.items()}
    return value


def decode_wire(value):
    """还原存储值，Linux 路径始终使用纯 POSIX 表示。"""
    if isinstance(value, list):
        return [decode_wire(item) for item in value]
    if not isinstance(value, dict):
        return value
    if _TYPE not in value:
        return {key: decode_wire(item) for key, item in value.items()}
    kind, data = value[_TYPE], value["value"]
    if kind == "bytes":
        return b64decode(data, validate=True)
    if kind == "path":
        return PurePosixPath(data)
    if kind == "datetime":
        return datetime.fromisoformat(data)
    if kind == "tuple":
        return tuple(decode_wire(item) for item in data)
    if kind == "mapping":
        return {decode_wire(key): decode_wire(item) for key, item in data}
    raise ValueError("无效的文件服务数据类型")


class NasFilesClient:
    """调用受信任的 Linux 文件服务，失败时保留文件存储的业务错误。"""

    def __init__(self, settings: WorkspaceFilesConfig):
        token = os.environ.get(settings.token_env, "")
        if not token or not token.isascii() or any(ch.isspace() for ch in token):
            raise ValueError(f"文件服务凭据 {settings.token_env} 缺失或格式无效")
        assert settings.endpoint is not None
        self._endpoint = settings.endpoint.rstrip("/")
        self._headers = {"Authorization": f"Bearer {token}"}
        self._timeout = settings.timeout_seconds

    def call(self, scope, operation, **arguments):
        """发起一次操作；写请求失败后由业务调用方决定是否重试。"""
        try:
            response = httpx.post(
                f"{self._endpoint}/v1/nas/{scope}/{operation}",
                headers=self._headers,
                json=encode_wire(arguments),
                timeout=self._timeout,
                trust_env=False,
            )
            payload = response.json()
            if response.is_success:
                return decode_wire(payload)
            if not isinstance(payload, dict):
                raise ValueError("无效的文件服务错误响应")
        except (httpx.HTTPError, ValueError, TypeError, KeyError) as exc:
            raise AgentKitError("工作区文件服务暂时不可用", 503) from exc

        if response.status_code == 401:
            raise AgentKitError("工作区文件服务鉴权失败，请核对服务配置", 503)
        message = payload.get("detail")
        error = payload.get("error")
        if error == "UploadNameConflict":
            raise UploadNameConflict()
        if isinstance(error, str) and error in _ERRORS and isinstance(message, str):
            raise _ERRORS[error](message)
        raise AgentKitError("工作区文件服务暂时不可用", 503)
