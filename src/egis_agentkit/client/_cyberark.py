"""封装 CyberArk CCP REST 取密，隔离凭据响应与上游异常。"""

import ssl

import httpx
from pydantic import SecretStr

from egis_agentkit.client.nacos._settings import CyberArkConfig


class CyberArkError(RuntimeError):
    """仅携带安全错误类别，不保留请求 URL 或凭据响应。"""


class CyberArkClient:
    """在每次调用时读取 CCP 当前密码，不在客户端缓存密码。"""

    def __init__(
        self, settings: CyberArkConfig, *, http_client: httpx.AsyncClient | None = None
    ):
        """注入的 HTTP 会话由调用方管理，默认会话在单次取密结束后关闭。"""
        self._settings = settings
        self._http_client = http_client

    def _tls_context(self) -> ssl.SSLContext:
        """先验证 CA 与成对的客户端证书，任何加载失败都禁止继续发送请求。"""
        settings = self._settings
        if bool(settings.client_cert_file) != bool(settings.client_key_file):
            raise CyberArkError(
                "CyberArk client certificate and key are required together"
            )
        try:
            context = ssl.create_default_context(cafile=settings.tls_ca_file)
            if settings.client_cert_file:
                context.load_cert_chain(
                    settings.client_cert_file, settings.client_key_file
                )
            return context
        except (OSError, ValueError):
            raise CyberArkError("CyberArk TLS configuration invalid") from None

    async def _request_password(self, http: httpx.AsyncClient) -> SecretStr:
        """只接受 CCP 的非空 Content 字符串，不输出正文或账号定位参数。"""
        settings = self._settings
        try:
            response = await http.get(
                settings.endpoint,
                params={
                    "AppID": settings.app_id,
                    "Safe": settings.safe,
                    "Folder": settings.folder,
                    "Object": settings.object_name,
                },
                timeout=settings.timeout_seconds,
                follow_redirects=False,
            )
        except (httpx.HTTPError, httpx.InvalidURL) as exc:
            raise CyberArkError(
                f"CyberArk transport unavailable ({type(exc).__name__})"
            ) from None
        if not 200 <= response.status_code < 300:
            raise CyberArkError(
                f"CyberArk request rejected (HTTP {response.status_code})"
            )
        try:
            body = response.json()
        except ValueError:
            raise CyberArkError("CyberArk password response invalid") from None
        password = body.get("Content") if isinstance(body, dict) else None
        if not isinstance(password, str) or not password:
            raise CyberArkError("CyberArk password response invalid")
        return SecretStr(password)

    async def get_password(self) -> SecretStr:
        """获取当前密码；默认会话逐次创建以免连接池需要额外管理 HTTP 生命周期。"""
        if self._http_client is not None:
            return await self._request_password(self._http_client)
        async with httpx.AsyncClient(
            verify=self._tls_context(),
            timeout=self._settings.timeout_seconds,
            follow_redirects=False,
            trust_env=False,
        ) as http:
            return await self._request_password(http)
