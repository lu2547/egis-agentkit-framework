"""Small Nacos 3 REST boundary. Never forward upstream bodies into errors."""

from typing import Any
from urllib.parse import urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, SecretStr, model_validator


class NacosError(RuntimeError):
    def __init__(self, message: str, status_code: int = 502):
        self.status_code = status_code
        super().__init__(message)


class NacosBootstrap(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    server_url: str
    console_url: str | None = None
    namespace_id: str = "public"
    group: str = "EGIS_AGENTKIT"
    identity_key: str | None = None
    identity_value: SecretStr | None = None
    access_token: SecretStr | None = None

    @model_validator(mode="after")
    def validate_connection(self):
        for url in (self.server_url, self.console_url):
            if url is None:
                continue
            parsed = urlsplit(url)
            if (
                parsed.scheme not in {"http", "https"}
                or not parsed.netloc
                or parsed.username
                or parsed.password
                or parsed.query
                or parsed.fragment
                or parsed.path not in ("", "/")
            ):
                raise ValueError(
                    "Nacos addresses must be HTTP origins without credentials"
                )
        if bool(self.identity_key) != bool(self.identity_value):
            raise ValueError("identity key and value must be supplied together")
        return self


class NacosClient:
    def __init__(
        self, bootstrap: NacosBootstrap, http_client: httpx.AsyncClient | None = None
    ):
        self.bootstrap = bootstrap
        self._owns_http = http_client is None
        self.http = http_client or httpx.AsyncClient(timeout=30, follow_redirects=False)

    def _url(self, path: str) -> str:
        if (
            not path.startswith("/nacos/v3/")
            or ".." in path
            or "?" in path
            or "#" in path
        ):
            raise ValueError("Only absolute Nacos v3 API paths are allowed")
        if self.bootstrap.console_url and path.startswith("/nacos/v3/admin/"):
            return self.bootstrap.console_url.rstrip("/") + path.replace(
                "/nacos/v3/admin/", "/v3/console/", 1
            )
        return self.bootstrap.server_url.rstrip("/") + path

    def _headers(self) -> dict[str, str]:
        result = {}
        if self.bootstrap.identity_key:
            result[self.bootstrap.identity_key] = (
                self.bootstrap.identity_value.get_secret_value()
            )
        if self.bootstrap.access_token:
            result["accessToken"] = self.bootstrap.access_token.get_secret_value()
        return result

    async def request(
        self, method: str, path: str, *, params=None, data=None, json=None, files=None
    ) -> httpx.Response:
        url = self._url(path)
        try:
            response = await self.http.request(
                method,
                url,
                headers=self._headers(),
                params=params,
                data=data,
                json=json,
                files=files,
                follow_redirects=False,
            )
        except httpx.HTTPError:
            raise NacosError("Nacos transport unavailable", 503) from None
        self.check_response(response)
        return response

    @staticmethod
    def check_response(response: httpx.Response) -> None:
        if response.status_code >= 300:
            raise NacosError("Nacos request rejected", response.status_code)
        if "json" in response.headers.get("content-type", ""):
            try:
                body = response.json()
            except ValueError:
                raise NacosError("Invalid Nacos JSON response") from None
            if isinstance(body, dict) and body.get("code", 0) != 0:
                code = body.get("code")
                raise NacosError(
                    "Nacos operation failed",
                    code if isinstance(code, int) and 400 <= code < 600 else 502,
                )

    @staticmethod
    def data(response: httpx.Response) -> Any:
        try:
            body = response.json()
            if not isinstance(body, dict) or "data" not in body:
                raise ValueError
            return body["data"]
        except (ValueError, KeyError):
            raise NacosError("Invalid Nacos response envelope") from None

    async def get_config(self, data_id: str) -> str:
        response = await self.request(
            "GET",
            "/nacos/v3/client/cs/config",
            params={
                "namespaceId": self.bootstrap.namespace_id,
                "groupName": self.bootstrap.group,
                "dataId": data_id,
            },
        )
        content = self.data(response)
        if isinstance(content, dict):
            if content.get("success") is not True or content.get("errorCode", 0) != 0:
                raise NacosError("Nacos configuration query failed")
            content = content.get("content")
        if not isinstance(content, str):
            raise NacosError("Nacos configuration content is not text")
        return content

    async def publish_config(
        self, data_id: str, content: str, config_type: str = "yaml"
    ) -> bool:
        result = self.data(
            await self.request(
                "POST",
                "/nacos/v3/admin/cs/config",
                data={
                    "namespaceId": self.bootstrap.namespace_id,
                    "groupName": self.bootstrap.group,
                    "dataId": data_id,
                    "content": content,
                    "type": config_type,
                },
            )
        )
        if result not in (True, "ok", "true"):
            raise NacosError("Nacos configuration publication was not acknowledged")
        return True

    async def close(self) -> None:
        if self._owns_http:
            await self.http.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()
