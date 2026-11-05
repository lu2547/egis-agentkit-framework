"""Management writes called by authorized enterprise Services, never native hubs."""

import io
import json
import re
import stat
import zipfile
from pathlib import PurePosixPath

import yaml

from ..config import NacosClient
from .nacos import BASE


class NacosHubManagement:
    def __init__(self, client: NacosClient, max_upload_bytes: int = 20 * 1024 * 1024):
        self.client = client
        self.max_upload_bytes = max_upload_bytes

    async def _write(self, method, path, **data):
        data = {"namespaceId": self.client.bootstrap.namespace_id, **data}
        response = await self.client.request(
            method,
            BASE + path,
            **({"params": data} if method == "DELETE" else {"data": data}),
        )
        return self.client.data(response)

    @staticmethod
    def _exact_version(version):
        if not re.fullmatch(r"\d+\.\d+\.\d+(?:-[A-Za-z0-9.-]+)?", version):
            raise ValueError("An exact semantic version is required")

    def _skill_name(self, archive: bytes):
        if len(archive) > self.max_upload_bytes:
            raise ValueError("Skill archive too large")
        try:
            with zipfile.ZipFile(io.BytesIO(archive)) as z:
                infos = z.infolist()
                if (
                    sum(i.file_size for i in infos) > self.max_upload_bytes
                    or len(infos) > 2000
                ):
                    raise ValueError("Expanded skill archive too large")
                for info in infos:
                    path = PurePosixPath(info.filename)
                    if (
                        path.is_absolute()
                        or ".." in path.parts
                        or "\\" in info.filename
                        or stat.S_ISLNK(info.external_attr >> 16)
                    ):
                        raise ValueError("Unsafe archive path")
                manifests = [
                    i for i in infos if PurePosixPath(i.filename).name == "SKILL.md"
                ]
                if len(manifests) != 1:
                    raise ValueError("Exactly one SKILL.md is required")
                text = z.read(manifests[0]).decode("utf-8")
                if not text.startswith("---\n"):
                    raise ValueError("Skill front matter required")
                front = yaml.safe_load(text.split("---", 2)[1])
                name = front["name"]
                if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", name):
                    raise ValueError("Invalid skill name")
                return name
        except (zipfile.BadZipFile, UnicodeError, KeyError, TypeError, yaml.YAMLError):
            raise ValueError("Invalid skill ZIP archive") from None

    async def upload_skill(
        self,
        archive: bytes,
        version: str,
        *,
        overwrite: bool = False,
        commit_msg: str = "",
    ):
        self._exact_version(version)
        name = self._skill_name(archive)
        result = self.client.data(
            await self.client.request(
                "POST",
                BASE + "skills/upload",
                data={
                    "namespaceId": self.client.bootstrap.namespace_id,
                    "targetVersion": version,
                    "overwrite": str(overwrite).lower(),
                    "autoPublishIfNew": "false",
                    "commitMsg": commit_msg,
                },
                files={"file": (name + ".zip", archive, "application/zip")},
            )
        )
        await self._write("PUT", "skills/scope", skillName=name, scope="PRIVATE")
        return result

    async def create_skill(self, skill_card: dict, version: str):
        self._exact_version(version)
        name = skill_card["name"]
        result = await self._write(
            "POST",
            "skills/draft",
            skillName=name,
            targetVersion=version,
            skillCard=json.dumps(skill_card),
        )
        await self._write("PUT", "skills/scope", skillName=name, scope="PRIVATE")
        return result

    async def update_skill(self, skill_card: dict, *, commit_msg: str = ""):
        return await self._write(
            "PUT",
            "skills/draft",
            skillName=skill_card["name"],
            skillCard=json.dumps(skill_card),
            commitMsg=commit_msg,
        )

    async def publish_skill(self, name: str, version: str):
        self._exact_version(version)
        return await self._write(
            "POST", "skills/publish", skillName=name, version=version
        )

    async def submit_skill(self, name: str, version: str):
        self._exact_version(version)
        return await self._write(
            "POST", "skills/submit", skillName=name, version=version
        )

    async def delete_skill(self, name: str):
        return await self._write("DELETE", "skills", skillName=name)

    async def _mcp_draft(
        self,
        method,
        name,
        version,
        server_specification,
        tool_specification=None,
        endpoint_specification=None,
    ):
        self._exact_version(version)
        for entry in (server_specification.get("localServerConfig") or {}).values():
            for value in (entry.get("env") or {}).values():
                if not re.fullmatch(r"\$\{[A-Za-z_][A-Za-z0-9_]*\}", str(value)):
                    raise ValueError(
                        "MCP environment values must be install input references"
                    )
        data = {
            "mcpName": name,
            "version": version,
            "serverSpecification": json.dumps(
                {**server_specification, "scope": "PRIVATE"}
            ),
        }
        for key, value in [
            ("toolSpecification", tool_specification),
            ("endpointSpecification", endpoint_specification),
        ]:
            if value is not None:
                data[key] = json.dumps(value)
        return await self._write(method, "mcp/draft", **data)

    async def create_mcp(
        self,
        name,
        version,
        server_specification,
        tool_specification=None,
        endpoint_specification=None,
    ):
        result = await self._mcp_draft(
            "POST",
            name,
            version,
            server_specification,
            tool_specification,
            endpoint_specification,
        )
        await self._write("PUT", "mcp/scope", mcpName=name, scope="PRIVATE")
        return result

    async def update_mcp(
        self,
        name,
        version,
        server_specification,
        tool_specification=None,
        endpoint_specification=None,
    ):
        return await self._mcp_draft(
            "PUT",
            name,
            version,
            server_specification,
            tool_specification,
            endpoint_specification,
        )

    async def submit_mcp(self, name, version):
        self._exact_version(version)
        return await self._write("POST", "mcp/submit", mcpName=name, version=version)

    async def publish_mcp(self, name, version):
        self._exact_version(version)
        return await self._write("POST", "mcp/publish", mcpName=name, version=version)

    async def delete_mcp(self, name):
        return await self._write("DELETE", "mcp", mcpName=name)

    async def force_publish_skill(self, name, version):
        """Privileged operator operation; never used by ordinary resource saves."""
        self._exact_version(version)
        return await self._write(
            "POST", "skills/force-publish", skillName=name, version=version
        )

    async def force_publish_mcp(self, name, version):
        """Privileged operator operation; never used by ordinary resource saves."""
        self._exact_version(version)
        return await self._write(
            "POST", "mcp/force-publish", mcpName=name, version=version
        )
