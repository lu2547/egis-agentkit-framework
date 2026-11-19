"""维护管理员登记的 MCP 和公司组授权。

核心能力
- 登记或编辑同一 ID 的当前连接配置，递增配置修订号。
- 在同一事务内初始化公司组入口。
"""

from uuid import uuid4

from egis_agentkit.client._mcp_config import mcp_config_checksum
from egis_agentkit.core.mcp import MCPAuthorization
from egis_agentkit.errors import BadRequest, InvalidRequest
from egis_agentkit.service._user import UserService, require_admin
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.storage.model import Asset
from egis_agentkit.storage.model._resource import (
    MCP_MANAGED_FLAG,
    MCP_STORAGE_VERSION,
    validate_resource_id,
)

CONNECTOR_NAME_MAX_LENGTH = 200
CONNECTOR_DESCRIPTION_MAX_LENGTH = 2000


class ConnectorManager:
    """协调连接配置校验、保存和发布授权。"""

    def __init__(self, storage, policies):
        self.storage, self.policies = storage, policies

    async def save(self, admin, data):
        require_admin(admin)
        if "resource_version" in data:
            raise InvalidRequest("MCP 配置只使用资源 ID")
        identifier = data.get("resource_id") or "mcp-" + uuid4().hex
        try:
            validate_resource_id(identifier)
        except ValueError as exc:
            raise BadRequest("Invalid resource identifier") from exc
        try:
            config = MCPAuthorization.validate_config(data.get("config"))
            checksum = mcp_config_checksum(config)
        except ValueError as exc:
            raise BadRequest("Invalid MCP connection configuration") from exc
        company = UserService(self.storage, self.policies).company_group()

        # 串行化同一个连接器的配置写入和启停操作。
        async with self.storage.definition_write() as write:
            await write.assets.lock_resource("mcp", identifier)
            current = await write.assets.get("mcp", identifier, MCP_STORAGE_VERSION)
            payload = {
                MCP_MANAGED_FLAG: True,
                "config": config,
                "display_name": str(data.get("display_name") or identifier)[
                    :CONNECTOR_NAME_MAX_LENGTH
                ],
                "description": str(data.get("description") or "")[
                    :CONNECTOR_DESCRIPTION_MAX_LENGTH
                ],
                "avatar": "code",
            }
            asset = Asset(
                resource_type="mcp",
                resource_id=identifier,
                resource_version=MCP_STORAGE_VERSION,
                publication_revision=await write.assets.next_publication_revision(
                    "mcp", identifier
                ),
                owner_type="system",
                owner_id="system",
                checksum_sha256=checksum,
                payload=payload,
            )
            if current:
                result = await write.assets.update_connector(asset, actor=admin.user_id)
            else:
                await write.assets.insert_asset(asset, actor=admin.user_id)
                result = asset.model_dump()

            # 编辑保留原有授权状态。管理员再次启用可恢复公司组入口。
            grants = await write.access.direct_grants(
                "mcp", identifier, include_revoked=True
            )
            if not any(
                row["grant_source_type"] == "manual"
                and row["principal_type"] == "group"
                and row["principal_id"] == company.group_id
                and row["resource_view_type"] == "system"
                and row["resource_owner_id"] == "system"
                for row in grants
            ):
                await GrantService(write).publish_connector(
                    identifier, company, actor=admin.user_id, write=write
                )
            return result
