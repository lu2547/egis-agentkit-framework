from copy import deepcopy

from egis_agentkit.errors import Forbidden
from egis_agentkit.models import Asset, Definition
from egis_agentkit.storage.definition import DefinitionStorage


def definition_data(row: dict) -> dict:
    return Definition.model_validate(
        {k: row[k] for k in Definition.model_fields}
    ).model_dump(mode="json")


async def require_current_shared_asset(conn, asset):
    """Validate a new shared edge against its exact current approved asset."""
    if asset["owner_type"] != "user":
        raise Forbidden("共享资源归属不匹配")
    review = await DefinitionStorage(conn.engine).approved_review(
        asset["resource_id"], resource_type=asset["resource_type"], conn=conn
    )
    approved = review["snapshot"].get("asset") if review else None
    if (
        review is None
        or review["owner_id"] != asset["owner_id"]
        or approved != {key: asset[key] for key in Asset.model_fields}
    ):
        raise Forbidden("此资源版本未通过当前共享审核")


class DefinitionViews:
    """Read validated definition views without making USE authorization decisions."""

    def __init__(self, storage):
        self.repository = DefinitionStorage(storage.engine)

    async def raw_view(self, definition_id: str, view: str, *, conn=None) -> dict:
        row = await self.repository.get_definition(definition_id, conn=conn)
        if not row["is_enabled"]:
            raise Forbidden("资源已停用")
        if view == "shared":
            review = await self.repository.approved_review(definition_id, conn=conn)
            if review is None:
                raise Forbidden("资源尚未通过共享审核")
            root = deepcopy(review["snapshot"]["root"])
            root["shared_version"] = review["shared_version"]
            root["shared_review_id"] = review["review_id"]
            return root
        if (
            view == "system"
            and row["owner_type"] != "system"
            or view == "private"
            and row["owner_type"] != "user"
        ):
            raise Forbidden("资源视图不匹配")
        return definition_data(row)
