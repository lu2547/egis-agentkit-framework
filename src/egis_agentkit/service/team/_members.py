"""为依赖授权解析定义中已经固定的 Agent 引用。"""


async def team_member_definitions(storage, root: dict) -> dict[str, dict]:
    """按定义引用读取固定版本，独立 Agent 返回自身。"""
    if root["definition_type"] == "agent":
        return {root["id"]: root}
    payload = root["payload"]
    return {
        ref["definition_id"]: await storage.definitions.get_definition_view(
            ref["definition_id"],
            ref["resource_view_type"],
            version=ref["resource_version"],
            dependency=True,
        )
        for ref in (payload["orchestrator"], *payload["members"])
    }
