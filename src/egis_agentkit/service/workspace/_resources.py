"""在管理层解析当前 Agent 的资源授权，再交给执行环境装载。"""

from egis_agentkit.errors import AgentKitError, Forbidden
from egis_agentkit.service._user import UserService


async def configure_workspace_resources(workspace, resources, scope):
    """每轮及每次工具执行前重查授权，Core 仅接收工具与 MCP 声明。"""
    if resources is None:
        return
    if scope is None or (
        scope.user_id,
        scope.project_id,
        scope.agent_id,
        scope.session_id,
    ) != (
        workspace.egis_user_id,
        workspace.egis_project_id,
        workspace.egis_agent_id,
        workspace.egis_session_id,
    ):
        raise Forbidden("资源装配缺少一致的会话上下文")
    user = await UserService(resources.storage).get(scope.user_id)
    refs = scope.definition["payload"].get("bindings", [])
    if scope.definition["agent_type"] == "personal":
        selected = await resources.selected_for_personal(user, "mcp", enabled_only=True)
        refs = [
            {
                key: row[key]
                for key in ("resource_type", "resource_id", "resource_version")
            }
            for row in selected.values()
        ]
    mcps = await resources.mcps_for_refs(
        user, refs, authorization_scope=scope, skip_unavailable=True
    )
    tools = []
    registry = getattr(resources, "tool_registry", None)
    for tool in await workspace.get_builtin_tools():
        try:
            if registry is not None:
                await registry.authorize(user, scope, None, tool)
        except AgentKitError:
            continue
        tools.append(tool)
    workspace.configure_resources(tools=tools, mcps=mcps)
    await workspace.list_mcps(agent_id=scope.agent_id, session_id=scope.session_id)
