"""User-authorized additions to a personal conversation's native team.

Snapshots live in native session state. Staging creates no grants and starts no
execution; a real native user turn consumes the selected invitations.
"""

from copy import deepcopy

from agentscope.app.message_bus import MessageBusKeys

from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.models import new_id
from egis_agentkit.service.access.authorization import PermissionService
from egis_agentkit.service.access.snapshot import SnapshotService


def collaboration_allowed(session, binding):
    root = binding.get("snapshot", {}).get("root", {})
    return (
        session.origin.type == "user"
        and not binding.get("is_team")
        and root.get("definition_type") == "agent"
        and root.get("agent_type") == "personal"
    )


def collaborators(binding):
    return binding.get("collaboration", {}).get("members", [])


def team_say_allowed(session, binding):
    return bool(
        binding.get("is_team")
        or (
            getattr(session, "team_id", None)
            and (
                binding.get("collaboration_parent_session_id")
                or any(m.get("status") == "joined" for m in collaborators(binding))
            )
        )
    )


def pending_request_id(binding):
    return next(
        (
            r["request_id"]
            for r in binding.get("collaboration", {}).get("requests", [])
            if not r.get("completed")
        ),
        None,
    )


async def validate_collaborators(permissions, user, binding):
    for member in collaborators(binding):
        await permissions.validate_snapshot(user, member["snapshot"])


class CollaborationService:
    def __init__(self, tasks):
        self.tasks = tasks

    async def stage(self, user, session_id, bus, request_id, experts):
        tasks = self.tasks
        # A revoked, unconsumed selection must still be removable. Only root
        # authority and the resulting roster are required for preparation.
        session = await tasks.session(user, session_id, require_use=False)
        binding = session.state.middle_context["egis_agentkit"]
        async with tasks.projects.guard(user, binding["project_id"]):
            key = MessageBusKeys.session_lock(session_id)
            if not await bus.try_lock(key):
                raise Conflict("对话正在运行，请等待本轮完成后邀请专家")
            try:
                session = await tasks.session(user, session_id, require_use=False)
                binding = session.state.middle_context["egis_agentkit"]
                if not collaboration_allowed(session, binding):
                    raise Forbidden(
                        "只有个人智能体主对话可以邀请专家协作；专家不能担任组长，固定专家团不能调整成员"
                    )
                policies = tasks.config.snapshot.policies if tasks.config else None
                permissions = PermissionService(tasks.storage, policies)
                await permissions.require(
                    user,
                    binding["root_resource_type"],
                    binding["definition_id"],
                    binding["resource_view_type"],
                )
                refs = sorted(
                    {
                        (item["definition_id"], item["resource_view_type"])
                        for item in experts
                    }
                )
                selection = [
                    dict(definition_id=identifier, resource_view_type=view)
                    for identifier, view in refs
                ]
                data = binding.setdefault(
                    "collaboration", {"requests": [], "members": []}
                )
                previous = next(
                    (r for r in data["requests"] if r["request_id"] == request_id), None
                )
                if previous is not None:
                    if previous["experts"] != selection:
                        raise Conflict("邀请请求编号已用于不同的专家选择")
                    return await tasks.get(user, session_id)
                if not request_id or len(request_id) > 128 or len(selection) > 16:
                    raise Conflict("邀请请求需要编号，且最多选择 16 位专家")
                # A new request replaces outstanding selections; actual joined
                # members remain native team members. No membership tool runs.
                retained = [
                    m
                    for m in data["members"]
                    if m["status"] == "joined"
                    or (m["definition_id"], m["resource_view_type"]) in refs
                ]
                for member in retained:
                    await permissions.validate_snapshot(user, member["snapshot"])
                additions = []
                for item in selection:
                    if any(all(m[k] == item[k] for k in item) for m in retained):
                        continue
                    await permissions.require(
                        user, "agent", item["definition_id"], item["resource_view_type"]
                    )
                    snapshot = await SnapshotService(tasks.storage).capture(
                        item["definition_id"], item["resource_view_type"]
                    )
                    root = snapshot["root"]
                    if (
                        root["definition_type"] != "agent"
                        or root["agent_type"] != "expert"
                    ):
                        raise Forbidden("只能邀请预配置的专家智能体")
                    await permissions.validate_snapshot(user, snapshot)
                    additions.append(
                        dict(
                            **item,
                            display_name=root["agent_display_name"],
                            native_agent_id=new_id(),
                            snapshot=deepcopy(snapshot),
                            status="pending",
                        )
                    )
                if len(retained) + len(additions) > 16:
                    raise Conflict("当前对话最多邀请 16 位专家")
                for request in data["requests"]:
                    if not request.get("completed"):
                        request.update(completed=True, superseded=True)
                data["members"] = retained + additions
                data["requests"].append(
                    dict(
                        request_id=request_id,
                        experts=selection,
                        completed=not selection,
                    )
                )
                await tasks.storage.update_session_state(
                    user.user_id, session.agent_id, session.id, session.state
                )
            finally:
                await bus.unlock(key)
        return await tasks.get(user, session_id)
