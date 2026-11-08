import asyncio
import inspect
import json
import subprocess
import sys
from contextlib import asynccontextmanager
from dataclasses import replace
from functools import partial
from types import SimpleNamespace

import pytest
from agentscope.middleware import MiddlewareBase
from agentscope.permission import (
    PermissionBehavior,
    PermissionContext,
    PermissionDecision,
    PermissionEngine,
    PermissionRule,
)
from agentscope.skill import Skill
from agentscope.state import AgentState
from agentscope.tool import Read, Toolkit
from agentscope.workspace import WorkspaceBase
from test_packages import release

from egis_agentkit.errors import Forbidden
from egis_agentkit.extensions import middleware as components
from egis_agentkit.extensions.agent import factory as factories
from egis_agentkit.extensions.compatibility.execution import (
    ExecutionScope,
    execution_scope,
)
from egis_agentkit.extensions.compatibility.policy import (
    team_prompt_middleware,
    tool_access_middleware,
)
from egis_agentkit.extensions.middleware.file_changes import FileChangesMiddleware
from egis_agentkit.extensions.middleware.policy import EnterprisePolicyMiddleware
from egis_agentkit.packages import Assembly, AssemblyContext, PackageLoader


def test_core_middleware_import_leaves_enterprise_wiring_unloaded():
    result = subprocess.run(
        [sys.executable, "-c", """
import sys
from egis_agentkit.extensions.middleware import (
    FileChangesMiddleware,
    SkillBindingMiddleware,
    TeamPromptMiddleware,
    ToolAccessMiddleware,
    UserPreferenceMiddleware,
    WorkspacePromptMiddleware,
)
from egis_agentkit.extensions.skill import LiveSkillLoader

forbidden = (
    "egis_agentkit.service",
    "egis_agentkit.storage",
    "egis_agentkit.extensions.compatibility",
    "egis_agentkit_app",
)
loaded = sorted(
    name for name in sys.modules
    if any(name == root or name.startswith(root + ".") for root in forbidden)
)
assert not loaded, loaded
"""],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def component(name):
    result = getattr(components, name, None)
    assert result is not None, f"Missing runtime component: {name}"
    return result


def scope_at(tmp_path, *, kind="private", team=False, assembly=None):
    project = tmp_path / "user" / "alice" / "projects" / "project-one"
    return ExecutionScope(
        user_id="alice",
        agent_id="agent-one",
        session_id="session-two",
        definition={
            "agent_type": kind,
            "payload": {
                "bindings": [{
                    "resource_type": "skill",
                    "resource_id": "report",
                    "resource_version": "1.0.0",
                    "resource_view_type": "system",
                }],
            },
        },
        snapshot={"nodes": {
            "writer": {"resource_type": "agent", "data": {"agent_name": "writer"}},
            "reviewer": {"resource_type": "agent", "data": {"agent_name": "reviewer"}},
            "skill": {"resource_type": "skill", "data": {}},
        }},
        binding={"is_team": team},
        session_record=object(),
        workdir=project / ".work",
        artifacts_dir=project / "artifacts",
        assembly=assembly,
        project_id="project-one",
    )


class Resources:
    def __init__(self, tmp_path, trace):
        self.trace = trace
        self.workspace_root = tmp_path
        self.storage = self
        self.saved = []
        self.descriptor = Skill("report", "Report skill", "", "", 0)
        self.materialized = Skill(
            "report", "Report skill", str(tmp_path / "skills" / "report"),
            "Full report instructions", 0,
        )
        self.installed = []
        self.scopes = []

    async def installed_assets(self, user, resource_type, *, enabled_only):
        assert resource_type == "skill"
        assert enabled_only
        return {
            ref["resource_id"]: ref for ref in self.installed
            if ref["is_installed"] and ref["is_enabled"]
        }

    async def skill_descriptors(self, user, refs, *, authorization_scope):
        self.trace.append("descriptors")
        self.scopes.append(authorization_scope)
        return [self.descriptor for ref in refs]

    async def materialize_skill(self, user, resource_id, version, *, authorization_scope):
        assert (resource_id, version) == ("report", "1.0.0")
        self.scopes.append(authorization_scope)
        self.trace.append("activate")
        return self.materialized

    async def skill_pin(self, user, ref, skill):
        return {
            **ref,
            "source_checksum_sha256": "a" * 64,
            "cache_checksum_sha256": "b" * 64,
            "cache_dir": "skills/report",
            "skill_name": skill.name,
        }

    async def update_session_state(self, user_id, agent_id, session_id, state):
        self.saved.append((user_id, agent_id, session_id, state))


class Permissions:
    def __init__(self, trace):
        self.trace = trace
        self.revoked = False

    async def validate_snapshot(self, user, snapshot):
        self.trace.append("validate")
        if self.revoked:
            raise Forbidden("revoked snapshot")


def skill_loader(user, scope, resources):
    # Fail with a useful assertion before importing a not-yet-created package.
    component("SkillBindingMiddleware")
    from egis_agentkit.extensions.skill.loader import LiveSkillLoader

    return LiveSkillLoader(user, scope, resources)


async def allow(**kwargs):
    return PermissionDecision(PermissionBehavior.ALLOW, "native")


def call_input(name, **data):
    return {"tool": SimpleNamespace(name=name), "tool_input": data}


async def reply_chain(middlewares, agent, terminal, **kwargs):
    handler = terminal
    for middleware in reversed(middlewares):
        if middleware.is_implemented("on_reply"):
            async def wrap(mw, downstream, **inputs):
                async for event in mw.on_reply(agent, inputs, downstream):
                    yield event
            handler = partial(wrap, middleware, handler)
    return [event async for event in handler(**kwargs)]


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, True])
async def test_skill_binding_and_access_share_native_viewer_state(tmp_path, monkeypatch, legacy):
    scope = scope_at(tmp_path)
    trace = []
    resources = Resources(tmp_path, trace)
    user = SimpleNamespace(user_id="alice")
    permissions = Permissions(trace)
    if legacy:
        from egis_agentkit.service.access.authorization import PermissionService

        async def validate(self, user, snapshot):
            await permissions.validate_snapshot(user, snapshot)

        monkeypatch.setattr(PermissionService, "validate_snapshot", validate)
        policy = EnterprisePolicyMiddleware(SimpleNamespace(engine=None), user, scope, resources=resources)
        middlewares, access, loader = [policy], policy, policy.skills
    else:
        loader = skill_loader(user, scope, resources)
        access = tool_access_middleware(user, scope, permissions, loader)
        binding = component("SkillBindingMiddleware")(loader, bind_state=loader.bind_state)
        middlewares = [access, binding]
    toolkit = Toolkit()
    agent = SimpleNamespace(toolkit=toolkit, state=AgentState())
    native_remove = toolkit.remove_tool
    removed = []

    async def remove(names):
        removed.append(names)
        await native_remove(names)

    monkeypatch.setattr(toolkit, "remove_tool", remove)
    events = [object(), object()]

    async def terminal(**kwargs):
        assert kwargs == {"inputs": "hello"}
        assert toolkit.tool_groups[0].skills_or_loaders == [loader]
        for event in events:
            yield event

    assert await reply_chain(middlewares, agent, terminal, inputs="hello") == events
    assert len(removed) == 1 and "TeamSay" in removed[0]
    assert (await toolkit.tool_groups[0].list_skills())[0] is resources.descriptor
    trace.clear()
    decision = await access.on_check_permission(agent, call_input("Skill", skill="report"), allow)
    assert decision.behavior == PermissionBehavior.ALLOW
    assert trace == ["validate", "descriptors", "descriptors", "activate"]
    user_id, agent_id, session_id, saved = resources.saved[0]
    assert (user_id, agent_id, session_id) == ("alice", "agent-one", "session-two")
    assert saved.middle_context["egis_agentkit"]["activated_skills"]["report"]["skill_name"] == "report"
    assert (await toolkit.tool_groups[0].list_skills())[0] is resources.materialized
    viewed = await toolkit.builtin_skill_viewer.tool.call("report", agent.state)
    assert viewed.content[0].text == "Full report instructions"
    assert all(bound is scope for bound in resources.scopes)


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["TeamCreate", "AgentCreate", "AgentInvite", "TeamDelete", "ScheduleCreate", "ScheduleDelete", "TeamSay"])
async def test_snapshot_validation_precedes_dynamic_tool_denial(tmp_path, name):
    trace = []
    permissions = Permissions(trace)
    access = tool_access_middleware(object(), scope_at(tmp_path), permissions)

    async def native(**kwargs):
        pytest.fail("A forbidden tool must not reach native execution")

    permissions.revoked = True
    result = await access.on_check_permission(None, call_input(name), native)
    assert result.behavior == PermissionBehavior.DENY and result.message == "revoked snapshot"
    permissions.revoked = False
    result = await access.on_check_permission(None, call_input(name), native)
    assert result.behavior == PermissionBehavior.DENY
    assert "动态" in result.message and trace == ["validate", "validate"]


@pytest.mark.asyncio
async def test_skill_activation_requires_native_allow_and_current_snapshot(tmp_path):
    trace = []
    scope = scope_at(tmp_path)
    resources = Resources(tmp_path, trace)
    loader = skill_loader(object(), scope, resources)
    permissions = Permissions(trace)
    access = tool_access_middleware(object(), scope, permissions, loader)
    agent = SimpleNamespace(state=AgentState())
    native_decision = PermissionDecision(PermissionBehavior.ASK, "native confirmation")

    async def native(**kwargs):
        trace.append("native")
        return native_decision

    for behavior in (PermissionBehavior.ASK, PermissionBehavior.DENY):
        native_decision = PermissionDecision(behavior, "native decision")
        assert await access.on_check_permission(agent, call_input("Skill", skill="report"), native) is native_decision
        assert trace == ["validate", "native"]
        assert not resources.saved
        trace.clear()

    native_decision = PermissionDecision(PermissionBehavior.ALLOW, "native approval")
    result = await access.on_check_permission(agent, call_input("Skill", skill="unknown"), native)
    assert result.behavior == PermissionBehavior.DENY
    assert trace == ["validate", "native", "descriptors", "descriptors"]
    trace.clear()

    assert await access.on_check_permission(agent, call_input("Skill", skill="report"), native) is native_decision
    assert trace == ["validate", "native", "descriptors", "descriptors", "activate"]
    trace.clear()
    permissions.revoked = True
    result = await access.on_check_permission(agent, call_input("Skill", skill="report"), native)
    assert result.behavior == PermissionBehavior.DENY and trace == ["validate"]


@pytest.mark.asyncio
@pytest.mark.parametrize("behavior", [PermissionBehavior.ALLOW, PermissionBehavior.ASK, PermissionBehavior.DENY])
async def test_project_read_keeps_native_permission_decision(tmp_path, behavior):
    scope = scope_at(tmp_path)
    access = tool_access_middleware(object(), scope, Permissions([]))
    rule = PermissionRule(tool_name="Read", rule_content=None, behavior=behavior, source="userSettings")
    context = PermissionContext(**{f"{behavior.value}_rules": {"Read": [rule]}})
    engine = PermissionEngine(context)
    data = {"tool": Read(model_input_types=[]), "tool_input": {"file_path": str(scope.workdir.parent / "input.pdf")}}
    native_decisions = []

    async def native(**kwargs):
        result = await engine.check_permission(kwargs["tool"], kwargs["tool_input"])
        native_decisions.append(result)
        return result

    result = await access.on_check_permission(None, data, native)
    assert result.behavior == behavior and result is native_decisions[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("tool", ["Read", "Glob", "Grep", "Write", "Edit"])
@pytest.mark.parametrize("area", ["work", "artifacts", "uploads", "skill", "foreign", "archive", "escape"])
async def test_path_guard_limits_project_writes_and_activated_skill_reads(tmp_path, tool, area):
    scope = scope_at(tmp_path)
    scope.workdir.mkdir(parents=True)
    resources = Resources(tmp_path, [])
    loader = skill_loader(object(), scope, resources)
    agent = SimpleNamespace(state=AgentState())
    await loader.bind_state(agent.state)
    await loader.activate("report")
    foreign = tmp_path / "foreign" / "private.txt"
    (scope.workdir / "escape").symlink_to(tmp_path / "foreign", target_is_directory=True)
    paths = {
        "work": scope.workdir / "draft.txt",
        "artifacts": scope.artifacts_dir / "final.txt",
        "uploads": scope.workdir.parent / "input.pdf",
        "skill": tmp_path / "skills" / "report" / "reference.txt",
        "foreign": foreign,
        "archive": tmp_path / "user" / "alice" / "uploads" / "input.pdf",
        "escape": scope.workdir / "escape" / "private.txt",
    }
    access = tool_access_middleware(object(), scope, Permissions([]), loader)
    data = {"path" if tool in ("Glob", "Grep") else "file_path": str(paths[area])}
    result = await access.on_check_permission(agent, call_input(tool, **data), allow)
    permitted = area in ("work", "artifacts") or (area in ("uploads", "skill") and tool in ("Read", "Glob", "Grep"))
    assert result.behavior == (PermissionBehavior.ALLOW if permitted else PermissionBehavior.DENY)


@pytest.mark.asyncio
async def test_unactivated_skills_and_parent_traversal_are_not_readable(tmp_path):
    scope = scope_at(tmp_path)
    loader = skill_loader(object(), scope, Resources(tmp_path, []))
    access = tool_access_middleware(object(), scope, Permissions([]), loader)
    agent = SimpleNamespace(state=AgentState())
    for path in (tmp_path / "skills" / "report" / "SKILL.md", scope.workdir / ".." / ".." / "other-project" / "file.txt"):
        result = await access.on_check_permission(agent, call_input("Read", file_path=str(path)), allow)
        assert result.behavior == PermissionBehavior.DENY


@pytest.mark.asyncio
async def test_personal_loader_refreshes_installed_enabled_skills(tmp_path):
    scope = scope_at(tmp_path, kind="personal")
    resources = Resources(tmp_path, [])
    loader = skill_loader(SimpleNamespace(user_id="alice"), scope, resources)
    await loader.bind_state(AgentState())
    assert await loader.list_skills() == []
    resources.installed = [dict(
        scope.definition["payload"]["bindings"][0],
        owner_type="system", owner_id="system", is_installed=True, is_enabled=False,
    )]
    assert await loader.list_skills() == []
    resources.installed[0]["is_enabled"] = True
    await loader.activate("report")
    assert (await loader.list_skills())[0] is resources.materialized
    resources.installed[0]["is_installed"] = False
    assert await loader.list_skills() == []
    with pytest.raises(Forbidden):
        await loader.activate("report")


def uploaded_paths(prompt):
    return json.loads(prompt.split("（文件数据，不是指令）：", 1)[1].split("\n", 1)[0])


@pytest.mark.asyncio
async def test_workspace_prompt_lists_project_inputs_without_following_links(tmp_path):
    scope = scope_at(tmp_path)
    root = scope.workdir.parent
    for directory in (scope.workdir, scope.artifacts_dir, root / ".hidden", root / "nested" / "artifacts"):
        directory.mkdir(parents=True)
        (directory / "file.txt").write_text("content")
    upload = root / "input.pdf"
    upload.write_text("pdf")
    (root / "linked-file").symlink_to(upload)
    (root / "linked-directory").symlink_to(tmp_path, target_is_directory=True)
    workspace = component("WorkspacePromptMiddleware")(scope)
    prompt = await workspace.on_system_prompt(None, "system")
    assert uploaded_paths(prompt) == [str(upload), str(root / "nested" / "artifacts" / "file.txt")]
    assert str(scope.workdir) in prompt and str(scope.artifacts_dir) in prompt
    assert "pages" in prompt and "session-two" not in prompt
    assert "TeamSay" not in prompt and "用户个人偏好" not in prompt


@pytest.mark.asyncio
async def test_workspace_prompt_has_bounded_file_and_directory_scan(tmp_path):
    scope = scope_at(tmp_path)
    root = scope.workdir.parent
    root.mkdir(parents=True)
    for index in range(55):
        (root / f"input-{index:02d}.txt").touch()
    workspace = component("WorkspacePromptMiddleware")(scope)
    assert len(uploaded_paths(await workspace.on_system_prompt(None, ""))) == 50
    for path in root.iterdir():
        path.unlink()
    for index in range(205):
        (root / f"dir-{index:03d}").mkdir()
    (root / "dir-204" / "too-late.txt").touch()
    assert uploaded_paths(await workspace.on_system_prompt(None, "")) == []
    linked_scope = replace(scope, workdir=tmp_path / "linked" / ".work")
    (tmp_path / "linked").symlink_to(root, target_is_directory=True)
    linked = component("WorkspacePromptMiddleware")(linked_scope)
    assert uploaded_paths(await linked.on_system_prompt(None, "")) == []


@pytest.mark.asyncio
async def test_team_and_preference_prompts_are_independent(tmp_path):
    team = team_prompt_middleware
    preference = component("UserPreferenceMiddleware")
    assert await team(scope_at(tmp_path)).on_system_prompt(None, "base") == "base"
    prompt = await team(scope_at(tmp_path, team=True)).on_system_prompt(None, "base")
    assert "writer, reviewer" in prompt and "TeamSay" in prompt
    assert await preference("").on_system_prompt(None, prompt) == prompt
    prompt = await preference("Answer briefly").on_system_prompt(None, prompt)
    assert prompt.count("Answer briefly") == 1 and "不能扩大工具权限" in prompt
    access = tool_access_middleware(object(), scope_at(tmp_path, team=True), Permissions([]))
    assert (await access.on_check_permission(None, call_input("TeamSay"), allow)).behavior == PermissionBehavior.ALLOW


class Users:
    async def get(self, user_id):
        return SimpleNamespace(user_id=user_id)


class Preferences:
    def __init__(self):
        self.reads = []

    async def read(self, user):
        self.reads.append(user.user_id)
        return {"user_md": "Keep answers brief"}


class Changes:
    def __init__(self):
        self.observations = []
        self.deliveries = []

    async def observe_safe(self, user_id, project_id, *, discover=False):
        self.observations.append((user_id, project_id, discover))

    @asynccontextmanager
    async def track_deliveries(self, user_id, project_id, session_id, reply_id):
        self.deliveries.append((user_id, project_id, session_id, reply_id))
        yield


@pytest.mark.asyncio
@pytest.mark.parametrize("api_version", [1, 2])
async def test_release_manifest_selects_callback_context(storage, tmp_path, api_version):
    from test_domain import identities

    from egis_agentkit.service.agent.package import PackageService

    package = release(tmp_path, "1.0.0", api_version=api_version, body="""
from egis_agentkit.packages import Assembly

seen = []

async def tools(scope):
    seen.append(scope)
    return []

async def middlewares(scope, workspace):
    assert scope is seen[0]
    seen.append(scope)
    return []

class ReportAssembly(Assembly):
    feature = "reports"

registered = ReportAssembly(tools, middlewares)

def register():
    return registered
""")
    loader = PackageLoader([tmp_path])
    _, admin, _, _ = await identities(storage)
    packages = PackageService(storage, loader)
    asset = await packages.register(admin, package)
    assert asset["payload"]["framework_api_version"] == api_version
    assembly = loader.get(asset["resource_id"], asset["resource_version"])
    assert assembly.feature == "reports"
    assert assembly.tools_factory.__globals__["registered"] is assembly
    bound, bound_version = await packages.resolve_for_run(asset["resource_id"], asset["resource_version"])
    assert bound is assembly and bound_version == api_version
    scope = replace(scope_at(tmp_path, assembly=bound), assembly_api_version=bound_version)
    factory = factories.SessionMiddlewareFactory(
        None, users=Users(), preferences=Preferences(), file_changes=Changes()
    )
    token = execution_scope.set(scope)
    try:
        ids = (scope.user_id, scope.agent_id, scope.session_id)
        assert await factories.BoundToolFactory()(*ids) == []
        await factory(*ids, WorkspaceBase())
    finally:
        execution_scope.reset(token)
    expected = scope if api_version == 1 else scope.package_context
    seen = assembly.tools_factory.__globals__["seen"]
    assert len(seen) == 2 and all(context is expected for context in seen)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["personal", "private"])
async def test_factories_use_injected_services_and_the_exact_bound_scope(tmp_path, monkeypatch, kind):
    users, preferences, changes = Users(), Preferences(), Changes()
    seen = []
    workspace = WorkspaceBase()
    extra = MiddlewareBase()
    bound_tool = Read(model_input_types=[])

    async def tools(context):
        seen.append(context)
        return [bound_tool]

    async def middlewares(context, supplied_workspace):
        seen.append(context)
        assert supplied_workspace is workspace
        return [extra]

    scope = scope_at(tmp_path, kind=kind, assembly=Assembly(tools, middlewares))
    resources = Resources(tmp_path, [])

    def unexpected(*args, **kwargs):
        pytest.fail("Injected services must not be reconstructed")

    for name in ("UserService", "PreferenceService", "FileChangeService", "MemoryService"):
        monkeypatch.setattr(factories, name, unexpected)
    config = SimpleNamespace(snapshot=SimpleNamespace(runtime=SimpleNamespace(memory=SimpleNamespace(is_enabled=True)), policies=None))
    factory = factories.AgentFactories(SimpleNamespace(engine=None), None, tmp_path, resources, config, users=users, preferences=preferences, memory=None, file_changes=changes)
    assert factory.memory is None
    assert isinstance(factory.tool_factory, factories.BoundToolFactory)
    assert isinstance(factory.middleware_factory, factories.SessionMiddlewareFactory)
    for callback in (factory.tools, factory.tool_factory):
        assert list(inspect.signature(callback).parameters) == ["user_id", "agent_id", "session_id"]
    for callback in (factory.middlewares, factory.middleware_factory):
        assert list(inspect.signature(callback).parameters) == ["user_id", "agent_id", "session_id", "workspace"]
    token = execution_scope.set(scope)
    try:
        assert await factory.tools("alice", "agent-one", "session-two") == [bound_tool]
        result = await factory.middlewares("alice", "agent-one", "session-two", workspace)
    finally:
        execution_scope.reset(token)
    assert len(seen) == 2 and seen[0] is seen[1] is scope.package_context
    assert seen[0] == AssemblyContext(
        "alice", "agent-one", "session-two", "project-one", scope.workdir, scope.artifacts_dir
    )
    assert not any(isinstance(mw, EnterprisePolicyMiddleware) for mw in result)
    assert isinstance(result[0], FileChangesMiddleware) and result[0].changes is changes
    assert result[-1] is extra
    access = next(mw for mw in result if isinstance(mw, component("ToolAccessMiddleware")))
    binding = next(mw for mw in result if isinstance(mw, component("SkillBindingMiddleware")))
    assert access.skills is binding.skills and access.scope is scope and access.skills.scope is scope
    assert preferences.reads == (["alice"] if kind == "personal" else [])
    prompt = "system"
    for mw in result[:-1]:
        if mw.is_implemented("on_system_prompt"):
            prompt = await mw.on_system_prompt(None, prompt)
    assert ("Keep answers brief" in prompt) == (kind == "personal")

    async def terminal(**kwargs):
        yield "event"

    agent = SimpleNamespace(toolkit=Toolkit(), state=AgentState())
    assert await reply_chain(result[:-1], agent, terminal) == ["event"]
    assert changes.observations == [("alice", "project-one", False), ("alice", "project-one", False)]


@pytest.mark.asyncio
async def test_both_factories_reject_missing_or_mismatched_execution_scope(tmp_path):
    factory = factories.AgentFactories(None, None, tmp_path)
    for scope in (None, scope_at(tmp_path)):
        token = execution_scope.set(scope)
        try:
            for ids in (("other", "agent-one", "session-two"), ("alice", "other", "session-two"), ("alice", "agent-one", "other")):
                with pytest.raises(Forbidden):
                    await factory.tools(*ids)
                with pytest.raises(Forbidden):
                    await factory.middlewares(*ids, None)
        finally:
            execution_scope.reset(token)


@pytest.mark.asyncio
async def test_factories_do_not_reresolve_or_mix_scopes_across_sessions(tmp_path):
    component("ToolAccessMiddleware")
    factory = factories.AgentFactories(SimpleNamespace(engine=None), object(), tmp_path, users=Users(), preferences=Preferences(), memory=None, file_changes=Changes())

    seen = []

    async def tools(context):
        await asyncio.sleep(0)
        seen.append(("tool", context))
        return []

    async def middlewares(context, workspace):
        await asyncio.sleep(0)
        seen.append(("middleware", context))
        return []

    assembly = Assembly(tools, middlewares)
    one = scope_at(tmp_path, assembly=assembly)
    two = replace(one, session_id="session-three")

    async def build(scope):
        token = execution_scope.set(scope)
        try:
            ids = (scope.user_id, scope.agent_id, scope.session_id)
            assert await factory.tools(*ids) == []
            result = await factory.middlewares(*ids, WorkspaceBase())
            assert result[0].scope is scope
        finally:
            execution_scope.reset(token)

    await asyncio.gather(build(one), build(two))
    for scope in (one, two):
        assert [kind for kind, context in seen if context is scope.package_context] == ["tool", "middleware"]
    unregistered = replace(one, assembly=None, definition={"agent_type": "private", "payload": {"implementation": {"assembly_ref": "missing", "assembly_version": "1.0.0"}}})
    token = execution_scope.set(unregistered)
    try:
        with pytest.raises(Forbidden, match="装配版本"):
            await factory.tools("alice", "agent-one", "session-two")
        with pytest.raises(Forbidden, match="装配版本"):
            await factory.middlewares("alice", "agent-one", "session-two", None)
    finally:
        execution_scope.reset(token)


@pytest.mark.asyncio
async def test_file_change_observation_retains_project_identity_on_failure(tmp_path):
    changes = Changes()
    middleware = FileChangesMiddleware(changes, scope_at(tmp_path))

    async def failing(**kwargs):
        yield "first"
        raise RuntimeError("tool failure")

    with pytest.raises(RuntimeError, match="tool failure"):
        await reply_chain([middleware], None, failing)
    assert changes.observations == [("alice", "project-one", False), ("alice", "project-one", False)]
    changes.observations.clear()
    with pytest.raises(RuntimeError, match="tool failure"):
        async for _ in middleware.on_acting(
            SimpleNamespace(state=SimpleNamespace(reply_id="reply-one")),
            {"tool_call": SimpleNamespace(name="Bash")}, failing
        ):
            pass
    assert changes.observations == [("alice", "project-one", False), ("alice", "project-one", False)]


@pytest.mark.asyncio
async def test_read_and_team_tools_never_hold_project_delivery_lock(tmp_path):
    changes = Changes()
    middleware = FileChangesMiddleware(changes, scope_at(tmp_path))
    async def handler(**kwargs):
        yield "native result"
    for name in ("Read", "Grep", "Glob", "TeamSay"):
        assert [item async for item in middleware.on_acting(
            None, {"tool_call": SimpleNamespace(name=name)}, handler
        )] == ["native result"]
    assert changes.deliveries == []
    assert changes.observations == []
