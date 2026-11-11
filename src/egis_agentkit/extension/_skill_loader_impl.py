"""实现 SkillLoaderBase，列出可用 Skill 并恢复已激活版本。"""

import asyncio
from copy import deepcopy
from pathlib import Path
from typing import Protocol

from agentscope.skill import Skill, SkillLoaderBase
from agentscope.state import AgentState
from pydantic import ValidationError

from egis_agentkit.errors import AgentKitError, Forbidden
from egis_agentkit.storage.model import SkillActivation, User


class SkillStateStorage(Protocol):
    async def update_session_state(
        self, user_id: str, agent_id: str, session_id: str, state: AgentState
    ) -> None: ...


class SkillResources(Protocol):
    @property
    def storage(self) -> SkillStateStorage: ...

    @property
    def workspace_root(self) -> Path: ...

    async def installed_assets(
        self, user: User, resource_type: str, *, enabled_only: bool
    ) -> dict[str, dict]: ...

    async def skill_descriptors(
        self, user: User, refs: list[dict], *, authorization_scope: object
    ) -> list[Skill]: ...

    async def materialize_skill(
        self, user: User, resource_id: str, version: str, *, authorization_scope: object
    ) -> Skill: ...

    async def skill_pin(self, user: User, ref: dict, skill: Skill) -> dict: ...

    async def restore_skill(
        self, user: User, pin: dict, *, authorization_scope: object
    ) -> Skill: ...


class LiveSkillLoader(SkillLoaderBase):
    """Personal installs are dynamic; expert candidates come only from bindings.

    Candidate summaries do not download archives. Only Skill activation pins
    content. Native Session state owns those pins across turns and workers.
    """

    def __init__(self, user: User, scope, resources: SkillResources):
        self.user, self.scope, self.resources = user, scope, resources
        self.loaded = {}
        self.pins = {}
        self.unavailable = {}
        self._state = None
        self._lock = asyncio.Lock()

    async def bind_state(self, state: AgentState) -> None:
        if self._state is state:
            return
        self.loaded.clear()
        self.unavailable.clear()
        self._state = state
        binding = state.middle_context.setdefault("egis_agentkit", {})
        pins = binding.setdefault("activated_skills", {})
        if not isinstance(pins, dict):
            raise Forbidden("会话技能激活记录格式无效")
        try:
            for rid, pin in pins.items():
                parsed = SkillActivation.model_validate(pin)
                if parsed.resource_type != "skill" or parsed.resource_id != rid:
                    raise ValueError
        except (ValidationError, ValueError, TypeError):
            raise Forbidden("会话技能激活记录格式无效") from None
        self.pins = pins
        # Resource authorization sees the same server-persisted activation pins.
        self.scope.binding["activated_skills"] = pins
        relocated = False
        for rid, pin in list(pins.items()):
            try:
                await self._eligible_pin(pin)
                skill = await self.resources.restore_skill(
                    self.user, pin, authorization_scope=self.scope
                )
                self.loaded[skill.name] = skill
                restored_dir = str(self.resources.workspace_root / pin["cache_dir"])
                if str(Path(skill.dir)) != restored_dir:
                    pins[rid] = await self.resources.skill_pin(
                        self.user,
                        SkillActivation.model_validate(pin).reference(),
                        skill,
                    )
                    relocated = True
            except AgentKitError as exc:
                # Revoked/disabled pins must not break unrelated conversation,
                # reappear as a new version, or retain a readable directory.
                self.unavailable[pin["skill_name"]] = str(exc)
        if relocated:
            await self._persist()

    async def _persist(self):
        await self.resources.storage.update_session_state(
            self.scope.user_id,
            self.scope.agent_id,
            self.scope.session_id,
            deepcopy(self._state),
        )

    async def _eligible_pin(self, pin):
        if self.scope.definition["agent_type"] != "personal":
            bindings = self.scope.definition["payload"].get("bindings", [])
            if not any(
                all(
                    ref.get(key) == pin.get(key)
                    for key in (
                        "resource_type",
                        "resource_id",
                        "resource_version",
                        "resource_view_type",
                    )
                )
                for ref in bindings
            ):
                raise Forbidden("已激活技能已不在当前智能体的绑定范围内")
        await self.resources.skill_descriptors(
            self.user,
            [SkillActivation.model_validate(pin).reference()],
            authorization_scope=self.scope,
        )

    async def refs(self):
        refs = self.scope.definition["payload"].get("bindings", [])
        if self.scope.definition["agent_type"] == "personal":
            installed = await self.resources.installations.assets(
                self.user, "skill", enabled_only=True
            )
            refs = [
                {
                    "resource_type": "skill",
                    "resource_id": r["resource_id"],
                    "resource_version": r["resource_version"],
                    "resource_view_type": (
                        "private"
                        if r["owner_type"] == "user"
                        and r["owner_id"] == self.user.user_id
                        else "system"
                        if r["owner_type"] == "system"
                        else "shared"
                    ),
                }
                for r in installed.values()
            ]
        result = []
        for ref in refs:
            if ref["resource_type"] != "skill":
                continue
            pin = self.pins.get(ref["resource_id"])
            # Even an unavailable old pin prevents silent switching to v2.
            selected = pin or ref
            try:
                if pin:
                    await self._eligible_pin(pin)
                else:
                    await self.resources.skill_descriptors(
                        self.user, [selected], authorization_scope=self.scope
                    )
            except AgentKitError:
                continue
            result.append(
                SkillActivation.model_validate(pin).reference() if pin else selected
            )
        return result

    async def list_skills(self) -> list[Skill]:
        result = []
        for ref in await self.refs():
            try:
                descriptors = await self.resources.skill_descriptors(
                    self.user, [ref], authorization_scope=self.scope
                )
            except AgentKitError:
                continue
            for skill in descriptors:
                if skill.name not in self.unavailable:
                    result.append(self.loaded.get(skill.name, skill))
        return result

    async def refresh_access(self) -> None:
        """Recheck current USE/enablement before every protected tool action."""
        for name in list(self.loaded):
            pin = next(p for p in self.pins.values() if p["skill_name"] == name)
            try:
                await self._eligible_pin(pin)
            except AgentKitError as exc:
                self.loaded.pop(name, None)
                self.unavailable[name] = str(exc)

    async def activate(self, name: str) -> None:
        async with self._lock:
            if name in self.unavailable:
                raise Forbidden(self.unavailable[name])
            for ref in await self.refs():
                descriptors = await self.resources.skill_descriptors(
                    self.user, [ref], authorization_scope=self.scope
                )
                if not any(skill.name == name for skill in descriptors):
                    continue
                if name in self.loaded:
                    return
                skill = await self.resources.materialize_skill(
                    self.user,
                    ref["resource_id"],
                    ref["resource_version"],
                    authorization_scope=self.scope,
                )
                pin = await self.resources.skill_pin(self.user, ref, skill)
                self.pins[ref["resource_id"]] = pin
                try:
                    # Persist before exposing instructions/files. The native
                    # session lock is held; concurrent activations use this lock.
                    await self._persist()
                except BaseException:
                    self.pins.pop(ref["resource_id"], None)
                    raise
                self.loaded[name] = skill
                return
            raise Forbidden("技能不在当前智能体的可用范围内")
