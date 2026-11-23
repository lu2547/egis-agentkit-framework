"""Validate schedules and share management operations with the chat tools."""

import asyncio
import logging
from datetime import UTC, datetime
from pathlib import PurePosixPath
from uuid import uuid4

from agentscope.permission import PermissionMode

from egis_agentkit.errors import Conflict, Forbidden, InvalidRequest, NotFound

from ._rules import ScheduleSettings, cron_expression, next_occurrence, normalize_rule

logger = logging.getLogger(__name__)

_RUN_FIELDS = (
    "id",
    "schedule_id",
    "schedule_name",
    "project_id",
    "trigger_type",
    "status",
    "scheduled_for",
    "created_at",
    "started_at",
    "finished_at",
    "session_id",
    "reply_id",
    "error_code",
    "error_message",
    "archived_at",
    "summary",
)
_FILE_AREAS = {"work": "", "artifacts": "../artifacts/", "uploads": "../"}


def public_run(row):
    return {key: row[key] for key in _RUN_FIELDS}


def _schedule_values(settings, now, *, require_future=True):
    settings = normalize_rule(settings, now)
    following = next_occurrence(settings, now)
    if require_future and settings.enabled and following is None:
        raise InvalidRequest("此时间规则没有未来的执行时间")
    values = settings.model_dump(mode="json")
    # Store the computed expression separately from the editable rule.
    expression = cron_expression(settings.rule)
    return {**values, "cron_expression": expression, "next_run_at": following}


class ScheduleService:
    def __init__(self, storage, users, sessions, projects, models, personal):
        self.storage = storage.schedules
        self.users = users
        self.sessions = sessions
        self.projects = projects
        self.models = models
        self.personal = personal

    async def list(self, user):
        return await self.storage.list(user.user_id)

    async def get(self, user, schedule_id):
        task = await self.storage.get(user.user_id, schedule_id)
        if task is None:
            raise NotFound("定时任务不存在")
        return task

    async def find(self, user, project_id, task):
        """Resolve a tool argument within the current project."""
        tasks = [
            row for row in await self.list(user) if row["project_id"] == project_id
        ]
        by_id = next((row for row in tasks if row["id"] == task), None)
        if by_id is not None:
            return by_id
        matches = [row for row in tasks if row["name"] == task]
        if len(matches) > 1:
            raise Conflict("当前项目有同名的定时任务，请提供任务 ID")
        if not matches:
            raise NotFound("当前项目没有此定时任务")
        return matches[0]

    async def check_files(self, user, project_id, entries):
        files = self.projects.files
        directories = {}
        for entry in entries:
            area, _, path = entry.partition("/")
            if (
                area not in _FILE_AREAS
                or not path
                or "\\" in path
                or any(part in ("", ".", "..") for part in path.split("/"))
            ):
                raise InvalidRequest("项目文件路径须包含区域和相对路径")
            parent = str(PurePosixPath(path).parent)
            key = area, "" if parent == "." else parent
            if key not in directories:
                items = await asyncio.to_thread(
                    files.list, user.user_id, project_id, *key
                )
                directories[key] = {
                    item["path"] for item in items if not item["is_directory"]
                }
            if path not in directories[key]:
                raise NotFound(f"项目文件不存在或不可访问：{entry}")

    async def _check_settings(self, user, settings):
        if settings.permission_mode == "explore":
            self.sessions.validate_permission_mode(PermissionMode.EXPLORE)
        if settings.model_key is not None:
            config = self.models.config.snapshot.model.selection
            if not config.allow_override:
                raise Forbidden("此部署不允许覆盖默认模型")
            if not any(
                item["key"] == settings.model_key
                for item in self.models.list()["items"]
            ):
                raise InvalidRequest("此模型未开放")
        await self.check_files(user, settings.project_id, settings.files)

    async def create(self, user, settings):
        now = datetime.now(UTC)
        values = _schedule_values(settings, now)
        async with self.projects.guard(user, settings.project_id):
            await self._check_settings(user, settings)
            definition = await self.personal.get_or_add_definition(user)
            record = await self.storage.create(
                {
                    **values,
                    "id": uuid4().hex,
                    "user_id": user.user_id,
                    "agent_id": definition["id"],
                    "created_at": now,
                }
            )
        logger.info(
            "Scheduled task created", extra={"context": {"schedule_id": record["id"]}}
        )
        return record

    async def update(self, user, schedule_id, settings):
        current = await self.get(user, schedule_id)
        now = datetime.now(UTC)
        if isinstance(settings, bool):
            if settings == current["enabled"]:
                return current
            if settings:
                previous = ScheduleSettings.from_record(current).model_copy(
                    update={"enabled": True}
                )
                following = _schedule_values(previous, now)["next_run_at"]
            else:
                following = None
            changes = {"enabled": settings, "next_run_at": following}
            project_id = current["project_id"]
        else:
            changes = _schedule_values(settings, now, require_future=False)
            project_id = settings.project_id
        async with self.projects.guard(user, project_id):
            if not isinstance(settings, bool):
                await self._check_settings(user, settings)
                old = ScheduleSettings.from_record(current).model_dump(mode="json")
                timing = ("rule", "timezone", "starts_on", "ends_on", "enabled")
                if all(changes[key] == old[key] for key in timing):
                    del changes["next_run_at"]
                elif settings.enabled and changes["next_run_at"] is None:
                    raise InvalidRequest("此时间规则没有未来的执行时间")
            updated = await self.storage.update(
                user.user_id,
                schedule_id,
                changes,
                expected_updated_at=current["updated_at"],
            )
        if updated is None:
            raise NotFound("定时任务不存在")
        logger.info(
            "Scheduled task updated", extra={"context": {"schedule_id": schedule_id}}
        )
        return updated

    async def delete(self, user, ids, *, project_id=None):
        count = await self.storage.delete(
            user.user_id, ids, datetime.now(UTC), project_id=project_id
        )
        if count != len(set(ids)):
            raise NotFound("定时任务不存在")
        logger.info("Scheduled tasks deleted", extra={"context": {"schedule_ids": ids}})

    async def run(self, user, schedule_id, *, project_id=None):
        task = await self.get(user, schedule_id)
        if project_id is not None and task["project_id"] != project_id:
            raise NotFound("当前项目没有此定时任务")
        async with self.projects.guard(user, task["project_id"]):
            record = await self.storage.enqueue_test(
                user.user_id,
                schedule_id,
                datetime.now(UTC),
                project_id=task["project_id"],
            )
        if record is None:
            raise NotFound("定时任务不存在")
        logger.info(
            "Scheduled task queued", extra={"context": {"run_id": record["id"]}}
        )
        return public_run(record)

    async def list_runs(self, user, **filters):
        return [
            public_run(row)
            for row in await self.storage.list_runs(user.user_id, **filters)
        ]

    async def archive_run(self, user, run_id, archived):
        result = await self.storage.archive(
            user.user_id, run_id, archived, datetime.now(UTC)
        )
        if result is None:
            if await self.storage.get_run(user.user_id, run_id) is not None:
                raise Conflict("运行结束后才能归档")
            raise NotFound("运行记录不存在")
        return public_run(result)

    async def delete_run(self, user, run_id):
        if not await self.storage.delete_run(user.user_id, run_id, datetime.now(UTC)):
            if await self.storage.get_run(user.user_id, run_id) is not None:
                raise Conflict("运行结束后才能删除")
            raise NotFound("运行记录不存在")

    async def prepare_run(self, run):
        user = await self.users.get(run["user_id"])
        project_id = run["project_id"]
        snapshot = run["input_snapshot"]
        entries = snapshot.get("files", [])
        async with self.projects.guard(user, project_id):
            await self.check_files(user, project_id, entries)
        session = await self._find_session(user, run)
        if session is None:
            mode = (
                PermissionMode.EXPLORE
                if snapshot.get("permission_mode") == "explore"
                else PermissionMode.DONT_ASK
            )
            name_limit = self.sessions.config.snapshot.session.naming.create_max_length
            view = await self.sessions.add_session(
                user,
                run["agent_id"],
                "private",
                name=run["schedule_name"][:name_limit],
                project_id=project_id,
                model_key=snapshot.get("model_key"),
                permission_mode=mode,
                schedule_id=run["schedule_id"],
            )
            session = await self.sessions.get_session_record(user, view["session_id"])
        instruction = snapshot["description"]
        if entries:
            paths = []
            for entry in entries:
                area, _, path = entry.partition("/")
                paths.append(f"- {_FILE_AREAS[area]}{path}")
            instruction += "\n\n项目文件（相对于工作目录）\n" + "\n".join(paths)
        return session, instruction

    async def _find_session(self, user, run):
        snapshot = run["input_snapshot"]
        if run["trigger_type"] == "test" or not snapshot.get("stateful"):
            return None
        previous = await self.storage.last_stateful_run(
            run["schedule_id"], run["project_id"]
        )
        if not previous or previous["status"] != "succeeded":
            return None
        fields = ("model_key", "permission_mode", "agent_id")
        if any(
            previous["input_snapshot"].get(key) != snapshot.get(key) for key in fields
        ):
            return None
        try:
            return await self.sessions.get_session_record(user, previous["session_id"])
        except NotFound:
            return None
