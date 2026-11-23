"""Optional personal-assistant tools for creating, trying and deleting schedules."""

from zoneinfo import ZoneInfo

from agentscope.message import TextBlock, ToolResultState
from agentscope.permission import PermissionBehavior, PermissionDecision, PermissionRule
from agentscope.tool import ToolBase, ToolChunk
from pydantic import ValidationError

from egis_agentkit.core.agent._runtime import DEFAULT_TIMEZONE
from egis_agentkit.errors import AgentKitError
from egis_agentkit.service.schedule._rules import ScheduleInput, ScheduleSettings


def _result(text, *, state=ToolResultState.SUCCESS, **metadata):
    return ToolChunk(content=[TextBlock(text=text)], state=state, metadata=metadata)


class _ScheduleTool(ToolBase):
    is_concurrency_safe = False
    is_read_only = False

    def __init__(self, service, user, project_id):
        super().__init__()
        self.service = service
        self.user = user
        self.project_id = project_id

    async def check_permissions(self, tool_input, context):
        return PermissionDecision(
            PermissionBehavior.ASK,
            message=self.label,
            bypass_immune=True,
        )


class CreateScheduledTask(_ScheduleTool):
    name = "CreateScheduledTask"
    label = "创建定时任务"
    description = (
        "在当前项目创建定时任务。description 是到时执行的任务指令，"
        f"时区默认为 {DEFAULT_TIMEZONE}。每周规则的 weekdays 中 0 表示周一，6 表示周日。"
        "引用项目文件时使用 uploads/、work/ 或 artifacts/ 开头的相对路径。"
    )
    input_schema = ScheduleInput.model_json_schema()

    async def call(self, **arguments):
        try:
            fields = ScheduleInput.model_validate(arguments)
            settings = ScheduleSettings(
                project_id=self.project_id, **fields.model_dump()
            )
            task = await self.service.create(self.user, settings)
        except ValidationError as error:
            messages = [
                f"{'.'.join(map(str, issue['loc']))} {issue['msg']}".strip()
                for issue in error.errors(include_url=False, include_input=False)
            ]
            return _result(
                "任务参数无效。 " + "；".join(messages), state=ToolResultState.ERROR
            )
        except AgentKitError as error:
            return _result(str(error), state=ToolResultState.ERROR)
        following = task["next_run_at"]
        subtitle = (
            f"下次运行 {following.astimezone(ZoneInfo(task['timezone'])).isoformat()}。"
            if following
            else "任务已暂停。"
        )
        return _result(
            f"已创建「{task['name']}」。{subtitle} 任务 ID {task['id']}",
            schedule_id=task["id"],
        )


class _ExistingScheduleTool(_ScheduleTool):
    input_schema = {
        "type": "object",
        "properties": {
            "task": {
                "type": "string",
                "minLength": 1,
                "maxLength": 80,
                "description": "当前项目中的任务 ID 或完整任务名。",
            }
        },
        "required": ["task"],
        "additionalProperties": False,
    }


class RunScheduledTask(_ExistingScheduleTool):
    name = "RunScheduledTask"
    label = "运行定时任务"
    description = (
        "在当前项目立即试跑一个定时任务。试跑有独立的运行记录，后续触发时间照常。"
    )

    async def call(self, *, task):
        try:
            schedule = await self.service.find(self.user, self.project_id, task.strip())
            run = await self.service.run(
                self.user, schedule["id"], project_id=self.project_id
            )
        except AgentKitError as error:
            return _result(str(error), state=ToolResultState.ERROR)
        return _result(
            f"「{schedule['name']}」已加入试跑队列。运行 ID {run['id']}",
            schedule_id=schedule["id"],
            run_id=run["id"],
        )


class DeleteScheduledTask(_ExistingScheduleTool):
    name = "DeleteScheduledTask"
    label = "删除定时任务"
    description = "在当前项目删除一个定时任务，停止之后的触发并保留运行记录。"

    async def call(self, *, task):
        try:
            schedule = await self.service.find(self.user, self.project_id, task.strip())
            await self.service.delete(
                self.user, [schedule["id"]], project_id=self.project_id
            )
        except AgentKitError as error:
            return _result(str(error), state=ToolResultState.ERROR)
        return _result(f"已删除「{schedule['name']}」。", schedule_id=schedule["id"])


SCHEDULE_TOOLS = (CreateScheduledTask, RunScheduledTask, DeleteScheduledTask)
SCHEDULE_TOOL_NAMES = frozenset(tool.name for tool in SCHEDULE_TOOLS)


def require_schedule_tool_confirmation(context):
    """Use native per-tool rules so full access still shows the confirmation card."""
    for name in SCHEDULE_TOOL_NAMES:
        rules = context.ask_rules.setdefault(name, [])
        if not any(rule.source == "egis-scheduled-tasks" for rule in rules):
            rules.append(
                PermissionRule(
                    tool_name=name,
                    rule_content=None,
                    behavior=PermissionBehavior.ASK,
                    source="egis-scheduled-tasks",
                )
            )
