"""实现原生 ToolBase，将报告写入当前项目并交付。"""

import asyncio

from agentscope.message import TextBlock, ToolResultState
from agentscope.permission import PermissionBehavior, PermissionDecision
from agentscope.tool import ToolBase, ToolChunk


class CreateDemoReport(ToolBase):
    name = "CreateDemoReport"
    description = "根据标题和发现生成报告，并交付 report.md。"
    input_schema = {
        "type": "object",
        "properties": {
            "title": {"type": "string", "minLength": 1, "maxLength": 200},
            "findings": {
                "type": "array",
                "items": {"type": "string", "maxLength": 2000},
                "maxItems": 100,
            },
        },
        "required": ["title", "findings"],
        "additionalProperties": False,
    }
    is_concurrency_safe = False
    is_read_only = False

    def __init__(self, scope, *, files, version):
        super().__init__()
        self.scope = scope
        self.files = files
        self.version = version

    async def call(self, *, title: str, findings: list[str]) -> ToolChunk:
        if not title.strip() or len(title) > 200:
            raise ValueError("报告标题须有 1 至 200 个字符")
        if len(findings) > 100 or any(len(item) > 2000 for item in findings):
            raise ValueError("报告最多包含 100 条发现，每条最多 2000 个字符")
        clean_title = " ".join(title.splitlines())
        items = ["- " + " ".join(item.splitlines()) for item in findings]
        markdown = "\n".join(
            [
                f"# {clean_title}",
                "",
                f"工具版本：{self.version}",
                "",
                "## 发现",
                "",
                *items,
                "",
            ]
        )
        await asyncio.to_thread(self._deliver, markdown.encode())
        return ToolChunk(
            content=[TextBlock(text="已创建并交付报告：[report.md](report.md)")],
            state=ToolResultState.SUCCESS,
            metadata={"artifacts": [{"path": "report.md", "area": "artifacts"}]},
        )

    async def check_permissions(self, tool_input, context):
        return PermissionDecision(
            behavior=PermissionBehavior.ALLOW,
            message="仅写入当前项目的 report.md",
        )

    def _deliver(self, content: bytes) -> None:
        user_id, project_id = self.scope.user_id, self.scope.project_id
        self.files.write(user_id, project_id, "report.md", content, area="work")
        self.files.deliver(user_id, project_id, "report.md")


def deploy_demo_report_tools(registry, files):
    """把已随平台部署的两个报告工具版本登记进工具目录。"""
    for version in ("1.0.0", "1.1.0"):

        def create(scope, version=version):
            return CreateDemoReport(scope, files=files, version=version)

        registry.deploy(
            CreateDemoReport.name,
            version,
            create,
            description=CreateDemoReport.description,
            input_schema=CreateDemoReport.input_schema,
            dependency_only=True,
        )
