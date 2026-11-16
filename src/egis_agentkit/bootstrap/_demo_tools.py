"""登记示例工具部署，实现由独立示例包提供。"""

from egis_agentkit_examples.report_tool import CreateDemoReport


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
