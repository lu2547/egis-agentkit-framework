"""演示如何登记已部署的报告工具；执行实现独立于框架业务层。"""

from egis_agentkit.bootstrap._demo_tools import deploy_demo_report_tools
from egis_agentkit_examples.report_tool import CreateDemoReport

__all__ = ["CreateDemoReport", "deploy_demo_report_tools"]
