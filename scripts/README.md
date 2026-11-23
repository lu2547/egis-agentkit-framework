# 部署与维护脚本

从 `egis-agentkit-framework` 仓库根目录执行命令。

| 目录/文件 | 用途 |
| --- | --- |
| [init/](init/README.md) | 首次环境初始化，面向空 schema；已有数据库不要重复执行建表脚本 |
| [releases/](releases/README.md) | 按发布版本保存增量 SQL、执行顺序、Nacos 和其他人工变更说明 |
| `build_workspace_image.py` | 根据当前配置和 AgentScope 源码构建沙箱镜像 |
| `align_tool_deployments.py` | 预览或应用业务工具部署地址对齐 |
| `audit_sql.py`、`report_sql.py` | 在测试环境采集 SQL 链路并生成审计清单 |

已有环境发布从 [版本清单](releases/README.md)进入，不用最新的首次初始化 SQL 代替增量发布脚本。
