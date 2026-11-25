# 部署与维护脚本

从 `egis-agentkit-framework` 仓库根目录执行命令。

| 目录/文件 | 用途 |
| --- | --- |
| [init/](init/README.md) | 首次环境初始化，面向空 schema；已有数据库不要重复执行建表脚本 |
| [releases/](releases/README.md) | 按发布版本保存增量 SQL、执行顺序、Nacos 和其他人工变更说明 |
| `build_workspace_image.py` | 根据当前配置和 AgentScope 源码构建沙箱镜像 |
| `align_tool_deployments.py` | 预览或应用业务工具部署地址对齐 |
| `publish_shared_skills.py` | 显式预览或补发已批准个人技能及共享专家/专家团的固定技能依赖，不修改归属、授权或版本指针 |
| `audit_sql.py`、`report_sql.py` | 在测试环境采集 SQL 链路并生成审计清单 |

已有环境发布从 [版本清单](releases/README.md)进入，不用最新的首次初始化 SQL 代替增量发布脚本。

部署公共副本读取前，在每个保存个人源目录的 NAS 对应配置上运行
`python scripts/publish_shared_skills.py --app-dir ../egis-agentkit-app` 预览，核对后增加
`--apply --database <目标数据库>` 发布。缺失源文件、摘要冲突或不安全路径会使命令失败；
范围包括直接共享的技能，以及已批准专家/专家团历史版本绑定的个人技能，避免旧会话缺失固定依赖；未审核的私人版本不在补发范围内。
多个 NAS 保存不同用户目录时，用可重复的 `--skill-id <技能 ID>` 指定当前 NAS 的源技能，
分别发布后由部署侧合并公共副本。
可以修复后幂等重跑。部署侧先将完整 `common/skills` 同步至所有 NAS，再切换应用版本；
`user` 目录无需同步。此命令不会启动跨 NAS 复制，运行时也不会回退到作者目录。
