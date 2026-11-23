# 1.0.2 发布步骤

本目录保存本次 SQL 优化及 AgentScope 2.0.12 升级的部署增量。首次安装继续使用 [scripts/init](../../init/README.md)。

产品 App、Framework 和前端包版本统一为 **1.0.2**；上游 AgentScope 保持 **2.0.12**。

## 发布行为基线

按用户确认的本机 Nacos revision 39 对齐以下功能设置，发布示例不再另设一套行为默认值：

| 配置 | 1.0.2 发布值 |
| --- | --- |
| `session.permissions.defaultMode` | `bypass` |
| `client.kafka.connection.deploymentMode` | `cluster` |
| `project.memory.isEnabled` | `true` |
| `client.tracing.enabled` | `true` |
| `project.upload.maxBytes` | `20971520`（20 MiB） |
| `workspace.runtime.sandbox.maxSkillBytes` | `52428799` |

镜像和知识库地址由部署环境选择。数据库、身份服务、模型、NAS、字体和 OTLP 的实际地址、凭据及绝对路径使用环境值，不把本机 localhost、macOS 路径或密钥复制到生产。Nacos 是运行时配置来源，示例文件用于核对字段和上述行为基线，不覆盖现有环境配置。

App 安装依赖包含 Framework 的 `cluster,memory` extra，使已开启的项目记忆具备对应运行依赖。前端删除系统减少动态效果条件下全局及组件的动画/过渡关闭规则；仅拖拽调整面板宽度时临时关闭过渡，结束拖拽后恢复。

## 变更清单

| 文件/步骤 | 内容 |
| --- | --- |
| [01_query_indexes.sql](01_query_indexes.sql) | 并发创建 3 个索引，校验有效性和定义后并发移除 2 个冗余索引；无业务数据迁移 |
| [02_verify_indexes.sql](02_verify_indexes.sql) | 只读核验 3 个新索引有效、2 个旧索引不存在，并输出实际定义 |
| [nacos.md](nacos.md) | 本次精确的配置修改、删除项及重启说明；SQL 优化本身没有新增配置项 |
| [nacos-diff.md](nacos-diff.md) | 与正式标签 1.0.1 逐字段比较的完整示例配置差异，包含此前累计的配置调整 |
| 依赖及沙箱镜像 | API 与沙箱网关使用本次验证的 AgentScope 2.0.12 源码制品，包含会话 SSE 重放分页修复 |

**本轮 SQL 优化**不新增表、列、Kafka Topic 或 NAS 目录。本目录的索引脚本面向已经具备当前企业表结构的环境，尤其是 `sessions.project_id` 生成列、项目文件变更表和资源授权表。正式标签 1.0.1 与当前结构之间还累计过技能汇总/明细、虚拟用户、候选专家及技能导入审核等变化；本轮索引脚本不冒充这些历史结构的数据迁移。新项目首次部署直接用当前 `scripts/init/pg.sql`；已有环境缺少基线时先核对实际结构，再准备对应的数据迁移步骤，不能对有数据的 schema 重跑首次建表 SQL。

## 1. 数据库索引

新增定义：

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_sessions_main_user_updated
    ON sessions (user_id, updated_at DESC) INCLUDE (id)
    WHERE source = 'user' AND project_id IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_ak_project_changes_page
    ON ak_project_file_changes (project_id, change_id DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_ak_permission_active_resource_principal
    ON ak_resource_permission (resource_type, resource_id, principal_type, principal_id)
    WHERE revoked_at IS NULL;
```

随后移除 `ix_sessions_updated_at`、`ix_ak_project_file_changes_project_id`。保留直接授权专用部分索引，后者更小、用于目录查询；没有机械地给每个 JOIN 列重复建索引。

数据库账号需有目标表的索引维护权限。通过标准 PG 连接参数选择实际库；密码使用平台注入的 `PGPASSWORD` 或受保护的 `PGPASSFILE`，不要写进版本脚本。显式指定与应用相同的 **单一 schema**，下例以 `public` 为例：

```bash
# 命令从 egis-agentkit-framework 根目录执行。
# PGHOST / PGPORT / PGUSER 及认证由部署环境提供。
PGOPTIONS='-c search_path=public' psql -X -v ON_ERROR_STOP=1 \
  --dbname='<实际数据库名>' \
  --file=scripts/releases/1.0.2/01_query_indexes.sql

PGOPTIONS='-c search_path=public' psql -X -v ON_ERROR_STOP=1 \
  --dbname='<实际数据库名>' \
  --file=scripts/releases/1.0.2/02_verify_indexes.sql
```

每条语句自动提交，**不要加 `BEGIN/COMMIT`、`psql -1/--single-transaction` 或外层迁移事务**。`CONCURRENTLY` 适用于已有大表，不将整个建索引过程放在阻塞业务写入的事务中；仍会消耗 IO，并可能等待已有事务结束。DDL 锁等待最多 5 秒，等待超时直接停止本次执行。

脚本使用维护互斥锁，一次只运行一个任务。先验证旧索引身份，再建新索引；三个新索引的表、键顺序、INCLUDE、谓词和有效状态全部通过后才删除旧索引。失败后以新的连接重新执行；已经正确存在的索引会保留。若并发创建失败留下 `INVALID` 索引，脚本会停止，由 DBA 核实没有仍在执行的创建任务后清理该无效索引，再重试，不自动删除同名异常对象。

`02_verify_indexes.sql` 预期输出 5 行，`passed` 全为 `true`。保存输出作为发布记录。首次安装的 `scripts/init/pg.sql` 以及 Python 显式建表流程已包含这些新索引；应用启动不执行这组 DDL。

## 2. Nacos、依赖及服务切换

1. 运行上述索引脚本并核验；索引维护可在旧服务仍提供读写时提前完成。
2. 暂停新请求，等待已有 Chat、专家任务和待发队列处理完，再停止 API/Worker。避免旧自研引导链路与新原生队列混跑。
3. 按 [nacos.md](nacos.md)修改本环境的 `agentkit.yaml` 并增加配置 `revision`；记录原 revision 和实际修改。
4. 部署配套的 App/Framework 和本次 AgentScope 2.0.12 制品；按需重建沙箱镜像并填写本环境镜像标识。已有沙箱内网关不会因 API 依赖升级而自动换版本，等待其运行结束后回收、重建，保留 NAS 数据。
5. 启动 API/Worker，确认 `/health/ready` 就绪、Nacos revision 正确；检查会话列表、邀请专家、队列和引导及长回复重放。数据库性能用实际用户与接近生产的数据量复测，包含完整 HTTP 耗时。



沙箱镜像构建从 Framework 根目录执行，配置使用本环境导出的受保护文件：

```bash
PYTHONPATH=src python scripts/build_workspace_image.py \
  --config /path/to/agentkit.yaml \
  --agentscope-source ../../harness/agentscope
```

API 和镜像中的 SDK 均应包含同一份重放修复；仅安装同名但未包含修复的官方 2.0.12 包不满足本次发布要求。记录实际 wheel/源码提交及镜像摘要。

## 回退说明

索引不改变业务数据，通常可先保留新增索引再回退服务制品。若回退后的其他入口确实依赖被移除的全局/单列索引，可由 DBA 并发恢复：

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_sessions_updated_at ON sessions (updated_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_ak_project_file_changes_project_id
    ON ak_project_file_changes (project_id);
```

回退 API、Framework、SDK、镜像及 Nacos 时按同一发布窗口成套恢复；新旧引导队列没有自动互转脚本，切换前先排空。无需恢复或清空业务表，不运行 `cleanup-db`、`reset-skills` 或首次建表脚本。
