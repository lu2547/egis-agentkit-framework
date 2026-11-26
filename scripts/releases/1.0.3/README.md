# 1.0.3 发布与部署

发布日期：2026-11-23。功能和修复清单见 [Release Notes](release-notes.md)，配置核对见 [Nacos 说明](nacos.md)。App、Framework、Frontend 统一为 **1.0.3**，Python AgentScope 保持 **2.0.12**。Mock 为独立开发服务，包版本保持 0.1.0，以本次 Git 提交和发布标签确定代码。

## 数据库执行入口

首次安装只执行 [scripts/init/pg.sql](../../init/pg.sql)，配套步骤见 [初始化说明](../../init/README.md)。该文件包含当前完整结构，不能在已有业务数据的 schema 重跑。

从 1.0.2 升级时，先确认该环境已经执行 [1.0.2 索引脚本](../1.0.2/README.md#1-数据库索引)，再按顺序执行本目录 SQL。1.0.2 的并发索引脚本必须单独运行，不能套在本版的事务中。

| 顺序 | 文件 | 作用 |
| --- | --- | --- |
| 1 | [01_schedules.sql](01_schedules.sql) | 扩展 `schedules` 的项目关联、下次执行时间、软删除字段；创建 `ak_schedule_runs` 及调度/历史索引 |
| 2 | [02_personal_optional_tools.sql](02_personal_optional_tools.sql) | 个人助理资源选择的类型约束增加 `tool`；保留已有 Skill/MCP 选择，不默认勾选工具 |
| 3 | [nas-history.sql](nas-history.sql) | 创建 NAS 历史提交位置表、删除任务表和写入序列，可在既有 1.0.3 环境单独执行 |
| 4 | [04_share_review_workflow.sql](04_share_review_workflow.sql) | 增加撤回申请原审批关联、提交内容快照和待审撤回唯一索引；不修改现有授权或审核状态 |
| 5 | [03_verify_schema.sql](03_verify_schema.sql) | 只读输出 38 项结构、约束和索引检查，`passed` 必须全部为 `true` |

在发布窗口停止 API/Worker 后执行 DDL。四个 DDL 脚本自带事务，可重复执行，锁等待超时为 5 秒；失败时该脚本的事务回滚。普通建索引和 ALTER TABLE 会持锁，因此不要在长任务仍在运行时直接执行。校验脚本不修改业务数据。

以下命令从 Framework 根目录执行。使用与应用相同的数据库及**单一 schema**，不要照抄 `public` 代替实际部署值。`PGHOST/PGPORT/PGUSER` 和认证由平台提供，密码放受保护的 `PGPASSFILE` 或平台注入的环境变量，不写进脚本。

```bash
PGOPTIONS='-c search_path=public' psql -X -v ON_ERROR_STOP=1 \
  --dbname='<实际数据库名>' --file=scripts/releases/1.0.3/01_schedules.sql

PGOPTIONS='-c search_path=public' psql -X -v ON_ERROR_STOP=1 \
  --dbname='<实际数据库名>' --file=scripts/releases/1.0.3/02_personal_optional_tools.sql

PGOPTIONS='-c search_path=public' psql -X -v ON_ERROR_STOP=1 \
  --dbname='<实际数据库名>' --file=scripts/releases/1.0.3/nas-history.sql

PGOPTIONS='-c search_path=public' psql -X -v ON_ERROR_STOP=1 \
  --dbname='<实际数据库名>' --file=scripts/releases/1.0.3/04_share_review_workflow.sql

PGOPTIONS='-c search_path=public' psql -X -v ON_ERROR_STOP=1 \
  --dbname='<实际数据库名>' --file=scripts/releases/1.0.3/03_verify_schema.sql
```

共享撤回在管理员通过前继续保留当前授权；所有者不能通过删除或停用已共享版本绕过审批。`04_share_review_workflow.sql` 为既有 1.0.3 环境的追加升级。旧申请的快照保持空值，详情明确标识为重建内容，历史 MCP 只能展示当前重建配置。新申请保存安全连接配置及修订号，技能包仍保存在版本文件存储中。

受管定时任务只读取带项目关联的任务；已有原生 `schedules` 行保持原状。Kafka 继续处理集群唤醒和广播，PG 继续保存队列、注册信息和租约。Redis Streams 保存会话事件回放。旧自研引导链路的待发消息应在切换前排空。

## 服务与制品切换

1. 暂停新请求，等待 Chat、专家任务和待发队列处理完，再停止所有 API/Worker。保存当前制品、Nacos revision 和数据库备份。
2. 执行上述数据库增量及校验，保存结果。数据库初始化和升级由部署流程负责，应用启动不自动建表。
3. 按 [nacos.md](nacos.md)核对配置和工具选择。保留 `agentkit.client.kafka` 的唤醒、消费组和广播配置，在 `agentkit.client.redis` 配置集群共用的回放库地址、逻辑库、密码环境变量和 `retentionSeconds`。保留公司实际的模型配置和 NAS 路径。
4. 使用 NAS HTTP 模式的环境先更新 Linux 文件服务 `serve-files`，确认共享凭据及 NAS 根目录。运行时从 `{workspace.runtime.root}/user/{uid}/session/history/{sid}/messages.jsonl` 读取原始消息。旧 PG 的 `messages` 表中已有历史的环境，按下节在停写窗口导出并核对。增量 SQL 仅创建元数据，运行时直接读取 NAS。
5. 部署 Framework/App/Frontend 1.0.3，API 和沙箱网关统一使用官方原版 AgentScope 2.0.12。
6. 启动 API/Worker，确认 `/health/ready` 的 `is_nacos_connected: true`、`restart_required: false` 以及正确的 `config_version`。
7. 完成发布抽查：新任务直接进入对话、队列编辑/排序/引导、专家邀请与移除、定时任务试跑及运行记录、工具勾选与撤销、知识库回答中的引用入口及右侧标签页。

应用 CLI 仅保留 `serve`、`serve-files`、`provision-admin --user-id <已登记用户ID>`。不要使用已删除的初始化、清理或重置命令；首次建表走 init SQL，升级走版本目录 SQL。

## 将旧会话原文复制到 NAS

所有 API/Worker 停止后，执行 `nas-history.sql`，用部署环境的 Python 从 Framework 根目录运行以下命令。导出工具读取相邻 App 的 `.env` 和本环境当前 Nacos 配置。使用其他环境文件时，每次调用均传入 `--env-file /实际路径/.env`。

```bash
python scripts/releases/1.0.3/export_history.py
python scripts/releases/1.0.3/export_history.py --apply
python scripts/releases/1.0.3/export_history.py
```

第一条只核对。核对结果中的 `source_sessions`、`source_messages` 统计能匹配原生会话归属的历史，`orphan_messages` 单列无法匹配会话的旧消息数。这些行保留在 PG。

应用复制时，工具检查 PG 中的活动会话租约，逐会话按原始创建时间和消息 ID 的顺序复制，并从 NAS 读回核对。已有内容完全一致的会话计入 `identical_sessions`。已提交的目标历史与源数据冲突时，工具退出并回滚本次事务。旧 PG 行始终保留。最后一次核对应显示 `pending_sessions` 和 `conflicting_sessions` 为 0，`identical_sessions` 等于 `source_sessions`，再恢复 API/Worker。

## AgentScope 与沙箱镜像

API 和沙箱网关使用官方原版 AgentScope 2.0.12，可安装官方发布包或从对应官方源码构建。Redis 会话事件按已保存的消息检查点裁剪，并使用 `client.redis.retentionSeconds` 设置流的生存时间。完整消息历史由 NAS 提供。

沙箱镜像从 Framework 根目录构建，配置文件使用当前环境导出的受保护文件：

```bash
PYTHONPATH=src python scripts/build_workspace_image.py \
  --config /path/to/agentkit.yaml \
  --agentscope-source ../../harness/agentscope
```

已有沙箱不会自动换网关依赖。停止其执行后回收、重建运行实例，保留 NAS 数据；公司使用自己的沙箱 Client/镜像流程时沿用公司流程。

## 发布验证记录

- PostgreSQL 17.9 隔离 schema 演练：真实 1.0.2 初始化结构 → 1.0.2 索引脚本 → 本版 01/02 各执行两遍，核对当时首次安装基线的 502 项列、约束、索引和序列。
- 1.0.2 的 5 项和本版的 38 项核验全部通过；已有原生计划和 Skill/MCP 选择保留，调度唯一约束、枚举、时间规则及外键检查通过。
- 本版迁移、初始化、调度存储、调度服务、时间规则及个人可选工具相关测试：30 passed。
- App/Framework 1.0.3 wheel 构建和元数据核验通过，两个 uv 锁文件检查通过；前端 1.0.3 类型检查及生产构建通过。Mock wheel 包含全部 5 篇知识库文档。
- NAS/Redis 切换另行验证首次安装的 35 张表与当前模型一致。增量 SQL 连续执行两次后，原有消息、总线记录、已提交历史长度和写入序列均保留。
上述数据库操作只作用于临时 schema，不涉及公司环境。

## 回退

先停止新调度和所有运行，再成套恢复前端、App、Framework、SDK/镜像和配置。新版写入的消息保存在 NAS。需要在旧版显示这些新增消息时，在停写窗口完成反向复制与逐会话核对。新增列、约束与执行记录默认保留，不通过 DROP 表或删业务数据实现回退。

原版 1.0.2 不支持本版受管定时任务：存在此类记录时，不可直接启动会读取原生 `schedules` 的旧调度器。需要原样回退数据库时，使用发布前备份和对应恢复窗口。原生队列与旧引导链路不自动互转，升级和回退前均须排空。
