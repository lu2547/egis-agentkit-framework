# 1.0.3 发布与部署

发布日期：2026-11-23。功能和修复清单见 [Release Notes](release-notes.md)，配置核对见 [Nacos 说明](nacos.md)。App、Framework、Frontend 统一为 **1.0.3**，Python AgentScope 保持 **2.0.12**。Mock 为独立开发服务，包版本保持 0.1.0，以本次 Git 提交和发布标签确定代码。

## 数据库执行入口

首次安装只执行 [scripts/init/pg.sql](../../init/pg.sql)，配套步骤见 [初始化说明](../../init/README.md)。该文件包含当前完整结构，不能在已有业务数据的 schema 重跑。

从 1.0.2 升级时，先确认该环境已经执行 [1.0.2 索引脚本](../1.0.2/README.md#1-数据库索引)，再按顺序执行本目录 SQL。1.0.2 的并发索引脚本必须单独运行，不能套在本版的事务中。

| 顺序 | 文件 | 作用 |
| --- | --- | --- |
| 1 | [01_schedules.sql](01_schedules.sql) | 扩展 `schedules` 的项目关联、下次执行时间、软删除字段；创建 `ak_schedule_runs` 及调度/历史索引 |
| 2 | [02_personal_optional_tools.sql](02_personal_optional_tools.sql) | 个人助理资源选择的类型约束增加 `tool`；保留已有 Skill/MCP 选择，不默认勾选工具 |
| 3 | [03_verify_schema.sql](03_verify_schema.sql) | 只读输出 38 项结构、约束和索引检查，`passed` 必须全部为 `true` |

在发布窗口停止 API/Worker 后执行 DDL。前两个脚本自带事务，可重复执行，锁等待超时为 5 秒；失败时该脚本的事务回滚。普通建索引和 ALTER TABLE 会持锁，因此不要在长任务仍在运行时直接执行。校验脚本不修改业务数据。

以下命令从 Framework 根目录执行。使用与应用相同的数据库及**单一 schema**，不要照抄 `public` 代替实际部署值。`PGHOST/PGPORT/PGUSER` 和认证由平台提供，密码放受保护的 `PGPASSFILE` 或平台注入的环境变量，不写进脚本。

```bash
PGOPTIONS='-c search_path=public' psql -X -v ON_ERROR_STOP=1 \
  --dbname='<实际数据库名>' --file=scripts/releases/1.0.3/01_schedules.sql

PGOPTIONS='-c search_path=public' psql -X -v ON_ERROR_STOP=1 \
  --dbname='<实际数据库名>' --file=scripts/releases/1.0.3/02_personal_optional_tools.sql

PGOPTIONS='-c search_path=public' psql -X -v ON_ERROR_STOP=1 \
  --dbname='<实际数据库名>' --file=scripts/releases/1.0.3/03_verify_schema.sql
```

受管定时任务只读取带项目关联的任务；已有原生 `schedules` 行保持原状。消息队列和短期执行凭据复用现有 MessageBus 存储，不新增队列表或凭据表。旧自研引导链路已移除，本版不迁移旧待发消息，切换前先排空队列。

## 服务与制品切换

1. 暂停新请求，等待 Chat、专家任务和待发队列处理完，再停止所有 API/Worker。保存当前制品、Nacos revision 和数据库备份。
2. 执行上述数据库增量及校验，保存结果。数据库初始化和升级由部署流程负责，应用启动不自动建表。
3. 按 [nacos.md](nacos.md)核对配置和工具选择。保留公司实际地址、凭据引用、模型和 NAS 路径。
4. 部署 Framework/App/Frontend 1.0.3；API 和沙箱网关使用同一份 AgentScope 2.0.12 制品，包含下节的 SSE 重放补丁。
5. 启动 API/Worker，确认 `/health/ready` 的 `is_nacos_connected: true`、`restart_required: false` 以及正确的 `config_version`。
6. 完成发布抽查：新任务直接进入对话、队列编辑/排序/引导、专家邀请与移除、定时任务试跑及运行记录、工具勾选与撤销、知识库回答中的引用入口及右侧标签页。

应用 CLI 仅保留 `serve`、`serve-files`、`provision-admin --user-id <已登记用户ID>`。不要使用已删除的初始化、清理或重置命令；首次建表走 init SQL，升级走版本目录 SQL。

## AgentScope 与沙箱镜像

本地 SDK 已提交的重放修复为 `e719efa6`，修复长回复跨多页重放。补丁随本版归档：[agentscope-2.0.12-sse-replay.patch](agentscope-2.0.12-sse-replay.patch)，构建不依赖开发机未提交文件。

构建环境先准备上游 2.0.12 对应源码（提交 `28ab5ef6`），然后在 SDK 根目录执行：

```bash
git apply --check /path/to/agentscope-2.0.12-sse-replay.patch
git apply /path/to/agentscope-2.0.12-sse-replay.patch
```

已经包含该修复的内部制品无需重复应用。记录 SDK 源码提交、wheel 摘要和镜像摘要。仅使用没有补丁的同名官方 wheel 不包含本次长回复修复。

沙箱镜像从 Framework 根目录构建，配置文件使用当前环境导出的受保护文件：

```bash
PYTHONPATH=src python scripts/build_workspace_image.py \
  --config /path/to/agentkit.yaml \
  --agentscope-source ../../harness/agentscope
```

已有沙箱不会自动换网关依赖。停止其执行后回收、重建运行实例，保留 NAS 数据；公司使用自己的沙箱 Client/镜像流程时沿用公司流程。

## 发布验证记录

- PostgreSQL 17.9 隔离 schema 演练：真实 1.0.2 初始化结构 → 1.0.2 索引脚本 → 本版 01/02 各执行两遍；与当前首次安装的 502 项列、约束、索引和序列结构一致。
- 1.0.2 的 5 项和本版的 38 项核验全部通过；已有原生计划和 Skill/MCP 选择保留，调度唯一约束、枚举、时间规则及外键检查通过。
- 本版迁移、初始化、调度存储、调度服务、时间规则及个人可选工具相关测试：30 passed。
- App/Framework 1.0.3 wheel 构建和元数据核验通过，两个 uv 锁文件检查通过；前端 1.0.3 类型检查及生产构建通过。Mock wheel 包含全部 5 篇知识库文档。
- SDK 归档补丁在原版 2.0.12 源码上应用成功，结果与已提交的本地修复一致。上述数据库操作只作用于临时 schema，不涉及公司环境。

## 回退

先停止新调度和所有运行，再成套恢复前端、App、Framework、SDK/镜像和配置。新增列、约束与执行记录默认保留，不通过 DROP 表或删业务数据实现回退。

原版 1.0.2 不支持本版受管定时任务：存在此类记录时，不可直接启动会读取原生 `schedules` 的旧调度器。需要原样回退数据库时，使用发布前备份和对应恢复窗口。原生队列与旧引导链路不自动互转，升级和回退前均须排空。
