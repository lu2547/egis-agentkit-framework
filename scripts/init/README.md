# 首次环境初始化

这组脚本用于企业已经提供的 **NAS、Nacos、Redis、Kafka、PostgreSQL**，初始化应用需要的目录、配置、Topic 和表。服务安装、数据库/账号创建、网络、NAS 挂载和平台权限由企业运维提供。

脚本不启动基础设施，不创建演示用户、专家或项目，不执行旧库升级。命令从 `egis-agentkit-framework` 目录执行。

| 文件 | 执行内容 |
| --- | --- |
| [nas.sh](nas.sh) | 检查 NAS 已挂载，创建用户根目录、系统技能目录和技能上传暂存目录，验证文件写入、读回 |
| [pg.sql](pg.sql) | 在空的目标 schema 中创建 32 张表及约束、索引，包括 Session 项目生成列 |
| [kafka.sh](kafka.sh) | 创建 `<前缀>.triggers`、`<前缀>.broadcast`，核对分区数和副本数 |
| [redis.sh](redis.sh) | 验证地址、认证、TLS 和逻辑库；当前框架没有需要预置的 Redis 业务 Key |
| [nacos.py](nacos.py) | 校验并首次发布 `agentkit.yaml`，然后读回核对 |
| [init.env.example](init.env.example) | 初始化参数名称与格式；没有实际密钥 |

## 1 准备参数

执行环境需要 Bash、Python 3、`mountpoint`、`psql`、`redis-cli` 和 Kafka 的 `kafka-topics.sh`。Nacos 脚本使用已安装的 Framework Python 依赖，运行时选用与应用相同的 Python 环境。

把参数文件放在仓库外的受保护目录，填写企业提供的真实地址和路径：

```bash
install -d -m 700 "$HOME/.config/agentkit"
cp -n scripts/init/init.env.example "$HOME/.config/agentkit/init.env"
chmod 600 "$HOME/.config/agentkit/init.env"
# 编辑 init.env 后导出参数；平台也可以直接注入同名环境变量。
set -a
source "$HOME/.config/agentkit/init.env"
set +a
```

密码/token 使用平台密钥注入：PG 使用 `PGPASSWORD` 或 `PGPASSFILE`，Redis 使用 `REDISCLI_AUTH`，Nacos 按平台规则使用 `NACOS_ACCESS_TOKEN` 或成对的身份头变量。Kafka 的 SASL/TLS 信息放在平台提供的受保护 `client.properties`，脚本只传文件路径。

PG 的库、目标 schema 和账号必须预先存在，账号具备该 schema 的建表权限；示例目标为 `agentscope/public`。自定义 schema 时由 DBA 为应用账号配置对应 `search_path`，初始化脚本和应用连接应使用相同规则。Nacos namespace 使用平台提供的 **ID**，不使用显示名称。

## 2 NAS

以实际 API 服务账号在已经挂好 NAS 的运行节点执行：

```bash
bash scripts/init/nas.sh
```

- `NAS_MOUNTPOINT` 是实际挂载点。未挂载时直接失败，不在本地盘降级创建目录。
- `NAS_ROOT` 是应用持久根目录；脚本建立其 `user/` 子目录。
- `SKILL_STORAGE_ROOT` 暂存待处理的技能 ZIP，通常是 `NAS_ROOT/skill-uploads`。正式的系统技能保存在 `NAS_ROOT/common/skills`，个人技能保存在各用户的 `skills` 子目录。
- 两个业务根目录均须位于该挂载点内；新目录权限由 `NAS_DIR_MODE` 决定。
- 已存在的 `user/` 不能是符号链接；持久根、用户根和技能根均执行读写探针。
- 不递归修改已有权限，不预建具体用户或项目；文件探针执行后会清理。

所有 API/技能导入节点与沙箱执行宿主机必须将同一份 NAS 挂载到相同绝对路径。Nacos 的 `workspace.runtime.root` 是唯一持久根目录，值与 `NAS_ROOT` 一致；沙箱直接挂载当前项目目录，无需复制项目文件。使用 OpenSandbox 开发适配器时，服务端的 `storage.allowed_host_paths` 必须包含该根目录。用户 NAS 目录按 `user/{uid}/{skills,session,projects}` 组织，由 Workspace 初始化补齐；初始化脚本不预建具体用户。多模态卸载文件放在当前项目的 `.work/data/`，`skills/session` 下按执行绑定分区并挂入沙箱，NAS `session` 对应 SDK `sessions`。项目创建时建立项目基础目录，会话创建时安全补齐缺失目录。

## 3 PostgreSQL

使用有 DDL 权限的应用数据库账号连接目标库执行；DBA 代执行时应先切换到应用角色，或按平台流程授予应用账号对应表和序列权限：

```bash
psql -X -v ON_ERROR_STOP=1 -f scripts/init/pg.sql
```

连接参数来自标准 `PGHOST/PGPORT/PGDATABASE/PGUSER` 及平台认证设置。SQL 为当前企业 cluster 形态：原生 6 表、AgentKit 21 表、PG 消息总线 5 表，共 32 表；包含当前模型的约束、索引与 `sessions.project_id` 生成列。

本文件只用于首次空 schema：`BEGIN/COMMIT` 保证整批执行，同名表已存在会报错并回滚，不覆盖、不删除原数据。它不创建数据库或角色，不插入默认用户和示例资源；平台默认工具由应用启动流程登记。

已有部署先停止旧版本服务，再通过 App 的 `upgrade-db --database <目标库>` 预览，追加 `--apply` 执行。技能升级会在同一事务内迁移已有版本和共享指针，保留语义版本、授权和 NAS 路径。更早版本的授权结构会先升级；无法确认发布顺序或归属时，升级回滚并保留原记录。

`ak_skill_summary` 保存技能归属、首次上传人和 `owner_version`、`shared_version`。两列引用 `ak_skill_detail` 的数字发布序号；明细里的 `resource_version` 保存技能包版本，用于定位 NAS 文件。经审批发布的系统技能（含虚拟用户提交）存入 `NAS_ROOT/common/skills/<skill_id>/<resource_version>/`，个人技能存入 `NAS_ROOT/user/<user_id>/skills/<skill_id>/<resource_version>/`。

完整表清单见 [数据库表清单](../../../egis-agentkit-app/docs/数据库表清单.md)。外部系统上传接口见 [Skill 接入指南](../../../egis-agentkit-app/docs/Skill接入指南.md)。

## 4 Kafka

将 `KAFKA_TOPICS_BIN` 设置为 Kafka 客户端程序的实际路径，或将其加入 PATH。需要平台认证文件时设置 `KAFKA_CLIENT_PROPERTIES`：

```bash
bash scripts/init/kafka.sh
```

只建立两个业务 Topic：

```text
<KAFKA_TOPIC_PREFIX>.triggers    会话唤醒
<KAFKA_TOPIC_PREFIX>.broadcast   节点广播
```

`KAFKA_TOPIC_PARTITIONS`、`KAFKA_TOPIC_REPLICATION_FACTOR` 由平台确定，副本数不得超过实际可用 broker。脚本可重复执行；已有 Topic 参数不符会失败，不自动扩分区或改副本。

Topic 前缀、分区、副本数必须与 Nacos 对应配置一致。Kafka 运行时当前标准 App 未通过 Nacos 装配 SASL/TLS 参数；脚本可以使用平台认证文件创建 Topic，但这不代表应用运行连接已完成相同认证接入。

## 5 Redis

需要核对平台提供的 Redis 时执行：

```bash
bash scripts/init/redis.sh
```

脚本使用 `REDIS_HOST/REDIS_PORT/REDIS_USER/REDIS_DB`，TLS 由 `REDIS_TLS` 和可选 `REDIS_CA_FILE` 指定。密码仅通过 `REDISCLI_AUTH` 传递。

**当前框架运行不依赖 Redis，没有 Redis 队列、表或业务 Key 需要初始化。** 因此本脚本只发 `PING` 检查连接，不预造 Key，不清库，不修改 ACL；本次初始化不能据此推断应用已经使用 Redis。

## 6 Nacos

从[完整 YAML](../../examples/nacos/agentkit.yaml)准备企业环境配置：

```bash
cp -n examples/nacos/agentkit.yaml "$HOME/.config/agentkit/agentkit.yaml"
chmod 600 "$HOME/.config/agentkit/agentkit.yaml"
# 编辑下面对应项后执行；python 应来自已安装 Framework 的应用环境。
python scripts/init/nacos.py --file "$HOME/.config/agentkit/agentkit.yaml"
```

至少对齐以下内容，路径均省略 `agentkit.`：

| YAML 字段 | 初始化参数或平台提供内容 |
| --- | --- |
| `revision` | 首次配置版本，模板为 1 |
| `workspace.runtime.root` | 唯一的 NAS 根目录，与 `NAS_ROOT` 一致；`workdir` 自动派生为该根目录下的 `user/{uid}`，不单独配置 |
| `skill.storage.root` | `SKILL_STORAGE_ROOT` |
| `project.upload.maxBytes` | 项目单文件上传上限，模板为 50 MiB（52,428,800 字节） |
| `workspace.runtime.mode` | 固定为 `pks` |
| `workspace.runtime.sandbox` | 沙箱服务地址、镜像、凭据引用及执行参数；公司 Client 由接入方注入 |
| `client.postgres.connection` | PostgreSQL 的 mode、host、port、database、username、TLS 和取密配置，见下节 |
| `logging.level/format/directory/accessLog` | 应用日志级别、格式、文件目录和 HTTP 访问日志开关。默认 INFO、JSON、仅 stdout、开启访问日志 |
| `logging.textPattern/textDateFormat` | 文本日志的单行模板和 UTC 日期格式，使用 `format: text` 时生效 |
| `client.tracing` | Phoenix OTLP HTTP 端点、项目、采样及导出限制；需要时设 enabled 为 true |
| `client.kafka.connection.deploymentMode` | 企业跨进程运行设为 `cluster` |
| `client.kafka.connection.bootstrapServers/topicPrefix` | Kafka 地址、`KAFKA_TOPIC_PREFIX` |
| `client.kafka.topics.partitions/replicationFactor` | 与创建 Topic 时相同的分区和副本数 |
| `model.catalog/selection` | 实际模型地址、模型标识、凭据环境变量和选择配置 |
| `user.identity` | 公司 issuer、audience、RS256 和 JWKS 地址 |

数据库 `key` 模式按约定从 Nacos `client.postgres.connection.password` 读取密码，因此必须限制此配置的读取、发布及导出权限；模板的空密码必须填写后才能启动。`cyberark` 模式删除 password，填写 `cyberark.endpoint/appId/safe/objectName`，通过 HTTPS CCP REST `AIMWebService/api/Accounts` 获取响应 Content；按公司要求配置 CA 和成对的客户端证书/私钥。每次新建物理数据库连接前取密，池中已连接的会话正常复用，不按每条 SQL 取密；取密失败拒绝新建连接。应用不再读取完整连接串环境变量。

数据库普通连接参数、取密、日志与 Tracing 配置变更需要重启应用。模型及沙箱等其他凭据仍通过其配置项引用部署环境变量。地址只填 Nacos origin，不带 `/nacos` 路径。脚本直接读取环境变量中的 `NACOS_SERVER_URL/NACOS_NAMESPACE_ID/NACOS_GROUP`；Data ID 固定为 `agentkit.yaml`，默认 Group 为 `EGIS_AGENTKIT`。

Phoenix 自建部署可使用 `http://phoenix.company.internal:6006/v1/traces`；这是 API 进程可访问的 OTLP HTTP 地址，不能只填 UI 根地址，也不要填容器内部才可达的主机名。打开 `agentkit.client.tracing.enabled` 后，Framework 管理导出生命周期，原生 TracingMiddleware 记录回复、模型及工具调用。`headers` 可填写平台要求的鉴权头，按密码同样保护；开发 Phoenix 未开启认证可保持空对象。

运行主项目 `projectName` 默认 `egis-agentkit`：会话运行根关联 session/user/project/agent 标识，包含锁等待、会话及项目准备、沙箱创建/接入、网关、资源和工具装配、原生 Agent/模型/工具、记忆作业、产物汇总、状态保存等实际阶段。以 `session.id` 查询同一会话；根上的 Link 可关联触发它的 HTTP 请求。`httpProjectName` 默认 `egis-agentkit-http`，普通 API 和 FastAPI 内部 Span 放入这个独立项目；设为 null 则不导出这些 HTTP Span。已有历史 Trace 不移动。标准 OTLP 在 Span 结束后导出，尚未结束的父阶段不会提前伪造成已完成；排查执行中状态可结合结构化日志和已完成子阶段。

App CLI 统一配置 Framework、App、AgentScope 和 Uvicorn 的日志。日志记录关键节点、耗时、标识和安全的错误描述。Nacos 的 `agentkit.logging.directory` 可设为服务进程可写的绝对路径，例如 `/var/log/agentkit`。运行在容器内时，路径以应用容器为准，可挂载日志卷持久化。服务启动时创建目录，并用 UTF-8 追加写入 `agentkit-<主机名>-<PID>.log`。设置为 `null` 时仅输出 stdout。

需要经 Nacos 3 console 发布时另设 `NACOS_CONSOLE_URL`；读取配置使用 server 接口，发布权限单独按平台认证方式提供。脚本不创建 namespace，不执行用户名密码登录或自动续 token。

首次不存在时才发布；配置相同则保留，不同则拒绝覆盖。仅明确的配置不存在码 `20004` 当作首次创建，网络、403 等其他错误直接失败。发布后读回比较，输出只包含结果和版本，不打印配置正文或密钥。

同一个 namespace/group/Data ID 的首次初始化必须由平台安排**单个任务串行执行**。当前客户端的“先查再发布”不是分布式条件创建，不能让两个持有不同 YAML 的初始化任务并发运行。

## 7 Nginx 上传限制

在转发上传请求的 `server` 或 `location` 中设置请求体上限。

```nginx
client_max_body_size 50m;
```

Nginx 按整个 `multipart/form-data` 请求体计大小。要支持恰好 50 MiB 的文件，在上传路由生效的 `location` 中设为 `client_max_body_size 51m;`，给表单封装留出空间。修改后执行 `nginx -t`，通过后重新加载配置。 [Nginx 指令文档](https://nginx.org/en/docs/http/ngx_http_core_module.html#client_max_body_size)

执行顺序为 **NAS → PG → Kafka → Redis 检查 → Nacos**。全部成功后，由应用部署流程注入实际运行密钥并启动服务；本组脚本不安装和启动基础设施，也不执行旧环境迁移。
