# 首次环境初始化

这组脚本用于企业已经提供的 **NAS、Nacos、Redis、Kafka、PostgreSQL**，初始化应用需要的目录、配置、Topic 和表。服务安装、数据库/账号创建、网络、NAS 挂载和平台权限由企业运维提供。

脚本不启动基础设施，不创建演示用户、专家或项目，不执行旧库升级。命令从 `egis-agentkit-framework` 目录执行。

| 文件 | 执行内容 |
| --- | --- |
| [nas.sh](nas.sh) | 检查 NAS 已挂载，创建用户根目录和技能归档目录，验证文件写入、读回 |
| [pg.sql](pg.sql) | 在空的目标 schema 中创建 29 张表及约束、索引，包括 Session 项目生成列 |
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
- `SKILL_STORAGE_ROOT` 保存技能 ZIP 和待处理上传文件，通常是 `NAS_ROOT/skill-archives`。
- 两个业务根目录均须位于该挂载点内；新目录权限由 `NAS_DIR_MODE` 决定。
- 已存在的 `user/` 不能是符号链接；持久根、用户根和技能根均执行读写探针。
- 不递归修改已有权限，不预建具体用户或项目；文件探针执行后会清理。

所有 API/技能导入节点与 OpenSandbox Docker 宿主机必须将同一份 NAS 挂载到相同绝对路径。Nacos 的 `workspace.runtime.root` 是唯一持久根目录，值与 `NAS_ROOT` 一致；沙箱直接挂载当前项目目录，无需复制项目文件。OpenSandbox 服务的 `storage.allowed_host_paths` 必须包含该根目录。原生 `data/skills/sessions` 运行目录由 Workspace 初始化流程创建，不在这里预置。

## 3 PostgreSQL

使用有 DDL 权限的应用数据库账号连接目标库执行；DBA 代执行时应先切换到应用角色，或按平台流程授予应用账号对应表和序列权限：

```bash
psql -X -v ON_ERROR_STOP=1 -f scripts/init/pg.sql
```

连接参数来自标准 `PGHOST/PGPORT/PGDATABASE/PGUSER` 及平台认证设置。SQL 为当前企业 cluster 形态：原生 6 表、AgentKit 18 表、PG 消息总线 5 表，共 29 表；包含当前模型的约束、索引与 `sessions.project_id` 生成列。

本文件只用于首次空 schema：`BEGIN/COMMIT` 保证整批执行，同名表已存在会报错并回滚，不覆盖、不删除原数据。它不创建数据库或角色，不插入默认用户和示例资源；平台默认工具由应用启动流程登记。

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
| `workspace.runtime.root` | 唯一的 NAS 根目录，与 `NAS_ROOT` 一致；API 与 Docker 宿主机使用相同路径 |
| `skill.storage.root` | `SKILL_STORAGE_ROOT` |
| `workspace.runtime.opensandbox` | 平台提供的服务地址、镜像与 API 密钥环境变量引用 |
| `client.postgres.connection.urlEnv` | 应用运行时数据库连接串的环境变量名称，默认 `AGENTKIT_DATABASE_URL` |
| `client.kafka.connection.deploymentMode` | 企业跨进程运行设为 `cluster` |
| `client.kafka.connection.bootstrapServers/topicPrefix` | Kafka 地址、`KAFKA_TOPIC_PREFIX` |
| `client.kafka.topics.partitions/replicationFactor` | 与创建 Topic 时相同的分区和副本数 |
| `model.catalog/selection` | 实际模型地址、模型标识、凭据环境变量和选择配置 |
| `user.identity` | 公司 issuer、audience、RS256 和 JWKS 地址 |

配置正文不放实际密码或令牌。地址只填 Nacos origin，不带 `/nacos` 路径。脚本直接读取环境变量中的 `NACOS_SERVER_URL/NACOS_NAMESPACE_ID/NACOS_GROUP`；Data ID 固定为 `agentkit.yaml`，默认 Group 为 `EGIS_AGENTKIT`。

需要经 Nacos 3 console 发布时另设 `NACOS_CONSOLE_URL`；读取配置使用 server 接口，发布权限单独按平台认证方式提供。脚本不创建 namespace，不执行用户名密码登录或自动续 token。

首次不存在时才发布；配置相同则保留，不同则拒绝覆盖。仅明确的配置不存在码 `20004` 当作首次创建，网络、403 等其他错误直接失败。发布后读回比较，输出只包含结果和版本，不打印配置正文或密钥。

同一个 namespace/group/Data ID 的首次初始化必须由平台安排**单个任务串行执行**。当前客户端的“先查再发布”不是分布式条件创建，不能让两个持有不同 YAML 的初始化任务并发运行。

执行顺序为 **NAS → PG → Kafka → Redis 检查 → Nacos**。全部成功后，由应用部署流程注入实际运行密钥并启动服务；本组脚本不安装和启动基础设施，也不执行旧环境迁移。
