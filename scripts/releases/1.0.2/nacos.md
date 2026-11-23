# 1.0.2 Nacos 变更

配置 Data ID：`agentkit.yaml`，Group 和 namespace 使用当前环境已有值。SQL 优化没有新增 Nacos 参数。已经同步此前功能配置的环境，本次重点处理下表中的 SDK 2.0.12 配套项；如果环境仍使用正式标签 1.0.1 的配置，同时按下节及 [完整字段差异](nacos-diff.md)核对累计变化。只修改对应路径，不用示例 YAML 整体覆盖公司的现有配置。

| 配置路径 | 操作 | 内容 |
| --- | --- | --- |
| `agentkit.session.steering.wakeupRetrySeconds` | 删除 | 新队列使用 SDK 原生持久化和引导能力，不再使用自研唤醒重试字段；保留旧字段会被配置的未知字段校验拒绝 |
| `agentkit.workspace.runtime.sandbox.bootstrapCommands` | 修改安装 SDK 的那一项（如有） | 将无版本的 `agentscope` 安装命令改为 `uv pip install --python {gateway_python} --no-deps agentscope==2.0.12`；保留同数组的其他本环境命令 |
| `agentkit.workspace.runtime.sandbox.image` | 如重建镜像则修改 | 写入本次构建输出的真实镜像标识/摘要，不照抄示例裸 Python 镜像 |
| `agentkit.revision` | 增加 | 在本环境当前值上递增，不固定为某个开发环境的 revision |

本次发布的功能基线与本机 Nacos 一致：`session.permissions.defaultMode: bypass`、`client.kafka.connection.deploymentMode: cluster`、`project.memory.isEnabled: true`、`client.tracing.enabled: true`、`project.upload.maxBytes: 20971520`。镜像和知识库地址按环境设置，其他环境地址/凭据保持实际部署值。完整基线见 [发布说明](README.md#发布行为基线)。

引导配置保留如下三个限制项，值沿用本环境已有配置；以下为模板默认值：

```yaml
agentkit:
  session:
    steering:
      maxBatchSize: 20
      maxMessageBytes: 1048576
      maxTextLength: 32000
```

若使用预构建镜像，更新镜像中的网关 SDK，运行时安装步骤可能走快速跳过路径。公司自定义 bootstrap 若已有固定来源或内部 wheel，使用包含本次修复的对应 2.0.12 制品，保留其来源；不要用一条外网 pip 命令覆盖公司的安装流程。

## 从正式标签 1.0.1 配置升级时的累计调整

| 配置路径 | 操作与取值 |
| --- | --- |
| `agentkit.agent.personal.memory` → `agentkit.project.memory` | 移动 `isEnabled/topK`，删除旧节点。记忆现在按用户及项目隔离；Windows HTTP 文件模式保持关闭 |
| `agentkit.workspace.runtime.mode` | 从 `opensandbox` 改为 `pks`，表示当前通用沙箱装配配置，不代表强制使用 OpenSandbox 提供方 |
| `agentkit.workspace.runtime.opensandbox` → `agentkit.workspace.runtime.sandbox` | 将本环境原沙箱参数整体移到新节点，删除旧节点。删除新节点内的 `workdir`；它由 NAS root 和用户派生 |
| `agentkit.client.postgres.connection.urlEnv` | 删除；不再从完整 URL 环境变量读取连接配置 |
| `agentkit.client.postgres.connection` | 配置 `mode: key` 或 `cyberark` 及实际 `host/port/database/username`、连接超时和 TLS。key 模式填写 password；cyberark 模式删除 password、配置实际 CCP 参数。字段格式见 [初始化说明](../../init/README.md#6-nacos)；保持原实际数据库，不切库 |
| `agentkit.user.identity` | 删除 `signingSecretEnv/demoPasswordEnv/jwtLifetimeSeconds/jwtMinSigningSecretLength/demoMinPasswordLength`。保留本环境 issuer、audience、RS256/JWKS 和校验参数；签发配置属于独立 Mock/公司身份服务 |
| `agentkit.access.companyGroupId/companyGroupName` | 新增，模板为 `company` / `公司组`；与已有公司组标识对齐 |
| `agentkit.logging` | 新增 `level/format/textPattern/textDateFormat/directory/accessLog`。模板 INFO、json、仅 stdout；文本格式和可写日志目录按环境设置，重启生效 |
| `agentkit.client.tracing` | 新增 Phoenix OTLP 导出配置；本次发布基线为 `enabled: true`，与本机一致。填写 API 可达的真实 `/v1/traces` 地址和所需鉴权，不使用示例 localhost 代替公司的端点 |
| `agentkit.skill.catalog/connector.catalog/tool.catalog` | 新增分页及查询限制；默认每页 10、最多 100、查询长度 500、游标长度 2048 |
| `agentkit.agent.catalog.defaultPageSize` | 模板从 50 调为 10；按环境需求保留有效值 |
| `agentkit.admin.pagination` | 新增 `defaultPageSize: 20`、`maxPageSize: 100` |
| `agentkit.skill.avatar` | 新增 `enabled: true`、`modelKey: null`、`timeoutSeconds: 45`；使用现有模型选择，生成失败回退名称首字 |
| `agentkit.skill.import.approvalBatchLimit` | 新增，默认 50 |
| `agentkit.skill.storage.root` | 示例从 `skill-archives` 改为 `skill-uploads`，这是导入 ZIP 暂存目录；保留公司实际可写路径，不据示例自动移动或删除 NAS 文件 |
| `agentkit.project.upload.maxBytes` | 保持本机的 20 MiB（20971520），并核对 Nginx/网关的整个 multipart 请求体上限 |
| `agentkit.workspace.files` | 新增 `win/endpoint/tokenEnv/timeoutSeconds`。已挂 NAS 的 Linux 节点保持 `win: false`；原生 Windows Agent 用 `win: true` 并指向同一 NAS 的 Linux 文件服务 |
| `agentkit.tool.knowledgeBase` | 新增 `queryUrl: null`、`timeoutSeconds: 1800.0`。只有填入实际完整 SSE POST `/query` 地址才开放检索；无需使用时保持 null |


完整字段清单包含默认值与数组项变化；上表用于解释迁移方式。日志目录、取密、连接、Tracing 和 Workspace 等固定装配配置修改后统一重启服务。

发布前保存现有配置和 revision；提交修改后读回核对上述路径，再在同一窗口切换新服务并重启所有 API/Worker。`/health/ready` 中的 `config_version` 应等于发布的 revision，`is_nacos_connected` 为 `true`，`restart_required` 为 `false`。

本轮 SQL 优化不要求更换 JWT issuer/audience、实际 PostgreSQL 目标、Kafka Topic、模型或 NAS 路径；累计调整只改变对应配置的表达和能力选项。脚本不自动向 Nacos 发布配置。首次初始化的 `scripts/init/nacos.py` 只处理首次创建，不用来覆盖已有环境。
