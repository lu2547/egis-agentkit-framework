# 1.0.3 Nacos 核对

Data ID 为 `agentkit.yaml`，namespace 和 Group 沿用当前环境。集群部署沿用 `agentkit.client.kafka` 的连接、消费组、Topic 和传输设置。将会话回放库的 `agentkit.client.redis` 配置合并到本环境现有 YAML，递增 `agentkit.revision`，发布后核对并重启 API/Worker。

| 场景 | 配置或操作 |
| --- | --- |
| 知识库检索 | `agentkit.tool.knowledgeBase.queryUrl` 填实际完整 SSE POST `/query` 地址；`timeoutSeconds` 沿用现有值。未配置地址时不能使用检索 |
| 个人助理使用知识库 | 用户在工具目录勾选 `KnowledgeBaseSearch`。URL 控制服务可用性，勾选控制个人装配，两者都需要 |
| 系统默认工具白名单 | 若 `agentkit.tool.defaults.registeredIds` 为显式列表，从中移除 `KnowledgeBaseSearch`。它现在是可选工具；`registeredIds: null` 无需调整 |
| 定时任务 | 本版没有新增 Nacos 参数。随 App 生命周期启动受管调度 Worker，数据库增量必须先执行。用户通过页面管理任务；通过对话操作时先勾选对应定时任务工具 |
| 互联网搜索 | `InternetSearch` 可勾选，但当前只返回“待实现”，未接入真实搜索服务，无新增搜索地址配置 |
| 会话事件回放 | 参照[完整示例](../../../examples/nacos/agentkit.yaml)填写 `agentkit.client.redis` 的 `host`、`port`、`db` 及可选的 `passwordEnv`。各 API/Worker 使用同一逻辑库和 `retentionSeconds` |
| 原始消息历史 | 沿用 `workspace.runtime.root` 指定的 NAS 根目录。使用 `workspace.files.win: true` 时先更新 Linux 文件服务，再启动 API/Worker |
| 原生队列 | 沿用 `agentkit.session.steering.maxBatchSize/maxMessageBytes/maxTextLength`。如果环境尚未完成 1.0.2 配置升级，先按该版说明删除 `wakeupRetrySeconds` |
| 沙箱依赖 | API/网关统一使用官方原版 AgentScope 2.0.12。需要重建镜像时，只修改本环境 `workspace.runtime.sandbox.image` 和实际内部制品来源 |

个人工具选择保存在数据库，不写入 Nacos；更新或撤销选择后运行时重新校验。知识库查询继续携带已验证的当前用户凭据，不增加匿名查询开关或额外共享 Token。

如需修改上表中的地址、白名单、镜像等配置，在本环境当前 `agentkit.revision` 上递增，发布后读回核对并按配置的重启要求重启所有实例。公司地址、密钥、NAS 根目录、issuer/audience 和日志设置沿用已验证的环境值。

仍停留在 1.0.1 或更早配置的环境，先按 [1.0.2 Nacos 说明](../1.0.2/nacos.md)完成字段调整；本版不复制或隐式兼容旧字段。
