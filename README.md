# egis-agentkit-framework

AgentScope 的配置管理与企业增强框架。首版已实现，不修改 AgentScope service 源码。

首次环境初始化见 [scripts/init](scripts/init/README.md)：面向企业已提供的 NAS、Nacos、Redis、Kafka、PG，包含目录初始化、配置发布、Topic 创建和业务建表 SQL；脚本不安装或启动基础设施。

数据库支持 Nacos `agentkit.client.postgres.connection.mode: key | cyberark`：key 使用 password 字段，cyberark 使用 CCP REST 获取新建连接所需密码。日志配置位于 `agentkit.logging`；Phoenix 导出位于 `agentkit.client.tracing`，启动后由原生 TracingMiddleware 记录回复、模型和工具调用，Framework 管理导出和关闭。配置示例及运维步骤见上述初始化说明。

Phoenix 的运行主项目默认 `egis-agentkit`，以 `chat.run` 为根记录会话、项目、沙箱、资源、记忆、原生 Agent/模型/工具和收尾阶段，使用 `session.id` 对应实际会话。普通 HTTP 与 FastAPI 内部 Span 默认进入 `egis-agentkit-http`，由 `client.tracing.httpProjectName` 配置。Span 结束后导出，执行中的父阶段不提前标记完成。

App CLI 使用 Python `logging.config.dictConfig` 统一配置 AgentKit、AgentScope 和 Uvicorn 的日志。Nacos `agentkit.logging` 控制 `level`、`format`（`json` 或 `text`）、`directory` 和 `accessLog`。默认输出到 stdout。设置 `directory` 为服务进程可写的绝对目录后，同时写入 `agentkit-<主机名>-<PID>.log`，文件和 stdout 使用相同的格式与级别。目录变更在服务重启后生效。管理与聊天请求的响应带有 `X-Request-ID`，可关联访问日志与错误记录。访问日志包含路由模板、状态和耗时，身份验证成功时也包含用户标识。业务拒绝记录异常类型、公开说明和抛出位置。系统故障保留异常类型、栈和原因链，可识别的操作系统错误附带错误编号和描述。

`format: text` 时，`textPattern` 控制单条日志的字段顺序和分隔符，默认是 `{time} [{level}] {package}.{function} - {message}`。其中 `function` 为调用方法全名，类方法显示为 `类名.方法名`。模板还支持 `{logger}`、`{component}`、`{file}` 和 `{line}`，并须包含 `{message}`。请求、链路和异常详情自动附在行尾。`textDateFormat` 使用 Python `strftime` 格式设置 UTC 时间的显示方式，设为 `null` 时使用带毫秒的 ISO 8601。JSON 使用固定字段，包含 `function`、`file` 与 `line`。模板变更在服务重启后生效。

`src/egis_agentkit/` 的 Agent 能力采用以下分层；其他业务仍保留现有目录，后续逐项梳理。

| 目录 | 职责 |
| --- | --- |
| `service/` | 面向 App 的管理入口，组合 Core 和 Storage；Agent 只暴露 PersonalManager、AgentManager |
| `core/` | Agent、Project、Workspace 自有能力；Agent 定义及原生对象装配 |
| `storage/` | 企业持久化和原生 SQL Storage 复用 |
| `extension/` | AgentScope 扩展实现，包括中间件、工作区、远程专家和消息总线 |
| `client/` | PostgreSQL 连接池、Nacos 配置、Kafka 通信及远程 Agent 协议 |
| `bootstrap/` | 对象创建、依赖装配和生命周期 |

App 通过具名 `AgentKitServices` 调用业务服务。聊天循环、团队通信、消息事件、HITL、SSE 和模型调用继续使用 AgentScope。

运行中引导复用 `POST /chat/`：请求带 `delivery: "steer"`，`input` 接收一条原生用户 Msg 或有序的 Msg 数组。每条 Msg 使用稳定的 `id`，重试时复用同一 ID 和内容；冲突复用返回 409。普通聊天省略 delivery，审批事件沿用原协议。引导不取消当前模型或工具调用；原生 InboxMiddleware 在下一次 reasoning 前接收，等待审批时先保留，恢复后接收。SSE `HINT_BLOCK.block_id` 对应引导消息 ID，表示已加入模型上下文。

集群模式使用现有 `messages` 和 PG Inbox，在同一事务内保存用户记录与队列内容；没有新增表或 Topic。`agentkit.session.steering` 配置 `maxBatchSize`、`maxMessageBytes`、`maxTextLength`、`wakeupRetrySeconds`，输入区通过 `/manager/runtime-options` 读取。standalone 继续使用原生内存总线，其队列不跨进程重启保留。

长回复重放：企业消息总线保留尚未被消息检查点覆盖的完整事件，下一条回复开始时清理已落库的前缀。不能按固定 1000 条裁掉正在生成的回复开头。当前同时依赖仓库内 AgentScope 会话 SSE 路由的分页修复（`app/_router/_session.py`），重放读完所有页后才切到实时订阅；发布时需一并构建这份依赖，不能替换成未包含修复的同名发行包。通过 `tests/test_session_replay.py` 验证超过单页长度的重放与 PostgreSQL 保留规则。

专家定义可以绑定已授权的 Skill、MCP 和平台工具。第三方专家通过 HTTP 或 WebSocket 执行。

Agent 当前实现见 [Agent 管理与定义](./docs/Agent管理与定义.md)，包含能力归属、汇总/明细表、SQL 权限分页及开发库升级方式。

其他设计文档：

1. [数据结构](./docs/数据结构.md)：User/Group、三类 Agent、私有与共享、资源、执行快照。
2. [表设计](./docs/表设计草案.md)：企业增量表的字段、约束和案例，原生表复用清单。
3. [运行流程](./docs/运行流程.md)：启动、私有保存、系统注册、共享审核、任务/团队创建、钉版、按需加载及 HTTP 封口。

专题：[权限](./docs/Definition与权限继承.md)、[Workspace](./docs/Workspace与用户资源范围.md)。[原生 create_app 分析](./docs/create_app机制.md) 与 [历史改造点](./docs/改造点草稿.md) 保留作参考，首版以当前主设计为准。

配置管理统一使用已安装的 [Nacos](./docs/配置管理.md)，只保留连接中心所需的最小部署引导，不另建本地业务配置中心。

审批流程记录使用 `ak_approve`，已生效的资源授权使用 `ak_resource_permission`，两者分别记录审批过程和最终使用权限。

系统技能正式文件保存在 `{workspaceRoot}/common/skills/{skill_id}/{version}/`，个人技能保存在 `{workspaceRoot}/user/{owner_id}/skills/{skill_id}/{version}/`。私人技能获准共享后仍从原作者目录加载。数据库记录资源版本、内容摘要、实际上传人和使用授权。运行时按获准使用的版本加载，下载时从正式文件生成 ZIP。

普通用户、管理员和经授权的外部系统使用 `POST /manager/skills/imports` 提交 ZIP。管理员在用户管理签发虚拟用户 Bearer 凭证，外部系统可先用 `GET /manager/skills/imports` 验证凭证。系统技能通过导入审核后发布，实际上传人和审核人分别记录。上传 ZIP 暂存在 Nacos 的 `agentkit.skill.storage.root` 下，任务成功或审核拒绝后删除。

已有开发库可使用 App 的显式重置命令。先停止 API 和导入 Worker，将 `DB_NAME` 设为 Nacos 配置中的当前开发库名，预览目标库及目录后再执行：

```bash
python -m egis_agentkit_app reset-skills --database "$DB_NAME"
python -m egis_agentkit_app reset-skills --database "$DB_NAME" --apply
```

重置清理技能记录、授权、导入文件和 Agent 定义中的技能绑定，重建导入表。新部署直接执行 `init-db`。

启动和真实联调见 [App](../egis-agentkit-app/README.md) 与 [实施记录](../egis-agentkit-app/docs/实施记录.md)。当前实际验收使用本地 AgentScope 2.0.11 源码；依赖范围为 `>=2.0.10,<2.1`，未单独声称已通过 2.0.10 的另一套完整验收。

项目共享工作空间与删除规则见 [文件生命周期](docs/session-files.md)。

启用 `agentkit.project.memory.isEnabled` 后，同一用户项目内的本地 Agent 共享 ReMe 记忆。首次执行记忆作业时，ReMe 在 NAS 的 `user/{uid}/projects/{pid}/.memory/` 初始化存储。第三方专家不装配本地记忆中间件。

OpenSandbox Docker 工作区通过 `EgisSandboxManager` 接入。Manager 实现原生 `WorkspaceManagerBase`，负责执行绑定、缓存、并发、续期和回收；Core 继承原生 `SandboxedWorkspaceBase`，复用初始化模板并实现项目挂载、Backend 和单实例生命周期；OpenSandbox SDK 通信位于 Client。授权资源由 SessionService 调用 ResourceService.prepare_workspace 装载。运行容器按用户、项目、Agent、会话隔离，同一绑定复用环境；项目上传资料默认只读，`.work/`、`artifacts/` 可写，用户标准目录位于 NAS 的 `user/{uid}/{skills,session,projects}`。多模态卸载文件放在当前项目的 `.work/data/`；`skills/session` 下按执行绑定分区并挂入对应沙箱，避免运行资源串用；NAS 的 `session` 映射到 SDK 的 `sessions`。项目目录由创建项目时初始化，创建会话时安全补齐缺失目录，Workspace 初始化只负责用户标准目录和挂载。项目数据由文件存储共享和持久化，不新增工作区表。

沙箱配置统一使用 `agentkit.workspace.runtime.mode: pks` 和同级 `sandbox` 节点。`root` 是唯一的 NAS 持久根目录，沙箱执行宿主机与实际处理文件的 Linux 服务或 Agent 节点须通过同一绝对路径访问同一份 NAS。Workspace 的 `workdir` 固定派生为 `{root}/user/{uid}`，不再单独配置 `/workspace`；Workspace Backend 默认目录、提示词和用户目录挂载目标均使用这个用户路径。沙箱直接挂载该根目录下的当前用户项目，文件无需复制。当前开发通信仍由 `OpenSandboxClient` 对接已有 SDK；开发环境需安装 OpenSandbox extra，服务端的 `storage.allowed_host_paths` 必须包含 `root`。`apiKeyEnv` 引用部署环境变量，不把密钥写进 Nacos。公司 PKS 接入通过 `init_agentkit(..., sandbox_client_factory=...)` 注入实际 Client，配置命名调整不代表已经实现公司通信协议。修改镜像和挂载后需要重启 API；已有沙箱的挂载不会自动改变，需在停止其执行后重建实例，NAS 文件保留。Manager、Workspace 直接实现原生通用基类，Backend 在 Core，提供方 SDK 仅位于 Client。


`agentkit.workspace.files.win` 默认是 `false`，用于已挂载 NAS 的 Agent 节点。原生 Windows Agent 将 `win` 设为 `true`，并配置 `endpoint` 指向 Linux 文件服务。个人偏好、项目文件和 Skill 由该服务操作同一份 NAS，`runtime.root` 仍填写 Linux 上的 NAS 根目录。文件服务通过 `tokenEnv` 从部署环境读取两端共享的服务凭据。HTTP 模式下将 `agentkit.project.memory.isEnabled` 设为 `false`，因为原生 ReMe 直接访问 Agent 宿主的目录。启动方式见 [App 部署说明](../egis-agentkit-app/README.md)。

工具通过 EgisSandboxBackend 调用注入的沙箱 Client 执行；MCP 通过容器内原生网关连接，连接配置不写入 `.mcp`，网关临时内容只允许经过启动时验证为 tmpfs 的 `gatewayTmpDir`；技能在激活后暂存完整再安装，完成的同一内容直接复用，授权失效会移除运行副本。持久激活记录继续校验原资源内容。MCP 中的 `localhost` 指沙箱自身；访问宿主开发服务应使用沙箱可达地址。

上线和日常开发使用预构建镜像。裸 `python:3.11-slim` 会使每个新会话现场安装依赖，网络慢时聊天长时间没有回复。先将当前 Nacos YAML 导出到受保护文件，然后在框架目录运行：

```bash
PYTHONPATH=src python scripts/build_workspace_image.py \
  --config /path/to/agentkit.yaml \
  --agentscope-source ../../harness/agentscope
```

构建复用原生镜像模板，网关和 AgentScope wheel 来自同一份源码，额外依赖读取 YAML；配置正文、密钥和项目文件不进入镜像。将最后输出的 `agentkit-workspace:<内容摘要>` 写入 Nacos `agentkit.workspace.runtime.sandbox.image`，重启 API。包源通过 uv 的部署环境配置；本机私有包源不可用时应在构建环境配置可达包源。已有沙箱不会自动换镜像，升级时需回收旧运行环境，持久项目目录保留。

沙箱代码按职责收敛为：

```text
service/workspace/_egis_sandbox_manager.py   缓存、并发构建、续期调度、空闲回收
core/workspace/_identity.py                 用户身份和固定执行绑定
core/workspace/_egis_sandbox_workspace.py   挂载、技能、MCP 和单实例生命周期
core/workspace/_egis_sandbox_backend.py      原生工具的命令、字节文件执行能力
core/workspace/_checks/                     Workspace 内部检查，不注册独立组件
client/sandbox/_protocol.py                 SandboxClient 通信契约
client/sandbox/_models.py                   中立挂载与实例数据
client/sandbox/_opensandbox.py              当前开发提供方 SDK 通信
middleware/workspace/                       Read 二进制拦截
client/mcp/_gateway_mcp_client_override.py   网关 MCP 工具来源标记
```

`EgisSandboxManager → WorkspaceManagerBase`，`EgisSandboxWorkspace → SandboxedWorkspaceBase`，`EgisSandboxBackend → BackendBase`，均直接实现通用原生基类。调用链为 `Manager → Core Workspace/Backend → 注入的 Client → 提供方`；不按 init/start/runtime/end 再建目录，也不新增表。用户逻辑工作区、会话执行绑定及 NAS 路径保持稳定。

公司沙箱通过 `init_agentkit(..., sandbox_client_factory=...)` 注入，每次工厂调用返回独立的 `SandboxClient`。`find_instance/list_instances` 负责完整查询；`create_instance/connect_instance/renew_instance/close_instance/destroy_instance` 管实例；`exec_command/read_file/write_file` 管通信；`close` 只释放连接。Core 先检查实例绑定和挂载身份再恢复；只有 `SandboxInstanceNotFound` 可以触发缓存重建。公司模式同样使用 `agentkit.workspace.runtime.mode: pks` 和 `sandbox` 节点，并显式注入公司 Client 工厂。注入时不读取开发适配器的凭据；未注入工厂时仅支持当前开发 SDK 连接，必须提供 `sandbox.domain` 和 `apiKeyEnv` 对应的凭据。

日常回归运行 `pytest -q`。真实 Docker 验收需要显式提供 `AGENTKIT_TEST_OPENSANDBOX_ROOT`、`AGENTKIT_TEST_OPENSANDBOX_DOMAIN` 和 `OPEN_SANDBOX_API_KEY`，再运行 `pytest tests/test_opensandbox_live.py -q -s`；该测试仅清理自己生成的唯一沙箱和文件目录。额外设置 `AGENTKIT_TEST_OPENSANDBOX_IMAGE` 可验证预构建镜像；此时任何会话内安装依赖都会令测试失败。验收还需通过真实 `/chat/` 和 SSE 收到正常结束的模型回复，健康检查不能替代聊天验收。

Workspace/Project 的职责、目录、表结构和运行顺序见 [Workspace与Project](docs/Workspace与Project.md)。项目文件与预览配置位于 `agentkit.project`，Workspace 只保留 `agentkit.workspace.runtime`；升级前需运行显式数据库升级。

外部系统连接与配置入口见 [Client 接入](docs/Client接入.md)。

Core、Service、Middleware 的能力归属、依赖方向和原生接入流程见 [框架分层](ARCHITECTURE.md)。隔离工作区运行集成测试时，可用 `AGENTKIT_TEST_PG_ENV` 指定现有测试数据库环境文件；测试始终创建独立临时 schema，不清理业务表。

### Workspace 就绪检查和跨节点保护

Nacos `agentkit.workspace.probe` 配置轻量/完整检查间隔、单阶段超时、网络重连次数及环境命令；`agentkit.workspace.lock` 配置分布式实例租约和等待预算。两组属于固定装配配置，修改后需要重启 API。

- 创建、恢复后完整验证实例归属、执行通道、网关、Python/依赖和 NAS；缓存命中仍确认当前实例，命令/网关和完整检查分别按配置间隔复核。
- NAS 使用 `.work/.agentkit-probes` 和 `artifacts/.agentkit-probes` 下的独立随机文件，执行宿主写/沙箱读、沙箱写/宿主读。保留目录不进入项目列表、变更扫描或自动交付，失败和取消只清理自己的文件。
- Client 的 `check_connection(reconnect=False)` 返回当前 `SandboxInstance` 并验证命令通道。网络重试只连接原实例，不重放业务命令。只有明确不存在才创建；确认暂停则恢复；其他实例状态、认证、环境和存储故障拒绝使用。
- `EgisSandboxWorkspace.check_ready` 是工作区自身的校验能力，内部实现收在 `core/workspace/_checks/`，不注册独立 Core 组件；`WorkspaceProbeError` 统一在公共错误模块定义，保留 `stage/reason/retryable`。非临时的初始化检查错误按配置短路，避免反复初始化同一个错误环境。
- 集群中，同一运行绑定的构建、复核、暂停和回复使用保护复用现有 PG 总线租约。进程内锁仅保护本节点缓存，不承担跨节点互斥。失锁取消受保护执行，不重放命令。
- 空闲回收还检查原生 Session 分布式锁，覆盖工具装配到回复结束；本节点关闭时若另一节点仍在使用，只断开本地句柄，不暂停共享实例或注销远端 MCP。项目删除继续使用 PG 项目生命周期锁和 Session 锁后再销毁实例。
- `standalone` 明确是单节点模式；多实例部署使用 `cluster`。API `/health/ready` 仍表示 API/PG/消息总线就绪，不将单个 Workspace 的故障升级为整个 API 的存活失败。

若从示例工具拆包前的开发库升级，先运行 `python scripts/align_tool_deployments.py` 预览；确认是纯部署地址移动后，使用 `--apply --database <目标库名>` 应用。脚本校验原内容及签名并在同一事务更新，运行时仍保留不可变版本校验。
