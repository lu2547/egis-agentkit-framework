# egis-agentkit-framework

AgentScope 的配置管理与企业增强框架。首版已实现，不修改 AgentScope service 源码。

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

长回复重放：企业消息总线保留尚未被消息检查点覆盖的完整事件，下一条回复开始时清理已落库的前缀。不能按固定 1000 条裁掉正在生成的回复开头。当前同时依赖仓库内 AgentScope 会话 SSE 路由的分页修复（`app/_router/_session.py`），重放读完所有页后才切到实时订阅；发布时需一并构建这份依赖，不能替换成未包含修复的同名发行包。通过 `tests/test_session_replay.py` 验证超过单页长度的重放与 PostgreSQL 保留规则。

专家定义可以绑定已授权的 Skill、MCP 和平台工具。第三方专家通过 HTTP 或 WebSocket 执行。

Agent 当前实现见 [Agent 管理与定义](./docs/Agent管理与定义.md)，包含能力归属、汇总/明细表、SQL 权限分页及开发库升级方式。

其他设计文档：

1. [数据结构](./docs/数据结构.md)：User/Group、三类 Agent、私有与共享、资源、执行快照。
2. [表设计](./docs/表设计草案.md)：企业增量表的字段、约束和案例，原生表复用清单。
3. [运行流程](./docs/运行流程.md)：启动、私有保存、系统注册、共享审核、任务/团队创建、钉版、按需加载及 HTTP 封口。

专题：[权限](./docs/Definition与权限继承.md)、[Workspace](./docs/Workspace与用户资源范围.md)。[原生 create_app 分析](./docs/create_app机制.md) 与 [历史改造点](./docs/改造点草稿.md) 保留作参考，首版以当前主设计为准。

配置管理统一使用已安装的 [Nacos](./docs/配置管理.md)，只保留连接中心所需的最小部署引导，不另建本地业务配置中心。

启动和真实联调见 [App](../egis-agentkit-app/README.md) 与 [实施记录](../egis-agentkit-app/docs/实施记录.md)。当前实际验收使用本地 AgentScope 2.0.11 源码；依赖范围为 `>=2.0.10,<2.1`，未单独声称已通过 2.0.10 的另一套完整验收。

项目共享工作空间与删除规则见 [文件生命周期](docs/session-files.md)。

OpenSandbox Docker 工作区已通过 `EgisSandboxManager` 接入，继承原生 `OpenSandboxWorkspaceManager`。缓存、并发复用、TTL、暂停和重连使用原生实现；Manager 只负责执行绑定和实例生命周期；Core 负责项目挂载及工具执行。授权资源由 SessionService 调用 ResourceService.prepare_workspace 装载。运行容器按用户、项目、Agent、会话隔离，同一绑定复用环境；项目上传资料只读，`.work/`、`artifacts/` 可写，项目数据仍由文件存储共享和持久化，不新增工作区表。

部署时安装 `uv sync --extra opensandbox`，在 Nacos `agentkit.workspace.runtime` 设置 `mode: opensandbox`，并填写示例中的 `opensandbox` 配置。`root` 是应用访问的持久根目录，`hostRoot` 是 Docker daemon 看到的同一份存储；两者可以不同，但相对目录必须一致。OpenSandbox 服务的 `storage.allowed_host_paths` 必须包含该宿主根目录；本地 compose 通过 `AGENTKIT_WORKSPACE_HOST_ROOT` 设置白名单和服务端只读挂载。API 进程使用 `apiKeyEnv` 引用的环境变量，不把密钥写进 Nacos。修改镜像和挂载后需要重启 API；仅支持 OpenSandbox Docker 运行时。

工具通过 OpenSandbox Backend 执行；MCP 通过容器内原生网关连接，连接配置不写入 `.mcp`，网关临时内容只允许经过启动时验证为 tmpfs 的 `gatewayTmpDir`；技能在激活后暂存完整再安装，完成的同一内容直接复用，授权失效会移除运行副本。持久激活记录继续校验原资源内容。MCP 中的 `localhost` 指沙箱自身；访问宿主开发服务应使用沙箱可达地址。

上线和日常开发使用预构建镜像。裸 `python:3.11-slim` 会使每个新会话现场安装依赖，网络慢时聊天长时间没有回复。先将当前 Nacos YAML 导出到受保护文件，然后在框架目录运行：

```bash
PYTHONPATH=src python scripts/build_workspace_image.py \
  --config /path/to/agentkit.yaml \
  --agentscope-source ../../harness/agentscope
```

构建复用原生镜像模板，网关和 AgentScope wheel 来自同一份源码，额外依赖读取 YAML；配置正文、密钥和项目文件不进入镜像。将最后输出的 `agentkit-workspace:<内容摘要>` 写入 Nacos `agentkit.workspace.runtime.opensandbox.image`，重启 API。包源通过 uv 的部署环境配置；本机私有包源不可用时应在构建环境配置可达包源。已有沙箱不会自动换镜像，升级时需回收旧运行环境，持久项目目录保留。

日常回归运行 `pytest -q`。真实 Docker 验收需要显式提供 `AGENTKIT_TEST_OPENSANDBOX_ROOT`、`AGENTKIT_TEST_OPENSANDBOX_DOMAIN` 和 `OPEN_SANDBOX_API_KEY`，再运行 `pytest tests/test_opensandbox_live.py -q -s`；该测试仅清理自己生成的唯一沙箱和文件目录。额外设置 `AGENTKIT_TEST_OPENSANDBOX_IMAGE` 可验证预构建镜像；此时任何会话内安装依赖都会令测试失败。验收还需通过真实 `/chat/` 和 SSE 收到正常结束的模型回复，健康检查不能替代聊天验收。

Workspace/Project 的职责、目录、表结构和运行顺序见 [Workspace与Project](docs/Workspace与Project.md)。项目文件与预览配置位于 `agentkit.project`，Workspace 只保留 `agentkit.workspace.runtime`；升级前需运行显式数据库升级。

外部系统连接与配置入口见 [Client 接入](docs/Client接入.md)。
