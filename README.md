# egis-agentkit-framework

AgentScope 的配置管理与企业增强框架。首版已实现，不修改 AgentScope service 源码。

`src/egis_agentkit/` 按五个一级目录组织。

| 目录 | 职责 |
| --- | --- |
| `service/` | Agent、Team、Access、Model、Session 等业务服务 |
| `storage/` | 企业持久化和原生 SQL Storage 复用 |
| `extension/` | AgentScope 扩展实现，包括中间件、工作区、远程专家和消息总线 |
| `backend/` | PostgreSQL、Kafka、Nacos 和配置加载 |
| `bootstrap/` | 对象创建、依赖装配和生命周期 |

App 通过具名 `AgentKitServices` 调用业务服务。聊天循环、团队通信、消息事件、HITL、SSE 和模型调用继续使用 AgentScope。

专家定义可以绑定已授权的 Skill、MCP 和平台工具。第三方专家通过 HTTP 或 WebSocket 执行。

主要设计文档：

1. [数据结构](./docs/数据结构.md)：User/Group、三类 Agent、私有与共享、资源、执行快照。
2. [表设计](./docs/表设计草案.md)：企业增量表的字段、约束和案例，原生表复用清单。
3. [运行流程](./docs/运行流程.md)：启动、私有保存、系统注册、共享审核、任务/团队创建、钉版、按需加载及 HTTP 封口。

专题：[权限](./docs/Definition与权限继承.md)、[Workspace](./docs/Workspace与用户资源范围.md)。[原生 create_app 分析](./docs/create_app机制.md) 与 [历史改造点](./docs/改造点草稿.md) 保留作参考，首版以当前主设计为准。

配置管理统一使用已安装的 [Nacos](./docs/配置管理.md)，只保留连接中心所需的最小部署引导，不另建本地业务配置中心。

启动和真实联调见 [App](../egis-agentkit-app/README.md) 与 [实施记录](../egis-agentkit-app/docs/实施记录.md)。当前实际验收使用本地 AgentScope 2.0.11 源码；依赖范围为 `>=2.0.10,<2.1`，未单独声称已通过 2.0.10 的另一套完整验收。

项目共享工作空间与删除规则见 [文件生命周期](docs/session-files.md)。
