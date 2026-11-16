# 框架分层与原生接入

本框架在 AgentScope 之上提供企业能力。目录首先表达依赖层次，目录内部按业务归集；继承 Base、Override、Middleware 是实现方式，初始化、回复、工具调用等是触发时机。

## 依赖约束

- `service` 可以组织 Core、其他 Service、Client 和 Storage；Service 之间不允许循环依赖。
- `core` 实现能力自身的构建、规则和执行行为，不调用 Service，不引用具体 Middleware。
- `middleware` 只包含 AgentScope 或 ASGI 中间件，按需调用 Service 或 Core。
- `client` 管外部通信；`storage` 管 SQL、文件持久化及原生存储接口实现；两者不依赖 Service 或 Middleware。
- `bootstrap` 创建并注入对象、注册回调、管理组件生命周期，不直接实现工具授权或查询团队关系。

中间件不必强制拆成转发壳。Read 二进制拦截属于完整局部增强，可以直接在中间件实现；远程任务管理、团队邀请等已有业务能力，由中间件调用所属 Service。

## 本轮归位后的目录与能力

```text
egis_agentkit/
├── core/
│   ├── agent/                    各类型 Definition、AgentRecord 和会话配置构建
│   ├── workspace/                用户身份、执行绑定、挂载、工具 Backend 与沙箱实例能力
│   └── session/_context.py       本轮身份、固定定义、项目路径及上下文变量
├── service/
│   ├── agent/
│   │   ├── _agent_manager.py     专家维护、版本及受权查询
│   │   ├── _personal_manager.py  个人助理初始化、偏好文档及安装项管理
│   │   ├── _personal_memory.py   个人记忆组件的创建、缓存和关闭
│   │   └── _remote_agent.py      远程任务发送、回包校验、结果持久化及通知
│   ├── resource/
│   │   ├── _hubs.py              SkillHub、MCPHub 原生契约及受权目录查询
│   │   ├── _skill_loader.py      技能候选、激活、恢复、撤权及状态保存
│   │   └── _catalog.py           既有资源管理及调用时重新授权
│   ├── session/
│   │   ├── _session.py           会话管理、平台工具解析、组长查询及回复续期编排
│   │   ├── _preparation.py       锁内权限校验、项目及模型准备、上下文绑定
│   │   └── _message_bus_override.py  在原生锁和订阅上接入会话准备与就绪通知
│   ├── team/
│   │   ├── _invitation.py        原生入团、任务投递、进度保存及取消恢复
│   │   └── _tool_callbacks.py    实时授权、协作资格与成员提示词参数
│   └── workspace/
│       └── _egis_sandbox_manager.py  缓存、并发、续期、关闭及销毁协调
├── middleware/
│   ├── agent/                    个人偏好提示词、远程专家模型调用接入
│   ├── team/                     团队角色与成员提示词
│   ├── resource/                 工具资格检查、Skill Loader 接入
│   ├── session/                  转接回复、工具执行及系统提示词回调
│   ├── workspace/                Read 二进制拦截
│   └── http/                     ASGI 身份校验及 SSE 就绪处理
├── client/
│   ├── mcp/                      网关 MCP Client 与工具来源标记
│   ├── remote_agent/             HTTP、异步回包、WebSocket 通信
│   └── sandbox/                  厂商中立协议与 OpenSandbox 通信实现
├── storage/
│   └── _session_execution.py     按本轮上下文覆盖原生 Agent、Session 读取
└── bootstrap/
    ├── _init.py                  创建服务、工作区、消息总线及外部 Client
    ├── _middlewares.py           按会话创建具体中间件并注入业务回调
    └── _demo_tools.py            显式登记现有示例工具部署，保持现有种子行为
```

此处列出本轮涉及的能力，其他 Core、Service、Client 和 Storage 仍保留在各自业务目录。示例工具登记用法见 `examples/report_tool.py`；执行实现在独立的 `src/egis_agentkit_examples/report_tool.py`，发布包明确包含该示例包，保持现有演示部署可用。示例不属于框架的 Core、Service 或 Middleware。

## 一次对话的调用顺序

1. Bootstrap 把 SessionMessageBus、SessionExecutionStorage、Workspace Manager、工具及中间件回调传给原生 App。
2. 原生会话锁触发 SessionPreparationService：读取固定定义，组织用户、权限、项目、模型及个人偏好管理，绑定 Core 的 SessionExecutionContext。
3. 原生读取通过 SessionExecutionStorage 得到本轮配置副本；Core 按 Definition 规则装配 AgentRecord，业务持久化记录不被替换。
4. Workspace Manager 取得执行实例。Read Guard 工厂在 Bootstrap 注入，Manager、Core 均不导入具体中间件。
5. 中间件装配点调用 ResourceService 准备受权资源，再按原有顺序装配中间件；工具解析直接绑定 SessionService 方法，不经过额外 Factory。
6. 原生 Agent 执行回复。中间件调用资源授权、团队邀请、远程任务、项目产物等所属能力；文件、SQL、外部通信仍由 Storage、Client 完成。
7. 正常结束、异常及取消均保持原来的清理路径；退出原生锁后恢复上下文变量。

ASGI 中间件由 App 单独注册，不参与 AgentScope 对话中间件链。

## 变更范围与验证

本轮移除旧 `extension` 包及无必要的工具 Factory，不保留旧路径兼容重导出。App 的身份、SSE 和示例工具登记引用同步更新。没有表结构变更，也没有新增业务规则或 Nacos 配置项。

`tests/test_middleware_architecture.py` 检查中间件目录、依赖方向、Service 无环及中间件装配不直接操作 Storage。现有回归测试覆盖授权、技能激活、团队邀请、远程回包、会话锁、取消、项目文件及 HTTP/SSE。
