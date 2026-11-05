# ReMe 长期记忆

在部署环境安装 `egis-agentkit-framework[memory]`，在 Nacos 的 `agentkit-runtime.yaml` 中设置：

```yaml
memory:
  is_enabled: true
```

默认关闭；开启但缺少 `reme-ai` 时应用构建立即报错。当前固定 ReMe 0.4.0.6：本地 AgentScope 的内置任务配置与 0.4.2.1 不兼容（缺少 `dream_topics_step`）。升级时须先验证 AgentScope 原生中间件启动及真实写入和检索。

仅 personal 智能体使用 AgentScope 原生 `ReMeMiddleware`；每个用户一个实例，工作区是 `<workspace_root>/user/<uid>/memory`。记忆的创建、提取、检索和文件布局由原生 ReMe 管理；`memory.md` 不是偏好文件，偏好接口只管理 `agent.md`、`soul.md` 和 `user.md`。中间件读取 AgentScope 的 session ID，回复结束后进行记忆提取；没有配置 embedding，使用 ReMe 默认关键词检索。检索时机和写入失败处理遵循原生中间件。

模型从 Nacos 默认模型通过 `ModelService.resolve` 和 AgentScope 原生 `get_model` 构造；用户第一次使用时固定，模型配置变更需重启进程以更新已有用户的记忆模型。服务在应用关闭时必须调用 `await factories.memory.close()`（当它不为 `None` 时）。缓存是进程内的；多进程共享同一个用户记忆目录的并发行为未经验证，生产部署需保证同一用户的记忆由单个进程管理。

本地以 `reme-ai==0.4.0.6` 和离线模型验证了原生应用启动、关闭，未调用真实模型，也未验证真实记忆写入或检索。
