# 会话技能版本与激活实施记录

**目标：** 个人助理使用用户实际安装版本；首次激活按会话锁定版本和内容，后续轮次恢复，专家只使用 Definition 绑定。
**方式：** 复用原生 SkillRecord、AgentState.middle_context、Session state 持久化和 SkillLoaderBase，不改 AgentScope 源码，不新增表。
**依据：** 本轮已确认的会话锁版、不可变缓存和 agent.md 技能指引方案。
**约束：** 用户明确要求不跑测试；仅做静态检查、代码审阅和无活动任务时重启。

- [x] ResourceService 区分已安装版本与广场最新版，同一资源仅选择最近一次安装/启停的记录。
- [x] LiveSkillLoader 记录 resource/version/source digest/cache digest/目录，恢复后重新校验当前权限；工具读取仅开放实际激活目录。
- [x] 个人助理默认 agent.md 补充匹配任务优先使用技能、明确指定必须先读取技能，保留用户自定义内容。
- [x] 静态检查、审阅改动与部署；不运行测试。

审阅重点：升级不覆盖旧缓存、旧会话不随广场最新版切换、停用/撤权即时生效、并行 Skill 调用保存不互相覆盖、失败不静默换版本。

## 落地记录

- 新增 SkillActivation 数据结构；激活记录复用 Session state，未改原生源码、未新增表。
- 六个 Python 文件语法解析与 Ruff 通过，git diff --check 通过；按用户要求未执行测试或模型验收。
- 独立只读审阅指出缺省搜索目录绕过与 SemVer 解析问题，均已修改；Bash 仍由原生权限控制，不声称是 OS 隔离，范围已在说明文档写明。
- 当前本地 1 份已保存 agent.md 追加指引，原文备份于 app/var/backups/skill-guidance-20261106-225331。
- 检查无运行中用户任务后重启 API，PID 17188；/health/ready 返回 200、is_ready=true、Nacos 已连接。
