# Nacos 配置与 Skill/MCP Hub 验收

2026-11-03，按 `egis-agentkit-app/docs/V1执行计划.md` Task 3 实施。仅修改 framework `config/`、`hubs/`、Nacos 测试及 demo `hub_cases/`，没有修改 AgentScope 源码、App、领域 Storage/Services 或前端。

## 接入接口

```python
from egis_agentkit.config import (
    NacosBootstrap, NacosClient, NacosConfigProvider, NacosError,
    RuntimeConfig, AuthConfig, ModelsConfig, ModelSpec, PoliciesConfig,
    ConfigSnapshot,
)
from egis_agentkit.hubs import NacosSkillHub, NacosMCPHub, NacosHubManagement
```

`NacosBootstrap` 仅接收 `server_url`、可选 `console_url`、`namespace_id`、`group`、`identity_key`、`identity_value`、`access_token`。敏感值为 `SecretStr`。客户端公共 `request(method,path,params/data/json/files)` 保留 httpx Response；只允许同源 Nacos v3 路径，不跟随重定向，不透传上游错误原文、密钥或网络异常内容。`get_config(data_id)` 读取 Client API；`publish_config(data_id,content,config_type='yaml')` 调用管理 API。`close()` 只关闭自身创建的 HTTP 客户端。

`NacosConfigProvider.refresh()` 读取 `agentkit-runtime.yaml`、`agentkit-models.yaml`、`agentkit-policies.yaml`，先完整校验类型、统一 `config_version`、默认模型/模型白名单引用，再一次替换快照。`.snapshot` 和 refresh 返回深拷贝，业务消费者不能修改共享快照内部列表/字典。首次配置缺失不就绪；断线、非法配置、版本不齐时抛错并保留最后有效配置。固定组件 RuntimeConfig 变化时保留旧快照并设置 `.restart_required=True`；不假称活动 DB/Workspace 已热切换。`.is_connected` 表示最近一次完整刷新成功；`start(interval=10)` 首次拉取后轮询，`close()` 取消轮询，不关闭共享 client。不提供本地业务配置降级。

`NacosSkillHub` 和 `NacosMCPHub` 继承当前本地 AgentScope 原生 HubBase。默认 hub_id 为 `nacos-skills`/`nacos-mcps`。构造参数 `authorize` 是可选 async `(user_id,resource_type,resource_id,operation)->bool`；未提供时默认拒绝。资源类型为 `skill`/`mcp`，操作为 `list`/`get`/`download`。列表逐条过滤，详情和下载在上游请求前检查，猜 ID 不能绕过。`is_skill_market_visible` 仅为元数据，不产生 USE 授权。

Skill 提供原生 `list_skills/get_skill/download`，另有 `get_skill_version(user,id,version)`。列表只请求一页，不逐项取 SKILL.md；下载转发 ZIP 字节流并关闭响应，完整保留附属文件。未指定版本时固定本次读取到的 latest 标签。MCP 提供原生 `list_mcps/get_mcp`，另有 `get_mcp_version(user,id,version)`；列表没有远程 endpoint 的卡片仅携带 `${endpoint_url}` 模板，原生安装链会重新 get_mcp 获取实际模板。精确 MCP 版本不匹配则拒绝。MCP env 只接受 `${input_name}`，生成原生 inputs_schema 密码字段，不把实际密钥输出到卡片。

`NacosHubManagement` 是单独的 Service 写入适配器，不混入原生只读 Hub。实现 skill 的 upload/create/update/submit/publish/delete，以及 MCP 的 create/update/submit/publish/delete 和精确版本参数。新 Skill/MCP 在发布前设置 Nacos `scope=PRIVATE`。Skill ZIP 校验体积、解压总量、条目数、路径穿越、绝对路径、symlink 和唯一 SKILL.md。MCP 管理使用 Nacos 3.3 draft 生命周期，不覆盖已发布内容。普通 publish 使用正常审核发布端点；另有明确命名的特权 `force_publish_skill/force_publish_mcp`，仅用于本次用户要求的本地示例，业务保存流程不自动调用。

## 本地 Nacos 3.3 实际条件

- 服务 `http://127.0.0.1:8848`，namespace `public`，group `EGIS_AGENTKIT`。
- identity header 能通过 Admin API 的入口校验，但当前 AI PRIVATE 资源可见性还要求已认证用户上下文。单独的 server identity 无法读取自己的匿名 private draft；默认 `nacos/nacos` 登录实际返回 403。本次没有重置密码、修改鉴权配置或把资源改成 PUBLIC。
- 现有 compose 已明确关闭 console auth。按总协调确认，新增显式可选 `console_url`，本地填 `http://127.0.0.1:8080`，将规范 Admin 路径映射到官方 `/v3/console/...`。Client 配置读取仍走 8848。这个选择由部署引导明确提供，不自动探测、不自动降级。
- 生产必须使用已认证的 Admin API/accessToken，并省略 console_url。当前本地开放 Console 是开发部署的信任边界，PRIVATE 不能被描述成阻止能直接访问无鉴权 Console 的人；App Hub 仍独立执行企业授权。
- 当前 Skill 发布 pipeline 无审批插件：submit 后实际为 IN_PROGRESS，正常 publish 返回 400 / Pipeline not approved。仅两个显式演示对象通过官方 force-publish 发布。没有修改其他资源。

## 两个真实示例

1. `egis-demo-report@1.0.0`，Nacos scope `PRIVATE`、版本 online、market visible true。ZIP 包含四个真实文件：`SKILL.md`、`references/checklist.md`、`scripts/summarize.py`、`templates/report.md`。
2. `egis-demo-tools@1.0.0`，Nacos scope `PRIVATE`、版本 online。原生卡片为 HttpMCPConfig，URL `http://127.0.0.1:8766/mcp`。服务是 demo 目录真实 FastMCP 进程，并非模拟 registry 或固定响应路由。

服务运行环境 `/tmp/egis-agentkit-mcp-venv`，依赖 `mcp==1.30.0`。共享 demo 环境已安装 MCP2，FastMCP 在其上被重命名，故使用独立 venv，不改变共享应用依赖。服务日志 `/tmp/egis-demo-tools.log`，PID 文件 `/tmp/egis-demo-tools.pid`。它是持久子进程，重启主机后需按 demo README 重新启动，不声称具备生产进程托管。

## 可重复命令与实际输出

工作目录为 ai-app workspace root。测试先于实现编写，首次运行因 config/hub 公共类尚不存在而失败；随后添加了已观察失败的快照深层修改保护、显式 Console 映射、MCP 凭证输入和独立 force-publication 测试。最终领域验证：

```sh
PYTHONPATH=egis/egis-agentkit-framework/src demo/agentscope-demo/.venv/bin/python -m pytest egis/egis-agentkit-framework/tests/test_nacos_config.py egis/egis-agentkit-framework/tests/test_nacos_hubs.py -q -o asyncio_default_fixture_loop_scope=function
```

实际输出：`14 passed in 0.10s`。覆盖正常读写 envelope、namespace/group、错误脱敏/跨源拒绝、混版本/非法引用保留、组件配置变化、快照拷贝隔离、连接失败保留、轮询停止、PRIVATE 默认拒绝、详情/下载绕过防护、分页过滤、ZIP 路径防护、精确 MCP 版本及 native HttpMCPConfig 解析、凭证模板输入、管理 lifecycle/PRIVATE scope。

```sh
NACOS_CONSOLE_URL=http://127.0.0.1:8080 PYTHONPATH=egis/egis-agentkit-framework/src demo/agentscope-demo/.venv/bin/python demo/agentscope-demo/agentkit_demo/hub_cases/seed_nacos.py --seed --verify
```

实际输出：

```text
skill browse: ['egis-demo-report']
skill ZIP files: ['egis-demo-report/SKILL.md', 'egis-demo-report/references/checklist.md', 'egis-demo-report/scripts/summarize.py', 'egis-demo-report/templates/report.md']
skill version: 1.0.0 market visible: True
mcp browse: ['egis-demo-tools']
egis-demo-report registry scope: PRIVATE
egis-demo-tools registry scope: PRIVATE
unauthorized direct access: denied
MCP list_tools: ['sum_numbers', 'report_outline']
MCP sum_numbers: {'total': 47.0, 'count': 3} isError: False
MCP report_outline: {'title': 'Quarterly delivery', 'sections': ['Summary', 'Evidence', 'Next actions']} isError: False
```

seed 脚本只创建不存在的两个精确名称，不删除或覆盖已有对象。去掉 `--seed` 即为只读 registry 验证及对无副作用演示工具的真实调用。MCP1 server 与共享 MCP2 client 已按真实协议互通。

全框架测试命令：

```sh
PYTHONPATH=egis/egis-agentkit-framework/src demo/agentscope-demo/.venv/bin/python -m pytest egis/egis-agentkit-framework/tests -q -o asyncio_default_fixture_loop_scope=function
```

第一次完整运行实际 `18 passed, 13 skipped in 0.95s`；跳过项是其他工作域以环境变量控制的真实集成测试，不声称其已通过。最终完整运行结果在本报告末尾记录。

## 边界

配置中心三份业务配置的真实发布、App 引导、企业授权回调注入及 PG 业务资源记录由总协调负责。本次验证完整覆盖 Nacos Hub 真实资源和 MCP 工具，不把示例的回调白名单当作企业生产策略。原生 Hub 的 browse/install 链保持复用；管理写入仍由领域 Services 完成业务授权和 PG 事实落库后显式调用。轮询刷新支持运行期更新，不宣称实现 Nacos gRPC 原生 push 订阅。

最终完整运行（其余工作域仍在并行实现）未进入执行阶段：`tests/test_teams.py` 无法导入 `egis_agentkit.services.teams`，`tests/test_workspace.py` 无法导入 `egis_agentkit.workspace`，2 个 collection error。已向总协调报告；不修改其他工作域代码。Nacos 边界 14 项已独立全绿，真实 seed/verify 命令全绿。
