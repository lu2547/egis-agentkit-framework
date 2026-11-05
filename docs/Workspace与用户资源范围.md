# Workspace 与技能范围

日期：2026-11-03。首版 local，后续 sandbox 沿用逻辑目录。数据与执行以 [数据结构](./数据结构.md)、[运行流程](./运行流程.md) 为准。

## 1. 两个区域

```text
/workspace/
├── common/
│   ├── {agent_definition_id}/{version}/skills/   # Agent 配套，只读共享
│   └── skills/{resource_id}/{version}/          # 显式选择的共享技能缓存
└── user/{um_id}/                                # 实际使用者
    ├── .mcp                                    # 原生 agent/session 声明文件
    ├── data/
    ├── skills/                                 # 本人私有技能缓存/原生分区
    │   ├── <resource_id>/<version-hash>/         # 本人私有不可变版本缓存
    │   └── <runtime_agent_id>/                  # 原生 Agent 分区
    ├── work/<session_id>/
    ├── artifacts/<session_id>/
    ├── memory/
    ├── preferences/                            # personal 的三个偏好文档
    └── sessions/<session_id>/
```

公共资源只存一份，不按用户复制。路径使用稳定定义 ID/发布物版本，不用可修改英文名；用户运行投影 ID 不改变公共资源定位。work/artifacts/memory/sessions 始终属于实际 um_id，不使用系统虚拟 owner 作为共享可写目录。

私有源通过管理页修改并形成 Nacos 不可变版本；用户区为按需物化缓存。申请共享固定完整包和摘要，对外用只读发布物，不指向作者可继续改的目录。定义/权限/审核数据在 PG，目录不另放一套管理真源。

实现中，配套技能路径细化为 `common/<definition_id>/<assembly_version 或 config-N>/skills/<resource_id>/<version-hash>`；同一 Agent 被不同用户调用复用同一份文件。通用系统/已共享技能使用 `common/skills/<resource_id>/<version-hash>`。缓存目录和完整文件摘要均检查；多进程同时首次下载只复用校验通过的获胜目录。每次激活仍向来源校验，不把已缓存文件重写一遍。

## 2. 各 Agent 加载范围

| 场景 | 加载范围 |
| --- | --- |
| personal | 基础技能 + 本人已安装/启用且获准的个人技能；可在同一对话刷新 |
| 系统 expert | 自己明确绑定的配套技能，不自动加用户安装的其他技能 |
| 用户轻量 expert | 本人私有配置或所选已审共享快照里的明确绑定 |
| orchestrator | Team 所选视图中自己的绑定，只在团内运行 |
| 固定专家团 | 各成员分别用自己的集合，不并成主理人的全量集合 |

调用获授权的系统专家时，配套 Skills 自动作为专家能力，无需用户再逐项安装/开启；实际文件仍可按首次使用物化。个人助理主动安装是另一条流程，不影响系统专家集合。

## 3. 技能广场展示

Skill 扩展字段为 is_skill_market_visible：true 在广场展示，false 不展示。属于某个系统 Agent 与是否上广场独立，不能一概隐藏配套技能，也不用复制成第二份通用技能。

该值不是全局可见性/文件保密开关，不代表 USE、已安装、可编辑。用户私有资源对外展示/使用还需通过共享流程；我的技能管理可以看到自己的未共享资源。运行轨迹不用因为“不在技能广场”就隐去技能正文。

## 4. 按需与原生复用

- 原生 SkillRecord 已有 version/enabled，复用；运行态 Skill 没有版本，版本由资源引用/发布物负责。
- public list_skills 供给允许的元数据/资源，SkillLoader/middleware 公开组合支持后续发现；不是写文件就刷新已装配的静态列表。
- Skill 首次受控调用才取得完整包；Tool 首次权限解析/使用取得真实对象；Middleware 必须在 Agent 构造之前取得并分流钩子。
- MCP 声明与句柄分开，公开 add/remove/list_mcps 管理，直接改 .mcp 不会自动刷新已缓存 Workspace。
- 管理员赋予 USE，安装/下载/运行不写 grant；私有本人使用来自平台预置规则，不给别人扩权。

参考技能材料的“所有 Agent 扫 Common-Skills”不采用，来源必须按 Agent/用户/视图选择；skill_paths=[]，不通过 .seed 全量复制。

## 5. 权限、删除、生命周期

普通用户可无审核保存自己的轻量资源，只有共享审核；系统源、他人源不能修改。安装他人/系统技能只拥有自己的安装引用，不能修改原件。管理员管理全部资源。

remove_skill/add_skill/archive/purge_agent/reset 都需校验用户、Agent、源资源 ID 和规范化真实路径；不得因 list_skills 返回公共路径就调用基类删除共享目录。不能用 Workspace 上一个共享的 _read_only 名称集合判断所有用户/Agent。

close 只关闭连接，不删磁盘。删除任务清相应上下文、工作文件与 MCP；artifacts 默认保留为用户交付；data 回收/孤儿文件清理另做引用核验，不假定原生会自动清完。

本机逻辑资源范围不等于 OS 强隔离；仅过滤列表不能防止同权限任意 Shell 读其他路径。后续 sandbox 按所选资源只读挂载，不能用隐藏技能条目冒充文件安全。
