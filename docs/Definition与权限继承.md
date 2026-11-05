# Definition、共享与权限

日期：2026-11-03。本轮规则以 [数据结构](./数据结构.md)、[表设计](./表设计草案.md)、[运行流程](./运行流程.md) 为主。未实现代码。

## 1. 三种范围

| 范围 | 配置来源 | 使用规则 |
| --- | --- | --- |
| 用户私有 | 本人当前 payload | 管理员预配置的个人助理/轻量资源自用规则，无审核；引用的其他资源仍检查 USE |
| 系统注册 | 管理员保存的当前定义/发布物 | 初始无 USE，没有授权来源则 all denied，包括发布者和管理员 |
| 用户共享 | 当前 approved、is_current 的完整共享快照 | 只在管理员批准共享并明确配置 USE 后对外使用，默认 all_users |

只有共享审核，没有用户自用发布审核。自己改 prompt/Skill 不会自动修改已通过的共享内容；共享更新重新提交。审核记录不是 Definition 草稿/回退版本。

User.role_type 只有 user/admin；Group 不是专家团 Team。普通用户不见授权/审核/用户分组管理页，相应 API 也拒绝；admin 可以管理全部资源、用户、组和授权，但管理资格不等于运行时 USE。

## 2. 使用权限的来源

```text
实际用户
  ├─ 用户直接 USE
  ├─ 所属有效 Group 的 USE
  └─ all_users 的 USE
       ↓
Team → orchestrator / expert → Skill / Tool / MCP
```

绑定是权限继承图，不是归属树。expert 可被多个 Team 引用，能力可被多个 Agent 引用。只有 USE 继承，不产生下层永久 grant。

对系统/共享目标，无匹配来源就拒绝。共享审核批准生成/更新 share 来源的明确授权；不是下载/运行时授权。private 自用仅限本人，不能用它执行他人私有最新配置。

共享权限边读取批准的共享快照，不能读取源私有最新 bindings，更不能把不同快照的边拼成一条路径。授权判断需返回来源路径和配置视图；被 Team 共享闭包引用的成员/能力，其配置同样来自这份闭包，不能回落到原作者最新内容。

## 3. 修改与管理资格

- 普通用户维护自己的轻量专家、专家团和 Skill，以及开放的个人助理偏好。
- 不开放任意 Python Tool/Middleware/包入口；可以引用获准的 Skill/MCP 与平台基础能力。
- 系统注册、用户/Group/角色管理、授权和共享批准仅 admin。
- 企业表不授予 EDIT，不再讨论“EDIT 是否包含 USE”。原生 EDIT 等字段保留原名，只在适配处按企业管理资格处理。
- owner_type/owner_id 认定源资源所有者，created_by 仅审计；运行投影属于使用者不意味着使用者能改共享模板。

## 4. 共享与撤销

1. 所有者提交 all_users 共享申请，完整配置/文件摘要固定，自己继续自用。
2. admin 审核来源、内容、依赖闭包和共享影响；源变更后必须重新提交。
3. 通过时切换唯一当前共享记录，并幂等维护该资源 share 授权来源。
4. 拒绝/取消不影响自用和上一份共享内容。
5. 撤销 grant 只使其来源失效，其他用户/Group/Team 路径不删除；停用资源/共享视图则另外检查可用性。

示例：张三有 Group→系统 EXP-PM，以及自己的直接 EXP-PM USE。退出 Group 不删除直接授权；系统 EXP-PM 未获得任何有效来源时不能用。TEAM-U100 共享批准后得到 all_users USE，只沿其已审 bindings 传播。

## 5. 展示、安装、执行分开

is_skill_market_visible 只决定技能广场展示。属于某系统 Agent 的 Skill 可以展示，也可以不展示；展示不授 USE、安装或编辑资格。用户私有内容不能仅凭设置 true 对外共享。

拥有 USE 也不代表所有 Agent 自动加载：系统专家只用自己的配套，用户轻量专家/编排者用明确绑定，个人助理用基础配置与个人已安装集合。团队成员的技能不全量合并给主理人。

## 6. 原生接入

ResourceAccessPolicyBase 接系统/共享可用范围；HTTP 管理入口先验角色/归属；Workspace、工厂、permission middleware 执行当前来源规则。企业检查通过仍继续原生 Permission/人工确认，不自动 ALLOW。

V1 不开放动态组团/增员工具。静态 Team 用原生 Record/Storage 先创建固定关系，TeamSay/Inbox/成员唤醒由原生完成。不修改 service，不运行时写 grant，不使用 Redis 权限缓存。
