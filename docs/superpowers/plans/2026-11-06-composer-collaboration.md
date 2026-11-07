# 会话邀请与输入选择器 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. 按下面任务实现与验证。

**Goal:** 当前个人助理会话邀请已授权专家；@ 与 / 的四类候选独立分页并跟随键盘高亮滚动。

**Architecture:** Framework 保存明确邀请请求与专家快照，原生工具处理建团、邀请、消息和运行；App 只暴露薄接口。选择器每页请求十条，前端不预取全量再切片。

**Tech Stack:** AgentScope 2.0.10、PG、FastAPI、Vue3、TDesign。

**Spec:** 用户已选择当前对话协作；@ 为上下文/专家，/ 为技能/命令，一次10条。

## Global Constraints

- 不改 AgentScope service 源码，不重复实现 AgentLoop/SSE。
- USE 提前授权；邀请、运行不得隐式扩权。
- 专家不能当主理人；个人助理支持协作，固定专家团不自由扩员。
- 保留同一主会话与项目文件；被邀专家运行使用原生隐藏成员会话。
- 浅/深主题使用中性灰、单层细边框。

## Tasks

- [x] Framework：先补权限、角色、重复请求、快照和成员初始化回归，再接原生 TeamCreate/AgentInvite/TeamSay；成员初始化先于唤醒，原生记录正常持久化。
- [x] App：POST /manager/tasks/{sid}/collaborators 保存明确请求；GET /manager/composer/options 返回 active category 的10条候选及游标。
- [x] Frontend：当前助理保持不变；专家作为协作标签待发送，准备邀请成功后复用原生 chat；403/409/网络失败保留输入和同一 request_id；切换会话不得误发送。
- [x] Picker：键盘移动自动滚入可见区，触底/到分页边界增量加载；切 tab/词/会话取消旧请求；命令无真实来源时显示空态。
- [x] 验证：完整后端、前端单元、浏览器交互及原生协作链路；原生会话 JSON 保存增量，不新增迁移；运行空闲后部署重启。

验收：后端 373、前端单元 107、Chrome 浏览器 106 项通过，构建成功；真实 Kafka 协作回到原主会话。记录见 [实施记录](../../../../egis-agentkit-app/docs/2026-11-06-会话协作与候选分页.md)。
