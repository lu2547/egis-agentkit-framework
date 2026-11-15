# Java 知识库工具和可追溯来源 Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans inline. 用户明确要求直接实现、独立目录、无需提问；新分支只在隔离目录开发，不合并、不推送。

**Goal:** 用户提问时由 Agent 自动选择知识库 Function Tool，回答来源随消息持久化，右侧可查看来源文档和命中片段。
**Architecture:** JavaKnowledgeClient 按文档 OpenAPI HMAC 协议请求现有 Java API；Java 继续承接 LlamaIndex、ACL 和 llm_* 存储。Core 处理知识库范围、引用归一化与消息读取；KnowledgeManager 组合身份、Client 和会话检查。SearchKnowledge 作为系统默认 Function Tool 登记，通过原生 ToolBase 和 extra_agent_tools 回调接入，沿用原生模型循环、ToolResult、SSE、消息落库。前端从真实工具结果恢复来源，正文引用和右侧面板相互定位。
**Tech Stack:** Python/httpx/Pydantic/AgentScope、Vue/TypeScript、Nacos、pytest/Vitest/Playwright。
**Spec:** 用户 2026-11-16 的两份 DOCX；完整原件和 SHA256 在 ../contracts。Java 检索接口第8节、HMAC第11节、权限第12节；数据库 llm_* 归 Java 服务所有。

## Decisions
- 不引入第二套 LlamaIndex，也不使用原生 KnowledgeBase CRUD/表；后者不能直接表达 Java HMAC、QueryScope 和既有文档契约。
- 使用 POST /openapi/v1/query 和 GET /openapi/v1/kb。签名 PATH 不含 /knowledge servlet context；请求体字节与签名完全一致，query 参数按 key 排序。
- 应用权限不等于用户权限：Nacos 显式 user/group → kbIds 授权，默认拒绝，真实用户/启用组每次重读；查询范围和应用可见库求交，模型不能扩大范围。
- 原始文件与预览使用用户 JWT Cookie 接口；优先 /kb/{kb_id}/documents/{doc_id}/raw/preview，缺少 KB 身份时使用 /query/documents/{docId}/raw。由经过会话+当前授权检查的 App 接口返回 Java resolver URL，浏览器使用知识库自身 Cookie 请求，不伪造 OpenAPI 文件接口或代用应用权限。
- 返回片段、章节、docId、parentId，ver 只在唯一可匹配的 RecallItem 明确提供时显示；没有页码、版本或 KB ID 时不编造。不承诺上游接口未提供的历史版本原件。
- 来源存于原生 ToolResult JSON；以 replyId/callId/source index 绑定，不新增知识库和引用表，不信任用户文本伪造引用。
- 所有可调限额在 agentkit.knowledge 下；HTTP Secret 只引用环境变量，业务错误不泄露上游正文和签名。
- 单独的开发数据库/Nacos group/Kafka topic、端口及 workspace；真实 Java 地址/凭证缺失时仅在显式 contract-preview 模式启动测试契约服务，生产能力没有本地假检索回退。

## Review Focus
- HMAC 的 context path、中文 JSON 字节、空 body、排序 query、nonce 和过期 timestamp。
- 管理员无隐式知识库权限、停用组/用户、授权撤销、模型扩大 scope、返回文档 KB ID 缺失时保守校验。
- 超时、非零 code、异常 JSON/类型、超大响应、重定向、缺凭证与错误正文泄密。
- 多次检索编号冲突、SSE 重放、刷新、跨会话切换/旧请求迟到、伪造消息和未完成回复的原件请求。
- 无权限/无引用/无原件链接、恶意 HTML/URL、窄屏、深浅主题、原文弹窗和面板键盘操作。

### Task 1 客户端与核心规则
- [x] tests/test_knowledge_client.py：先 RED；固定 HMAC 向量、精确请求/响应契约、错误边界。
- [x] client/knowledge/_client.py、_models.py、_signing.py；core/knowledge/_sources.py、_access.py。
- [x] client/nacos/_knowledge_settings.py；配置加载/重启要求与 examples YAML。
- [x] tests/test_knowledge_access.py：默认拒绝和用户/启用组范围，引用去重/版本不编造。

### Task 2 运行工具及管理接口
- [x] service/knowledge/_knowledge_manager.py：真实身份和 KB 范围检查、查询、已保存回复来源校验、原件 resolver。
- [x] extension/knowledge/_knowledge_search_tool_impl.py；登记系统默认工具，BoundToolFactory 按权限装配。
- [x] bootstrap/_init.py / _services.py 组装、关闭客户端；App controller/knowledge。
- [x] tests/test_knowledge_runtime.py：原生工具实际调用、权限绕过拒绝、消息持久化及回放；App HTTP 原件接口鉴权。

### Task 3 来源展示
- [x] domain/knowledgeSources.ts、api/knowledge.ts、KnowledgeSourcesPanel.vue、回答来源按钮/正文链接。
- [x] WorkspacePanel 增加知识来源标签，与原有文件/预览共存；切会话清状态、异步取消，不覆盖未保存编辑。
- [x] Vitest 解析测试；浏览器测试验证从问题到工具结果/回答/右侧来源，刷新可恢复和原件入口。

### Task 4 隔离环境与验收
- [x] scripts/knowledge_dev.py + 独立运行参数；不修改当前运行目录/服务数据。
- [x] tests/contracts/java_kb_server.py 仅作协议验收；实际模型通过正常 /chat 和 SSE 自动调用工具。
- [x] 全量 Python、前端单测/类型构建、关键浏览器测试、深浅主题截图和安全失败路径。
- [x] 一次独立终审，记录问题并按 RED→GREEN 修复；交付目录、启动方式、已验证范围和真实接入配置。
