# 项目入口与 Office 读取修复

**目标：** 首页新对话默认选“默认项目”，可切项目；历史会话只读显示项目。项目旁 `+` 打开该项目新会话首页。修复 DOCX 被 Read 当 ZIP 文本读取导致 PG replay 写入空字符失败。

**实现边界：** 独立 Vue 组件/CSS 继续深浅灰主题；复用项目和任务 API，不提供历史会话移动。文档读取使用原生 Read 的 ToolMiddlewareBase 扩展与原生 WordParser/PPTParser/ExcelParser，不改 AgentScope service、Read 或模型执行源码。原生权限/模型能力处理保留。

**用户最新决定：** Read 改造先讨论，本轮不实施。曾在本地测试的 DocumentReadMiddleware、Workspace 接入、新依赖及对应测试已撤出项目，未部署，草案仅暂存在 `/private/tmp/agentkit-read-discussion-draft`。下述文档读取方案是待讨论的提案。当前只交付项目入口变化。

- [x] 项目首页选择器、只读标签、项目新建会话按钮，先 RED 后 GREEN；保留草稿、附件和当前会话归属。
- [x] 复现 Office 乱码、PG JSONB 空字符与连带工具中断；复现测试随 Read 草案撤出，留待讨论。
- [ ] 待讨论、不实施：文档读取中间件支持 DOCX/PPTX/XLSX/XLS，输出有界文本、表格；无法解析或二进制返回可恢复工具错误。其他格式继续原生读取。
- [x] 项目入口前端单元/浏览器全量验证、构建；只更新前端，无后端部署。107 项单元、112 项浏览器回归通过，构建成功。

**重点验证：** 含空格中文文件名、权限拒绝不先读文件、原生 PDF/图像/代码行为不回退、损坏包、输出 offset/limit 和上限，项目选择无跨项目误上传、窄屏下拉不越界。
