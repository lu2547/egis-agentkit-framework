# 原生 Read 工具 Guard

**已确认方案：** `extensions/middleware/builtin_tool_guard/read_tool_guard.py` 定义 `ReadToolGuardMiddleware(ToolMiddlewareBase)`；在 `ManagedLocalWorkspace.list_tools()` 显式挂到原生 Read，其他工具不经过它。不增加文档解析器、不改 AgentScope service 源码。系统提示只说明实际支持格式和项目路径，删除 uploads 归档猜测指令。

- [x] 先补二进制故障回归，确认 Office/未知二进制/采样后空字符/旧 prompt 用例失败。
- [x] 原生支持的图片/PDF 直接放行；其他文件按已知二进制扩展名和 4 KB 文件头检查，返回可恢复 `ToolChunk(ERROR)`，保留原生权限、文本分页和路径错误。
- [x] 文本结果额外检查空字符，防止采样未覆盖的位置污染 PG replay；输出错误后原生 Toolkit 仍可继续调用其他工具。
- [ ] 后端全量回归（开启 PG/Kafka/Nacos 集成）、空闲后重启服务、真实模型工具错误后继续回复的验收。

Guard 的文件读取使用原生 Backend，不读取宿主固定路径。文件头流只请求一块并关闭，无完整文件预读；图片仍交原生 Read 做模型能力检查。旧目录的文档读取解析中间件提案已被本方案替代，不部署。
