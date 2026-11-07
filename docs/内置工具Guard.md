# 内置工具 Guard

内置工具的防护按工具拆文件，显式挂载；不更改 AgentScope service 源码，不在全局工具入口按名称分发。

```text
extensions/middleware/builtin_tool_guard/
├── __init__.py
└── read_tool_guard.py   # ReadToolGuardMiddleware(ToolMiddlewareBase)
```

`ManagedLocalWorkspace.list_tools()` 在原生 Read 构造时传入 `middlewares=[ReadToolGuardMiddleware(backend)]`。Read 的类型、参数校验、权限检查、文本分页与媒体处理继续原生；Guard 通过同一个 Backend 访问文件。其他工具没有挂载此 Guard。

## Read

```text
原生参数与权限检查
  → ReadToolGuardMiddleware
      → 原生支持的 PDF / 图片：跳过二进制预检
      → 其他文件：已知二进制后缀 + 最多 4 KB 文件头检查
          → 二进制：返回 ToolChunk(ERROR)
          → 文本：原生 Read
  → 文本输出空字符检查
  → 原生工具事件、ToolResponse、上下文和模型下一轮
```

检测到不支持的二进制时，返回：

> Read 工具不支持此类二进制文件读取，请先转成 Markdown，或尝试使用其他工具。

已知二进制包括 Office、压缩包、可执行文件等；未知格式采样含空字节或不可打印字符占比超过 30% 时拒绝。流只消费第一块并关闭，不为预检读取整个文件。

有限采样不能证明整个文件为文本，因此对原生返回的 TextBlock 再检查空字符，包括 PDF 文本抽取结果。异常文本返回明确的工具错误，避免进入 PG JSONB replay；图片 DataBlock 不做文本转换。磁盘文件不被修改，不执行转换或 OCR，不引入文档解析依赖。

工作区系统提示同步收窄：提供项目真实绝对路径；明确 Read 只用于文本、PDF 和原生支持的图片；Office 等文件先转换 Markdown 或使用其他工具。删除“不要猜测用户 uploads 归档路径”的旧指令。
