# 个人助理的三份配置文档

默认模板位于包内 `src/egis_agentkit/templates/preferences/`：`agent.md`、`soul.md`、`user.md`。内容来自用户提供的 `Code_20261105.txt`，按原文件的三个 front matter 分段提取，正文保持不变。接口继续使用 `agent_md`、`soul_md`、`user_md`，运行装配沿用现有 PreferenceService 和原生 middleware 链。

读取规则：用户已经保存的文件优先；只有文件缺失才返回对应的默认模板。用户明确保存的空白内容也会保留。查看页面不会写入文件，保存仍写入该用户工作区的 preferences 目录，用户之间隔离。

界面使用顶部三个文件标签和下方编辑区，切换文件保留尚未保存的修改，保存提交三份文档。模板作为 framework 包资源分发，不依赖开发者机器上的原文件路径。
