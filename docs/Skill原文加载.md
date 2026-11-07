# Skill 原文加载

导入包的 `SKILL.md` 原文保存在资源 payload.original_markdown。Nacos 发布传输文件为注册身份重写 frontmatter，不能直接当作运行内容。

下载先校验 Nacos 传输包 checksum，再还原原始 SKILL.md；编辑、导出和运行物化统一使用这条链路。物化后的完整文件摘要决定缓存目录，新内容生成新目录，不修改仍被使用的旧缓存。其他脚本和文件按下载包保持完整，不修改供应方完整性校验。

2026-11-06 定位：ppt-master 6.8.0 原 ZIP 与已保存原文均保留 metadata 的引号，但 materialize_skill 使用 raw_source=True，导致运行缓存使用 safe_dump 重新序列化的字段。现已去掉运行链路该参数，让运行文件和编辑/导出一致。此前 Skill 自身的失败提示不代表原始包有错。

按用户要求未运行测试；修改后检查语法，空闲时重启服务。下一次技能激活按还原后的文件摘要物化新缓存。
