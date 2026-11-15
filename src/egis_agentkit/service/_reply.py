"""回复正文及分支对话上下文：展示历史和模型运行记录分别处理。"""

from agentscope.message import Msg, TextBlock, URLSource


def get_final_reply_blocks(message: Msg):
    """与工作台一致：工具/思考之后的正文才是最终回答，内部提示不导出。"""
    visible = [
        block
        for block in message.content
        if block.type in {"thinking", "tool_call", "hint", "data"}
        or block.type == "text"
        and block.text.strip()
    ]
    start = len(visible)
    while start and visible[start - 1].type in {"text", "hint", "data"}:
        start -= 1
    return [block for block in visible[start:] if block.type in {"text", "data"}]


def get_reply_text(message: Msg):
    blocks = (
        get_final_reply_blocks(message)
        if message.role == "assistant"
        else message.content
    )
    return "\n\n".join(block.text for block in blocks if block.type == "text")


def build_branch_context(messages: list[Msg], native_context: list[Msg]):
    """保留对话和附件引用，不重放 UI 内完整工具输出、旧 Team 指令或执行控制块。"""
    normalized_users = {msg.id: msg for msg in native_context if msg.role == "user"}
    result = []
    for message in messages:
        if message.role == "user":
            normalized = normalized_users.get(message.id)
            if normalized is not None:
                content = normalized.content
            else:
                content = []
                for block in message.content:
                    if block.type != "data" or block.source.media_type.startswith(
                        "image/"
                    ):
                        content.append(block)
                    else:
                        # 历史已压缩时原转换记录可能不存在。保留真实 URL；没有路径则明确格式，不虚构位置。
                        location = (
                            str(block.source.url)
                            if isinstance(block.source, URLSource)
                            else ""
                        )
                        description = (
                            f"附件 {block.name or ''}：{location}"
                            if location
                            else f"附件 {block.name or ''}（{block.source.media_type}），请从项目文件中读取。"
                        )
                        content.append(TextBlock(text=description))
        elif message.role == "assistant":
            content = get_final_reply_blocks(message)
        else:
            continue
        if content:
            result.append(message.model_copy(update={"content": content}, deep=True))
    return result
