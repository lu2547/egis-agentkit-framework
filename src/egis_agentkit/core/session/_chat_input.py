"""规范聊天队列的用户身份，校验消息批量、内容及大小。"""

from agentscope.message import Msg

from egis_agentkit.errors import InvalidRequest

NATIVE_MESSAGE_ID_MAX_LENGTH = 255


def prepare_chat_inputs(user, inputs, settings):
    """只接受用户文本/附件，服务端生成投递状态，不接受伪造的 Agent 事件。"""
    messages = inputs if isinstance(inputs, list) else [inputs]
    if not messages or len(messages) > settings.max_batch_size:
        raise InvalidRequest(f"每次引导需包含 1 到 {settings.max_batch_size} 条消息")
    result, identifiers = [], set()
    for message in messages:
        if not isinstance(message, Msg) or message.role != "user":
            raise InvalidRequest("聊天队列只能包含用户消息")
        if (
            not message.id
            or len(message.id) > NATIVE_MESSAGE_ID_MAX_LENGTH
            or message.id in identifiers
        ):
            raise InvalidRequest("引导消息 ID 无效或重复")
        identifiers.add(message.id)
        if not message.content or any(
            block.type not in {"text", "data"} for block in message.content
        ):
            raise InvalidRequest("引导只支持文本和附件")
        if not any(
            block.type == "data" or block.text.strip() for block in message.content
        ):
            raise InvalidRequest("引导内容不能为空")
        if len(message.get_text_content()) > settings.max_text_length:
            raise InvalidRequest(f"引导正文最多 {settings.max_text_length} 个字符")
        canonical = message.model_copy(
            deep=True,
            update={
                "name": user.username,
            },
        )
        if len(canonical.model_dump_json().encode()) > settings.max_message_bytes:
            raise InvalidRequest("引导消息超过配置的大小限制")
        result.append(canonical)
    return result
