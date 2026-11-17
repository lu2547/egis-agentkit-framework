"""校验用户引导并构造原生 HintBlock，不执行投递或持久化。

核心能力
- prepare_steering 规范用户身份，校验批量、正文和消息大小。
- steering_hint 保持消息 ID 和顺序，将用户内容交给原生 Inbox 消费。
- steering_identity 比较用户内容，排除自动生成的块 ID、时间和投递状态。
"""

from enum import StrEnum

from agentscope.message import HintBlock, Msg

from egis_agentkit.errors import InvalidRequest

STEERING_SOURCE = "user_steering"
STEERING_EVENT = "user_steering"
DELIVERY_METADATA = "agentkit_delivery"
NATIVE_MESSAGE_ID_MAX_LENGTH = 255


class ChatDelivery(StrEnum):
    MESSAGE = "message"
    STEER = "steer"


def prepare_steering(user, inputs, settings):
    """只接受用户文本/附件，服务端生成投递状态，不接受伪造的 Agent 事件。"""
    messages = inputs if isinstance(inputs, list) else [inputs]
    if not messages or len(messages) > settings.max_batch_size:
        raise InvalidRequest(f"每次引导需包含 1 到 {settings.max_batch_size} 条消息")
    result, identifiers = [], set()
    for message in messages:
        if not isinstance(message, Msg) or message.role != "user":
            raise InvalidRequest("引导只能包含用户消息")
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
                "metadata": {
                    **message.metadata,
                    DELIVERY_METADATA: {"kind": ChatDelivery.STEER, "state": "queued"},
                },
            },
        )
        if len(canonical.model_dump_json().encode()) > settings.max_message_bytes:
            raise InvalidRequest("引导消息超过配置的大小限制")
        result.append(canonical)
    return result


def steering_hint(message):
    """使用同一个 ID 关联真实用户消息、Inbox 内容和模型接收事件。"""
    return HintBlock(
        id=message.id,
        source=STEERING_SOURCE,
        hint=message.content,
        created_at=message.created_at,
    )


def steering_identity(message):
    """比较用户内容，排除 SDK 可自动生成的块 ID、时间和服务端投递状态。"""
    return {
        "role": message.role,
        "content": [
            block.model_dump(mode="json", exclude={"id", "created_at", "finished_at"})
            for block in message.content
        ],
        "metadata": {
            key: value
            for key, value in message.metadata.items()
            if key != DELIVERY_METADATA
        },
    }
