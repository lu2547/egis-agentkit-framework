"""保留原生引导的消息身份和批注，仅扩展展示与历史，不参与模型投递。"""

import json

from agentscope.app._chat_input_store import hydrate_input
from agentscope.app.message_bus import MessageBusKeys
from agentscope.message import UserMsg


def steering_presentation_namespace(session_id):
    return f"egis:steering_presentations:{session_id}"


class SteeringPresentation:
    def __init__(self, bus, storage):
        self.bus, self.storage = bus, storage

    async def stage(self, key, matches):
        """在原生确认移除前保存展示元数据；失败时原生仍可回滚这次注入。"""
        prefix = MessageBusKeys.chat_inputs("")
        if not key.startswith(prefix) or not matches:
            return []
        entries = await self.bus.queue_read(key, MessageBusKeys.CHAT_INPUT_MAX_LEN)
        selected = []
        for expected in matches:
            candidates = [
                payload
                for _, payload in entries
                if all(payload.get(name) == value for name, value in expected.items())
            ]
            if len(candidates) != 1 or candidates[0]["id"] in [
                item["id"] for item in selected
            ]:
                return []
            selected.append(candidates[0])
        sid = key[len(prefix) :]
        staged = []
        for payload in selected:
            if payload.get("state") != "steering":
                continue
            hydrated = await hydrate_input(self.bus, payload)
            if hydrated is None:
                raise ValueError("引导消息内容已不可用")
            inputs = hydrated["input"]
            inputs = inputs if isinstance(inputs, list) else [inputs]
            # 文本与图片由原生 HintBlock 保存，辅助记录不再复制这些内容。
            value = {
                "user_id": payload["user_id"],
                "inputs": [
                    {
                        key: message[key]
                        for key in ("id", "name", "created_at", "metadata")
                    }
                    for message in inputs
                ],
            }
            await self.bus.registry_set(
                steering_presentation_namespace(sid), payload["id"], json.dumps(value)
            )
            staged.append(payload["id"])
        return staged

    async def discard(self, sid, identifiers):
        for identifier in identifiers:
            await self.bus.registry_del(
                steering_presentation_namespace(sid), identifier
            )

    async def project(self, sid, event):
        """实际 HintBlock 已产生，保存原消息并把相同展示信息写入回放事件。"""
        block_id = event.get("block_id", "")
        identifier, separator, index = block_id.rpartition(":")
        if not separator or not index.isdecimal():
            return
        raw = await self.bus.registry_get(
            steering_presentation_namespace(sid), identifier
        )
        if raw is None:
            return
        value = json.loads(raw)
        position = int(index)
        if position >= len(value["inputs"]):
            return
        original = value["inputs"][position]
        message = UserMsg(
            id=original["id"],
            name=original["name"],
            created_at=original["created_at"],
            content=event["hint"],
            metadata={
                **original["metadata"],
                "agentkit_delivery": {
                    "kind": "steer",
                    "state": "received",
                    "hint_id": block_id,
                },
            },
        )
        await self.storage.upsert_message(value["user_id"], sid, message)
        event["user_message"] = message.model_dump(mode="json")
        if position == len(value["inputs"]) - 1:
            await self.discard(sid, [identifier])
