"""从 AgentScope 原生协作提示中提取消息正文。"""

import json
import re

from agentscope.event import HintBlockEvent
from agentscope.message import HintBlock

_TEAM_MESSAGE = re.compile(r'<team-message from="[^"]+">\n(.*)\n</team-message>$', re.S)
_TEAM_MESSAGE_LABELS = ("team", "team_message")


def team_message_text(hint: HintBlock | HintBlockEvent) -> str | None:
    """识别 AgentInvite 和 TeamSay 写入 Inbox 的消息。"""
    if not isinstance(hint.hint, str):
        return None
    try:
        source = json.loads(hint.source or "{}")
    except ValueError:
        return None
    if not isinstance(source, dict) or source.get("label") not in _TEAM_MESSAGE_LABELS:
        return None
    match = _TEAM_MESSAGE.search(hint.hint)
    if match and match[1].strip():
        return match[1]
    return None
