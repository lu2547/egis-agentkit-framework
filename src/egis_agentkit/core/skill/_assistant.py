"""定义平台 Skills 助手的稳定身份。"""

from uuid import NAMESPACE_URL, uuid5

SKILL_ASSISTANT_NAME = "skills-assistant"
SKILL_ASSISTANT_ID = uuid5(NAMESPACE_URL, f"egis-agentkit:{SKILL_ASSISTANT_NAME}").hex
