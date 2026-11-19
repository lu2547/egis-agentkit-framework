"""Generate an avatar using the existing chat model catalogue."""

import asyncio
import json
import logging

from agentscope.message import SystemMsg, UserMsg

from egis_agentkit.core.skill._avatar import AVATAR_PROMPT, SkillAvatarDrawing
from egis_agentkit.service._model import ModelService

logger = logging.getLogger(__name__)


async def generate_skill_avatar(user, storage, config, name, description):
    if config is None:
        return None
    snapshot = config.snapshot
    settings = snapshot.skill.avatar
    if not settings.enabled:
        return None
    try:
        async with asyncio.timeout(settings.timeout_seconds):
            model = await ModelService(storage, config).build_chat_model(
                user, settings.model_key, snapshot=snapshot
            )
            result = await model.generate_structured_output(
                messages=[
                    SystemMsg("system", AVATAR_PROMPT),
                    UserMsg(
                        "user",
                        json.dumps(
                            {"name": name, "description": description},
                            ensure_ascii=False,
                        ),
                    ),
                ],
                structured_model=SkillAvatarDrawing,
            )
            return SkillAvatarDrawing.model_validate(result.content).render()
    except Exception as exc:
        logger.warning("skill.avatar_generation_failed (%s)", type(exc).__name__)
        return None
