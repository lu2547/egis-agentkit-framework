"""回复反馈管理：检查身份及会话归属，保存供管理员排查的不可伪造上下文。"""

from egis_agentkit.client.nacos._settings import SessionFeedbackConfig
from egis_agentkit.errors import InvalidRequest, NotFound
from egis_agentkit.service._reply import get_reply_text
from egis_agentkit.service._user import UserService, require_admin


class SessionFeedbackService:
    def __init__(self, storage, sessions, config_provider=None):
        self.storage, self.sessions, self.config = storage, sessions, config_provider

    def _settings(self):
        return (
            self.config.snapshot.session.feedback
            if self.config
            else SessionFeedbackConfig()
        )

    async def list_reply_feedback(self, user, session_id):
        await self.sessions.get_session_record(user, session_id, require_use=False)
        return await self.storage.session_feedback.list_reply_feedback(
            user.user_id, session_id
        )

    async def save_reply_feedback(self, user, session_id, reply_id, comment):
        user = await UserService(self.storage).get(user.user_id)
        session = await self.sessions.get_session_record(
            user, session_id, require_use=False
        )
        settings = self._settings()
        comment = comment.strip()
        if not comment or len(comment) > settings.max_comment_length:
            raise InvalidRequest(
                f"请填写反馈内容，最多 {settings.max_comment_length} 个字符"
            )
        message = await self.storage.get_message(user.user_id, session_id, reply_id)
        if message is None or message.role != "assistant" or not message.finished_at:
            raise NotFound("回复不存在或尚未完成")
        project = await self.sessions.projects.session_project(user, session)
        binding = session.state.middle_context["egis_agentkit"]
        definition = await self.storage.definitions.get_definition_by_reference(binding)
        question = await self.storage.sessions.get_question_before_reply(
            session_id, reply_id
        )
        return await self.storage.session_feedback.save_reply_feedback(
            {
                "user_id": user.user_id,
                "username": user.username,
                "project_id": project["id"],
                "project_name": project["project_name"],
                "session_id": session_id,
                "session_name": session.config.name,
                "reply_id": reply_id,
                "agent_id": session.agent_id,
                "definition_id": binding["definition_id"],
                "agent_display_name": definition.get("team_display_name")
                or definition.get("agent_display_name")
                or message.name,
                "comment": comment,
                "question_text": (get_reply_text(question) if question else "")[
                    : settings.max_snapshot_chars
                ],
                "reply_text": get_reply_text(message)[: settings.max_snapshot_chars],
            }
        )

    async def page_reply_feedback(self, user, *, limit=None, cursor=None):
        require_admin(await UserService(self.storage).get(user.user_id))
        settings = self._settings()
        limit = settings.default_page_size if limit is None else limit
        if not 1 <= limit <= settings.max_page_size:
            raise InvalidRequest(f"分页大小必须介于 1 和 {settings.max_page_size} 之间")
        return await self.storage.session_feedback.page_reply_feedback(
            limit=limit, cursor=cursor
        )
