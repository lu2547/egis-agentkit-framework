from agentscope.middleware import MiddlewareBase

from egis_agentkit.service.team.collaboration import collaborators


class TeamPromptMiddleware(MiddlewareBase):
    """Describe a verified roster without changing native membership."""

    def __init__(self, scope):
        self.scope = scope

    async def on_system_prompt(self, agent, current_prompt):
        invited = collaborators(self.scope.binding)
        if invited:
            roster = [
                m["snapshot"]["root"]["agent_name"] + "@" + m["native_agent_id"][:8]
                for m in invited if m["status"] == "joined"
            ]
            return (
                current_prompt
                + "\n用户已明确邀请以下专家在同一项目协作。成员只能通过 TeamSay 通信；"
                + "使用以下完整收件人名称分工、汇总成果，不得创建或邀请额外成员："
                + ", ".join(roster)
            )
        if self.scope.binding.get("collaboration_parent_session_id"):
            return (
                current_prompt
                + "\n你是用户明确邀请的专家成员；与组长共用当前项目目录，"
                + "完成任务后通过 TeamSay 汇报；不得创建或邀请成员。"
            )
        if not self.scope.binding["is_team"]:
            return current_prompt
        roster = [
            node["data"]["agent_name"]
            for node in self.scope.snapshot["nodes"].values()
            if node["resource_type"] == "agent"
        ]
        return (
            current_prompt
            + "\n这是固定专家团，不得创建或邀请新成员。用 TeamSay(to=成员英文名,content=任务)协作。成员："
            + ", ".join(roster)
        )
