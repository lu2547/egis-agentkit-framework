"""实现 MiddlewareBase，把当前团队角色和成员信息写入系统提示。"""

from collections.abc import Callable, Sequence

from agentscope.middleware import MiddlewareBase

from egis_agentkit.core.agent._invite_tool import ExpertInvite


class TeamPromptMiddleware(MiddlewareBase):
    """Describe a verified roster without changing native membership."""

    def __init__(
        self,
        *,
        fixed_members: Sequence[str] | None = None,
        invited_member: bool = False,
        invited_members: Callable[[], Sequence[str] | None] | None = None,
    ):
        self.fixed_members = fixed_members
        self.invited_member = invited_member
        self.invited_members = invited_members

    async def on_system_prompt(self, agent, current_prompt):
        invited = self.invited_members() if self.invited_members else None
        if invited is not None:
            toolkit = getattr(agent, "toolkit", None)
            invite = await toolkit.get_tool("AgentInvite") if toolkit else None
            instruction = (
                "可通过 AgentInvite 邀请已勾选的候选专家"
                if type(invite) is ExpertInvite
                else "不得创建或邀请额外成员"
            )
            return (
                current_prompt
                + "\n用户已明确邀请以下专家在同一项目协作。成员只能通过 TeamSay 通信；"
                + f"使用以下完整收件人名称分工、汇总成果，{instruction}："
                + ", ".join(invited)
            )
        if self.invited_member:
            return (
                current_prompt
                + "\n你是用户明确邀请的专家成员；与组长共用当前项目目录，"
                + "完成任务后通过 TeamSay 汇报；不得创建或邀请成员。"
            )
        if self.fixed_members is None:
            return current_prompt
        return (
            current_prompt
            + "\n这是固定专家团，不得创建或邀请新成员。用 TeamSay(to=成员英文名,content=任务)协作。成员："
            + ", ".join(self.fixed_members)
        )
