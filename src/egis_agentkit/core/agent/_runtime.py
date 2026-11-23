"""Defaults for the AgentScope runtime clock."""

from agentscope.agent import Agent, InjectionConfig

DEFAULT_TIMEZONE = "Asia/Shanghai"


class AgentKitAgent(Agent):
    """Use Beijing time for the native runtime clock by default."""

    def __init__(
        self, *args, injection_config: InjectionConfig | None = None, **kwargs
    ):
        if injection_config is None:
            # Adding the offset also refreshes timestamps from older UTC contexts.
            injection_config = InjectionConfig(
                timezone=DEFAULT_TIMEZONE,
                time_format="%Y-%m-%dT%H:%M:%S%z",
            )
        super().__init__(*args, injection_config=injection_config, **kwargs)
