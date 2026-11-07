"""JSON-only skill activation stored inside native AgentState.middle_context."""

from pydantic import Field

from egis_agentkit.models import ResourceRef


class SkillActivation(ResourceRef):
    source_checksum_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    cache_checksum_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    cache_dir: str = Field(min_length=1)
    skill_name: str = Field(min_length=1)

    def reference(self):
        return {key: getattr(self, key) for key in ResourceRef.model_fields}
