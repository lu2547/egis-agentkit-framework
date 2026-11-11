"""Named services exposed to the hosting application, without HTTP dependencies."""

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..service._composer import ComposerService
    from ..service._model import ModelService
    from ..service._project import ProjectService
    from ..service._session import SessionService
    from ..service._user import UserService
    from ..service.access._grant import GrantService
    from ..service.access._share import ShareService
    from ..service.agent._expert_definition import ExpertDefinition
    from ..service.agent._personal_definition import PersonalDefinition
    from ..service.agent._query import AgentQuery
    from ..service.agent._remote_definition import RemoteDefinition
    from ..service.resource._catalog import ResourceService
    from ..service.resource._skill_import import SkillImportService
    from ..service.team._definition import TeamDefinition
    from ..service.workspace._files import WorkspaceService


@dataclass(slots=True)
class AgentKitServices:
    users: "UserService"
    agent_query: "AgentQuery"
    personal_definition: "PersonalDefinition"
    expert_definition: "ExpertDefinition"
    remote_definition: "RemoteDefinition"
    teams: "TeamDefinition"
    grants: "GrantService"
    sharing: "ShareService"
    models: "ModelService"
    projects: "ProjectService"
    resources: "ResourceService"
    sessions: "SessionService"
    workspace: "WorkspaceService"

    skill_imports: "SkillImportService | None" = None
    composer: "ComposerService | None" = None
