"""Named services composed by the hosting application, without HTTP dependencies."""

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .service.access.authorization import PermissionService
    from .service.access.sharing import ShareService
    from .service.agent.definition import AgentService
    from .service.agent.detail import DefinitionDetailService
    from .service.agent.package import PackageService
    from .service.model.service import ModelService
    from .service.project.service import ProjectService
    from .service.resource.imports import SkillImportService
    from .service.resource.service import ResourceService
    from .service.session.composer import ComposerService
    from .service.session.service import TaskService
    from .service.team.definition import TeamService
    from .service.user.preferences import PreferenceService
    from .service.user.service import UserService
    from .service.workspace.service import WorkspaceService


@dataclass(slots=True)
class AgentKitServices:
    users: "UserService"
    agents: "AgentService"
    teams: "TeamService"
    permissions: "PermissionService"
    sharing: "ShareService"
    models: "ModelService"
    preferences: "PreferenceService"
    projects: "ProjectService"
    packages: "PackageService"
    resources: "ResourceService"
    tasks: "TaskService"
    workspace: "WorkspaceService"
    definition_details: "DefinitionDetailService"

    skill_imports: "SkillImportService | None" = None
    composer: "ComposerService | None" = None
