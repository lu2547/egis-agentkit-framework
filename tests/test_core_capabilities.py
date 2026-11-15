"""验证 Project、Workspace Core 可脱离管理服务和数据库独立工作。"""

import importlib

import pytest

from egis_agentkit.errors import Conflict, NotFound


def test_project_core_builds_configuration_and_guards_lifecycle():
    Project = importlib.import_module("egis_agentkit.core.project._project").Project
    core = Project()
    project = core.build("alice", "  Research  ")
    assert project["owner_id"] == "alice"
    assert project["project_name"] == "Research"
    assert project["status_type"] == "active" and not project["is_default"]
    assert core.require_available(project) is project
    with pytest.raises(Conflict):
        core.build("alice", "   ")
    deleting = {**project, "status_type": "deleting"}
    with pytest.raises(Conflict):
        core.require_available(deleting)
    assert core.require_available(deleting, allow_deleting=True) is deleting
    with pytest.raises(NotFound):
        core.require_available(None)
