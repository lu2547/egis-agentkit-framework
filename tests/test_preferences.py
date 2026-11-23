from importlib.resources import files
from types import SimpleNamespace

import pytest

from egis_agentkit.service.agent._personal_manager import PersonalManager


def bundled(name):
    return (
        files("egis_agentkit.core.agent")
        .joinpath("templates", name + ".md")
        .read_text(encoding="utf-8")
    )


@pytest.mark.asyncio
async def test_new_assistant_reads_three_bundled_default_documents(storage, tmp_path):
    service = PersonalManager(storage, workspace_root=tmp_path)
    documents = await service.get_preferences(SimpleNamespace(user_id="alice"))
    assert set(documents) == {"agent_md", "soul_md", "user_md"}
    assert documents["agent_md"] == bundled("agent")
    assert documents["soul_md"] == bundled("soul")
    assert documents["user_md"] == bundled("user")
    assert not (tmp_path / "user/alice/preferences").exists()


@pytest.mark.asyncio
async def test_saved_preferences_and_intentionally_empty_document_are_preserved(
    storage, tmp_path
):
    service = PersonalManager(storage, workspace_root=tmp_path)
    alice = SimpleNamespace(user_id="alice")
    saved = {"agent_md": "# 自定义规范", "soul_md": "", "user_md": "# 我的背景"}
    assert await service.save_preferences(alice, saved) == saved
    assert await service.get_preferences(alice) == saved
    revised = {**saved, "agent_md": "# 更新规范"}
    assert await service.save_preferences(alice, revised) == revised
    assert await service.get_preferences(alice) == revised
    assert (tmp_path / "user/alice/preferences/agent.md").read_text() == "# 更新规范"
    bob = await service.get_preferences(SimpleNamespace(user_id="bob"))
    assert bob["soul_md"] == bundled("soul")
    assert bob["user_md"] != saved["user_md"]


@pytest.mark.asyncio
async def test_only_missing_preference_files_use_defaults(storage, tmp_path):
    service = PersonalManager(storage, workspace_root=tmp_path)
    root = tmp_path / "user/alice/preferences"
    root.mkdir(parents=True)
    (root / "user.md").write_text("# 已维护的背景", encoding="utf-8")
    documents = await service.get_preferences(SimpleNamespace(user_id="alice"))
    assert documents["user_md"] == "# 已维护的背景"
    assert documents["agent_md"] == bundled("agent")
    assert documents["soul_md"] == bundled("soul")
