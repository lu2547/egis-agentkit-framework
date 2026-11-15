from types import SimpleNamespace

import pytest

from egis_agentkit.service.agent._personal_manager import PersonalManager


@pytest.mark.asyncio
async def test_new_assistant_reads_three_bundled_default_documents(storage, tmp_path):
    service = PersonalManager(storage, workspace_root=tmp_path)
    documents = await service.get_preferences(SimpleNamespace(user_id="alice"))
    assert set(documents) == {"agent_md", "soul_md", "user_md"}
    assert "# AGENTS.md - 工作空间" in documents["agent_md"]
    assert "# SOUL.md - 你是谁" in documents["soul_md"]
    assert "# USER.md - 关于你的用户" in documents["user_md"]
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
    assert "# SOUL.md - 你是谁" in bob["soul_md"]
    assert bob["user_md"] != saved["user_md"]


@pytest.mark.asyncio
async def test_only_missing_preference_files_use_defaults(storage, tmp_path):
    service = PersonalManager(storage, workspace_root=tmp_path)
    root = tmp_path / "user/alice/preferences"
    root.mkdir(parents=True)
    (root / "user.md").write_text("# 已维护的背景", encoding="utf-8")
    documents = await service.get_preferences(SimpleNamespace(user_id="alice"))
    assert documents["user_md"] == "# 已维护的背景"
    assert "## 安全红线" in documents["agent_md"]
    assert "## 核心准则" in documents["soul_md"]
