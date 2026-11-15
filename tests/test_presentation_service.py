"""项目幻灯片管理：权限、摘要冲突及真实文件保存。"""

import hashlib

import pytest
from test_domain import identities
from test_presentation import FIXTURE
from test_workspace_service import make_workspace

from egis_agentkit.errors import Conflict, NotFound
from egis_agentkit.service.project._presentation_manager import (
    ProjectPresentationManager,
)


@pytest.mark.asyncio
async def test_page_management_saves_original_project_file_and_rejects_stale_revision(
    storage, tmp_path
):
    _, _, alice, bob = await identities(storage)
    svc = make_workspace(storage, tmp_path, max_bytes=20_971_520)
    manager = ProjectPresentationManager(svc.workspace)
    project = await svc.projects.create(alice, "PPT 编辑")
    pid = project["id"]
    original = FIXTURE.read_bytes()
    await svc.workspace.upload_project_file(alice, pid, "slides.pptx", original)
    info = await manager.get_presentation(alice, pid, "uploads", "slides.pptx")
    assert len(info["slides"]) == 2
    assert info["checksum"] == hashlib.sha256(original).hexdigest()
    with pytest.raises(NotFound):
        await manager.get_presentation(bob, pid, "uploads", "slides.pptx")
    saved = await manager.update_slides(
        alice,
        pid,
        "uploads",
        "slides.pptx",
        "add",
        info["slides"][0]["id"],
        info["checksum"],
    )
    assert len(saved["slides"]) == 3
    assert saved["slides"][1]["id"] == saved["active_slide_id"]
    actual, _ = await svc.workspace.read_project_file(
        alice, pid, "uploads", "slides.pptx"
    )
    assert hashlib.sha256(actual).hexdigest() == saved["checksum"]
    with pytest.raises(Conflict):
        await manager.update_slides(
            alice,
            pid,
            "uploads",
            "slides.pptx",
            "add",
            info["slides"][0]["id"],
            info["checksum"],
        )
    with pytest.raises(Conflict):
        await svc.workspace.preview_project_document(
            alice, pid, "uploads", "slides.pptx", checksum=info["checksum"]
        )
    assert (await manager.get_presentation(alice, pid, "uploads", "slides.pptx"))[
        "checksum"
    ] == saved["checksum"]
