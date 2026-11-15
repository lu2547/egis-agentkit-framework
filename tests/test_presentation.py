"""PPTX 页面结构操作：真实包、关系、原始部件和渲染输出。"""

import io
from pathlib import Path
from zipfile import ZipFile

import pytest
from lxml import etree

from egis_agentkit.client.nacos._settings import ProjectPresentationConfig
from egis_agentkit.core.project._presentation import Presentation
from egis_agentkit.errors import Conflict, InvalidRequest

FIXTURE = (
    Path(__file__).parents[2] / "egis-agentkit-frontend/tests/fixtures/preview.pptx"
)


def parts(content):
    with ZipFile(io.BytesIO(content)) as archive:
        return {name: archive.read(name) for name in archive.namelist()}


def test_add_slide_preserves_existing_parts_and_follows_current_slide():
    original = FIXTURE.read_bytes()
    deck = Presentation(original, ProjectPresentationConfig())
    slides = deck.list_slides()
    assert len(slides) == 2
    assert "Preview slide 1" in slides[0]["title"]
    updated, active = deck.update_slides("add", slides[0]["id"])
    new_deck = Presentation(updated, ProjectPresentationConfig())
    result = new_deck.list_slides()
    assert [row["id"] for row in result] == [slides[0]["id"], active, slides[1]["id"]]
    assert active not in {row["id"] for row in slides}
    before, after = parts(original), parts(updated)
    for name, content in before.items():
        if name not in {
            "ppt/presentation.xml",
            "ppt/_rels/presentation.xml.rels",
            "[Content_Types].xml",
            "docProps/app.xml",
        }:
            assert after[name] == content, name
    assert any(
        name not in before
        and name.startswith("ppt/slides/slide")
        and name.endswith(".xml")
        for name in after
    )


def test_copy_move_remove_keep_identity_and_reject_empty_document():
    deck = Presentation(FIXTURE.read_bytes(), ProjectPresentationConfig())
    first, second = deck.list_slides()
    copied, duplicate = deck.update_slides("duplicate", first["id"])
    deck = Presentation(copied, ProjectPresentationConfig())
    assert [r["id"] for r in deck.list_slides()] == [
        first["id"],
        duplicate,
        second["id"],
    ]
    assert deck.list_slides()[1]["title"] == first["title"]
    moved, _ = deck.update_slides("move", duplicate, 3)
    deck = Presentation(moved, ProjectPresentationConfig())
    assert [r["id"] for r in deck.list_slides()] == [
        first["id"],
        second["id"],
        duplicate,
    ]
    removed, active = deck.update_slides("remove", second["id"])
    deck = Presentation(removed, ProjectPresentationConfig())
    assert active == duplicate
    assert [r["id"] for r in deck.list_slides()] == [first["id"], duplicate]
    removed, _ = deck.update_slides("remove", duplicate)
    with pytest.raises(Conflict, match="至少"):
        Presentation(removed, ProjectPresentationConfig()).update_slides(
            "remove", first["id"]
        )


def test_presentation_limits_and_invalid_page_are_checked():
    content = FIXTURE.read_bytes()
    with pytest.raises(InvalidRequest):
        Presentation(b"not a zip", ProjectPresentationConfig())
    with pytest.raises(InvalidRequest):
        Presentation(content, ProjectPresentationConfig(max_expanded_bytes=1))
    deck = Presentation(content, ProjectPresentationConfig(max_slides=2))
    with pytest.raises(Conflict):
        deck.update_slides("add", deck.list_slides()[0]["id"])
    with pytest.raises(InvalidRequest):
        deck.update_slides("move", "not-a-slide", 1)
    deck = Presentation(content, ProjectPresentationConfig(max_entries=len(parts(content))))
    with pytest.raises(Conflict, match="部件"):
        deck.update_slides("add", deck.list_slides()[0]["id"])


@pytest.mark.asyncio
async def test_added_slide_is_present_in_real_pdf_render():
    from pypdf import PdfReader

    from egis_agentkit.service.project._document_preview import DocumentPreviewService

    preview = DocumentPreviewService()
    if not preview.binary:
        pytest.skip("LibreOffice is not installed")
    deck = Presentation(FIXTURE.read_bytes(), ProjectPresentationConfig())
    updated, _ = deck.update_slides("add", deck.list_slides()[0]["id"])
    pdf = await preview.convert(updated, "new-slide.pptx")
    assert len(PdfReader(io.BytesIO(pdf)).pages) == 3


@pytest.mark.asyncio
async def test_preview_includes_hidden_slides_so_directory_page_indexes_match():
    from pypdf import PdfReader

    from egis_agentkit.service.project._document_preview import DocumentPreviewService

    service = DocumentPreviewService()
    if not service.binary:
        pytest.skip("LibreOffice is not installed")
    data = parts(FIXTURE.read_bytes())
    slide = etree.fromstring(data["ppt/slides/slide1.xml"])
    slide.set("show", "0")
    data["ppt/slides/slide1.xml"] = etree.tostring(slide)
    output = io.BytesIO()
    with ZipFile(output, "w") as archive:
        for name, value in data.items():
            archive.writestr(name, value)
    pdf = await service.convert(output.getvalue(), "hidden.pptx")
    assert len(PdfReader(io.BytesIO(pdf)).pages) == 2
