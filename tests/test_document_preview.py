import asyncio
import io
from pathlib import Path

import pytest
from pypdf import PdfReader

from egis_agentkit.errors import AgentKitError
from egis_agentkit.services.document_preview import DocumentPreviewService

FIXTURES = Path(__file__).parents[2] / "egis-agentkit-frontend/tests/fixtures"


@pytest.mark.asyncio
async def test_converter_preserves_office_pages_and_cleans_up():
    service = DocumentPreviewService()
    if not service.binary:
        pytest.skip("LibreOffice executable is not installed")
    result = await service.convert(
        (FIXTURES / "preview.pptx").read_bytes(), "示例 演示.pptx"
    )
    pdf = PdfReader(io.BytesIO(result))
    assert len(pdf.pages) == 2
    assert "Preview slide 1" in pdf.pages[0].extract_text()


@pytest.mark.asyncio
async def test_converter_rejects_unsupported_and_oversized_input_before_execution():
    service = DocumentPreviewService(binary="/not-used", max_bytes=4)
    with pytest.raises(AgentKitError) as unsupported:
        await service.convert(b"a", "script.exe")
    assert unsupported.value.status_code == 415
    with pytest.raises(AgentKitError) as oversized:
        await service.convert(b"12345", "slides.pptx")
    assert oversized.value.status_code == 413


@pytest.mark.asyncio
async def test_converter_has_a_bound_on_process_execution(tmp_path):
    binary = tmp_path / "slow-converter"
    binary.write_text("#!/usr/bin/env python3\nimport time\ntime.sleep(30)\n")
    binary.chmod(0o700)
    service = DocumentPreviewService(binary=str(binary), timeout=0.05)
    with pytest.raises(AgentKitError) as timeout:
        await asyncio.wait_for(service.convert(b"input", "slides.pptx"), 5)
    assert timeout.value.status_code == 504


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["preview.docx", "preview.xlsx"])
async def test_converter_supports_word_and_excel(name):
    service = DocumentPreviewService()
    if not service.binary:
        pytest.skip("LibreOffice executable is not installed")
    result = await service.convert((FIXTURES / name).read_bytes(), name)
    assert len(PdfReader(io.BytesIO(result)).pages) >= 1
