import asyncio
import io
from pathlib import Path

import pytest
from pypdf import PdfReader

from egis_agentkit.errors import AgentKitError
from egis_agentkit.service.project._document_preview import DocumentPreviewService

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


@pytest.mark.asyncio
async def test_converter_keeps_chinese_text_with_isolated_home_and_explicit_fonts():
    """真实渲染回归：临时 HOME 不能让用户字体目录中的中文字形消失。"""
    import os
    import zipfile
    from types import SimpleNamespace

    font_directory = os.environ.get("AGENTKIT_TEST_CJK_FONT_DIR")
    if not font_directory:
        pytest.skip("set AGENTKIT_TEST_CJK_FONT_DIR to an installed CJK font directory")
    source = io.BytesIO()
    with zipfile.ZipFile(FIXTURES / "preview.pptx") as original:
        with zipfile.ZipFile(source, "w") as output:
            for item in original.infolist():
                content = original.read(item.filename)
                if item.filename == "ppt/slides/slide1.xml":
                    content = content.replace(
                        b"Preview slide 1", "中文预览验证".encode()
                    )
                output.writestr(item, content)
    converter = DocumentPreviewService()
    if not converter.binary:
        pytest.skip("LibreOffice executable is not installed")
    converter.defaults = SimpleNamespace(
        **converter.defaults.model_dump(
            exclude={"font_directories", "font_config_file"}
        ),
        font_directories=[font_directory],
        font_config_file=None,
    )
    result = await converter.convert(source.getvalue(), "中文.pptx")
    assert "中文预览验证" in PdfReader(io.BytesIO(result)).pages[0].extract_text()


@pytest.mark.asyncio
async def test_preview_cache_reuses_identical_conversion_across_requests_and_instances(
    tmp_path,
):
    import json

    from egis_agentkit.client.nacos._settings import ProjectPreviewConfig

    counter = tmp_path / "count"
    arguments = tmp_path / "arguments.json"
    binary = tmp_path / "converter"
    binary.write_text(
        "#!/usr/bin/env python3\nimport sys,pathlib,json,time\nargs=sys.argv[1:]\ncounter=pathlib.Path("
        + repr(str(counter))
        + ')\ncounter.write_text(str(int(counter.read_text())+1) if counter.exists() else "1")\npathlib.Path('
        + repr(str(arguments))
        + ').write_text(json.dumps(args))\ntime.sleep(0.05)\npathlib.Path(args[args.index("--outdir")+1],"document.pdf").write_bytes(pathlib.Path('
        + repr(str(FIXTURES / "preview.pdf"))
        + ").read_bytes())\n"
    )
    binary.chmod(0o700)
    limits = ProjectPreviewConfig.model_validate(
        {"cache": {"root": str(tmp_path / "cache")}}
    )
    service = DocumentPreviewService(binary=str(binary))
    service.defaults = limits
    content = (FIXTURES / "preview.pptx").read_bytes()
    first, second = await asyncio.gather(
        service.convert(content, "a.pptx"), service.convert(content, "b.pptx")
    )
    assert first == second and counter.read_text() == "1"
    another = DocumentPreviewService(binary=str(binary))
    another.defaults = limits
    assert await another.convert(content, "copy.pptx") == first
    assert counter.read_text() == "1"
    await another.convert(content, "copy.pptx", page=2)
    assert counter.read_text() == "2"
    args = json.loads(arguments.read_text())
    export = args[args.index("--convert-to") + 1]
    assert json.loads(export.split(":", 2)[2])["PageRange"] == {
        "type": "string",
        "value": "2",
    }


@pytest.mark.asyncio
async def test_disconnected_preview_cancels_conversion_without_polling():
    from types import SimpleNamespace

    from egis_agentkit_app.controller.base import run_until_disconnect
    from fastapi import HTTPException

    started = asyncio.Event()
    disconnected = asyncio.Event()
    cancelled = asyncio.Event()

    async def operation():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async def receive():
        await disconnected.wait()
        return {"type": "http.disconnect"}

    task = asyncio.create_task(
        run_until_disconnect(SimpleNamespace(receive=receive), operation())
    )
    await started.wait()
    disconnected.set()
    with pytest.raises(HTTPException) as error:
        await task
    assert error.value.status_code == 499
    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_identical_cache_waiters_also_obey_request_budget(tmp_path, monkeypatch):
    from egis_agentkit.client.nacos._settings import ProjectPreviewConfig

    service = DocumentPreviewService(binary="/not-executed")
    service.defaults = ProjectPreviewConfig(
        max_pending=1, cache={"root": str(tmp_path / "cache")}
    )
    started = asyncio.Event()
    finish = asyncio.Event()

    async def convert(*_):
        started.set()
        await finish.wait()
        return b"%PDF-test"

    monkeypatch.setattr(service, "_get_or_convert", convert)
    task = asyncio.create_task(service.convert(b"source", "slides.pptx"))
    await started.wait()
    try:
        with pytest.raises(AgentKitError) as error:
            await service.convert(b"source", "slides.pptx")
        assert error.value.status_code == 429
    finally:
        finish.set()
    assert await task == b"%PDF-test"
    assert service._requests == 0
