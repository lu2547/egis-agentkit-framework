"""Native Read must reject unsupported binary inputs without aborting a reply."""

import json
from pathlib import Path

import pytest
from agentscope.message import ToolCallBlock
from agentscope.state import AgentState
from agentscope.tool import Read, Toolkit
from runtime_helpers import project_prompt, sandbox_manager, workspace_dependencies

from egis_agentkit.middleware.workspace._workspace_read_guard_middleware_impl import (
    ReadToolGuardMiddleware,
)
from egis_agentkit.storage._message_bus import PostgresPrimitives

FIXTURES = Path(__file__).parents[2] / "egis-agentkit-frontend/tests/fixtures"
MESSAGE = "Read 不支持解析此类二进制文件。"


async def tools(root, monkeypatch):
    manager = sandbox_manager(root, monkeypatch)
    workspace = await manager.get_workspace(
        "alice", "agent", "session", project_id="project"
    )
    read = next(tool for tool in await workspace.list_tools() if tool.name == "Read")
    assert type(read) is Read
    toolkit = Toolkit()
    await toolkit.add_tool(read)
    return manager, toolkit


async def execute(toolkit, path, **kwargs):
    return [
        chunk
        async for chunk in toolkit.call_tool(
            ToolCallBlock(
                id="read-guard",
                name="Read",
                input=json.dumps({"file_path": str(path), **kwargs}),
            ),
            state=AgentState(),
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["preview.docx", "preview.pptx", "preview.xlsx"])
async def test_office_is_rejected_as_a_tool_error_before_raw_bytes(
    name, tmp_path, monkeypatch
):
    manager, toolkit = await tools(tmp_path, monkeypatch)
    try:
        chunks = await execute(toolkit, FIXTURES / name)
        assert all(c.state == "error" for c in chunks)
        assert all(c.content[0].text == MESSAGE for c in chunks)
        # The native tool loop remains usable after an unsupported file.
        text = tmp_path / "next.md"
        text.write_text("继续正常处理")
        assert (await execute(toolkit, text))[-1].state == "success"
    finally:
        await manager.close_all()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload", [b"unknown\0binary", bytes([1, 2, 3, 4]) * 50, b"PK\x03\x04\0zip"]
)
async def test_unknown_binary_content_is_detected_without_extension(
    tmp_path, payload, monkeypatch
):
    path = tmp_path / "unknown"
    path.write_bytes(payload)
    manager, toolkit = await tools(tmp_path / "workspace", monkeypatch)
    try:
        assert (await execute(toolkit, path))[-1].content[0].text == MESSAGE
    finally:
        await manager.close_all()


@pytest.mark.asyncio
async def test_nul_after_sniff_window_never_reaches_pg_replay(
    storage, tmp_path, monkeypatch
):
    path = tmp_path / "late.txt"
    path.write_bytes(b"normal line\n" * 500 + b"bad\0byte")
    manager, toolkit = await tools(tmp_path / "workspace", monkeypatch)
    try:
        chunks = await execute(toolkit, path)
        assert chunks[-1].state == "error"
        bus = PostgresPrimitives(storage.engine)
        await bus.init_schema()
        for chunk in chunks:
            await bus.log_append("binary-guard", chunk.model_dump(mode="json"))
        assert len(await bus.log_read("binary-guard")) == len(chunks)
    finally:
        await manager.close_all()


@pytest.mark.asyncio
async def test_native_pdf_text_with_nul_does_not_poison_pg_replay(
    storage, tmp_path, monkeypatch
):
    from pypdf import PdfWriter
    from pypdf.generic import DecodedStreamObject, DictionaryObject, NameObject

    writer = PdfWriter()
    page = writer.add_blank_page(300, 300)
    font = DictionaryObject(
        {
            NameObject("/Type"): NameObject("/Font"),
            NameObject("/Subtype"): NameObject("/Type1"),
            NameObject("/BaseFont"): NameObject("/Helvetica"),
        }
    )
    page[NameObject("/Resources")] = DictionaryObject(
        {NameObject("/Font"): DictionaryObject({NameObject("/F1"): font})}
    )
    stream = DecodedStreamObject()
    stream.set_data(b"BT /F1 12 Tf 20 200 Td (hello\\000world) Tj ET")
    page[NameObject("/Contents")] = stream
    path = tmp_path / "nul.pdf"
    writer.write(path)
    manager, toolkit = await tools(tmp_path / "workspace", monkeypatch)
    try:
        chunks = await execute(toolkit, path)
        assert chunks[-1].state == "error"
        bus = PostgresPrimitives(storage.engine)
        await bus.init_schema()
        for chunk in chunks:
            assert "\0" not in str(chunk.content)
            await bus.log_append("pdf-nul-guard", chunk.model_dump(mode="json"))
        assert len(await bus.log_read("pdf-nul-guard")) == len(chunks)
    finally:
        await manager.close_all()


@pytest.mark.asyncio
async def test_plain_text_pagination_and_pdf_and_image_remain_native(
    tmp_path, monkeypatch
):
    manager, toolkit = await tools(tmp_path / "workspace", monkeypatch)
    try:
        path = tmp_path / "code.py"
        path.write_text("one\ntwo\nthree\n")
        result = (await execute(toolkit, path, offset=2, limit=1))[-1]
        assert result.state == "success" and result.content[0].text == "     2\ttwo"
        pdf = (await execute(toolkit, FIXTURES / "preview.pdf", pages="1"))[-1]
        assert pdf.state == "success" and "Preview slide 1" in str(pdf.content)
        image = tmp_path / "image.png"
        image.write_bytes(b"\x89PNG\r\n\x1a\n\0image")
        result = (await execute(toolkit, image))[-1]
        assert result.state == "success" and result.content[0].type == "data"
    finally:
        await manager.close_all()


@pytest.mark.asyncio
async def test_prompt_names_read_formats_without_upload_archive_instruction(tmp_path):
    from types import SimpleNamespace

    work = tmp_path / ".work"
    work.mkdir()
    prompt = await project_prompt(
        SimpleNamespace(workdir=work, artifacts_dir=tmp_path / "artifacts")
    ).on_system_prompt(None, "system")
    assert "uploads 归档路径" not in prompt
    assert "直接用原生 Read" not in prompt
    assert "Word、PPT、Excel" in prompt and "二进制" in prompt
    assert "告知用户无法解析即可" in prompt
    assert "先转成 Markdown" not in prompt


@pytest.mark.asyncio
async def test_guard_sniffs_only_one_chunk_and_never_executes_binary_read(tmp_path):
    from agentscope.tool import LocalBackend

    from egis_agentkit.middleware.workspace._workspace_read_guard_middleware_impl import (
        ReadToolGuardMiddleware,
    )

    class BoundedBackend(LocalBackend):
        async def read_stream(self, path, chunk_size=1024):
            assert chunk_size == 4096
            yield b"header\0binary"
            raise AssertionError("Guard must not request a second chunk")

        async def read_file(self, path):
            raise AssertionError("Native binary Read must never execute")

    path = tmp_path / "unrecognized"
    path.write_bytes(b"header\0binary")
    backend = BoundedBackend()
    toolkit = Toolkit()
    await toolkit.add_tool(
        Read(backend=backend, middlewares=[ReadToolGuardMiddleware(backend)])
    )
    result = (await execute(toolkit, path))[-1]
    assert result.state == "error" and result.content[0].text == MESSAGE


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("sample_bytes", "threshold", "expected_state"),
    [(4, 0.3, "error"), (8, 0.3, "success"), (8, 0.2, "error")],
)
async def test_nacos_guard_limits_control_actual_workspace_read(
    tmp_path, monkeypatch, sample_bytes, threshold, expected_state
):
    """采样长度和阈值都必须实际影响 Read，不能只被配置模型接收。"""
    from config_helpers import sandbox_runtime
    from runtime_helpers import stub_sandbox_provider

    from egis_agentkit.client.nacos._settings import WorkspaceConfig
    from egis_agentkit.service.workspace._egis_sandbox_manager import EgisSandboxManager

    config = WorkspaceConfig.model_validate(
        {
            "runtime": sandbox_runtime(tmp_path),
            "readGuard": {
                "sampleBytes": sample_bytes,
                "controlByteRatioThreshold": threshold,
            },
        }
    )
    monkeypatch.setenv("TEST_SANDBOX_KEY", "test-only")
    stub_sandbox_provider(monkeypatch)
    from functools import partial

    from egis_agentkit.client.sandbox._opensandbox import OpenSandboxClient

    manager = EgisSandboxManager(
        tmp_path,
        config.runtime.sandbox,
        read_guard_factory=partial(ReadToolGuardMiddleware, settings=config.read_guard),
        **workspace_dependencies(),
        client_factory=partial(
            OpenSandboxClient, config.runtime.sandbox, api_key="test-only"
        ),
    )
    try:
        workspace = await manager.get_workspace(
            "alice", "agent", "session", project_id="project"
        )
        toolkit = Toolkit()
        await toolkit.add_tool(
            next(tool for tool in await workspace.list_tools() if tool.name == "Read")
        )
        path = tmp_path / "unknown"
        path.write_bytes(b"\x01\x02abcdef")
        result = (await execute(toolkit, path))[-1]
        assert result.state == expected_state
    finally:
        await manager.close_all()


@pytest.mark.asyncio
async def test_native_path_errors_and_empty_unicode_files_are_preserved(
    tmp_path, monkeypatch
):
    manager, toolkit = await tools(tmp_path / "workspace", monkeypatch)
    try:
        for path, expected in [
            ("relative.txt", "absolute"),
            (tmp_path / "missing.docx", "does not exist"),
            (tmp_path, "directory"),
        ]:
            response = (await execute(toolkit, path))[-1]
            assert response.state == "error" and expected in response.content[0].text
            assert response.content[0].text != MESSAGE
        for name, text in [
            ("empty.txt", ""),
            ("chinese.md", "中文内容和 emoji 😀\n" * 1000),
        ]:
            path = tmp_path / name
            path.write_text(text)
            assert (await execute(toolkit, path))[-1].state == "success"
    finally:
        await manager.close_all()
