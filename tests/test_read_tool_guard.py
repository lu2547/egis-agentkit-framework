"""Native Read must reject unsupported binary inputs without aborting a reply."""

import json
from pathlib import Path

import pytest
from agentscope.message import ToolCallBlock
from agentscope.state import AgentState
from agentscope.tool import Read, Toolkit

from egis_agentkit.extensions.middleware.workspace_prompt import (
    WorkspacePromptMiddleware,
)
from egis_agentkit.extensions.workspace.local import EgisLocalWorkspaceManager
from egis_agentkit.storage.message_bus import PostgresPrimitives

FIXTURES = Path(__file__).parents[2] / "egis-agentkit-frontend/tests/fixtures"
MESSAGE = "Read 工具不支持此类二进制文件读取，请先转成 Markdown，或尝试使用其他工具。"


async def tools(root):
    manager = EgisLocalWorkspaceManager(root)
    workspace = await manager.get_workspace("alice", "agent", "session", "project")
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
async def test_office_is_rejected_as_a_tool_error_before_raw_bytes(name, tmp_path):
    manager, toolkit = await tools(tmp_path)
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
async def test_unknown_binary_content_is_detected_without_extension(tmp_path, payload):
    path = tmp_path / "unknown"
    path.write_bytes(payload)
    manager, toolkit = await tools(tmp_path / "workspace")
    try:
        assert (await execute(toolkit, path))[-1].content[0].text == MESSAGE
    finally:
        await manager.close_all()


@pytest.mark.asyncio
async def test_nul_after_sniff_window_never_reaches_pg_replay(storage, tmp_path):
    path = tmp_path / "late.txt"
    path.write_bytes(b"normal line\n" * 500 + b"bad\0byte")
    manager, toolkit = await tools(tmp_path / "workspace")
    try:
        chunks = await execute(toolkit, path)
        assert chunks[-1].state == "error"
        bus = PostgresPrimitives(storage.engine)
        await bus.migrate()
        for chunk in chunks:
            await bus.log_append("binary-guard", chunk.model_dump(mode="json"))
        assert len(await bus.log_read("binary-guard")) == len(chunks)
    finally:
        await manager.close_all()


@pytest.mark.asyncio
async def test_native_pdf_text_with_nul_does_not_poison_pg_replay(storage, tmp_path):
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
    manager, toolkit = await tools(tmp_path / "workspace")
    try:
        chunks = await execute(toolkit, path)
        assert chunks[-1].state == "error"
        bus = PostgresPrimitives(storage.engine)
        await bus.migrate()
        for chunk in chunks:
            assert "\0" not in str(chunk.content)
            await bus.log_append("pdf-nul-guard", chunk.model_dump(mode="json"))
        assert len(await bus.log_read("pdf-nul-guard")) == len(chunks)
    finally:
        await manager.close_all()


@pytest.mark.asyncio
async def test_plain_text_pagination_and_pdf_and_image_remain_native(tmp_path):
    manager, toolkit = await tools(tmp_path / "workspace")
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
    prompt = await WorkspacePromptMiddleware(
        SimpleNamespace(workdir=work, artifacts_dir=tmp_path / "artifacts")
    ).on_system_prompt(None, "system")
    assert "uploads 归档路径" not in prompt
    assert "直接用原生 Read" not in prompt
    assert "Word、PPT、Excel" in prompt and "二进制" in prompt


@pytest.mark.asyncio
async def test_guard_sniffs_only_one_chunk_and_never_executes_binary_read(tmp_path):
    from agentscope.tool import LocalBackend

    from egis_agentkit.extensions.middleware.builtin_tool_guard import (
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
async def test_native_path_errors_and_empty_unicode_files_are_preserved(tmp_path):
    manager, toolkit = await tools(tmp_path / "workspace")
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
