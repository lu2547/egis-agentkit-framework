from datetime import datetime, timezone
from pathlib import PurePosixPath
from types import SimpleNamespace

import httpx
import pytest
from pydantic import ValidationError

from egis_agentkit.client.nacos._settings import WorkspaceFilesConfig
from egis_agentkit.client.nas_files import decode_wire, encode_wire
from egis_agentkit.errors import UploadNameConflict
from egis_agentkit.storage import _files


def test_win_requires_http_endpoint():
    assert WorkspaceFilesConfig().win is False
    with pytest.raises(ValidationError, match="endpoint"):
        WorkspaceFilesConfig(win=True)
    with pytest.raises(ValidationError, match="HTTP"):
        WorkspaceFilesConfig(win=True, endpoint="file:///mnt/nas")


def test_native_windows_requires_explicit_api_mode(monkeypatch):
    monkeypatch.setattr(_files, "os", SimpleNamespace(name="nt"))
    with pytest.raises(ValueError, match=r"workspace\.files\.win"):
        _files.FileStorage("/mnt/nas")


def test_http_preferences_preserve_posix_path_and_never_open_local_files(monkeypatch):
    monkeypatch.setenv("AGENTKIT_NAS_FILE_TOKEN", "test-token")
    monkeypatch.setattr(_files, "os", SimpleNamespace(name="nt"))
    config = SimpleNamespace(
        snapshot=SimpleNamespace(
            workspace=SimpleNamespace(
                files=WorkspaceFilesConfig(win=True, endpoint="http://files.example")
            )
        )
    )
    files = _files.FileStorage("/mnt/nas", config_provider=config)
    assert files.user_root("demo.user") == PurePosixPath("/mnt/nas/user/demo.user")

    def mock_post(url, *, headers, json, **_options):
        assert headers["Authorization"] == "Bearer test-token"
        assert url.endswith("/v1/nas/workspace/read_user_texts")
        assert decode_wire(json) == {
            "user_id": "demo.user",
            "directory": "preferences",
            "filenames": ("agent.md",),
            "max_bytes": 4096,
        }
        return httpx.Response(200, json=encode_wire({"agent.md": "偏好"}))

    monkeypatch.setattr(httpx, "post", mock_post)
    assert files.read_user_texts(
        "demo.user", "preferences", {"agent": "agent.md"}.values(), max_bytes=4096
    ) == {"agent.md": "偏好"}

    monkeypatch.setattr(
        httpx,
        "post",
        lambda *_args, **_kw: httpx.Response(
            409, json={"error": "UploadNameConflict", "detail": "同名文件已存在。"}
        ),
    )
    with pytest.raises(UploadNameConflict):
        files._nas.call("project", "write")


def test_nas_wire_preserves_storage_values():
    values = {
        ("work", "路径.xlsx"): (
            b"\xff",
            PurePosixPath("/mnt/nas/路径.xlsx"),
            datetime(2026, 11, 18, tzinfo=timezone.utc),
        )
    }
    assert decode_wire(encode_wire(values)) == values
