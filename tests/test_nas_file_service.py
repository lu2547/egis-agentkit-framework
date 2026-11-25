import asyncio
import socket
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import uvicorn
import yaml

from egis_agentkit.client.nacos import load_agentkit_config
from egis_agentkit.client.nas_files import decode_wire, encode_wire
from egis_agentkit.core.skill._archive import calculate_files_digest
from egis_agentkit.errors import UploadNameConflict
from egis_agentkit.storage._files import FileStorage
from egis_agentkit.storage.project._files import ProjectFileStorage
from egis_agentkit.storage.skill._upload import SkillUploadStorage
from egis_agentkit.storage.skill._version import SkillVersionStorage


@pytest.fixture
def settings(tmp_path):
    example = Path(__file__).resolve().parents[1] / "examples/nacos/agentkit.yaml"
    content = yaml.safe_load(example.read_text())
    app = content["agentkit"]
    app["workspace"]["runtime"]["root"] = str(tmp_path)
    app["workspace"]["files"] = {"win": True, "endpoint": "http://nas.test"}
    app["skill"]["storage"]["root"] = str(tmp_path / "skill-uploads")
    return load_agentkit_config(yaml.safe_dump(content))


@pytest.fixture
async def client(settings, monkeypatch):
    from egis_agentkit.storage._nas_service import create_nas_file_app

    monkeypatch.setenv(settings.workspace.files.token_env, "test-token")
    app = create_nas_file_app(settings)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://nas.test",
        headers={"Authorization": "Bearer test-token"},
    ) as client:
        yield client


async def test_requires_bearer_token_before_changing_nas(client, tmp_path):
    response = await client.post(
        "/v1/nas/workspace/initialize_user_workspace",
        json={"user_id": "alice"},
        headers={"Authorization": "Bearer wrong"},
    )

    assert response.status_code == 401
    assert not (tmp_path / "user").exists()


async def test_initializes_workspace_on_nas(client, tmp_path):
    response = await client.post(
        "/v1/nas/workspace/initialize_user_workspace",
        json={"user_id": "alice", "runtime_binding_id": "binding-1"},
    )

    assert response.status_code == 200
    assert sorted(path.name for path in (tmp_path / "user/alice").iterdir()) == [
        "projects",
        "session",
        "skills",
    ]
    assert (tmp_path / "user/alice/session/binding-1").is_dir()


async def test_rejects_unlisted_storage_helper(client):
    response = await client.post(
        "/v1/nas/workspace/_read_root",
        json={"root": "/any", "path": "file"},
    )

    assert response.status_code == 404


async def test_skill_avatar_round_trips_through_nas(client, tmp_path):
    asset = {
        "resource_id": "meeting",
        "resource_version": "1.0.0",
        "owner_type": "user",
        "owner_id": "alice",
    }
    content = b'<svg xmlns="http://www.w3.org/2000/svg"/>'
    write = await client.post(
        "/v1/nas/skill_version/write_avatar",
        json=encode_wire({"asset": asset, "content": content}),
    )
    assert write.status_code == 200
    read = await client.post("/v1/nas/skill_version/read_avatar", json={"asset": asset})
    assert read.status_code == 200
    assert decode_wire(read.json()) == content
    assert (tmp_path / "user/alice/skills/meeting/.avatars/1.0.0.svg").read_bytes() == content


async def test_only_preferences_can_be_accessed_as_user_texts(client, tmp_path):
    response = await client.post(
        "/v1/nas/workspace/write_user_texts",
        json={
            "user_id": "alice",
            "directory": "projects",
            "documents": {"file.md": "content"},
            "max_bytes": 100,
        },
    )

    assert response.status_code == 403
    assert not (tmp_path / "user").exists()


async def test_preference_write_checks_server_limit_before_any_write(
    client, settings, tmp_path
):
    limit = settings.agent.personal.documents.max_bytes
    response = await client.post(
        "/v1/nas/workspace/write_user_texts",
        json={
            "user_id": "alice",
            "directory": "preferences",
            "documents": {"agent.md": "allowed", "soul.md": "a" * (limit + 1)},
            "max_bytes": limit + 1,
        },
    )

    assert response.status_code == 409
    assert not (tmp_path / "user/alice/preferences/agent.md").exists()


async def test_chunked_body_limit_applies_before_storage(client, tmp_path):
    async def body():
        yield b'{"user_id":"alice","padding":"'
        for _ in range(8):
            yield b"x" * 65536
        yield b'"}'

    response = await client.post(
        "/v1/nas/workspace/initialize_user_workspace",
        content=body(),
        headers={"Content-Type": "application/json"},
    )

    assert response.status_code == 413
    assert not (tmp_path / "user").exists()


async def test_rejects_non_json_body(client):
    response = await client.post(
        "/v1/nas/workspace/initialize_user_workspace",
        content=b'{"user_id":"alice"}',
        headers={"Content-Type": "text/plain"},
    )

    assert response.status_code == 415


@pytest.fixture
async def loopback_config(settings, monkeypatch):
    from egis_agentkit.storage._nas_service import create_nas_file_app

    monkeypatch.setenv(settings.workspace.files.token_env, "test-token")
    app = create_nas_file_app(settings)
    server = uvicorn.Server(uvicorn.Config(app, log_level="error", access_log=False))
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        workspace = settings.workspace.model_copy(
            update={
                "files": settings.workspace.files.model_copy(
                    update={"endpoint": f"http://127.0.0.1:{port}"}
                )
            }
        )
        server_task = asyncio.create_task(server.serve(sockets=[listener]))
        try:
            async with asyncio.timeout(5):
                while not (server.started or server_task.done()):
                    await asyncio.sleep(0.01)
            if server_task.done():
                server_task.result()
                pytest.fail("NAS 文件服务没有启动")
            yield SimpleNamespace(
                snapshot=settings.model_copy(update={"workspace": workspace})
            )
        finally:
            server.should_exit = True
            await asyncio.wait_for(server_task, timeout=5)


async def test_workspace_and_project_files_round_trip_over_http(
    loopback_config, tmp_path
):
    root = loopback_config.snapshot.workspace.runtime.root
    workspace = FileStorage(root, config_provider=loopback_config)
    project = ProjectFileStorage(root, config_provider=loopback_config)

    def use_files():
        workspace.initialize_user_workspace("alice")
        workspace.write_user_texts(
            "alice", "preferences", {"agent.md": "偏好"}, max_bytes=1024
        )
        assert workspace.read_user_texts(
            "alice", "preferences", ["agent.md", "soul.md"], max_bytes=1024
        ) == {"agent.md": "偏好"}

        project.initialize_project("alice", "project-1")
        content = b"\x00\xff"
        info = project.write(
            "alice", "project-1", "data.bin", content, return_metadata=True
        )
        assert info["path"] == "data.bin"
        assert info["size"] == 2
        assert project.read("alice", "project-1", "work", "data.bin") == (
            content,
            "application/octet-stream",
        )
        assert [
            item["name"] for item in project.list("alice", "project-1", "work")
        ] == ["data.bin"]
        assert project.scan("alice", "project-1") == {
            ("work", "data.bin"): (sha256(content).hexdigest(), 2)
        }
        with pytest.raises(UploadNameConflict):
            project.write(
                "alice", "project-1", "data.bin", b"overwrite", overwrite=False
            )

    await asyncio.to_thread(use_files)
    assert (
        tmp_path / "user/alice/projects/project-1/.work/data.bin"
    ).read_bytes() == b"\x00\xff"


async def test_skill_files_round_trip_over_http(loopback_config, tmp_path):
    root = loopback_config.snapshot.workspace.runtime.root
    uploads = SkillUploadStorage.from_config(root, loopback_config)
    versions = SkillVersionStorage(root, config_provider=loopback_config)

    def use_files():
        job_id = "a" * 32
        content = b"PK\x00\xff"
        metadata = uploads.write(job_id, content)
        assert uploads.read(job_id, metadata) == content
        uploads.remove(job_id)
        assert not (tmp_path / f"skill-uploads/incoming/{job_id}.zip").exists()

        files = {"SKILL.md": b"---\nname: test\n---", "asset.bin": b"\x00\xff"}
        checksum = calculate_files_digest(files)
        asset = {
            "resource_id": "example-skill",
            "resource_version": "1.0.0",
            "owner_type": "user",
            "owner_id": "alice",
            "checksum_sha256": checksum,
        }
        versions.publish(asset, files, max_bytes=1024)
        directory, stored, updated_at = versions.read(asset, max_bytes=1024)
        assert updated_at > 0
        assert stored == files
        assert directory.as_posix() == str(
            tmp_path / "user/alice/skills/example-skill/1.0.0"
        )
        assert (
            versions.read_runtime(directory, checksum=checksum, max_bytes=1024) == files
        )

    await asyncio.to_thread(use_files)

async def test_shared_skill_publication_round_trips_through_nas(loopback_config, tmp_path):
    versions = SkillVersionStorage(tmp_path, config_provider=loopback_config)
    files = {"SKILL.md": b"# shared", "data.bin": b"\x00\xff"}
    asset = dict(resource_id="shared-skill", resource_version="1.0.0", owner_type="user",
                 owner_id="alice", checksum_sha256=calculate_files_digest(files))

    def publish_and_read():
        versions.publish(asset, files, max_bytes=1024)
        versions.publish_shared(asset, max_bytes=1024)
        directory, stored, _ = versions.read({**asset, "storage_scope": "shared"}, max_bytes=1024)
        assert directory == tmp_path / "common/skills/shared-skill/1.0.0"
        assert stored == files

    await asyncio.to_thread(publish_and_read)
