"""应用 CLI 只提供服务启动和显式管理员授权。"""

import sys

import pytest
from egis_agentkit_app.bootstrap import _cli, _provision
from test_app_http import app_snapshot

from egis_agentkit.errors import Forbidden, NotFound
from egis_agentkit.service._user import UserService


REMOVED_COMMANDS = (
    "init-db",
    "seed",
    "provision-skill-assistant",
    "cleanup-db",
    "upgrade-db",
    "optimize-db",
    "reset-skills",
)


@pytest.fixture
def cli(monkeypatch):
    monkeypatch.setattr(_cli, "load_dotenv", lambda *args: None)
    # 旧实现若仍接受维护命令，只关闭协程，绝不连接 Nacos 或数据库。
    monkeypatch.setattr(_cli.asyncio, "run", lambda operation: operation.close())
    return _cli


def test_help_only_advertises_runtime_commands_and_admin_provisioning(
    cli, monkeypatch, capsys
):
    monkeypatch.setattr(sys, "argv", ["egis-agentkit", "--help"])
    with pytest.raises(SystemExit) as ended:
        cli.main()
    assert ended.value.code == 0
    help_text = capsys.readouterr().out
    assert "{serve,serve-files,provision-admin}" in help_text
    assert "--apply" not in help_text
    assert "--database" not in help_text


@pytest.mark.parametrize("command", REMOVED_COMMANDS)
def test_removed_commands_are_rejected_before_any_operation(
    cli, monkeypatch, capsys, command
):
    args = ["egis-agentkit", command]
    if command == "seed":
        args.extend(["--user-id", "alice"])
    monkeypatch.setattr(sys, "argv", args)
    with pytest.raises(SystemExit) as ended:
        cli.main()
    assert ended.value.code == 2
    assert "invalid choice" in capsys.readouterr().err


def test_admin_provisioning_still_requires_an_explicit_user(cli, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["egis-agentkit", "provision-admin"])
    with pytest.raises(SystemExit) as ended:
        cli.main()
    assert ended.value.code == 2
    assert "--user-id" in capsys.readouterr().err


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "target, error", [("alice", None), ("unknown", NotFound), ("disabled", Forbidden)]
)
async def test_admin_provisioning_only_promotes_the_named_active_user(
    storage, tmp_path, monkeypatch, target, error
):
    users = UserService(storage)
    for name in ("alice", "bob", "disabled"):
        user = await users.sync_identity(name, name)
        if name == "disabled":
            async with storage.users.write() as writer:
                await writer.update_user(
                    user.model_copy(update={"is_enabled": False}), actor="test"
                )
    before = await storage.users.list_users()

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    class Provider:
        snapshot = app_snapshot(tmp_path)

        async def refresh(self):
            pass

    monkeypatch.setattr(_provision, "nacos_bootstrap", lambda: None)
    monkeypatch.setattr(_provision, "NacosClient", lambda *args: Client())
    monkeypatch.setattr(_provision, "NacosConfigProvider", lambda *args: Provider())
    monkeypatch.setattr(
        _provision, "create_postgres_engine_from_config", lambda *args: storage.engine
    )
    if error:
        with pytest.raises(error):
            await _provision.provision_admin(target)
    else:
        await _provision.provision_admin(target)

    after = await storage.users.list_users()
    assert len(after) == len(before)
    for old, current in zip(before, after, strict=True):
        if not error and current["user_id"] == target:
            assert current["role_type"] == "admin"
            assert current["updated_by"] == "deployment-cli"
            assert current["username"] == old["username"]
            assert current["group_ids"] == old["group_ids"]
        else:
            assert current == old
