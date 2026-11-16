"""验证开发提供方的真实 SDK 请求转换和错误边界。"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


@pytest.mark.asyncio
async def test_provider_maps_neutral_mounts_and_argv_without_losing_bytes(
    tmp_path, monkeypatch
):
    from opensandbox import Sandbox

    from egis_agentkit.client.nacos._settings import OpenSandboxConfig
    from egis_agentkit.client.sandbox import SandboxMount
    from egis_agentkit.client.sandbox._opensandbox import OpenSandboxClient

    raw = SimpleNamespace(
        id="sdk-instance",
        commands=SimpleNamespace(
            run=AsyncMock(
                return_value=SimpleNamespace(
                    exit_code=0,
                    logs=SimpleNamespace(
                        stdout=[SimpleNamespace(text="literal; value")], stderr=[]
                    ),
                )
            )
        ),
        files=SimpleNamespace(
            read_bytes=AsyncMock(return_value=b"\x00\xff"), write_files=AsyncMock()
        ),
        close=AsyncMock(),
        pause=AsyncMock(),
    )
    create = AsyncMock(return_value=raw)
    monkeypatch.setattr(Sandbox, "create", create)
    client = OpenSandboxClient(
        OpenSandboxConfig(domain="localhost:8090"),
        api_key="test-only",
    )
    await client.create_instance(
        {"owned": "yes"}, [SandboxMount("input", "/nas/source", "/project/input", True)]
    )
    volume = create.call_args.kwargs["volumes"][0]
    assert (volume.host.path, volume.mount_path, volume.read_only) == (
        "/nas/source",
        "/project/input",
        True,
    )
    result = await client.exec_command(
        ["printf", "%s", "literal; value"], cwd="/project"
    )
    assert raw.commands.run.call_args.args[0] == "printf %s 'literal; value'"
    assert result.stdout == b"literal; value" and result.exit_code == 0
    assert await client.read_file("/project/binary") == b"\x00\xff"
    await client.write_file("/project/binary", b"\x00\xff")
    assert raw.files.write_files.call_args.args[0][0].data == b"\x00\xff"
    await client.close_instance()
    assert client.instance_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [404, 403, 503])
async def test_provider_errors_become_safe_neutral_errors(tmp_path, status):
    from opensandbox.exceptions import SandboxApiException

    from egis_agentkit.client.nacos._settings import OpenSandboxConfig
    from egis_agentkit.client.sandbox import SandboxInstanceNotFound
    from egis_agentkit.client.sandbox._opensandbox import OpenSandboxClient
    from egis_agentkit.errors import AgentKitError

    client = OpenSandboxClient(
        OpenSandboxConfig(domain="localhost:8090"),
        api_key="test-only",
    )
    client._sandbox = SimpleNamespace(
        renew=AsyncMock(
            side_effect=SandboxApiException(
                "private-upstream-detail", status_code=status
            )
        )
    )
    with pytest.raises(AgentKitError) as error:
        await client.renew_instance(60)
    assert isinstance(error.value, SandboxInstanceNotFound) == (status == 404)
    assert "private-upstream-detail" not in str(error.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [404, 503])
async def test_pause_already_missing_instance_is_idempotent_but_failures_propagate(
    tmp_path, status
):
    from opensandbox.exceptions import SandboxApiException

    from egis_agentkit.client.nacos._settings import OpenSandboxConfig
    from egis_agentkit.client.sandbox._opensandbox import OpenSandboxClient
    from egis_agentkit.errors import AgentKitError

    raw = SimpleNamespace(
        id="expired-instance",
        pause=AsyncMock(
            side_effect=SandboxApiException("provider detail", status_code=status)
        ),
        close=AsyncMock(),
    )
    client = OpenSandboxClient(
        OpenSandboxConfig(domain="localhost:8090"),
        api_key="test-only",
    )
    client._sandbox = raw
    if status == 404:
        await client.close_instance()
    else:
        with pytest.raises(AgentKitError):
            await client.close_instance()
    raw.close.assert_awaited_once()
    assert client.instance_id is None


@pytest.mark.asyncio
async def test_non_http_sdk_failure_is_sanitized_and_never_marked_as_missing(tmp_path):
    from opensandbox.exceptions import SandboxInternalException

    from egis_agentkit.client.nacos._settings import OpenSandboxConfig
    from egis_agentkit.client.sandbox import SandboxInstanceNotFound
    from egis_agentkit.client.sandbox._opensandbox import OpenSandboxClient
    from egis_agentkit.errors import AgentKitError

    client = OpenSandboxClient(
        OpenSandboxConfig(domain="localhost:8090"),
        api_key="test-only",
    )
    client._sandbox = SimpleNamespace(
        renew=AsyncMock(side_effect=SandboxInternalException("private-upstream-detail"))
    )
    with pytest.raises(AgentKitError) as error:
        await client.renew_instance(60)
    assert not isinstance(error.value, SandboxInstanceNotFound)
    assert "private-upstream-detail" not in str(error.value)


@pytest.mark.asyncio
async def test_connection_probe_checks_runtime_instead_of_only_renewing(tmp_path):
    from egis_agentkit.client.nacos._settings import OpenSandboxConfig
    from egis_agentkit.client.sandbox._opensandbox import OpenSandboxClient
    from egis_agentkit.errors import AgentKitError

    client = OpenSandboxClient(
        OpenSandboxConfig(domain="localhost:8090"),
        api_key="test-only",
    )
    client._sandbox = SimpleNamespace(
        id="live",
        get_info=AsyncMock(
            return_value=SimpleNamespace(
                id="live",
                status=SimpleNamespace(state="Running"),
                metadata={"owner": "alice"},
            )
        ),
        is_healthy=AsyncMock(return_value=False),
        renew=AsyncMock(),
    )
    with pytest.raises(AgentKitError):
        await client.check_connection()
