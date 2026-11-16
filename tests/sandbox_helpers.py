"""独立于任何沙箱 SDK 的测试 Client，文件和命令使用真实临时目录。"""

import asyncio
import base64
import hashlib
import json
from dataclasses import replace

from agentscope.tool import ExecResult, LocalBackend


class TestSandboxClient:
    __test__ = False

    def __init__(self, instances=None, events=None, gate=None):
        self.instances = instances if instances is not None else {}
        self.events = events if events is not None else []
        self.gate = gate
        self.instance_id = None
        self.backend = LocalBackend()
        self.failure = None
        self.renewed = asyncio.Event()
        self.closed = False

    def mount_fingerprint(self, mounts):
        return (
            base64.b32encode(
                hashlib.sha256(
                    json.dumps([m.__dict__ for m in mounts], sort_keys=True).encode()
                ).digest()
            )
            .decode()
            .rstrip("=")
        )

    async def find_instance(self, binding_id):
        return next(
            (
                value
                for value in self.instances.values()
                if value.metadata.get("agentscope.workspace.id") == binding_id
            ),
            None,
        )

    async def create_instance(self, metadata, mounts):
        from egis_agentkit.client.sandbox import SandboxInstance

        self.events.append(("create", metadata["agentscope.workspace.id"]))
        if self.gate is not None:
            await self.gate.wait()
        self.instance_id = "instance-" + str(len(self.instances) + 1)
        self.instances[self.instance_id] = SandboxInstance(
            self.instance_id, "running", dict(metadata)
        )

    async def connect_instance(self, instance):
        self.instance_id = instance.id
        self.instances[instance.id] = replace(instance, state="running")
        self.events.append(("connect", instance.id))

    async def renew_instance(self, timeout_seconds):
        from egis_agentkit.client.sandbox import SandboxInstanceNotFound

        if self.failure:
            raise self.failure
        if self.instance_id not in self.instances:
            raise SandboxInstanceNotFound()
        self.events.append(("renew", self.instance_id))
        self.renewed.set()

    async def check_connection(self, *, reconnect=False):
        from egis_agentkit.client.sandbox import SandboxInstanceNotFound

        if self.failure:
            raise self.failure
        if self.instance_id not in self.instances:
            raise SandboxInstanceNotFound()
        return self.instances[self.instance_id]

    async def close_instance(self):
        if self.instance_id is not None:
            self.events.append(("pause", self.instance_id))
            if self.instance_id in self.instances:
                self.instances[self.instance_id] = replace(
                    self.instances[self.instance_id], state="paused"
                )
        self.instance_id = None

    async def close(self):
        self.closed = True

    async def list_instances(self, metadata):
        return [
            value
            for value in self.instances.values()
            if all(value.metadata.get(k) == v for k, v in metadata.items())
        ]

    async def destroy_instance(self, identifier):
        self.events.append(("destroy", identifier))
        self.instances.pop(identifier)

    async def exec_command(self, command, *, cwd, timeout=None):
        if command[:4] == ["stat", "-f", "-c", "%T"]:
            return ExecResult(0, b"tmpfs\n", b"")
        return await self.backend.exec_shell(command, cwd=cwd, timeout=timeout)

    async def read_file(self, path):
        return await self.backend.read_file(path)

    async def write_file(self, path, content):
        await self.backend.write_file(path, content)
