import os
from importlib.resources import files as package_files
from pathlib import Path
from uuid import uuid4

from sqlalchemy import text

from egis_agentkit.errors import Conflict
from egis_agentkit.service.workspace.files import WorkspaceFiles


class PreferenceService:
    names = {"agent_md": "agent.md", "soul_md": "soul.md", "user_md": "user.md"}

    def __init__(self, storage, workspace_root):
        self.storage, self.files = storage, WorkspaceFiles(Path(workspace_root))
        self.defaults = {
            key: package_files("egis_agentkit")
            .joinpath("templates", "preferences", name)
            .read_text(encoding="utf-8")
            for key, name in self.names.items()
        }

    async def read(self, user):
        async with self.storage.transaction() as conn:
            await conn.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                {"key": "preferences:" + user.user_id},
            )
            root = self.files.user_root(user.user_id) / "preferences"
            return {
                key: (root / name).read_text("utf-8")
                if (root / name).is_file()
                else self.defaults[key]
                for key, name in self.names.items()
            }

    async def save(self, user, documents):
        if set(documents) != set(self.names) or any(
            not isinstance(v, str) or len(v.encode()) > 64000
            for v in documents.values()
        ):
            raise Conflict("偏好必须包含 agent_md、soul_md、user_md，单份不超过 64KB")
        async with self.storage.transaction() as conn:
            await conn.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                {"key": "preferences:" + user.user_id},
            )
            root = self.files.user_root(user.user_id) / "preferences"
            root.mkdir(parents=True, exist_ok=True)
            for key, name in self.names.items():
                temporary = root / (name + "." + uuid4().hex)
                try:
                    with temporary.open("x", encoding="utf-8") as stream:
                        stream.write(documents[key])
                        stream.flush()
                        os.fsync(stream.fileno())
                    os.replace(temporary, root / name)
                finally:
                    temporary.unlink(missing_ok=True)
        return documents
