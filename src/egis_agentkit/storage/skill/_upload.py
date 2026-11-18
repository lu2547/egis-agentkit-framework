"""为单个导入任务保存可重试的 ZIP，任务完成后释放文件。"""

import re
from hashlib import sha256

from egis_agentkit.client.nacos._settings import SkillArchiveConfig
from egis_agentkit.errors import Conflict, Forbidden, NotFound
from egis_agentkit.storage._files import FileStorage


class SkillUploadStorage(FileStorage):
    """提供可跨实例恢复的 ZIP 暂存和受管删除。"""

    @classmethod
    def from_config(cls, workspace_root, config):
        if config is None:
            return cls(workspace_root / "skill-uploads", SkillArchiveConfig().max_bytes)
        root = config.snapshot.skill.storage.root
        if root is None:
            raise ValueError("必须配置 agentkit.skill.storage.root 的 NAS 上传目录")
        return cls(root, config_provider=config)

    @property
    def max_bytes(self):
        return (
            self.config.snapshot.skill.archive.max_bytes
            if self.config
            else self._max_bytes
        )

    @property
    def incoming(self):
        return self.root / "incoming"

    @staticmethod
    def _filename(job_id):
        if not re.fullmatch(r"[a-f0-9]{32}", job_id):
            raise Forbidden("无效的技能导入标识")
        return job_id + ".zip"

    def write(self, job_id, content):
        if self._nas is not None:
            return self._nas.call("skill_upload", "write", job_id=job_id, content=content)
        if len(content) > self.max_bytes:
            raise Conflict("上传的技能包超过大小限制")
        self._write_root(
            self.incoming,
            self._filename(job_id),
            content,
            overwrite=False,
            max_bytes=self.max_bytes,
        )
        with self.open_directory(self.incoming, []) as directory:
            directory.sync()
        location = {"sha256": sha256(content).hexdigest(), "size": len(content)}
        self.read(job_id, location)
        return location

    def read(self, job_id, metadata):
        if self._nas is not None:
            return self._nas.call(
                "skill_upload", "read", job_id=job_id, metadata=metadata
            )
        content, _ = self._read_root(
            self.incoming, self._filename(job_id), max_bytes=self.max_bytes
        )
        if (
            len(content) != metadata["size"]
            or sha256(content).hexdigest() != metadata["sha256"]
        ):
            raise Conflict("上传的技能包已损坏")
        return content

    def remove(self, job_id):
        if self._nas is not None:
            return self._nas.call("skill_upload", "remove", job_id=job_id)
        name = self._filename(job_id)
        try:
            with self.open_directory(self.incoming, []) as directory:
                directory.unlink(name)
        except (NotFound, FileNotFoundError):
            pass
