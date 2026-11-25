"""Store immutable Skill version files under their registered owner's workspace."""

import errno
import hashlib
import os
import re
import stat
from contextlib import suppress
from pathlib import PurePosixPath
from uuid import uuid4

from egis_agentkit.core.skill._archive import calculate_files_digest
from egis_agentkit.core.skill._avatar import AVATAR_MAX_BYTES
from egis_agentkit.errors import Conflict, Forbidden, NotFound
from egis_agentkit.storage._directory import is_link
from egis_agentkit.storage._files import FileStorage
from egis_agentkit.storage.model._resource import validate_resource_id


class SkillVersionStorage(FileStorage):
    @staticmethod
    def _validate_version(rid, version):
        validate_resource_id(rid)
        if not isinstance(version, str) or not re.fullmatch(
            r"\d+\.\d+\.\d+(?:[-+][A-Za-z0-9.-]+)?", version
        ):
            raise Forbidden("技能版本路径无效")

    def _location(self, asset):
        rid = asset["resource_id"]
        version = asset["resource_version"]
        self._validate_version(rid, version)
        scope = asset.get("storage_scope", "owner")
        if scope not in ("owner", "shared"):
            raise Forbidden("技能存储范围无效")
        if asset["owner_type"] == "system":
            return ["common", "skills", rid, version]
        if asset["owner_type"] == "user":
            owner = self._identifier(asset["owner_id"])
            return (
                ["common", "skills", rid, version]
                if scope == "shared"
                else ["user", owner, "skills", rid, version]
            )
        raise Forbidden("技能归属无效")

    @staticmethod
    def _asset_locator(asset):
        locator = {
            key: asset[key]
            for key in (
                "resource_id",
                "resource_version",
                "owner_type",
                "owner_id",
                "checksum_sha256",
            )
        }
        return {**locator, "storage_scope": asset.get("storage_scope", "owner")}

    def publish_shared(self, asset, *, max_bytes, avatar_checksum=None):
        """Publish verified personal files into common before activating grants."""
        if asset["owner_type"] != "user":
            raise Forbidden("只能发布个人技能公共副本")
        if self._nas is not None:
            return self._nas.call(
                "skill_version",
                "publish_shared",
                asset=self._asset_locator(asset),
                max_bytes=max_bytes,
                avatar_checksum=avatar_checksum,
            )
        source = {**asset, "storage_scope": "owner"}
        target = {**asset, "storage_scope": "shared"}
        try:
            self.read(target, max_bytes=max_bytes)
        except NotFound:
            _, files, _ = self.read(source, max_bytes=max_bytes)
            self.publish(target, files, max_bytes=max_bytes)
        if avatar_checksum:
            try:
                avatar = self.read_avatar(target)
            except NotFound:
                avatar = self.read_avatar(source)
                if hashlib.sha256(avatar).hexdigest() != avatar_checksum:
                    raise Conflict("技能头像摘要与版本记录不一致")
                self.write_avatar(target, avatar)
            if hashlib.sha256(avatar).hexdigest() != avatar_checksum:
                raise Conflict("技能头像摘要与版本记录不一致")

    def publish(self, asset, files, *, max_bytes):
        """Install a complete directory while the caller holds the resource write lock."""
        if self._nas is not None:
            return self._nas.call(
                "skill_version",
                "publish",
                asset=self._asset_locator(asset),
                files=files,
                max_bytes=max_bytes,
            )
        parts = self._location(asset)
        if (
            "SKILL.md" not in files
            or calculate_files_digest(files) != asset["checksum_sha256"]
        ):
            raise Conflict("技能内容摘要与版本记录不一致")
        if sum(map(len, files.values())) > max_bytes:
            raise Conflict("技能内容超过大小限制")
        for name in files:
            if self._parts(name) != name.split("/"):
                raise Forbidden("技能文件路径无效")

        version = parts[-1]
        staging = f".skill-{uuid4().hex}"
        with self.open_directory(self.root, parts[:-1], create=True) as parent:
            try:
                parent.stat(version)
            except FileNotFoundError:
                pass
            else:
                # A previous process may have finished writing before the DB commit.
                self.read(asset, max_bytes=max_bytes)
                return
            parent.mkdir(staging)
            staged_root = self.root.joinpath(*parts[:-1], staging)
            try:
                directories = {PurePosixPath(".")}
                for name, content in files.items():
                    self._write_root(
                        staged_root, name, content, overwrite=False, max_bytes=max_bytes
                    )
                    directories.update(PurePosixPath(name).parents)
                for directory in sorted(directories, key=lambda item: -len(item.parts)):
                    with self.open_directory(
                        staged_root, list(directory.parts)
                    ) as child:
                        child.sync()
                try:
                    parent.rename(staging, version)
                except OSError as exc:
                    if exc.errno not in (errno.EEXIST, errno.ENOTEMPTY):
                        raise
                    self.read(asset, max_bytes=max_bytes)
                else:
                    try:
                        parent.sync()
                    except OSError:
                        self._remove_entry(parent, version)
                        raise
            finally:
                with suppress(FileNotFoundError):
                    self._remove_entry(parent, staging)

    def read(self, asset, *, max_bytes):
        """Read and verify every file beneath the registered version."""
        if self._nas is not None:
            return self._nas.call(
                "skill_version",
                "read",
                asset=self._asset_locator(asset),
                max_bytes=max_bytes,
            )
        parts = self._location(asset)
        return self._read_version(parts, asset["checksum_sha256"], max_bytes=max_bytes)

    def write_avatar(self, asset, content):
        """Persist the generated thumbnail separately from the executable skill files."""
        if self._nas is not None:
            return self._nas.call(
                "skill_version", "write_avatar", asset=asset, content=content
            )
        directory, filename = self._avatar_path(asset)
        self._write_root(
            directory,
            filename,
            content,
            overwrite=True,
            max_bytes=AVATAR_MAX_BYTES,
        )

    def read_avatar(self, asset):
        if self._nas is not None:
            return self._nas.call("skill_version", "read_avatar", asset=asset)
        directory, filename = self._avatar_path(asset)
        content, _ = self._read_root(
            directory,
            filename,
            max_bytes=AVATAR_MAX_BYTES,
        )
        return content

    def _avatar_path(self, asset):
        parts = self._location(asset)
        return self.root.joinpath(*parts[:-1], ".avatars"), f"{parts[-1]}.svg"

    def read_runtime(self, source, *, checksum, max_bytes):
        """Read only a canonical skill version with the expected digest."""
        if self._nas is not None:
            return self._nas.call(
                "skill_version",
                "read_runtime",
                source=str(source),
                checksum=checksum,
                max_bytes=max_bytes,
            )
        try:
            parts = PurePosixPath(source).relative_to(self.root).parts
        except ValueError as exc:
            raise Forbidden("技能版本不在受管目录") from exc
        if len(parts) == 5 and parts[0] == "user" and parts[2] == "skills":
            self._identifier(parts[1])
        elif len(parts) != 4 or parts[:2] != ("common", "skills"):
            raise Forbidden("技能版本不在受管目录")
        try:
            self._validate_version(*parts[-2:])
        except ValueError as exc:
            raise Forbidden("技能版本路径无效") from exc
        _, files, _ = self._read_version(parts, checksum, max_bytes=max_bytes)
        return files

    def _read_version(self, parts, checksum, *, max_bytes):
        with self.open_directory(self.root, parts) as root:
            files = {}
            remaining = max_bytes

            def collect(directory, prefix=""):
                nonlocal remaining
                for name in directory.list():
                    info = directory.stat(name)
                    path = prefix + name
                    if self._parts(path) != path.split("/"):
                        raise Forbidden("技能文件路径无效")
                    if is_link(info):
                        raise Forbidden("技能目录只能包含普通文件及目录")
                    if stat.S_ISDIR(info.st_mode):
                        with directory.child(name) as child:
                            collect(child, path + "/")
                    elif stat.S_ISREG(info.st_mode):
                        item = directory.open_file(name, os.O_RDONLY)
                        with os.fdopen(item, "rb") as stream:
                            current = os.fstat(stream.fileno())
                            if not stat.S_ISREG(current.st_mode):
                                raise Forbidden("技能目录只能包含普通文件及目录")
                            if current.st_size > remaining:
                                raise Conflict("技能内容超过大小限制")
                            content = stream.read(remaining + 1)
                        remaining -= len(content)
                        if remaining < 0:
                            raise Conflict("技能内容超过大小限制")
                        files[path] = content
                    else:
                        raise Forbidden("技能目录只能包含普通文件及目录")

            try:
                collect(root)
            except OSError as exc:
                raise Forbidden("无法安全读取技能目录") from exc
            if (
                "SKILL.md" not in files
                or calculate_files_digest(files) != checksum
            ):
                raise Conflict("技能内容摘要与版本记录不一致")
            updated_at = root.stat().st_mtime
        return self.root.joinpath(*parts), files, updated_at
