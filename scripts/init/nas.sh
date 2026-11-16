#!/usr/bin/env bash
# 初始化已经挂载的 NAS：校验边界、建立共享目录、验证读写；保留已有文件。
set -euo pipefail

: "${NAS_MOUNTPOINT:?请设置已挂载的 NAS 挂载点}"
: "${NAS_ROOT:?请设置应用在 NAS 上的持久根目录}"
: "${SKILL_STORAGE_ROOT:?请设置技能归档目录}"
: "${NAS_DIR_MODE:?请设置新目录权限，例如 0750}"

# 1. 先确认挂载存在，避免 NAS 掉线后把业务目录建到本地盘。
for path in "$NAS_MOUNTPOINT" "$NAS_ROOT" "$SKILL_STORAGE_ROOT"; do
    if [[ "$path" != /* ]]; then
        echo "NAS 路径必须为绝对路径" >&2
        exit 1
    fi
done
if [[ ! "$NAS_DIR_MODE" =~ ^[0-7]{3,4}$ ]]; then
    echo "NAS_DIR_MODE 必须是八进制权限" >&2
    exit 1
fi
if ! mountpoint -q -- "$NAS_MOUNTPOINT"; then
    echo "NAS 挂载点不可用，未创建任何业务目录" >&2
    exit 1
fi

canonical_path() {
    python3 -c 'import os, sys; print(os.path.realpath(sys.argv[1]))' "$1"
}

mount_root=$(canonical_path "$NAS_MOUNTPOINT")
if [[ "$mount_root" == / ]]; then
    echo "NAS 挂载点不能使用系统根目录" >&2
    exit 1
fi
nas_root=$(canonical_path "$NAS_ROOT")
skill_root=$(canonical_path "$SKILL_STORAGE_ROOT")
for path in "$nas_root" "$skill_root"; do
    if [[ "$path" != "$mount_root" && "$path" != "$mount_root/"* ]]; then
        echo "目录不在 NAS 挂载点内：$path" >&2
        exit 1
    fi
done
user_root="$nas_root/user"
if [[ -L "$user_root" ]]; then
    echo "用户根目录不能是符号链接" >&2
    exit 1
fi

# 2. 以实际应用账号创建目录，不递归改权限、不生成用户或项目的测试数据。
umask 077
mkdir -p -m "$NAS_DIR_MODE" -- "$nas_root" "$user_root" "$skill_root"

# 3. 验证持久根、用户根及技能根可读写，只清理本次探针文件。
probe=""
trap 'if [[ -n "$probe" ]]; then rm -f -- "$probe"; fi' EXIT
for path in "$nas_root" "$user_root" "$skill_root"; do
    probe=$(mktemp "$path/.agentkit-init.XXXXXX")
    printf '%s\n' 'agentkit-nas-init' > "$probe"
    IFS= read -r actual < "$probe"
    if [[ "$actual" != agentkit-nas-init ]]; then
        echo "NAS 文件读回校验失败" >&2
        exit 1
    fi
    rm -f -- "$probe"
    probe=""
done
echo "NAS 初始化完成：用户根目录和技能归档目录可读写"
