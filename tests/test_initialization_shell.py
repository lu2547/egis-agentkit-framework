"""执行首次初始化脚本，验证目录边界、连接参数及 Topic 重复执行行为。"""

import json
import os
import subprocess
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts/init"


def run_script(name, values):
    return subprocess.run(
        ["bash", str(SCRIPTS / name)],
        env={**os.environ, **values},
        capture_output=True,
        text=True,
    )


def executable(path, content):
    path.write_text(content)
    path.chmod(0o700)
    return str(path)


@pytest.fixture
def nas_environment(tmp_path):
    mount = tmp_path / "nas"
    mount.mkdir()
    binary = tmp_path / "bin"
    binary.mkdir()
    executable(binary / "mountpoint", "#!/bin/sh\nexit 0\n")
    return {
        "NAS_MOUNTPOINT": str(mount),
        "NAS_ROOT": str(mount / "agentkit"),
        "SKILL_STORAGE_ROOT": str(mount / "agentkit/skill-archives"),
        "NAS_DIR_MODE": "0750",
        "PATH": str(binary) + os.pathsep + os.environ["PATH"],
    }


def test_nas_initializes_shared_roots_and_preserves_existing_files(nas_environment):
    result = run_script("nas.sh", nas_environment)
    assert result.returncode == 0, result.stderr
    root = Path(nas_environment["NAS_ROOT"])
    assert (root / "user").is_dir()
    assert Path(nas_environment["SKILL_STORAGE_ROOT"]).is_dir()
    content = root / "user/existing.txt"
    content.write_bytes(b"keep existing data")
    result = run_script("nas.sh", nas_environment)
    assert result.returncode == 0, result.stderr
    assert content.read_bytes() == b"keep existing data"
    assert not list(root.rglob(".agentkit-init.*"))


def test_nas_does_not_create_local_fallback_when_mount_is_missing(nas_environment):
    binary = Path(nas_environment["PATH"].split(os.pathsep)[0])
    executable(binary / "mountpoint", "#!/bin/sh\nexit 1\n")
    result = run_script("nas.sh", nas_environment)
    assert result.returncode != 0
    assert "NAS 挂载点不可用" in result.stderr
    assert not Path(nas_environment["NAS_ROOT"]).exists()


def test_nas_rejects_directories_outside_mount(nas_environment, tmp_path):
    nas_environment["SKILL_STORAGE_ROOT"] = str(tmp_path / "outside")
    result = run_script("nas.sh", nas_environment)
    assert result.returncode != 0
    assert "目录不在 NAS 挂载点内" in result.stderr
    assert not (tmp_path / "outside").exists()


def test_nas_rejects_existing_user_root_symlink(nas_environment, tmp_path):
    root = Path(nas_environment["NAS_ROOT"])
    root.mkdir()
    outside = tmp_path / "other-storage"
    outside.mkdir()
    (root / "user").symlink_to(outside, target_is_directory=True)
    result = run_script("nas.sh", nas_environment)
    assert result.returncode != 0
    assert "用户根目录不能是符号链接" in result.stderr
    assert not list(outside.iterdir())


def test_redis_auth_stays_in_environment_and_tls_arguments_are_forwarded(tmp_path):
    recorded = tmp_path / "redis-arguments.json"
    cli = executable(
        tmp_path / "redis-cli",
        "#!/usr/bin/env python3\n"
        "import json, os, pathlib, sys\n"
        "assert os.environ['REDISCLI_AUTH'] == 'test-secret'\n"
        "pathlib.Path(os.environ['RECORDED_ARGS']).write_text(json.dumps(sys.argv[1:]))\n"
        "print('PONG')\n",
    )
    certificate = tmp_path / "redis-ca.pem"
    certificate.write_text("test fixture")
    result = run_script(
        "redis.sh",
        {
            "REDIS_CLI": cli,
            "REDIS_HOST": "redis.internal",
            "REDIS_PORT": "6380",
            "REDIS_USER": "agentkit",
            "REDIS_DB": "0",
            "REDIS_TLS": "true",
            "REDIS_CA_FILE": str(certificate),
            "REDISCLI_AUTH": "test-secret",
            "RECORDED_ARGS": str(recorded),
        },
    )
    assert result.returncode == 0, result.stderr
    arguments = json.loads(recorded.read_text())
    assert arguments == [
        "--raw", "-h", "redis.internal", "-p", "6380", "-n", "0",
        "--user", "agentkit", "--tls", "--cacert", str(certificate), "PING",
    ]
    assert "test-secret" not in result.stdout + result.stderr + recorded.read_text()


@pytest.fixture
def kafka_environment(tmp_path):
    state = tmp_path / "topics.json"
    state.write_text("{}")
    cli = executable(
        tmp_path / "kafka-topics.sh",
        """#!/usr/bin/env python3
import json
import os
from pathlib import Path
import re
import sys
args = sys.argv[1:]
state = Path(os.environ['TOPIC_STATE'])
topics = json.loads(state.read_text())
name = args[args.index('--topic') + 1]
if '--create' in args:
    topics.setdefault(name, {
        'partitions': int(args[args.index('--partitions') + 1]),
        'replicas': int(args[args.index('--replication-factor') + 1]),
    })
    state.write_text(json.dumps(topics, sort_keys=True))
elif '--describe' in args:
    for candidate, row in topics.items():
        if re.fullmatch(name, candidate):
            print(f"Topic: {candidate}\\tPartitionCount: {row['partitions']}\\tReplicationFactor: {row['replicas']}\\tConfigs:")
else:
    raise SystemExit('unexpected Kafka operation')
""",
    )
    return {
        "KAFKA_TOPICS_BIN": cli,
        "KAFKA_BOOTSTRAP_SERVERS": "kafka.internal:9092",
        "KAFKA_TOPIC_PREFIX": "agentkit-test",
        "KAFKA_TOPIC_PARTITIONS": "8",
        "KAFKA_TOPIC_REPLICATION_FACTOR": "1",
        "TOPIC_STATE": str(state),
    }


def test_kafka_creates_only_two_topics_and_can_be_repeated(kafka_environment):
    result = run_script("kafka.sh", kafka_environment)
    assert result.returncode == 0, result.stderr
    state = Path(kafka_environment["TOPIC_STATE"])
    assert json.loads(state.read_text()) == {
        "agentkit-test.triggers": {"partitions": 8, "replicas": 1},
        "agentkit-test.broadcast": {"partitions": 8, "replicas": 1},
    }
    result = run_script("kafka.sh", kafka_environment)
    assert result.returncode == 0, result.stderr
    assert len(json.loads(state.read_text())) == 2


def test_kafka_rejects_existing_topic_shape_without_modifying_it(kafka_environment):
    state = Path(kafka_environment["TOPIC_STATE"])
    original = {"agentkit-test.triggers": {"partitions": 3, "replicas": 1}}
    state.write_text(json.dumps(original))
    result = run_script("kafka.sh", kafka_environment)
    assert result.returncode != 0
    assert "Topic 参数不一致" in result.stderr
    assert json.loads(state.read_text()) == original


def test_kafka_verifies_literal_topic_when_similar_names_exist(kafka_environment):
    state = Path(kafka_environment["TOPIC_STATE"])
    unrelated = {"agentkit-test-triggers": {"partitions": 1, "replicas": 1}}
    state.write_text(json.dumps(unrelated))
    result = run_script("kafka.sh", kafka_environment)
    assert result.returncode == 0, result.stderr
    assert json.loads(state.read_text()) == {
        **unrelated,
        "agentkit-test.triggers": {"partitions": 8, "replicas": 1},
        "agentkit-test.broadcast": {"partitions": 8, "replicas": 1},
    }
