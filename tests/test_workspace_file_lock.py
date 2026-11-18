"""Check preference files on a host that mounts the NAS."""

from egis_agentkit.storage._files import FileStorage


def test_preference_file_round_trip_on_mounted_nas(tmp_path):
    files = FileStorage(tmp_path)
    files.write_user_texts("alice", "data", {"agent.md": "first"}, max_bytes=100)
    files.write_user_texts("alice", "data", {"agent.md": "second"}, max_bytes=100)
    assert files.read_user_texts(
        "alice", "data", ["agent.md", "missing.md"], max_bytes=100
    ) == {"agent.md": "second"}
