"""稳定生成用户工作区身份，并区分底层沙箱运行绑定。"""

import base64
import hashlib
import json


def _encode(value: object) -> str:
    return (
        base64.b32encode(
            hashlib.sha256(
                json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
            ).digest()
        )
        .decode()
        .rstrip("=")
    )


def workspace_id_for(user_id: str) -> str:
    """同一用户跨项目、Agent、会话始终取得同一个工作区身份。"""
    return _encode(["user-workspace", user_id])


def sandbox_binding_for(
    user_id: str, project_id: str, agent_id: str, session_id: str
) -> str:
    """底层环境按执行绑定隔离，避免共享 Bash 绕过 Agent 资源选择。"""
    payload = json.dumps([user_id, project_id, agent_id, session_id]).encode()
    return base64.b32encode(hashlib.sha256(payload).digest()).decode().rstrip("=")
