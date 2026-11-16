"""单元测试使用显式沙箱连接配置，不依赖开发环境凭据。"""


def sandbox_runtime(root):
    return {
        "root": str(root),
        "mode": "pks",
        "sandbox": {
            "domain": "127.0.0.1:8090",
            "apiKeyEnv": "TEST_SANDBOX_KEY",
        },
    }
