"""首次初始化 Nacos 配置；命名空间和账号由平台提供。"""

import argparse
import asyncio
from pathlib import Path

from egis_agentkit.client.nacos import (
    AGENTKIT_DATA_ID,
    CONFIG_NOT_FOUND_CODE,
    NacosClient,
    NacosError,
    load_agentkit_config,
    nacos_bootstrap,
)


async def initialize(client: NacosClient, content: str) -> bool:
    """校验并首次发布配置；调用方须对同一配置串行初始化。"""
    desired = load_agentkit_config(content)
    try:
        existing = await client.get_config(AGENTKIT_DATA_ID)
    except NacosError as exc:
        if exc.operation_code != CONFIG_NOT_FOUND_CODE:
            raise
    else:
        if load_agentkit_config(existing) != desired:
            raise NacosError("Nacos 已存在不同配置，首次初始化不会覆盖")
        return False

    # 发布接口不提供本客户端可用的条件创建；首次任务由平台保证串行。
    await client.publish_config(AGENTKIT_DATA_ID, content)
    saved = load_agentkit_config(await client.get_config(AGENTKIT_DATA_ID))
    if saved != desired:
        raise NacosError("Nacos 配置读回不一致，初始化失败")
    return True


async def main() -> None:
    """读取准备好的 YAML，通过部署环境变量连接 Nacos。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", required=True, type=Path, help="准备好的 agentkit.yaml")
    args = parser.parse_args()
    try:
        content = args.file.read_text(encoding="utf-8")
        async with NacosClient(nacos_bootstrap()) as client:
            created = await initialize(client, content)
    except (OSError, UnicodeError):
        parser.exit(1, "配置文件读取失败，请检查 --file 路径和 UTF-8 编码\n")
    except NacosError as exc:
        parser.exit(1, f"Nacos 初始化失败：{exc}\n")
    except ValueError:
        parser.exit(1, "初始化参数无效，请检查 YAML 配置和部署环境变量\n")
    print("Nacos 配置首次发布并读回验证通过" if created else "Nacos 配置相同，已保留")


if __name__ == "__main__":
    asyncio.run(main())
