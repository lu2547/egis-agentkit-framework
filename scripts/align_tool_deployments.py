"""预览或应用示例工具纯模块移动的部署登记；不启动 API，不修改工具版本。"""

import argparse
import asyncio
import json
import os
from pathlib import Path

from dotenv import load_dotenv

from egis_agentkit.bootstrap import init_agentkit
from egis_agentkit.client.nacos import NacosClient, NacosConfigProvider, nacos_bootstrap
from egis_agentkit.storage.migration._tool_deployments import align_tool_deployments


async def main(args):
    load_dotenv(args.app_dir / ".env")
    async with NacosClient(nacos_bootstrap()) as client:
        config = NacosConfigProvider(client)
        await config.refresh()
        kit = init_agentkit(
            config,
            client,
            os.environ[config.snapshot.client.postgres.connection.url_env],
            owns_client=False,
        )
        try:
            if args.apply and kit._storage.engine.url.database != args.database:
                raise ValueError("目标数据库与 --database 不一致")
            async with kit._storage.engine.begin() as connection:
                changes = await align_tool_deployments(
                    connection,
                    kit.services.resources.tool_registry.catalog,
                    apply=args.apply,
                )
            print(
                json.dumps(
                    {"applied": args.apply, "changes": changes},
                    ensure_ascii=False,
                    indent=2,
                )
            )
        finally:
            await kit._storage.engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--app-dir",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "egis-agentkit-app",
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--database")
    args = parser.parse_args()
    if args.apply and not args.database:
        parser.error("应用迁移必须显式指定 --database")
    asyncio.run(main(args))
