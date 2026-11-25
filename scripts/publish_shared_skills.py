"""Preview or publish approved skills and shared expert/team fixed Skill dependencies."""

import argparse
import asyncio
import json
from pathlib import Path

from dotenv import load_dotenv

from egis_agentkit.client._postgres import (
    create_postgres_engine_from_config,
    postgres_url,
)
from egis_agentkit.client.nacos import NacosClient, NacosConfigProvider, nacos_bootstrap
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.storage import AgentKitStorage
from egis_agentkit.storage.migration._shared_skills import backfill_shared_skills
from egis_agentkit.storage.project._files import ProjectFileStorage


async def main(args):
    load_dotenv(args.app_dir / ".env")
    async with NacosClient(nacos_bootstrap()) as client:
        config = NacosConfigProvider(client)
        await config.refresh()
        snapshot = config.snapshot
        postgres = snapshot.client.postgres
        engine = create_postgres_engine_from_config(postgres)
        try:
            if args.apply and engine.url.database != args.database:
                raise ValueError("目标数据库与 --database 不一致")
            root = snapshot.workspace.runtime.root
            files = ProjectFileStorage(root, config_provider=config)
            storage = AgentKitStorage(postgres_url(postgres), engine, files=files)
            resources = ResourceService(
                storage, root, config=config, policies=snapshot.access
            )
            changes = await backfill_shared_skills(
                resources, apply=args.apply, resource_ids=args.skill_ids
            )
            print(
                json.dumps(
                    {"applied": args.apply, "changes": changes},
                    ensure_ascii=False,
                    indent=2,
                )
            )
        finally:
            await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--app-dir",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "egis-agentkit-app",
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--database")
    parser.add_argument(
        "--skill-id",
        action="append",
        dest="skill_ids",
        help="只发布此 NAS 可访问源目录的技能；可以重复指定",
    )
    args = parser.parse_args()
    if args.apply and not args.database:
        parser.error("发布副本必须显式指定 --database")
    asyncio.run(main(args))
