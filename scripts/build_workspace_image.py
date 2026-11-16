"""构建与应用同源的沙箱镜像，把依赖安装移出聊天请求。

核心能力
- 从导出的 Nacos YAML 读取沙箱依赖，不读取或打包用户工作区。
- 复用 AgentScope 的 Dockerfile 和网关脚本，打包指定源码的 wheel。
- 按构建内容生成镜像标签；构建成功后才允许部署该镜像。
"""

import argparse
import shutil
import subprocess
from pathlib import Path

import yaml
from agentscope.workspace._docker._make_dockerfile import (
    compute_image_tag,
    prepare_build_context,
)

from egis_agentkit.client.nacos._settings import SandboxConfig


def build_image(config_file: Path, agentscope_source: Path, base_image: str) -> str:
    """构建镜像并返回不可变的内容标签；任何安装失败都会中止发布。"""
    # 1. 校验配置，复用原生构建上下文，不把配置正文放进镜像。
    raw = yaml.safe_load(config_file.read_text())
    settings = SandboxConfig.model_validate(
        raw["agentkit"]["workspace"]["runtime"]["sandbox"]
    )
    context, _, files = prepare_build_context(
        base_image=base_image, extra_pip=settings.extra_pip
    )
    try:
        # 2. 应用与沙箱使用同一份源码，避免 PyPI 最新包与宿主接口不一致。
        subprocess.run(
            [
                "uv",
                "build",
                str(agentscope_source.resolve()),
                "--wheel",
                "--out-dir",
                str(context),
            ],
            check=True,
        )
        (wheel,) = context.glob("agentscope-*.whl")
        dockerfile = (
            (context / "Dockerfile")
            .read_text()
            .replace(
                'RUN uv pip install --no-deps "agentscope"',
                f"COPY {wheel.name} /tmp/{wheel.name}\n"
                f"RUN uv pip install --no-deps /tmp/{wheel.name} && rm /tmp/{wheel.name}",
            )
        )
        files[wheel.name] = wheel.read_bytes()
        tag = compute_image_tag(dockerfile, files).replace(
            "agentscope-workspace:", "agentkit-workspace:", 1
        )
        (context / "Dockerfile").write_text(dockerfile)
        # 3. 只有构建成功才输出可用于 Nacos image 字段的标签。
        subprocess.run(["docker", "build", "--tag", tag, str(context)], check=True)
        return tag
    finally:
        shutil.rmtree(context)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--agentscope-source", required=True, type=Path)
    parser.add_argument("--base-image", default="python:3.11-slim")
    args = parser.parse_args()
    print(build_image(args.config, args.agentscope_source, args.base_image))
