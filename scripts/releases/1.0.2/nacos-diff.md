# 1.0.1 → 1.0.2 示例配置字段差异

基线为 Git 标签 `1.0.1`，目标为当前 1.0.2 发布示例。功能设置按用户确认的本机 Nacos revision 39 对齐；镜像、知识库地址和实际部署绑定由环境提供。操作含义见 [nacos.md](nacos.md)。

数组下标从 0 开始；`—` 表示该字段不存在。不要用示例地址、凭据占位值或本机路径覆盖公司环境。

| 配置路径 | 操作 | 1.0.1 示例 | 1.0.2 示例 |
| --- | --- | --- | --- |
| `agentkit.access.companyGroupId` | 新增 | — | `"company"` |
| `agentkit.access.companyGroupName` | 新增 | — | `"公司组"` |
| `agentkit.admin.pagination.defaultPageSize` | 新增 | — | `20` |
| `agentkit.admin.pagination.maxPageSize` | 新增 | — | `100` |
| `agentkit.agent.catalog.defaultPageSize` | 修改 | `50` | `10` |
| `agentkit.agent.personal.memory.isEnabled` | 删除 | `false` | — |
| `agentkit.agent.personal.memory.topK` | 删除 | `5` | — |
| `agentkit.client.kafka.connection.deploymentMode` | 修改 | `"standalone"` | `"cluster"` |
| `agentkit.client.postgres.connection.connectTimeoutSeconds` | 新增 | — | `10.0` |
| `agentkit.client.postgres.connection.database` | 新增 | — | `"agentscope"` |
| `agentkit.client.postgres.connection.host` | 新增 | — | `"pg.company.internal"` |
| `agentkit.client.postgres.connection.mode` | 新增 | — | `"key"` |
| `agentkit.client.postgres.connection.password` | 新增 | — | `""` |
| `agentkit.client.postgres.connection.port` | 新增 | — | `5432` |
| `agentkit.client.postgres.connection.sslCaFile` | 新增 | — | `"/etc/agentkit/certs/pg-ca.pem"` |
| `agentkit.client.postgres.connection.sslMode` | 新增 | — | `"verify-full"` |
| `agentkit.client.postgres.connection.urlEnv` | 删除 | `"AGENTKIT_DATABASE_URL"` | — |
| `agentkit.client.postgres.connection.username` | 新增 | — | `"agentkit"` |
| `agentkit.client.tracing.enabled` | 新增 | — | `true` |
| `agentkit.client.tracing.endpoint` | 新增 | — | `"http://127.0.0.1:6006/v1/traces"` |
| `agentkit.client.tracing.exportTimeoutSeconds` | 新增 | — | `10.0` |
| `agentkit.client.tracing.headers` | 新增 | — | `{}` |
| `agentkit.client.tracing.httpProjectName` | 新增 | — | `"egis-agentkit-http"` |
| `agentkit.client.tracing.maxExportBatchSize` | 新增 | — | `512` |
| `agentkit.client.tracing.maxQueueSize` | 新增 | — | `2048` |
| `agentkit.client.tracing.projectName` | 新增 | — | `"egis-agentkit"` |
| `agentkit.client.tracing.sampleRatio` | 新增 | — | `1.0` |
| `agentkit.client.tracing.scheduleDelayMilliseconds` | 新增 | — | `5000` |
| `agentkit.client.tracing.serviceName` | 新增 | — | `"egis-agentkit"` |
| `agentkit.client.tracing.shutdownTimeoutMilliseconds` | 新增 | — | `5000` |
| `agentkit.connector.catalog.defaultPageSize` | 新增 | — | `10` |
| `agentkit.connector.catalog.maxCursorLength` | 新增 | — | `2048` |
| `agentkit.connector.catalog.maxPageSize` | 新增 | — | `100` |
| `agentkit.connector.catalog.maxQueryLength` | 新增 | — | `500` |
| `agentkit.logging.accessLog` | 新增 | — | `true` |
| `agentkit.logging.directory` | 新增 | — | `null` |
| `agentkit.logging.format` | 新增 | — | `"json"` |
| `agentkit.logging.level` | 新增 | — | `"INFO"` |
| `agentkit.logging.textDateFormat` | 新增 | — | `null` |
| `agentkit.logging.textPattern` | 新增 | — | `"{time} [{level}] {package}.{function} - {message}"` |
| `agentkit.project.memory.isEnabled` | 新增 | — | `true` |
| `agentkit.project.memory.topK` | 新增 | — | `5` |
| `agentkit.session.steering.maxBatchSize` | 新增 | — | `20` |
| `agentkit.session.steering.maxMessageBytes` | 新增 | — | `1048576` |
| `agentkit.session.steering.maxTextLength` | 新增 | — | `32000` |
| `agentkit.skill.avatar.enabled` | 新增 | — | `true` |
| `agentkit.skill.avatar.modelKey` | 新增 | — | `null` |
| `agentkit.skill.avatar.timeoutSeconds` | 新增 | — | `45` |
| `agentkit.skill.catalog.defaultPageSize` | 新增 | — | `10` |
| `agentkit.skill.catalog.maxCursorLength` | 新增 | — | `2048` |
| `agentkit.skill.catalog.maxPageSize` | 新增 | — | `100` |
| `agentkit.skill.catalog.maxQueryLength` | 新增 | — | `500` |
| `agentkit.skill.import.approvalBatchLimit` | 新增 | — | `50` |
| `agentkit.skill.storage.root` | 修改 | `"/mnt/company-nas/agentkit/skill-archives"` | `"/mnt/company-nas/agentkit/skill-uploads"` |
| `agentkit.tool.catalog.defaultPageSize` | 新增 | — | `10` |
| `agentkit.tool.catalog.maxCursorLength` | 新增 | — | `2048` |
| `agentkit.tool.catalog.maxPageSize` | 新增 | — | `100` |
| `agentkit.tool.catalog.maxQueryLength` | 新增 | — | `500` |
| `agentkit.tool.knowledgeBase.queryUrl` | 新增 | — | `null` |
| `agentkit.tool.knowledgeBase.timeoutSeconds` | 新增 | — | `1800.0` |
| `agentkit.user.identity.demoMinPasswordLength` | 删除 | `16` | — |
| `agentkit.user.identity.demoPasswordEnv` | 删除 | `"AGENTKIT_DEMO_PASSWORD"` | — |
| `agentkit.user.identity.jwtLifetimeSeconds` | 删除 | `3600` | — |
| `agentkit.user.identity.jwtMinSigningSecretLength` | 删除 | `32` | — |
| `agentkit.user.identity.signingSecretEnv` | 删除 | `"AGENTKIT_JWT_SECRET"` | — |
| `agentkit.workspace.files.timeoutSeconds` | 新增 | — | `60.0` |
| `agentkit.workspace.files.tokenEnv` | 新增 | — | `"AGENTKIT_NAS_FILE_TOKEN"` |
| `agentkit.workspace.files.win` | 新增 | — | `false` |
| `agentkit.workspace.runtime.mode` | 修改 | `"opensandbox"` | `"pks"` |
| `agentkit.workspace.runtime.opensandbox.apiKeyEnv` | 删除 | `"OPEN_SANDBOX_API_KEY"` | — |
| `agentkit.workspace.runtime.opensandbox.bootstrapCommands[0]` | 删除 | `"apt-get update -qq && apt-get install -y --no-install-recommends curl ca-certificates ripgrep && update-ca-certificates && rm -rf /var/lib/apt/lists/*"` | — |
| `agentkit.workspace.runtime.opensandbox.bootstrapCommands[1]` | 删除 | `"curl -LsSf https://astral.sh/uv/{uv_version}/install.sh \| env UV_INSTALL_DIR=/usr/local/bin INSTALLER_NO_MODIFY_PATH=1 sh"` | — |
| `agentkit.workspace.runtime.opensandbox.bootstrapCommands[2]` | 删除 | `"uv venv {gateway_venv}"` | — |
| `agentkit.workspace.runtime.opensandbox.bootstrapCommands[3]` | 删除 | `"uv pip install --python {gateway_python} {pip_requirements}"` | — |
| `agentkit.workspace.runtime.opensandbox.bootstrapCommands[4]` | 删除 | `"uv pip install --python {gateway_python} --no-deps agentscope"` | — |
| `agentkit.workspace.runtime.opensandbox.bootstrapTimeoutSeconds` | 删除 | `600` | — |
| `agentkit.workspace.runtime.opensandbox.domain` | 删除 | `"127.0.0.1:8090"` | — |
| `agentkit.workspace.runtime.opensandbox.entrypoint[0]` | 删除 | `"tail"` | — |
| `agentkit.workspace.runtime.opensandbox.entrypoint[1]` | 删除 | `"-f"` | — |
| `agentkit.workspace.runtime.opensandbox.entrypoint[2]` | 删除 | `"/dev/null"` | — |
| `agentkit.workspace.runtime.opensandbox.env` | 删除 | `{}` | — |
| `agentkit.workspace.runtime.opensandbox.extraPip[0]` | 删除 | `"docstring-parser"` | — |
| `agentkit.workspace.runtime.opensandbox.extraPip[1]` | 删除 | `"jinja2"` | — |
| `agentkit.workspace.runtime.opensandbox.extraPip[2]` | 删除 | `"aiofiles"` | — |
| `agentkit.workspace.runtime.opensandbox.extraPip[3]` | 删除 | `"python-frontmatter"` | — |
| `agentkit.workspace.runtime.opensandbox.extraPip[4]` | 删除 | `"tree-sitter"` | — |
| `agentkit.workspace.runtime.opensandbox.extraPip[5]` | 删除 | `"tree-sitter-bash"` | — |
| `agentkit.workspace.runtime.opensandbox.extraPip[6]` | 删除 | `"pypdf"` | — |
| `agentkit.workspace.runtime.opensandbox.gatewayHome` | 删除 | `"/root/.agentscope"` | — |
| `agentkit.workspace.runtime.opensandbox.gatewayPort` | 删除 | `5600` | — |
| `agentkit.workspace.runtime.opensandbox.gatewayTmpDir` | 删除 | `"/dev/shm"` | — |
| `agentkit.workspace.runtime.opensandbox.image` | 删除 | `"python:3.11-slim"` | — |
| `agentkit.workspace.runtime.opensandbox.inputsReadOnly` | 删除 | `true` | — |
| `agentkit.workspace.runtime.opensandbox.instructions` | 删除 | `"当前执行环境为企业沙箱，工作根目录：{workdir}。"` | — |
| `agentkit.workspace.runtime.opensandbox.maxSkillBytes` | 删除 | `52428800` | — |
| `agentkit.workspace.runtime.opensandbox.protocol` | 删除 | `"http"` | — |
| `agentkit.workspace.runtime.opensandbox.readyTimeoutSeconds` | 删除 | `60` | — |
| `agentkit.workspace.runtime.opensandbox.renewIntervalSeconds` | 删除 | `60` | — |
| `agentkit.workspace.runtime.opensandbox.requestTimeoutSeconds` | 删除 | `600` | — |
| `agentkit.workspace.runtime.opensandbox.resource.cpu` | 删除 | `"1"` | — |
| `agentkit.workspace.runtime.opensandbox.resource.memory` | 删除 | `"1Gi"` | — |
| `agentkit.workspace.runtime.opensandbox.sweepIntervalSeconds` | 删除 | `60` | — |
| `agentkit.workspace.runtime.opensandbox.timeoutSeconds` | 删除 | `3600` | — |
| `agentkit.workspace.runtime.opensandbox.ttlSeconds` | 删除 | `900` | — |
| `agentkit.workspace.runtime.opensandbox.useServerProxy` | 删除 | `true` | — |
| `agentkit.workspace.runtime.opensandbox.workdir` | 删除 | `"/workspace"` | — |
| `agentkit.workspace.runtime.sandbox.apiKeyEnv` | 新增 | — | `"OPEN_SANDBOX_API_KEY"` |
| `agentkit.workspace.runtime.sandbox.bootstrapCommands[0]` | 新增 | — | `"apt-get update -qq && apt-get install -y --no-install-recommends curl ca-certificates ripgrep && update-ca-certificates && rm -rf /var/lib/apt/lists/*"` |
| `agentkit.workspace.runtime.sandbox.bootstrapCommands[1]` | 新增 | — | `"curl -LsSf https://astral.sh/uv/{uv_version}/install.sh \| env UV_INSTALL_DIR=/usr/local/bin INSTALLER_NO_MODIFY_PATH=1 sh"` |
| `agentkit.workspace.runtime.sandbox.bootstrapCommands[2]` | 新增 | — | `"uv venv {gateway_venv}"` |
| `agentkit.workspace.runtime.sandbox.bootstrapCommands[3]` | 新增 | — | `"uv pip install --python {gateway_python} {pip_requirements}"` |
| `agentkit.workspace.runtime.sandbox.bootstrapCommands[4]` | 新增 | — | `"uv pip install --python {gateway_python} --no-deps 'agentscope==2.0.12'"` |
| `agentkit.workspace.runtime.sandbox.bootstrapTimeoutSeconds` | 新增 | — | `600` |
| `agentkit.workspace.runtime.sandbox.domain` | 新增 | — | `"127.0.0.1:8090"` |
| `agentkit.workspace.runtime.sandbox.entrypoint[0]` | 新增 | — | `"tail"` |
| `agentkit.workspace.runtime.sandbox.entrypoint[1]` | 新增 | — | `"-f"` |
| `agentkit.workspace.runtime.sandbox.entrypoint[2]` | 新增 | — | `"/dev/null"` |
| `agentkit.workspace.runtime.sandbox.env` | 新增 | — | `{}` |
| `agentkit.workspace.runtime.sandbox.extraPip[0]` | 新增 | — | `"docstring-parser"` |
| `agentkit.workspace.runtime.sandbox.extraPip[1]` | 新增 | — | `"jinja2"` |
| `agentkit.workspace.runtime.sandbox.extraPip[2]` | 新增 | — | `"aiofiles"` |
| `agentkit.workspace.runtime.sandbox.extraPip[3]` | 新增 | — | `"python-frontmatter"` |
| `agentkit.workspace.runtime.sandbox.extraPip[4]` | 新增 | — | `"tree-sitter"` |
| `agentkit.workspace.runtime.sandbox.extraPip[5]` | 新增 | — | `"tree-sitter-bash"` |
| `agentkit.workspace.runtime.sandbox.extraPip[6]` | 新增 | — | `"pypdf"` |
| `agentkit.workspace.runtime.sandbox.gatewayHome` | 新增 | — | `"/root/.agentscope"` |
| `agentkit.workspace.runtime.sandbox.gatewayPort` | 新增 | — | `5600` |
| `agentkit.workspace.runtime.sandbox.gatewayTmpDir` | 新增 | — | `"/dev/shm"` |
| `agentkit.workspace.runtime.sandbox.image` | 新增 | — | `"python:3.11-slim"` |
| `agentkit.workspace.runtime.sandbox.inputsReadOnly` | 新增 | — | `true` |
| `agentkit.workspace.runtime.sandbox.instructions` | 新增 | — | `"当前执行环境为企业沙箱，工作根目录：{workdir}。"` |
| `agentkit.workspace.runtime.sandbox.maxSkillBytes` | 新增 | — | `52428799` |
| `agentkit.workspace.runtime.sandbox.protocol` | 新增 | — | `"http"` |
| `agentkit.workspace.runtime.sandbox.readyTimeoutSeconds` | 新增 | — | `60` |
| `agentkit.workspace.runtime.sandbox.renewIntervalSeconds` | 新增 | — | `60` |
| `agentkit.workspace.runtime.sandbox.requestTimeoutSeconds` | 新增 | — | `600` |
| `agentkit.workspace.runtime.sandbox.resource.cpu` | 新增 | — | `"1"` |
| `agentkit.workspace.runtime.sandbox.resource.memory` | 新增 | — | `"1Gi"` |
| `agentkit.workspace.runtime.sandbox.sweepIntervalSeconds` | 新增 | — | `60` |
| `agentkit.workspace.runtime.sandbox.timeoutSeconds` | 新增 | — | `3600` |
| `agentkit.workspace.runtime.sandbox.ttlSeconds` | 新增 | — | `900` |
| `agentkit.workspace.runtime.sandbox.useServerProxy` | 新增 | — | `true` |
