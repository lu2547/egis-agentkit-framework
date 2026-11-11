# 第三方财务专家

财务专家进程只依赖 FastAPI、Uvicorn 和 HTTPX。示例汇总输入中的金额，按远端会话记住最近一次合计，支持询问「刚才的合计是多少」。

## 启动

在 Egis 和财务专家两个进程中设置相同的演示环境变量。实际部署使用各自环境中的凭证管理。

```sh
export EGIS_REMOTE_AGENT_FINANCE_TOKEN='local-finance-demo-token'
export EGIS_REMOTE_AGENT_FINANCE_CALLBACK_SECRET='local-finance-callback-signing-secret-32'
```

在财务专家进程中设置允许的回包目标，随后从 Framework 目录启动财务进程。

```sh
export FINANCE_CALLBACK_BASE_URL='http://127.0.0.1:38092'
python -m uvicorn demo_service:app --app-dir examples/remote_finance --host 127.0.0.1 --port 8120
```

在 Egis 的 Nacos `agentkit.yaml` 中将 `agentkit.agent.remote.callback.publicBaseUrl` 设为财务专家可访问的 App 地址，本机示例为 `http://127.0.0.1:38092`。启动 Egis App 和工作台。Egis 仍需可用的原生模型配置，组长使用它编排协作。以管理员登录并分别将 [sync.json](sync.json)、[callback.json](callback.json)、[websocket.json](websocket.json) 提交到 `POST /manager/agents`。也可以在工作台新建三个远程专家并填写同样的配置。系统专家需按既有权限流程授权后才可使用。

## 验收

1. 分别打开三位专家的独立会话，发送「汇总三笔报销，金额为 120、85、45 元。」预期回复「三笔报销共 250 元。」
2. 在个人助理会话中选择任一财务专家并发送同一任务。邀请使用原生成员 Inbox 投递，成员调用原生 `TeamSay` 向组长汇报后完成该轮。
3. 让组长追问财务专家「刚才的合计是多少？」确认专家沿用该成员会话的远端 `conversation_id`，组长收到第二条汇报。三种协议分别执行。
4. 启动样例前设置 `FINANCE_CALLBACK_DELIVERIES=2`。异步专家对同一结果回包两次，Egis 返回 `accepted` 和 `duplicate`，只写入一次结果。
5. 将样例进程的 `FINANCE_WORK_DELAY` 设置为 3 秒，将对应专家的 `timeout_seconds` 调为 1 秒。确认会话以超时结束，后续回包返回冲突。恢复较长超时后，可在运行期间用会话的停止按钮验证远端取消。

## 协议

所有端点都校验 `Authorization: Bearer …`、`X-Egis-Connection` 和 `X-Egis-Protocol: v1`。三种方式以同一个 `run_id` 重试。同步 HTTP 在同一次请求返回结果。回调模式接受 `callback_url` 并返回 202。WebSocket 使用 `run.start`、`run.progress`、`run.completed`、`run.failed` 和 `run.cancel` / `run.cancelled`。

回包提供 `X-Egis-Timestamp`（Unix 秒）和 `X-Egis-Signature: sha256=<hex>`。签名输入是以换行隔开的时间戳、`X-Egis-Connection`、`run_id`，再拼接一个换行和原始 HTTP 请求正文。回包密钥与远端执行凭证独立。回包使用 Egis 提供的 URL。HTTP 取消端点接受 `{"run_id":"…"}` 并以 202 确认，属于可选能力。
