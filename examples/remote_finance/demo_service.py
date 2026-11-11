"""An independently runnable third-party v1 finance expert."""

import asyncio
import hashlib
import hmac
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from decimal import Decimal

import httpx
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request, WebSocket
from fastapi.websockets import WebSocketDisconnect
from pydantic import BaseModel, Field

app = FastAPI(title="Third-party Finance Expert")
logger = logging.getLogger(__name__)


class TaskInput(BaseModel):
    text: str


class RunRequest(BaseModel):
    run_id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,64}$")
    conversation_id: str
    input: TaskInput
    attachments: list[dict] = Field(default_factory=list)
    callback_url: str | None = None


@dataclass
class Task:
    request: RunRequest
    cancel: asyncio.Event
    result: asyncio.Task


tasks: dict[tuple[str, str], Task] = {}
last_amounts: dict[tuple[str, str], tuple[int, Decimal]] = {}


def authenticate(headers):
    expected = os.environ.get("EGIS_REMOTE_AGENT_FINANCE_TOKEN", "")
    authorization = headers.get("authorization", "")
    connection = headers.get("x-egis-connection", "")
    if (
        not expected
        or not authorization.isascii()
        or not hmac.compare_digest(authorization, f"Bearer {expected}")
        or not re.fullmatch(r"[a-zA-Z0-9_-]{1,128}", connection)
        or headers.get("x-egis-protocol") != "v1"
    ):
        raise HTTPException(401, "Connection credentials required")
    return connection


async def calculate(connection, request, cancel):
    try:
        await asyncio.wait_for(
            cancel.wait(), timeout=float(os.environ.get("FINANCE_WORK_DELAY", "0.15"))
        )
    except TimeoutError:
        pass
    if cancel.is_set():
        return {
            "run_id": request.run_id,
            "status": "cancelled",
            "error": {"code": "cancelled", "message": "财务任务已停止。"},
        }

    key = (connection, request.conversation_id)
    numbers = re.findall(r"-?\d+(?:\.\d+)?", request.input.text)
    if numbers:
        last_amounts[key] = len(numbers), sum(map(Decimal, numbers), start=Decimal(0))
    if key not in last_amounts:
        return {
            "run_id": request.run_id,
            "status": "failed",
            "error": {"code": "no_amounts", "message": "请提供要汇总的报销金额。"},
        }
    count, amount = last_amounts[key]
    text = f"三笔报销共 {amount:f} 元。" if count == 3 else f"{count} 笔报销共 {amount:f} 元。"
    return {"run_id": request.run_id, "status": "completed", "output": {"text": text}}


def submit(connection, request):
    key = (connection, request.run_id)
    existing = tasks.get(key)
    if existing:
        if existing.request != request:
            raise HTTPException(409, "Run ID belongs to different input")
        return existing
    cancel = asyncio.Event()
    task = Task(
        request,
        cancel,
        asyncio.create_task(calculate(connection, request, cancel)),
    )
    tasks[key] = task
    return task


@app.post("/sync/v1/runs")
async def synchronous(body: RunRequest, request: Request):
    task = submit(authenticate(request.headers), body)
    return await asyncio.shield(task.result)


async def deliver_callback(connection, task, secret):
    result = await asyncio.shield(task.result)
    raw = json.dumps(result, ensure_ascii=False).encode()
    duplicates = int(os.environ.get("FINANCE_CALLBACK_DELIVERIES", "1"))
    async with httpx.AsyncClient(timeout=3, trust_env=False) as client:
        delivered = 0
        for attempt in range(3 + duplicates):
            timestamp = str(int(time.time()))
            signed = f"{timestamp}\n{connection}\n{task.request.run_id}\n".encode() + raw
            signature = hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()
            try:
                response = await client.post(
                    task.request.callback_url,
                    content=raw,
                    headers={
                        "Content-Type": "application/json",
                        "X-Egis-Connection": connection,
                        "X-Egis-Timestamp": timestamp,
                        "X-Egis-Signature": f"sha256={signature}",
                    },
                )
                if response.is_success:
                    delivered += 1
                    if delivered >= duplicates:
                        return
                elif response.status_code < 500:
                    logger.warning("Egis rejected finance callback (%d)", response.status_code)
                    return
            except httpx.TransportError:
                pass
            await asyncio.sleep(0.2 * (attempt + 1))


@app.post("/callback/v1/runs", status_code=202)
async def callback(body: RunRequest, request: Request, background: BackgroundTasks):
    connection = authenticate(request.headers)
    base = os.environ.get("FINANCE_CALLBACK_BASE_URL", "").rstrip("/")
    secret = os.environ.get("EGIS_REMOTE_AGENT_FINANCE_CALLBACK_SECRET", "")
    expected = f"{base}/callbacks/remote-agent/runs/{body.run_id}/result"
    if not base or len(secret.encode()) < 32 or body.callback_url != expected:
        raise HTTPException(400, "Unexpected callback target")
    task = submit(connection, body)
    background.add_task(deliver_callback, connection, task, secret)
    return {"run_id": body.run_id, "status": "accepted"}


@app.post("/v1/cancel", status_code=202)
async def cancel(body: dict, request: Request):
    task = tasks.get((authenticate(request.headers), body.get("run_id")))
    if task is None:
        raise HTTPException(404, "Unknown run")
    if task.result.done():
        raise HTTPException(409, "Run already finished")
    task.cancel.set()
    return {"run_id": task.request.run_id, "status": "accepted"}


@app.websocket("/ws/v1/runs")
async def websocket(socket: WebSocket):
    try:
        connection = authenticate(socket.headers)
    except HTTPException:
        await socket.close(code=1008)
        return
    await socket.accept()
    try:
        event = await socket.receive_json()
        if event.pop("event", None) != "run.start":
            await socket.close(code=1008)
            return
        task = submit(connection, RunRequest.model_validate(event))
        run_id = task.request.run_id
        await socket.send_json(
            {"event": "run.progress", "run_id": run_id, "message": "正在汇总报销金额"}
        )
        incoming = asyncio.create_task(socket.receive_json())
        try:
            done, _ = await asyncio.wait(
                (task.result, incoming), return_when=asyncio.FIRST_COMPLETED
            )
            if incoming in done:
                if incoming.result() != {"event": "run.cancel", "run_id": run_id}:
                    await socket.close(code=1008)
                    return
                task.cancel.set()
            result = await asyncio.shield(task.result)
            await socket.send_json({"event": "run." + result["status"], **result})
        finally:
            incoming.cancel()
            await asyncio.gather(incoming, return_exceptions=True)
    except WebSocketDisconnect:
        pass
    except (HTTPException, ValueError, AttributeError):
        await socket.close(code=1008)
