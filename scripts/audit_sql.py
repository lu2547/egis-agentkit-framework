"""Run pytest with SQL attribution; never record parameters, tokens or request bodies.

PYTHONPATH=../egis-agentkit-app/src .venv/bin/python scripts/audit_sql.py \
    --output /tmp/agentkit-sql-audit -- tests -q --ignore=tests/test_message_bus_kafka.py

Instrumentation exists only in this test process. Counts describe the exercised
fixtures/branches, not an upper bound for arbitrary production requests.
"""

import argparse
import contextvars
import csv
import functools
import hashlib
import inspect
import json
import re
import sys
import time
from pathlib import Path

import httpx
import pytest
from sqlalchemy import event
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[1]
CURRENT = contextvars.ContextVar("sql_audit", default=None)
CHAIN = contextvars.ContextVar("sql_audit_chain", default=())


class Audit:
    def __init__(self, output):
        self.output = output
        self.traces = []
        self.statements = {}
        self.nodeid = "collection"

    async def run(self, name, call):
        if CURRENT.get() is not None:
            return await call()
        trace = {"entry": name, "test": self.nodeid, "queries": []}
        self.traces.append(trace)
        marker = CURRENT.set(trace)
        started = time.perf_counter()
        try:
            result = await call()
            if isinstance(result, httpx.Response):
                trace["status"] = result.status_code
            return result
        except BaseException as exc:
            trace["error"] = type(exc).__name__
            raise
        finally:
            trace["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 3)
            trace["finished"] = True
            CURRENT.reset(marker)

    def before(self, conn, cursor, statement, parameters, context, executemany):
        trace = CURRENT.get()
        if trace is None:
            return
        # Omit literal values as well as bind values. Normalize generated schemas.
        sql = re.sub(r"'([^']|'')*'", "'?'", statement)
        sql = re.sub(r"\b(?:test|ak_test)_[a-f0-9]+\b", "test_schema", sql)
        sql = re.sub(r"\$\d+", "$?", sql)
        sql = re.sub(r"\s+", " ", sql).strip()
        # Carry attribution across greenlets with contextvars. Do not walk live
        # coroutine frames while SQLAlchemy's greenlet owns execution.
        chain = list(CHAIN.get())
        identity = hashlib.sha256((sql + "|" + "|".join(chain)).encode()).hexdigest()[
            :16
        ]
        self.statements.setdefault(identity, {"sql": sql, "chain": chain})
        query = {"id": identity, "background": bool(trace.get("finished"))}
        trace["queries"].append(query)
        context._audit_query = (query, time.perf_counter())

    def after(self, conn, cursor, statement, parameters, context, executemany):
        measurement = getattr(context, "_audit_query", None)
        if measurement:
            query, started = measurement
            query["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 3)

    @pytest.hookimpl(trylast=True)
    def pytest_collection_finish(self, session):
        # Public service entry points cover direct service tests; HTTP roots take
        # precedence and retain middleware SQL in the same request measurement.
        seen = set()
        for name, module in list(sys.modules.items()):
            if (
                not name.startswith(
                    (
                        "egis_agentkit.service.",
                        "egis_agentkit.storage.",
                        "egis_agentkit.middleware.http.",
                        "agentscope.app.storage._sql._storage",
                    )
                )
                or module is None
            ):
                continue
            for cls in list(vars(module).values()):
                if not inspect.isclass(cls) or cls in seen or cls.__module__ != name:
                    continue
                seen.add(cls)
                for method_name, descriptor in list(vars(cls).items()):
                    method = (
                        descriptor.__func__
                        if isinstance(descriptor, (staticmethod, classmethod))
                        else descriptor
                    )
                    if not inspect.iscoroutinefunction(method):
                        continue
                    entry = f"service:{cls.__name__}.{method_name}"
                    source = method.__code__.co_filename.split("/src/", 1)[-1]
                    location = f"{source}:{method.__code__.co_firstlineno} {cls.__name__}.{method_name}"
                    is_entry = name.startswith(
                        "egis_agentkit.service."
                    ) and not method_name.startswith("_")

                    def wrap(original, label, frame, root):
                        @functools.wraps(original)
                        async def invoke(*args, **kwargs):
                            marker = CHAIN.set((*CHAIN.get(), frame))
                            try:
                                if root:
                                    return await self.run(
                                        label, lambda: original(*args, **kwargs)
                                    )
                                return await original(*args, **kwargs)
                            finally:
                                CHAIN.reset(marker)

                        return invoke

                    wrapped = wrap(method, entry, location, is_entry)
                    if isinstance(descriptor, (staticmethod, classmethod)):
                        wrapped = type(descriptor)(wrapped)
                    setattr(cls, method_name, wrapped)

    def pytest_runtest_setup(self, item):
        self.nodeid = item.nodeid

    def install(self):
        event.listen(Engine, "before_cursor_execute", self.before)
        event.listen(Engine, "after_cursor_execute", self.after)
        original = httpx.ASGITransport.handle_async_request

        @functools.wraps(original)
        async def handle(transport, request):
            name = f"HTTP:{request.method} {request.url.path}"
            return await self.run(name, lambda: original(transport, request))

        httpx.ASGITransport.handle_async_request = handle

    def save(self, exit_code):
        self.output.mkdir(parents=True, exist_ok=True)
        (self.output / "measurements.json").write_text(
            json.dumps(
                {
                    "pytest_exit_code": exit_code,
                    "traces": self.traces,
                    "statements": self.statements,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        with (self.output / "entries.csv").open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(
                [
                    "entry",
                    "test",
                    "status_or_error",
                    "foreground_sql",
                    "background_sql",
                    "elapsed_ms",
                ]
            )
            for trace in self.traces:
                background = sum(q["background"] for q in trace["queries"])
                writer.writerow(
                    [
                        trace["entry"],
                        trace["test"],
                        trace.get("error", trace.get("status", "OK")),
                        len(trace["queries"]) - background,
                        background,
                        trace["elapsed_ms"],
                    ]
                )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    options, args = parser.parse_known_args()
    if args[:1] == ["--"]:
        args = args[1:]
    audit = Audit(options.output)
    audit.install()
    result = pytest.main(args, plugins=[audit])
    audit.save(int(result))
    return int(result)


if __name__ == "__main__":
    raise SystemExit(main())
