"""Build a route-by-route SQL inventory from audit_sql.py and checked-out source."""

import argparse
import ast
import csv
import hashlib
import inspect
import json
import os
import re
import textwrap
from collections import Counter, defaultdict
from pathlib import Path

from agentscope.app._router._session import session_router as native_sessions
from egis_agentkit_app.bootstrap._router import router

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT.parent / "egis-agentkit-app"
NATIVE = ROOT.parents[1] / "harness/agentscope"


def routes(component):
    for route in component.routes:
        if hasattr(route, "effective_route_contexts"):
            yield from route.effective_route_contexts()
        elif hasattr(route, "endpoint"):
            yield route


def expression(node):
    return ast.unparse(node).replace("\n", " ")


def source_link(output, filename, line):
    relative = os.path.relpath(filename, output)
    return f"[{Path(filename).name}:{line}]({relative}#L{line})"


def inventory(output):
    """Static edges remain source expressions; no claim of resolving dynamic dispatch."""
    trees = [
        ROOT / "src/egis_agentkit",
        APP / "src/egis_agentkit_app/controller",
        NATIVE / "src/agentscope/app",
    ]
    methods, sites = [], []
    for tree in trees:
        for path in sorted(tree.rglob("*.py")):
            module = ast.parse(path.read_text())

            def walk(node, parents=()):
                for child in ast.iter_child_nodes(node):
                    if isinstance(child, ast.ClassDef):
                        walk(child, (*parents, child.name))
                    elif isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        name = ".".join((*parents, child.name))
                        calls = [n for n in ast.walk(child) if isinstance(n, ast.Call)]
                        awaited = [
                            expression(n.value.func)
                            for n in ast.walk(child)
                            if isinstance(n, ast.Await)
                            and isinstance(n.value, ast.Call)
                        ]
                        loop_awaits = [
                            n.lineno
                            for loop in ast.walk(child)
                            if isinstance(
                                loop,
                                (
                                    ast.For,
                                    ast.While,
                                    ast.AsyncFor,
                                    ast.ListComp,
                                    ast.DictComp,
                                    ast.SetComp,
                                    ast.GeneratorExp,
                                ),
                            )
                            for n in ast.walk(loop)
                            if isinstance(n, ast.Await)
                        ]
                        methods.append(
                            {
                                "file": os.path.relpath(path, ROOT),
                                "line": child.lineno,
                                "method": name,
                                "awaited_calls": "; ".join(dict.fromkeys(awaited)),
                                "await_in_loops": "; ".join(
                                    map(str, sorted(set(loop_awaits)))
                                ),
                            }
                        )
                        for call in calls:
                            func = expression(call.func)
                            if (
                                "/storage/" in str(path) or path.name == "_schema.py"
                            ) and func.split(".")[-1] in {
                                "execute",
                                "scalar",
                                "scalars",
                                "stream",
                                "stream_scalars",
                                "exec_driver_sql",
                            }:
                                sites.append(
                                    {
                                        "file": os.path.relpath(path, ROOT),
                                        "line": call.lineno,
                                        "method": name,
                                        "call": expression(call),
                                    }
                                )
                        walk(child, (*parents, child.name))
                    else:
                        walk(child, parents)

            walk(module)
    for filename, rows in [("method-edges.csv", methods), ("sql-sites.csv", sites)]:
        with (output / filename).open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
    return len(methods), len(sites)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--measurements", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    data = json.loads(args.measurements.read_text())
    endpoints = list(routes(router))
    for route in routes(native_sessions):
        if re.fullmatch(
            r"/sessions/\{session_id\}/(stream|messages|status|interrupt)", route.path
        ):
            endpoints.append(route)
    measurements = defaultdict(list)
    unmatched = []
    for trace in data["traces"]:
        if not trace["entry"].startswith("HTTP:"):
            continue
        method, path = trace["entry"][5:].split(" ", 1)
        for route in endpoints:
            if method in route.methods and route.path_regex.fullmatch(path):
                measurements[(method, route.path)].append(trace)
                break
        else:
            unmatched.append(trace["entry"])

    statements = data["statements"]
    sql_shapes = {}
    for item in statements.values():
        key = hashlib.sha256(item["sql"].encode()).hexdigest()[:12]
        item["shape"] = key
        sql_shapes[key] = item["sql"]
    with (args.output / "sql-shapes.csv").open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["sql_id", "sql_without_values"])
        writer.writerows(sorted(sql_shapes.items()))

    lines = [
        "# HTTP 调用链逐项清单",
        "",
        "按实际注册路由生成，含所有 117 个企业入口及 4 个获准开放的原生会话入口。",
        "`有身份 SQL` 表示调用栈实测经过 IdentityGuard；`测试替身` 不计真实登录开销。",
        "成功样本只统计 HTTP <400；失败样本单列。计数包含 SELECT/INSERT/UPDATE/DELETE/锁 SQL，",
        "不包含驱动隐式 BEGIN/COMMIT；后台 SQL 单列，不算入 HTTP 前台耗时。",
        "未实测的入口保留静态调用表达式和源码位置，不填推测次数。SQL ID 在 sql-shapes.csv 查完整脱敏语句。",
        "",
    ]
    counts = Counter()
    for route in endpoints:
        filename = inspect.getsourcefile(route.endpoint)
        source, start = inspect.getsourcelines(route.endpoint)
        node = ast.parse(textwrap.dedent("".join(source)))
        calls = list(
            dict.fromkeys(
                expression(n.value.func)
                for n in ast.walk(node)
                if isinstance(n, ast.Await) and isinstance(n.value, ast.Call)
            )
        )
        for method in sorted(route.methods):
            counts["endpoints"] += 1
            samples = measurements[(method, route.path)]
            successful = [
                t for t in samples if not t.get("error") and t.get("status", 500) < 400
            ]
            counts["http_success"] += bool(successful)
            lines += [
                f"## {method} {route.path}",
                "",
                f"入口：{source_link(args.output, filename, start)} `{route.endpoint.__qualname__}`。",
                "静态下游："
                + (" → ".join(f"`{c}`" for c in calls) or "无异步下游调用")
                + "。",
                "",
            ]
            if not samples:
                lines += ["实测：未覆盖；只做静态审查。", ""]
                continue
            full = []
            for trace in samples:
                real_identity = any(
                    any(
                        "_http_identity_middleware_impl.py" in f
                        for f in statements[q["id"]]["chain"]
                    )
                    for q in trace["queries"]
                )
                if real_identity:
                    full.append(trace)
            for label, group in [
                ("有身份 SQL", full),
                ("测试替身/认证前拒绝", [t for t in samples if t not in full]),
            ]:
                valid = [t for t in group if t in successful]
                failures = len(group) - len(valid)
                if not group:
                    continue
                if not valid:
                    lines += [
                        f"实测 {label}：仅 {failures} 个失败分支，没有成功样本。",
                        "",
                    ]
                    continue
                sizes = [sum(not q["background"] for q in t["queries"]) for t in valid]
                trace = max(
                    valid, key=lambda t: sum(not q["background"] for q in t["queries"])
                )
                background = sum(q["background"] for q in trace["queries"])
                lines += [
                    f"实测 {label}：{len(valid)} 个成功样本，前台 SQL {min(sizes)}～{max(sizes)} 次；"
                    f"另有 {failures} 个失败分支。以下是前台查询最多的成功样本；后台另有 {background} 次。",
                    f"复现：`{trace['test']}`。",
                    "",
                    "| 次数 | SQL ID | 调用链 |",
                    "|---:|---|---|",
                ]
                grouped = Counter(
                    (statements[q["id"]]["shape"], tuple(statements[q["id"]]["chain"]))
                    for q in trace["queries"]
                    if not q["background"]
                )
                for (shape, chain), count in grouped.items():
                    display = " → ".join(f"`{f}`" for f in chain) or "无 Python 来源栈"
                    lines.append(f"| {count} | `{shape}` | {display} |")
                lines.append("")
    (args.output / "http-chains.md").write_text("\n".join(lines))

    services = defaultdict(list)
    for trace in data["traces"]:
        if trace["entry"].startswith("service:") and not trace.get("error"):
            services[trace["entry"]].append(trace)
    with (args.output / "service-entries.csv").open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(
            [
                "entry",
                "successful_samples",
                "min_sql",
                "max_sql",
                "max_example_test",
                "max_sql_ids_and_counts",
            ]
        )
        for entry, samples in sorted(services.items()):
            trace = max(samples, key=lambda t: len(t["queries"]))
            grouped = Counter(statements[q["id"]]["shape"] for q in trace["queries"])
            writer.writerow(
                [
                    entry,
                    len(samples),
                    min(len(t["queries"]) for t in samples),
                    len(trace["queries"]),
                    trace["test"],
                    json.dumps(grouped, sort_keys=True),
                ]
            )
    methods, sites = inventory(args.output)
    print(
        json.dumps(
            {
                **counts,
                "service_entries": len(services),
                "source_methods": methods,
                "sql_execution_sites": sites,
                "executed_sql_shapes": len(sql_shapes),
                "pytest_exit_code": data["pytest_exit_code"],
                "unmatched_http_entries": sorted(set(unmatched)),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
