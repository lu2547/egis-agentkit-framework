import json
import logging
import re
import subprocess
import sys
from datetime import datetime

import pytest
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    TraceFlags,
    use_span,
)
from pydantic import SecretStr

from egis_agentkit.client.nacos._settings import LoggingConfig


def settings(*, level="INFO", format="json"):
    return LoggingConfig(level=level, format=format)


@pytest.mark.parametrize("namespace", ["egis_agentkit", "egis_agentkit_app"])
def test_info_is_visible_as_json_without_configuring_root(namespace, capsys):
    from egis_agentkit.client._logging import LoggingClient

    root = logging.getLogger()
    before = (root.handlers[:], root.level, root.propagate)
    client = LoggingClient(settings())
    client.start()
    try:
        logging.getLogger(f"{namespace}.business").info("任务 %s 已创建", "A")
        logging.getLogger(f"{namespace}.business").debug("hidden debug")
    finally:
        client.close()

    captured = capsys.readouterr()
    event = json.loads(captured.out)
    assert event["level"] == "INFO"
    assert event["logger"] == f"{namespace}.business"
    assert event["message"] == "任务 A 已创建"
    assert datetime.fromisoformat(event["time"]).tzinfo is not None
    assert "trace_id" not in event and "span_id" not in event
    assert captured.err == ""
    assert (root.handlers[:], root.level, root.propagate) == before


def test_text_format_honors_level_and_preserves_structured_context(capsys):
    from egis_agentkit.client._logging import LoggingClient

    client = LoggingClient(settings(level="WARNING", format="text"))
    client.start()
    try:
        logger = logging.getLogger("egis_agentkit.client.test")
        logger.info("hidden info")
        logger.warning("连接失败", extra={"context": {"operation": "connect"}})
    finally:
        client.close()

    captured = capsys.readouterr()
    assert "WARNING" in captured.out
    assert "egis_agentkit.client.test" in captured.out
    assert "连接失败" in captured.out
    assert '"operation": "connect"' in captured.out
    assert "hidden info" not in captured.out
    assert len(captured.out.splitlines()) == 1
    assert captured.err == ""


def test_text_pattern_controls_field_order_and_date_without_losing_context(capsys):
    from egis_agentkit.client._logging import LoggingClient

    logger = logging.getLogger("egis_agentkit.service.project")

    class ProjectManager:
        def create(self):
            logger.warning("目录创建失败", extra={"context": {"category": "system"}})

    client = LoggingClient(
        LoggingConfig(
            format="text",
            text_pattern="[{level}] {package}.{function} {time} {file}:{line} {message}",
            text_date_format="%Y/%m/%d %H:%M:%S %z",
        )
    )
    client.start()
    try:
        ProjectManager().create()
    finally:
        client.close()

    output = capsys.readouterr().out
    assert re.fullmatch(
        r"\[WARNING\] egis_agentkit\.service\.project\.ProjectManager\.create "
        r"\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} \+0000 "
        r'test_logging_client\.py:\d+ 目录创建失败 \{"context": \{"category": "system"\}\}\n',
        output,
    )


def test_json_names_the_actual_class_method_when_using_stacklevel(capsys):
    from egis_agentkit.client._logging import LoggingClient

    logger = logging.getLogger("egis_agentkit.service.project")

    def report():
        logger.info("project.created", stacklevel=2)

    class ProjectManager:
        @staticmethod
        def create():
            report()

    client = LoggingClient(LoggingConfig())
    client.start()
    try:
        ProjectManager.create()
    finally:
        client.close()

    event = json.loads(capsys.readouterr().out)
    assert event["package"] == "egis_agentkit.service.project"
    assert event["function"] == "ProjectManager.create"
    assert event["file"] == "test_logging_client.py"
    assert isinstance(event["line"], int)


def test_embedded_logging_writes_configured_directory_with_the_same_format(
    tmp_path, capsys
):
    from egis_agentkit.client._logging import LoggingClient

    directory = tmp_path / "service" / "logs"
    client = LoggingClient(
        LoggingConfig(directory=directory, format="text", level="WARNING")
    )
    client.start()
    try:
        logger = logging.getLogger("egis_agentkit.client.test")
        logger.info("hidden info")
        logger.warning("工作区创建失败")
    finally:
        client.close()

    (log_file,) = directory.glob("agentkit-*.log")
    output = log_file.read_text(encoding="utf-8")
    assert output == capsys.readouterr().out
    assert "[WARNING] egis_agentkit.client.test." in output
    assert " - 工作区创建失败" in output
    assert len(output.splitlines()) == 1


def test_embedded_logging_rejects_unusable_directory(tmp_path):
    from egis_agentkit.client._logging import LoggingClient

    directory = tmp_path / "regular-file"
    directory.touch()
    client = LoggingClient(LoggingConfig(directory=directory))
    with pytest.raises(FileExistsError):
        client.start()
    client.close()


def test_start_and_close_are_repeatable_and_restore_namespace_state(capsys):
    from egis_agentkit.client._logging import LoggingClient

    namespaces = [
        logging.getLogger(name) for name in ("egis_agentkit", "egis_agentkit_app")
    ]
    original = [
        (logger.handlers[:], logger.level, logger.propagate) for logger in namespaces
    ]
    existing = logging.NullHandler()
    for logger in namespaces:
        logger.handlers[:] = [existing]
        logger.setLevel(logging.ERROR)
        logger.propagate = True
    client = LoggingClient(settings())
    try:
        client.close()
        for _ in range(2):
            client.start()
            client.start()
            logging.getLogger("egis_agentkit.lifecycle").info("started once")
            client.close()
            client.close()
            for logger in namespaces:
                assert logger.handlers == [existing]
                assert logger.level == logging.ERROR
                assert logger.propagate is True
        assert [
            json.loads(line)["message"] for line in capsys.readouterr().out.splitlines()
        ] == ["started once", "started once"]
    finally:
        client.close()
        for logger, (handlers, level, propagate) in zip(namespaces, original):
            logger.handlers[:] = handlers
            logger.setLevel(level)
            logger.propagate = propagate


def test_overlapping_clients_do_not_duplicate_or_restore_closed_handlers(capsys):
    from egis_agentkit.client._logging import LoggingClient

    logger = logging.getLogger("egis_agentkit")
    before = (logger.handlers[:], logger.level, logger.propagate)
    first = LoggingClient(settings())
    second = LoggingClient(settings(level="WARNING"))
    first.start()
    second.start()
    try:
        logging.getLogger("egis_agentkit.lifecycle").warning("both started")
        first.close()
        logging.getLogger("egis_agentkit.lifecycle").warning("first closed")
    finally:
        second.close()
        first.close()

    assert [
        json.loads(line)["message"] for line in capsys.readouterr().out.splitlines()
    ] == ["both started", "first closed"]
    assert (logger.handlers[:], logger.level, logger.propagate) == before


@pytest.mark.parametrize("format", ["json", "text"])
def test_current_valid_otel_span_is_correlated(format, capsys):
    from egis_agentkit.client._logging import LoggingClient

    span_context = SpanContext(
        trace_id=0x123456789ABCDEF0123456789ABCDEF0,
        span_id=0x123456789ABCDEF0,
        is_remote=False,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
    )
    client = LoggingClient(settings(format=format))
    client.start()
    try:
        with use_span(NonRecordingSpan(span_context)):
            logging.getLogger("egis_agentkit.trace").info("correlated")
    finally:
        client.close()

    output = capsys.readouterr().out
    assert "123456789abcdef0123456789abcdef0" in output
    assert "123456789abcdef0" in output
    if format == "json":
        event = json.loads(output)
        assert event["trace_id"] == "123456789abcdef0123456789abcdef0"
        assert event["span_id"] == "123456789abcdef0"


@pytest.mark.parametrize("format", ["json", "text"])
def test_context_redacts_nested_credentials_without_dumping_record(format, capsys):
    from egis_agentkit.client._logging import LoggingClient

    class SensitiveObject:
        def __repr__(self):
            raise AssertionError("arbitrary context objects must not be rendered")

    context = {
        "operation": "refresh",
        "password": "password-value",
        "nested": {
            "apiKey": "api-key-value",
            "Authorization": "Bearer bearer-value",
            "db_dsn": "postgresql://user:dsn-value@db/name",
            "values": [{"refresh_token": "refresh-value", "safe_id": 7}],
        },
        "clientSecret": "secret-value",
        "credential": SecretStr("wrapped-value"),
        "unsupported": SensitiveObject(),
    }
    client = LoggingClient(settings(format=format))
    client.start()
    try:
        logging.getLogger("egis_agentkit.context").info(
            "configuration changed",
            extra={"context": context, "raw_record_secret": "record-value"},
        )
    finally:
        client.close()

    output = capsys.readouterr().out
    for forbidden in (
        "password-value",
        "api-key-value",
        "bearer-value",
        "dsn-value",
        "refresh-value",
        "secret-value",
        "wrapped-value",
        "record-value",
    ):
        assert forbidden not in output
    assert "refresh" in output and "safe_id" in output
    assert context["password"] == "password-value"
    assert context["nested"]["apiKey"] == "api-key-value"
    if format == "json":
        event = json.loads(output)
        assert event["context"]["password"] == "[REDACTED]"
        assert event["context"]["nested"]["values"][0]["safe_id"] == 7
        assert event["context"]["unsupported"] == "[UNSUPPORTED]"
        assert "raw_record_secret" not in event


@pytest.mark.parametrize("format", ["json", "text"])
def test_exception_reports_type_and_location_without_exception_contents(format, capsys):
    from egis_agentkit.client._logging import LoggingClient

    client = LoggingClient(settings(format=format))
    client.start()
    try:
        try:
            raise ValueError("postgresql://user:exception-secret@db/name")
        except ValueError:
            logging.getLogger("egis_agentkit.exception").exception("operation failed")
    finally:
        client.close()

    output = capsys.readouterr().out
    assert "ValueError" in output
    assert "test_logging_client.py" in output
    assert "exception-secret" not in output
    assert "raise ValueError" not in output
    if format == "json":
        event = json.loads(output)
        assert event["exception"]["type"] == "ValueError"
        assert event["exception"]["frames"]


def test_unusual_context_cannot_trigger_logging_errors_or_invalid_json(capsys):
    from egis_agentkit.client._logging import LoggingClient

    class UnexpectedMapping(dict):
        def items(self):
            raise ValueError("context-secret")

    recursive = []
    recursive.append(recursive)
    client = LoggingClient(settings())
    client.start()
    try:
        logging.getLogger("egis_agentkit.context").info(
            "context checked",
            extra={
                "context": {
                    "nested": UnexpectedMapping(),
                    "recursive": recursive,
                    "non_finite": float("nan"),
                }
            },
        )
    finally:
        client.close()

    captured = capsys.readouterr()
    assert captured.err == ""
    assert "context-secret" not in captured.out
    event = json.loads(captured.out)
    assert event["context"]["nested"] == "[UNSUPPORTED]"
    assert event["context"]["recursive"] == ["[CIRCULAR]"]
    assert event["context"]["non_finite"] == "[UNSUPPORTED]"


def test_excessively_nested_context_does_not_break_logging(capsys):
    from egis_agentkit.client._logging import LoggingClient

    context = {}
    for _ in range(1500):
        context = {"nested": context}
    client = LoggingClient(settings())
    client.start()
    try:
        logging.getLogger("egis_agentkit.context").info(
            "context checked", extra={"context": context}
        )
    finally:
        client.close()

    captured = capsys.readouterr()
    assert captured.err == ""
    event = json.loads(captured.out)
    assert event["message"] == "context checked"
    assert event["context"] == "[UNSUPPORTED]"


def test_chained_failure_records_component_and_original_frames_safely(capsys):
    from egis_agentkit.client._logging import LoggingClient

    class Provider:
        def call(self):
            try:
                raise ConnectionError("upstream-api-secret")
            except ConnectionError as exc:
                raise RuntimeError("request-prompt-secret") from exc

    client = LoggingClient(settings())
    client.start()
    try:
        try:
            Provider().call()
        except RuntimeError:
            logging.getLogger("egis_agentkit.provider").exception(
                "Provider call failed"
            )
    finally:
        client.close()
    output = capsys.readouterr().out
    event = json.loads(output)
    assert event["package"] == "egis_agentkit.provider"
    assert event["component"].endswith(
        "test_chained_failure_records_component_and_original_frames_safely"
    )
    assert event["exception"]["type"] == "RuntimeError"
    cause = event["exception"]["cause"]
    assert cause["type"] == "ConnectionError"
    assert cause["frames"][-1]["function"].endswith("Provider.call")
    assert "upstream-api-secret" not in output
    assert "request-prompt-secret" not in output


@pytest.mark.parametrize("write_file", [False, True])
def test_process_logging_unifies_uvicorn_and_scrubs_native_runtime_messages(
    tmp_path, write_file
):
    script = """
import logging
import sys

from egis_agentkit.client._logging import configure_process_logging
from egis_agentkit.client.nacos._settings import LoggingConfig

directory = sys.argv[1] if len(sys.argv) > 1 else None
configure_process_logging(LoggingConfig(directory=directory))
logging.getLogger("uvicorn.error").info("Server started")
logging.getLogger("uvicorn.access").info("private-query")
try:
    raise RuntimeError("provider-secret")
except RuntimeError:
    logging.getLogger("as").exception("Unhandled operation %s", "private-prompt")
"""
    directory = tmp_path / "process-logs"
    command = [sys.executable, "-c", script]
    if write_file:
        command.append(str(directory))
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    assert result.stderr == ""
    assert len(result.stdout.splitlines()) == 2
    uvicorn, native = [json.loads(line) for line in result.stdout.splitlines()]
    assert uvicorn["logger"] == "uvicorn.error"
    assert uvicorn["message"] == "Server started"
    assert native["logger"] == "as"
    assert native["context"]["category"] == "system"
    assert native["exception"]["type"] == "RuntimeError"
    assert "provider-secret" not in result.stdout
    assert "private-query" not in result.stdout
    assert "private-prompt" not in result.stdout
    if write_file:
        (log_file,) = directory.glob("agentkit-*.log")
        assert log_file.read_text(encoding="utf-8") == result.stdout
    else:
        assert not directory.exists()
