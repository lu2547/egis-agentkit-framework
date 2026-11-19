import errno
import json
import logging
from types import SimpleNamespace

from egis_agentkit_app.controller.base._exception import register_exception_handlers
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from egis_agentkit.client._logging import LoggingClient
from egis_agentkit.client.nacos._settings import LoggingConfig
from egis_agentkit.errors import BadRequest, Forbidden
from egis_agentkit.middleware.http._request_logging import RequestLogging
from egis_agentkit.storage._directory import PosixDirectory
from egis_agentkit.storage._files import FileStorage


def test_http_logs_route_templates_and_classifies_exceptions(capsys):
    app = FastAPI()
    app.add_middleware(RequestLogging, access_log=True)
    register_exception_handlers(app)

    @app.get("/items/{item}")
    async def item(request: Request, item: str):
        request.state.egis_user = SimpleNamespace(user_id="authenticated-user")
        logging.getLogger("egis_agentkit_app.test").info("Item lookup")
        if item == "denied":
            raise Forbidden()
        if item == "invalid":
            raise BadRequest("Invalid filter")
        raise ValueError("database-url-with-secret")

    logging_client = LoggingClient(LoggingConfig())
    logging_client.start()
    try:
        with TestClient(app) as client:
            denied = client.get("/items/denied")
            invalid = client.get("/items/invalid")
            failure = client.get("/items/private-path?token=private-query")
    finally:
        logging_client.close()

    assert denied.status_code == 403
    assert invalid.status_code == 400
    assert invalid.json() == {"detail": "Invalid filter"}
    assert failure.status_code == 500
    assert failure.json() == {"detail": "服务器内部错误"}
    output = capsys.readouterr().out
    assert all(
        value not in output
        for value in ("database-url-with-secret", "private-path", "private-query")
    )
    events = [json.loads(line) for line in output.splitlines()]
    for response, category in ((denied, "business"), (failure, "system")):
        request_id = response.headers["x-request-id"]
        related = [event for event in events if event["request_id"] == request_id]
        assert len(related) == 3
        assert all(event["user_id"] == "authenticated-user" for event in related)
        rejected = related[1]
        assert rejected["context"]["category"] == category
        assert rejected["context"]["route"] == "/items/{item}"
        assert ("exception" in rejected) == (category == "system")
        if category == "business":
            assert rejected["context"]["error_description"] == "无权执行此操作"
            assert rejected["context"]["origin"]["function"].endswith(".item")
        assert related[-1]["context"]["status_code"] == response.status_code


def test_nas_permission_error_logs_a_system_cause_without_raw_path(
    capsys, tmp_path, monkeypatch
):
    def inaccessible(path):
        raise PermissionError(errno.EACCES, "private-error-text", path)

    monkeypatch.setattr(PosixDirectory, "open", inaccessible)
    files = FileStorage(tmp_path / "private-directory")
    app = FastAPI()
    register_exception_handlers(app)
    app.add_middleware(RequestLogging, access_log=False)

    @app.get("/project")
    def project():
        with files.open_directory(files.root, [], create=True):
            return {}

    client_logging = LoggingClient(LoggingConfig())
    client_logging.start()
    try:
        with TestClient(app) as client:
            response = client.get("/project")
    finally:
        client_logging.close()
    assert response.status_code == 503
    output = capsys.readouterr().out
    assert "private-directory" not in output and "private-error-text" not in output
    event = json.loads(output)
    assert event["context"]["category"] == "system"
    assert event["context"]["error_description"] == response.json()["detail"]
    assert event["exception"]["cause"]["type"] == "PermissionError"
    assert event["exception"]["cause"]["errno"] == errno.EACCES
    assert event["exception"]["cause"]["description"] == "Permission denied"


def test_access_log_can_be_disabled_without_disabling_rejection_logs(capsys):
    app = FastAPI()
    app.add_middleware(RequestLogging, access_log=False)
    register_exception_handlers(app)
    logging_client = LoggingClient(LoggingConfig())
    logging_client.start()
    try:
        with TestClient(app) as client:
            response = client.get("/unknown/private-path")
    finally:
        logging_client.close()

    assert response.status_code == 404
    event = json.loads(capsys.readouterr().out)
    assert event["request_id"] == response.headers["x-request-id"]
    assert event["context"]["category"] == "business"
    assert event["context"]["route"] == "<unmatched>"
