import contextlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dota import alerts


def context(**extra) -> dict:
    ti = SimpleNamespace(dag_id="replay_parse", task_id="parse_replay", run_id="manual__1", map_index=-1,
                         try_number=2)
    return {"ti": ti, "params": {"match_id": 42}, "exception": RuntimeError("parser exited 1"), **extra}


def test_record_has_the_fields_needed_to_find_the_problem():
    entry = alerts.record(context(), "failed")
    assert entry["event"] == "failed"
    assert (entry["dag_id"], entry["task_id"], entry["run_id"]) == ("replay_parse", "parse_replay", "manual__1")
    assert (entry["match_id"], entry["try_number"]) == (42, 2)
    assert entry["error"] == "RuntimeError: parser exited 1"


def test_mapped_task_match_id_comes_from_its_label():
    ctx = context(params={})
    ctx["ti"].rendered_map_index = "9079740225"
    assert alerts.record(ctx, "failed")["match_id"] == 9079740225
    ctx["ti"].rendered_map_index = None  # an unlabelled task: no match id to report
    assert alerts.record(ctx, "failed")["match_id"] is None


def test_long_errors_are_cut_and_missing_ones_are_none():
    entry = alerts.record(context(exception=ValueError("x" * 2000)), "retry")
    assert len(entry["error"]) == alerts.MAX_ERROR_CHARS
    assert alerts.record(context(exception=None, params=None), "failed")["error"] is None


def test_write_appends_json_lines(tmp_path: Path):
    path = tmp_path / "alerts" / "failures.jsonl"
    alerts.write({"event": "retry"}, path)
    alerts.write({"event": "failed"}, path)
    assert [json.loads(line)["event"] for line in path.read_text().splitlines()] == ["retry", "failed"]


def test_callback_never_raises(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture):
    monkeypatch.setattr(alerts, "write", lambda entry: (_ for _ in ()).throw(OSError("disk full")))
    notified = []
    monkeypatch.setattr(alerts, "notify", lambda entry: notified.append(entry["event"]))
    alerts.on_failure(context())
    assert "could not write alert log" in capsys.readouterr().out
    assert notified == ["failed"]  # a full disk must not also silence the webhook
    alerts.on_failure({})  # no task instance: nothing to report, still no exception


URL = "https://discord.example/api/webhooks/123/secret-token"


def test_record_redacts_api_key_in_error():
    error = ConnectionError("GET https://api.steampowered.com/x?key=ABC123&match_id=1 failed")
    entry = alerts.record(context(exception=error), "failed")
    assert "ABC123" not in entry["error"]
    assert "key=<redacted>&match_id=1" in entry["error"]


def test_notify_skips_without_url(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    calls = []
    alerts.notify(alerts.record(context(), "failed"), post=lambda *a, **k: calls.append(a))
    alerts.notify(alerts.record(context(), "failed"), url="", post=lambda *a, **k: calls.append(a))
    assert calls == []


def test_notify_posts_discord_payload(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", URL)
    sent = {}

    def post(request, timeout):
        sent.update(request=request, timeout=timeout)
        return contextlib.nullcontext()

    alerts.notify(alerts.record(context(), "failed"), post=post)
    request = sent["request"]
    assert request.full_url == URL and request.get_method() == "POST"
    assert request.get_header("Content-type") == "application/json"
    assert request.get_header("User-agent")
    assert sent["timeout"] == 5
    assert json.loads(request.data) == {
        "content": "replay_parse.parse_replay [match 42] failed (try 2): RuntimeError: parser exited 1"}


def test_notify_swallows_errors_and_does_not_print_url(capsys: pytest.CaptureFixture):
    def post(request, timeout):
        raise OSError(f"could not reach {request.full_url}")

    alerts.notify(alerts.record(context(), "failed"), url=URL, post=post)
    captured = capsys.readouterr()
    assert "OSError" in captured.out
    assert "secret-token" not in captured.out + captured.err


def test_retry_does_not_notify(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(alerts, "write", lambda entry: None)
    notified = []
    monkeypatch.setattr(alerts, "notify", lambda entry: notified.append(entry["event"]))
    alerts.on_retry(context())
    assert notified == []
    alerts.on_failure(context())
    assert notified == ["failed"]
