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
    alerts.on_failure(context())
    assert "could not write alert log" in capsys.readouterr().out
