import datetime as dt
import io
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import latency_report as lr

FIXTURES = Path(__file__).parent / "fixtures" / "latency"
UTC = dt.timezone.utc
T0 = dt.datetime(2026, 11, 15, 10, 0, tzinfo=UTC)


def at(s: float) -> dt.datetime:
    return T0 + dt.timedelta(seconds=s)


def fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def burst_session(name: str, file: str) -> list[dict]:
    f = fixture(file)
    stages, _ = lr.stage_times(f["listing"])
    return lr.burst_rows(name, f["plan"]["run_id"], f["plan"], f["log"], stages, {}, f["dbt_runs"])


def live_session(name: str = "live") -> list[dict]:
    f = fixture("live.json")
    wh = {**f["warehouse"], "end_times": {int(k): v for k, v in f["warehouse"]["end_times"].items()},
          "queue": {q["match_id"]: q for q in f["warehouse"]["queue"]}}
    stages, _ = lr.stage_times(f["listing"])
    return lr.live_rows(name, lr.to_utc(f["since"]), lr.to_utc(f["until"]), stages, wh, {}, f["dbt_runs"])


# ---------- maths ----------

def test_lag_is_processing_minus_event_and_skew_is_frontier_gap():
    # events at x = 0, 30, 60 processed at y = 10, 100, 110
    pts = [(0.0, 10.0), (30.0, 100.0), (60.0, 110.0)]
    assert [y - x for x, y in pts] == [10.0, 70.0, 50.0]
    assert lr.frontier(pts, 9) is None
    assert lr.frontier(pts, 50) == 0.0
    assert lr.frontier(pts, 105) == 30.0
    # just before p=100 the frontier is still 0, so the skew peaks at 100
    assert lr.skew_curve(pts) == [(10.0, 10.0), (100.0, 100.0), (100.0, 70.0), (110.0, 80.0), (110.0, 50.0)]
    assert lr.max_skew(pts) == 100.0


def test_skew_with_out_of_order_processing():
    # the later event is processed first; the frontier never moves back
    pts = [(0.0, 50.0), (30.0, 40.0)]
    assert lr.skew_curve(pts) == [(40.0, 10.0), (50.0, 20.0), (50.0, 20.0)]
    assert lr.max_skew(pts) == 20.0
    assert lr.max_skew([]) is None


def test_on_the_diagonal_lag_and_skew_are_zero():
    pts = [(float(x), float(x)) for x in range(0, 300, 30)]
    assert lr.max_skew(pts) == 30.0  # just before each arrival the frontier is one gap behind
    assert {round(y - x, 3) for x, y in pts} == {0.0}


def test_watermark_steps_at_completions_and_never_drops():
    # events 0, 30, 60, 90; the one at 30 finishes last, after 60 and 90
    pts = [(0.0, 10.0), (60.0, 100.0), (90.0, 120.0), (30.0, 150.0)]
    steps = lr.watermark(pts)
    assert steps == [(10.0, 0.0), (100.0, 60.0), (120.0, 90.0), (150.0, 90.0)]
    assert [p for p, _ in steps] == sorted({y for _, y in pts})  # one step per completion
    assert all(a[1] <= b[1] for a, b in zip(steps, steps[1:]))  # monotonic
    assert all(w == lr.frontier(pts, p) for p, w in steps)
    assert lr.watermark([]) == []


def test_watermark_path_climbs_then_steps_right():
    steps = lr.watermark([(0.0, 10.0), (60.0, 100.0), (90.0, 120.0), (30.0, 150.0)])
    # x = watermark (event), y = processing; the late event at 30 adds height, not a step
    assert lr.watermark_path(steps) == [(0.0, 10.0), (0.0, 100.0), (60.0, 100.0), (60.0, 120.0),
                                        (90.0, 120.0), (90.0, 150.0)]
    xs, ys = zip(*lr.watermark_path(steps))
    assert list(xs) == sorted(xs) and list(ys) == sorted(ys)
    # a batch finishing at once is one step
    assert lr.watermark_path(lr.watermark([(0.0, 50.0), (30.0, 50.0)])) == [(30.0, 50.0)]


def test_max_skew_and_max_lag_from_the_watermark():
    pts = [(0.0, 10.0), (60.0, 100.0), (90.0, 120.0), (30.0, 150.0)]
    # just before p=100 the watermark is still 0; the late match at 30 has the largest lag
    assert lr.max_skew_point(pts) == (100.0, 0.0, 100.0)
    assert lr.max_skew(pts) == 100.0
    assert lr.max_lag_point(pts) == (30.0, 150.0, 120.0)
    # the curve's largest vertical gap is the max skew; the out-of-order match lags more
    assert lr.max_lag_point(pts)[2] > lr.max_skew(pts)
    assert lr.max_skew_point([]) is None and lr.max_lag_point([]) is None


def test_percentile_and_rate():
    assert lr.percentile([], 50) is None
    assert lr.percentile([1.0, 2.0, 3.0, 4.0], 50) == 2.0
    assert lr.percentile([float(i) for i in range(1, 21)], 95) == 19.0
    assert lr.rate_per_min([at(0), at(30), at(60)]) == 2.0
    assert lr.rate_per_min([at(0)]) is None


def test_to_utc_reads_iso_naive_and_unix():
    assert lr.to_utc("2026-11-15T10:00:00Z") == T0
    assert lr.to_utc("2026-11-15T10:00:00") == T0  # naive is UTC
    assert lr.to_utc(int(T0.timestamp())) == T0
    assert lr.to_utc(dt.datetime(2026, 11, 15, 18, 0, tzinfo=dt.timezone(dt.timedelta(hours=8)))) == T0
    assert lr.to_utc(None) is None and lr.to_utc("") is None


# ---------- readers ----------

def test_stage_times_joins_markers_per_match():
    listing = [
        {"Key": "replays/7.dem", "LastModified": at(0)},
        {"Key": "parsed/_complete/match_id=7/part-1.parquet", "LastModified": at(40)},
        {"Key": "parsed/_complete/match_id=7/part-0.parquet", "LastModified": at(45)},
        {"Key": "clean/_complete/match_id=7/_SUCCESS", "LastModified": at(80)},
        {"Key": "clean/_complete/match_id=7/part-0.parquet", "LastModified": at(90)},  # _SUCCESS wins
        {"Key": "clean/_loaded/match_id=7/_loaded", "LastModified": at(95)},
        {"Key": "replays/8.dem", "LastModified": at(10)},
        {"Key": "clean/_rejected/match_id=8/_REJECTED", "LastModified": at(70)},
        {"Key": "clean/_review/match_id=8/report.json", "LastModified": at(69)},
        {"Key": "replays/9.dem.tmp", "LastModified": at(1)},
    ]
    times, parquet = lr.stage_times(listing)
    assert times[7] == {"arrived_at": at(0), "parsed_at": at(45), "cleaned_at": at(80), "loaded_at": at(95)}
    assert times[8] == {"arrived_at": at(10), "rejected": True, "rejected_at": at(70)}
    assert 9 not in times
    assert parquet == {7: "parsed/_complete/match_id=7/part-1.parquet"}


def test_parsed_at_comes_from_the_parquet_column():
    buf = io.BytesIO()
    pq.write_table(pa.table({"parsed_at": pa.array([at(42)], pa.timestamp("us", tz="UTC"))}), buf)
    assert lr.parsed_at_from_parquet(buf.getvalue()) == at(42)
    naive = io.BytesIO()
    pq.write_table(pa.table({"parsed_at": pa.array([at(43).replace(tzinfo=None)], pa.timestamp("us"))}), naive)
    assert lr.parsed_at_from_parquet(naive.getvalue()) == at(43)
    other = io.BytesIO()
    pq.write_table(pa.table({"x": [1]}), other)
    assert lr.parsed_at_from_parquet(other.getvalue()) is None


def dated_s3(modified: dt.datetime):
    """FakeS3 whose listings carry LastModified."""
    from s3_fakes import FakeS3

    class Dated(FakeS3):
        def get_paginator(self, name):
            objects = self.objects

            class P:
                def paginate(self, Bucket, Prefix):
                    keys = sorted(k for b, k in objects if b == Bucket and k.startswith(Prefix))
                    yield {"Contents": [{"Key": k, "LastModified": modified} for k in keys]}
            return P()

    return Dated()


def test_read_stage_times_prefers_parquet_parsed_at():
    s3 = dated_s3(at(100))
    buf = io.BytesIO()
    pq.write_table(pa.table({"parsed_at": pa.array([at(42)], pa.timestamp("us", tz="UTC"))}), buf)
    s3.put_object("raw", "replays/5.dem", b"x")
    s3.put_object("raw", "parsed/_complete/match_id=5/part-0.parquet", buf.getvalue())
    s3.put_object("raw", "replays/6.dem", b"x")
    times = lr.read_stage_times(s3, "raw", lambda m, _: m == 5)
    assert times == {5: {"arrived_at": at(100), "parsed_at": at(42)}}
    assert lr.read_stage_times(s3, "raw", lambda m, _: True, use_parquet=False)[5]["parsed_at"] == at(100)


def test_restore_log_reads_plan_and_rows_and_old_jsonl():
    plan, rows = lr.parse_restore_log({
        "cold/restore_log/r1/_plan.json": b'{"t0": "2026-11-15T10:00:00Z", "speed": 2}',
        "cold/restore_log/r1/2.json": b'{"match_id": 2}',
        "cold/restore_log/r1/1.json": b'{"match_id": 1}',
    })
    assert plan["speed"] == 2 and [r["match_id"] for r in rows] == [1, 2]
    plan, rows = lr.parse_restore_log({"cold/restore_log/r0.jsonl": b'{"match_id": 3}\n\n{"match_id": 4}\n'})
    assert plan is None and [r["match_id"] for r in rows] == [3, 4]
    assert lr.restore_log_prefix("cold", "r1") == "cold/restore_log/r1"


def test_burst_origin_from_plan_else_schedule():
    assert lr.burst_origin({"t0": "2026-11-15T10:00:00Z"}, []) == T0
    rows = [{"scheduled_at": lr.iso(at(45)), "scheduled_offset_s": 15},
            {"scheduled_at": lr.iso(at(30.5)), "scheduled_offset_s": 0.5}]
    assert lr.burst_origin({}, rows) == at(30)
    with pytest.raises(SystemExit):
        lr.burst_origin(None, [])


def test_task_times_keeps_latest_success_per_stage():
    tis = [
        {"task_id": "parse_and_check.parse_replay", "rendered_map_index": "7", "state": "failed",
         "start_date": lr.iso(at(1)), "end_date": lr.iso(at(5))},
        {"task_id": "parse_and_check.parse_replay", "rendered_map_index": "7", "state": "success",
         "start_date": lr.iso(at(10)), "end_date": lr.iso(at(40))},
        {"task_id": "clean_and_check.spark_clean", "rendered_map_index": "7", "state": "success",
         "start_date": lr.iso(at(50)), "end_date": lr.iso(at(80))},
        {"task_id": "load_and_retain.load_one", "rendered_map_index": "7", "state": "success",
         "start_date": lr.iso(at(85)), "end_date": lr.iso(at(90))},
        {"task_id": "parse_and_check.check_contract", "rendered_map_index": "7", "state": "success",
         "start_date": lr.iso(at(41)), "end_date": lr.iso(at(42))},
        {"task_id": "find_unparsed", "rendered_map_index": None, "state": "success"},
    ]
    assert lr.task_times(tis) == {7: {"parse_started_at": at(10), "parsed_at": at(40),
                                      "clean_started_at": at(50), "cleaned_at": at(80),
                                      "load_started_at": at(85), "loaded_at": at(90)}}


def test_mart_visible_is_first_successful_dbt_run_started_after_load():
    runs = [
        {"state": "success", "start_date": lr.iso(at(50)), "end_date": lr.iso(at(110))},  # started before
        {"state": "failed", "start_date": lr.iso(at(110)), "end_date": lr.iso(at(130))},
        {"state": "success", "start_date": lr.iso(at(200)), "end_date": lr.iso(at(260))},
        {"state": "success", "start_date": lr.iso(at(130)), "end_date": lr.iso(at(190))},
        {"state": "running", "start_date": lr.iso(at(300)), "end_date": None},
    ]
    assert lr.mart_visible_at(at(100), runs) == at(190)
    assert lr.mart_visible_at(at(250), runs) is None
    assert lr.mart_visible_at(None, runs) is None


# ---------- rows ----------

def test_burst_rows_rebase_on_t0_and_lag_against_the_schedule():
    rows = burst_session("b1", "burst_1x.json")
    assert len(rows) == 16
    first, second = rows[0], rows[1]
    assert first["origin"] == T0 and first["sim_event_time"] == T0
    assert second["sim_event_time"] == at(30)
    assert second["event_time"] == lr.to_utc(1793440000 + 97)  # the real end time is kept
    assert second["lag_released_s"] == pytest.approx(2.7)
    assert second["lag_arrived_s"] == second["lag_released_s"]
    assert second["lag_parsed_s"] == pytest.approx(second["lag_arrived_s"] + 5 + 40)
    assert second["final_stage"] == "mart_visible"
    assert second["lag_final_s"] == lr.seconds(second["mart_visible_at"], second["sim_event_time"])
    assert second["speed"] == 1.0 and second["rate_per_min"] == 2.0


def test_burst_rows_missing_stages_are_null():
    rows = {r["match_id"]: r for r in burst_session("b2", "burst_2x.json")}
    skipped = rows[9070200007]
    assert skipped["status"] == "skipped"
    assert all(skipped[s] is None for s in lr.STAGES) and skipped["lag_final_s"] is None
    assert skipped["sim_event_time"] == at(3600 + 7 * 15)
    unloaded = rows[9070200015]
    assert unloaded["cleaned_at"] is not None and unloaded["loaded_at"] is None
    assert unloaded["mart_visible_at"] is None and unloaded["final_stage"] is None
    rejected = {r["match_id"]: r for r in burst_session("b1", "burst_1x.json")}[9070100005]
    assert rejected["rejected"] and rejected["parsed_at"] is not None and rejected["cleaned_at"] is None


def test_burst_rows_drop_markers_older_than_the_release():
    plan = {"t0": lr.iso(T0)}
    log = [{"match_id": 1, "scheduled_offset_s": 0, "status": "released", "released_at": lr.iso(at(2)),
            "end_time": 1}]
    stages = {1: {"arrived_at": at(2), "parsed_at": at(-500), "loaded_at": at(-400)}}
    row = lr.burst_rows("s", "r", plan, log, stages)[0]
    assert row["parsed_at"] is None and row["loaded_at"] is None and row["arrived_at"] == at(2)


def test_burst_rows_fall_back_to_airflow_task_end():
    plan = {"t0": lr.iso(T0)}
    log = [{"match_id": 1, "scheduled_offset_s": 0, "status": "released", "released_at": lr.iso(at(2))}]
    row = lr.burst_rows("s", "r", plan, log, {1: {"arrived_at": at(2)}},
                        {1: {"parse_started_at": at(5), "parsed_at": at(40)}})[0]
    assert row["parse_started_at"] == at(5) and row["parsed_at"] == at(40)
    assert row["final_stage"] is None


def test_burst_arrival_is_never_before_the_release():
    # real Garage run: LastModified of a multipart replay is when its upload began, 0.3 s
    # before the release log saw the upload complete
    plan = {"t0": lr.iso(T0)}
    log = [{"match_id": 1, "scheduled_offset_s": 0, "status": "released", "released_at": lr.iso(at(2.6))},
           {"match_id": 2, "scheduled_offset_s": 4, "status": "released", "released_at": lr.iso(at(6))}]
    stages = {1: {"arrived_at": at(2.3)}, 2: {"arrived_at": at(7)}}
    rows = {r["match_id"]: r for r in lr.burst_rows("s", "r", plan, log, stages)}
    assert rows[1]["arrived_at"] == at(2.6) and rows[1]["lag_arrived_s"] == rows[1]["lag_released_s"] == 2.6
    assert rows[2]["arrived_at"] == at(7)  # a later LastModified is kept


def test_live_rows_have_replay_and_steam_lanes():
    f = fixture("live.json")
    rows = live_session()
    replay = [r for r in rows if r["lane"] == "replay"]
    steam = [r for r in rows if r["lane"] == "steam"]
    assert len(replay) == 6 and 9068000000 not in {r["match_id"] for r in replay}  # arrived before --since
    assert len(steam) == 24
    r = replay[0]
    assert r["sim_event_time"] == r["event_time"] == lr.to_utc(f["warehouse"]["end_times"][str(r["match_id"])])
    assert r["origin"] == lr.to_utc(f["since"]) and r["queued_at"] is not None
    assert r["lag_arrived_s"] == pytest.approx(24 * 3600 + 20 * 60)
    s = steam[0]
    src = next(x for x in f["warehouse"]["steam_rows"] if x["match_id"] == s["match_id"])
    assert s["event_time"] == lr.to_utc(src["start_time"] + src["duration"])
    assert s["loaded_at"] == lr.to_utc(src["loaded_at"]) and s.get("parsed_at") is None
    assert s["lag_loaded_s"] == pytest.approx(10 * 60)
    assert s["final_stage"] == "mart_visible"


def test_live_replay_event_time_falls_back_to_steam():
    since, until = at(0), at(3600)
    stages = {1: {"arrived_at": at(60)}}
    row = lr.live_rows("l", since, until, stages, {"steam_end_times": {1: int(at(-86400).timestamp())}})[0]
    assert row["event_time"] == at(-86400) and row["lag_arrived_s"] == 86460
    row = lr.live_rows("l", since, until, stages, {})[0]
    assert row["event_time"] is None and row["lag_arrived_s"] is None


def test_csv_round_trip(tmp_path):
    rows = burst_session("b1", "burst_1x.json") + live_session()
    path = tmp_path / "x.csv"
    lr.write_csv(rows, path)
    back = lr.read_csv(path)
    assert len(back) == len(rows)
    for a, b in zip(rows, back):
        for c in lr.COLUMNS:
            va, vb = a.get(c), b.get(c)
            if isinstance(va, dt.datetime):
                assert abs((va - vb).total_seconds()) < 0.001
            elif isinstance(va, float):
                assert vb == pytest.approx(va)
            else:
                assert vb == va or (va is None and vb is None), c


# ---------- summary ----------

def test_summary_per_stage():
    rows = burst_session("b1", "burst_1x.json") + burst_session("b2", "burst_2x.json") + live_session()
    summary = {(s["session"], s["lane"], s["stage"]): s for s in lr.summarize(rows)}
    b1 = summary[("b1", "replay", "released")]
    assert b1["count"] == 16 and b1["in_per_min"] == pytest.approx(2.0) and b1["planned_per_min"] == 2.0
    assert b1["p50_lag_s"] == pytest.approx(2.7) and b1["backlog"] == 0
    loaded = summary[("b1", "replay", "loaded")]
    assert loaded["count"] == 15 and loaded["backlog"] == 0 and loaded["rejected"] == 1  # rejected is not backlog
    lags = sorted(r["lag_loaded_s"] for r in rows if r["session"] == "b1" and r["lag_loaded_s"] is not None)
    assert loaded["p50_lag_s"] == lags[7] and loaded["p95_lag_s"] == lags[14] and loaded["max_lag_s"] == lags[-1]
    b2 = summary[("b2", "replay", "loaded")]
    assert b2["planned_per_min"] == 4.0 and b2["backlog"] == 1 and b2["count"] == 14
    assert summary[("b2", "replay", "released")]["count"] == 15
    # at 2x the parser (2 slots x 40 s = 3/min) falls behind, so lag grows
    assert b2["max_lag_s"] > loaded["max_lag_s"]
    assert summary[("b2", "replay", "parsed")]["out_per_min"] < 4.0
    assert ("live", "steam", "loaded") in summary and ("live", "steam", "parsed") not in summary
    assert summary[("live", "replay", "loaded")]["backlog"] == 1
    assert summary[("live", "replay", "arrived")]["p50_lag_s"] > 86400


def test_summary_skips_mart_without_airflow_and_renders_markdown():
    rows = [lr.finish_row({"session": "s", "kind": "burst", "lane": "replay", "match_id": 1, "origin": T0,
                           "sim_event_time": T0, "released_at": at(1), "arrived_at": at(1), "loaded_at": at(90)})]
    summary = lr.summarize(rows)
    assert [s["stage"] for s in summary] == ["released", "arrived", "parsed", "cleaned", "loaded"]
    assert summary[2]["backlog"] == 1 and summary[2]["count"] == 0 and summary[2]["p50_lag_s"] is None
    text = lr.markdown(summary)
    assert text.splitlines()[0].startswith("| Session | Lane | Stage |")
    assert "| s | replay | loaded | 1 | 90.0s |" in text
    assert lr.fmt(7200.0, "s") == "2.0h" and lr.fmt(150.0, "s") == "2.5m" and lr.fmt(None) == "–"


# ---------- chart and CLI ----------

def test_annotation_defaults_to_the_maxima_and_takes_a_processing_time():
    pts = [(0.0, 10.0), (30.0, 100.0), (60.0, 110.0)]
    a = lr.annotation(pts, at=105)
    assert a["frontier"] == 30.0 and a["skew"] == 75.0
    assert (a["lag_x"], a["lag_y"], a["lag"]) == (30.0, 100.0, 70.0)
    m = lr.annotation(pts)
    assert (m["p"], m["frontier"], m["skew"]) == (100.0, 0.0, 100.0) and m["skew"] == lr.max_skew(pts)
    assert (m["lag_x"], m["lag_y"], m["lag"]) == (30.0, 100.0, 70.0)
    assert lr.annotation([]) is None


def test_chart_maxima_match_the_summary():
    rows = burst_session("b1", "burst_1x.json")
    summary = {s["stage"]: s for s in lr.summarize(rows)}
    final = summary["mart_visible"] if all(r.get("mart_visible_at") for r in rows if r.get("loaded_at")) \
        else summary["loaded"]
    a = lr.annotation(lr.final_points(rows))
    assert a["lag"] == final["max_lag_s"] and a["skew"] == final["max_skew_s"]


def test_chart_writes_png_and_html(tmp_path):
    sessions = {"live": live_session(), "b1": burst_session("b1", "burst_1x.json"),
                "b2": burst_session("b2", "burst_2x.json")}
    written = lr.chart(sessions, tmp_path / "chart.png")
    assert written[0].read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"
    assert written[1].suffix == ".html" and "plotly" in written[1].read_text()
    only = lr.chart({"b1": sessions["b1"]}, tmp_path / "burst.png", html=False)
    assert only == [tmp_path / "burst.png"] and only[0].stat().st_size > 0


def test_top_panel_draws_watermarks_live_in_hours_and_burst_in_minutes():
    import matplotlib.pyplot as plt

    sessions = {"live": live_session(), "b1": burst_session("b1", "burst_1x.json")}
    style, groups = lr.chart_style(sessions)
    assert groups == ["burst", "live"]
    fig = lr.chart_figure(sessions)
    burst_ax, live_ax = fig.axes[0], fig.axes[1]  # top row; the lag row follows
    assert "h since --since" in live_ax.get_xlabel() and "h since --since" in live_ax.get_ylabel()
    assert "min since T0" in burst_ax.get_xlabel() and "min since T0" in burst_ax.get_ylabel()

    def line(ax, label):
        return next(l for l in ax.get_lines() if l.get_label() == label)

    live = lr.lane_curves([r for r in sessions["live"] if r["lane"] == "replay"])
    wm = line(live_ax, f"live: {live['stage']} watermark")
    assert list(wm.get_ydata()) == pytest.approx([y / 3600 for _, y in live["watermark"]])
    b1 = lr.lane_curves(sessions["b1"])
    wm = line(burst_ax, f"b1: {b1['stage']} watermark")
    assert list(wm.get_xdata()) == pytest.approx([x / 60 for x, _ in b1["watermark"]])
    assert list(wm.get_ydata()) == pytest.approx([y / 60 for _, y in b1["watermark"]])
    assert all(a <= b for a, b in zip(wm.get_ydata(), wm.get_ydata()[1:]))  # monotonic, no zig-zag
    dots = next(l for l in burst_ax.get_lines() if l.get_label().startswith("b1: matches"))
    assert len(dots.get_xdata()) == len(lr.final_points(sessions["b1"]))
    assert all(not r.get("final_at") for r in sessions["b1"] if r["rejected"])  # rejected: no dot
    # the lag panel below stays in seconds for live
    lag_line = next(l for l in fig.axes[3].get_lines() if l.get_label() == "live")
    assert max(lag_line.get_ydata()) > 86400
    plt.close(fig)

    html = lr.html_figure(sessions, style, groups)
    live_trace = next(t for t in html.data if t.name == f"live: {live['stage']} watermark" and t.xaxis == "x2")
    assert list(live_trace.y) == pytest.approx([y / 3600 for _, y in live["watermark"]])
    assert html.layout.yaxis2.title.text == "processing time, h"
    assert html.layout.yaxis.title.text == "processing time, min"


def test_loaded_watermark_is_drawn_only_when_dbt_made_matches_visible():
    rows = burst_session("b1", "burst_1x.json")
    assert lr.lane_curves(rows)["loaded"]
    no_dbt = [lr.finish_row({**r, "mart_visible_at": None}) for r in rows]
    curves = lr.lane_curves(no_dbt)
    assert curves["loaded"] == [] and curves["stage"] == "loaded"


def test_single_column_footer_stays_inside_the_figure():
    import matplotlib.pyplot as plt

    for sessions in ({"b1": burst_session("b1", "burst_1x.json")},
                     {"live": live_session(), "b1": burst_session("b1", "burst_1x.json")}):
        fig = lr.chart_figure(sessions)
        fig.canvas.draw()
        renderer = fig.canvas.get_renderer()
        footer = fig.texts[0].get_window_extent(renderer)
        assert fig.bbox.x0 <= footer.x0 and footer.x1 <= fig.bbox.x1 and fig.bbox.y0 <= footer.y0
        lowest = min(ax.get_tightbbox(renderer).y0 for ax in fig.axes)
        assert footer.y1 <= lowest  # the bottom panels' axis labels sit above the footer
        plt.close(fig)


def test_burst_lag_panel_is_in_plain_minutes():
    import matplotlib.pyplot as plt

    rows = burst_session("b1", "burst_1x.json")
    # a run whose lags are all about ten hours apart by seconds gets an offset like "+3.559e4"
    late = [{**r, "final_at": r["final_at"] + dt.timedelta(seconds=35590)} if r.get("final_at") else r
            for r in rows]
    fig = lr.chart_figure({"b1": late})
    fig.canvas.draw()
    lag_ax = fig.axes[1]
    assert "min since T0" in lag_ax.get_xlabel() and "lag, min" in lag_ax.get_ylabel()
    assert lag_ax.yaxis.get_offset_text().get_text() == ""
    assert lag_ax.xaxis.get_offset_text().get_text() == ""
    final = sorted(lr.final_points(late), key=lambda q: q[1])
    line = next(l for l in lag_ax.get_lines() if l.get_label() == "b1")
    assert list(line.get_ydata()) == pytest.approx([(y - x) / 60 for x, y in final])
    plt.close(fig)


def test_cli_chart_and_summary_from_csv(tmp_path, capsys):
    lr.write_csv(burst_session("b1", "burst_1x.json"), tmp_path / "b1.csv")
    lr.write_csv(live_session(), tmp_path / "live.csv")
    lr.main(["summary", "--out-dir", str(tmp_path), "--sessions", "b1,live"])
    assert "| b1 | replay | loaded | 15 |" in (tmp_path / "summary.md").read_text()
    assert "| live | steam | loaded | 24 |" in capsys.readouterr().out
    lr.main(["chart", "--out-dir", str(tmp_path), "--sessions", "b1,live", "--no-html"])
    assert (tmp_path / "chart.png").exists() and not (tmp_path / "chart.html").exists()
    with pytest.raises(SystemExit):
        lr.main(["chart", "--out-dir", str(tmp_path), "--sessions", "missing"])


def test_cli_extract_burst_from_fake_s3(tmp_path, monkeypatch):
    s3 = dated_s3(at(100))
    s3.put_object("raw", "cold/restore_log/r1/_plan.json", json.dumps({"t0": lr.iso(T0), "speed": 1.0}).encode())
    for i, mid in enumerate((11, 12)):
        s3.put_object("raw", f"cold/restore_log/r1/{mid}.json", json.dumps({
            "run_id": "r1", "match_id": mid, "end_time": 1793440000 + i, "scheduled_offset_s": 30 * i,
            "scheduled_at": lr.iso(at(30 * i)), "released_at": lr.iso(at(30 * i + 2)), "status": "released",
            "speed": 1.0, "rate_per_min": 2.0, "spacing": "uniform"}).encode())
        s3.put_object("raw", f"replays/{mid}.dem", b"x")
    s3.put_object("raw", "clean/_loaded/match_id=11/_loaded")
    s3.put_object("raw", "clean/_loaded/match_id=99/_loaded")  # not in the run
    monkeypatch.setattr(lr, "s3_client", lambda: s3)
    lr.main(["extract", "--out-dir", str(tmp_path), "--session", "b", "--kind", "burst", "--run-id", "r1"])
    rows = {r["match_id"]: r for r in lr.read_csv(tmp_path / "b.csv")}
    assert set(rows) == {11, 12}
    assert rows[11]["loaded_at"] == at(100) and rows[11]["lag_loaded_s"] == 100.0
    assert rows[12]["loaded_at"] is None and rows[12]["lag_arrived_s"] == 70.0
    assert rows[12]["mart_visible_at"] is None  # no --airflow
