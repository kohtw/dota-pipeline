import hashlib
import io
import json
import random

import pytest
import zstandard

from dota import cold
from dota.errors import PermanentError
from dota.storage import DEM_MAGIC
from s3_fakes import Clock, FakeS3

HOT, COLD = "raw", "archive"


def mean_rate_per_min(offs):
    return (len(offs) - 1) / (offs[-1] / 60)


@pytest.mark.parametrize("spacing", cold.SPACINGS)
def test_offsets_start_at_zero_and_keep_the_mean_rate(spacing):
    ends = [1000, 1030, 1200, 1210, 1600]
    offs = cold.offsets(ends, rate_per_min=2.0, spacing=spacing)
    assert offs[0] == 0
    assert mean_rate_per_min(offs) == pytest.approx(2.0)
    assert offs == sorted(offs)


def test_uniform_offsets_are_evenly_spaced():
    assert cold.offsets([5, 900, 901], rate_per_min=2.0, spacing="uniform") == [0, 30, 60]


def test_event_offsets_keep_the_real_gaps_in_proportion():
    ends = [0, 10, 40]  # gaps 10 and 30
    offs = cold.offsets(ends, rate_per_min=1.0, spacing="event")
    assert offs == pytest.approx([0, 30, 120])  # (count-1) / rate = 2 min across the span
    assert (offs[2] - offs[1]) / (offs[1] - offs[0]) == pytest.approx(3)


@pytest.mark.parametrize("spacing", cold.SPACINGS)
def test_speed_two_halves_every_offset(spacing):
    ends = [100, 160, 400, 401]
    base = cold.offsets(ends, 2.0, 1.0, spacing)
    fast = cold.offsets(ends, 2.0, 2.0, spacing)
    assert fast == pytest.approx([o / 2 for o in base])
    assert mean_rate_per_min(fast) == pytest.approx(4.0)


@pytest.mark.parametrize("spacing", cold.SPACINGS)
def test_one_replay_is_released_at_once(spacing):
    assert cold.offsets([12345], 2.0, 1.0, spacing) == [0]


def test_event_with_zero_span_falls_back_to_uniform():
    assert cold.offsets([50, 50, 50], 2.0, 1.0, "event") == [0, 30, 60]


@pytest.mark.parametrize("kwargs", [{"rate_per_min": 0}, {"speed": -1}, {"spacing": "random"}])
def test_offsets_reject_bad_params(kwargs):
    args = {"rate_per_min": 2.0, "speed": 1.0, "spacing": "event", **kwargs}
    with pytest.raises(PermanentError):
        cold.offsets([1, 2], **args)


def test_offsets_reject_unordered_end_times():
    with pytest.raises(PermanentError):
        cold.offsets([10, 5], 2.0)


def entry(match_id, end_time, seq=None, sha="x", nbytes=1, seed_run="a"):
    return cold.manifest_entry(match_id, seq or match_id, end_time - 100, 100, nbytes, sha, seed_run)


def test_manifest_entry_end_time_is_start_plus_duration():
    e = cold.manifest_entry(1, 2, 1_000, 2_400, 37, "ab")
    assert e == {"match_id": 1, "match_seq_num": 2, "start_time": 1_000, "duration": 2_400,
                 "end_time": 3_400, "bytes": 37, "sha256": "ab", "seed_run": None}


def test_manifest_round_trips_and_ignores_blank_lines():
    entries = [entry(2, 500), entry(1, 900)]
    text = cold.dump_manifest(entries)
    assert len(text.splitlines()) == 2
    assert cold.parse_manifest(text + "\n\n") == entries
    assert list(json.loads(text.splitlines()[0])) == list(cold.MANIFEST_FIELDS)


def test_manifest_lines_from_before_seed_runs_still_dump():
    old = {k: v for k, v in entry(1, 100).items() if k != "seed_run"}
    assert json.loads(cold.dump_manifest([old]))["seed_run"] is None


def test_merge_manifest_has_no_duplicates_and_new_entries_win():
    old = [entry(1, 100, sha="old"), entry(2, 200)]
    merged = cold.merge_manifest(old, [entry(1, 100, sha="new"), entry(3, 50)])
    assert [e["match_id"] for e in merged] == [3, 1, 2]  # end_time order
    assert next(e for e in merged if e["match_id"] == 1)["sha256"] == "new"


def ids(entries):
    return [e["match_id"] for e in entries]


def test_pick_takes_the_next_entries_in_end_time_order():
    entries = [entry(1, 300), entry(2, 100), entry(3, 200), entry(4, 200)]
    assert ids(cold.pick(entries, 3)) == [2, 3, 4]
    assert ids(cold.pick(entries, 5, start_after_match_id=3)) == [4, 1]
    with pytest.raises(PermanentError):
        cold.pick(entries, 1, start_after_match_id=99)


def test_pick_leaves_out_matches_already_in_the_pipeline():
    entries = [entry(i, 100 * i) for i in range(1, 6)]
    assert ids(cold.pick(entries, 2, done={1, 3})) == [2, 4]
    assert cold.pick(entries, 2, done={1, 2, 3, 4, 5}) == []


def test_pick_stays_within_the_seed_run_of_its_first_match():
    # run b sits between two halves of run a in end time; a pick never mixes them
    entries = [entry(1, 100, seed_run="a"), entry(2, 110, seed_run="a"), entry(3, 90_000, seed_run="b"),
               entry(4, 90_010, seed_run="b"), entry(5, 200_000, seed_run="a")]
    assert ids(cold.pick(entries, 10)) == [1, 2, 5]
    assert ids(cold.pick(entries, 10, done={1, 2})) == [3, 4]
    assert ids(cold.pick(entries, 10, start_after_match_id=3)) == [4]


@pytest.mark.parametrize("bucket, prefix", [("raw", ""), ("raw", "replays"), ("raw", "parsed/cold/"),
                                            ("raw", "clean/x")])
def test_check_prefix_refuses_the_pipeline_prefixes_of_the_hot_bucket(bucket, prefix):
    with pytest.raises(PermanentError, match="overlaps"):
        cold.check_prefix(bucket, prefix, "raw")


def test_check_prefix_normalises():
    assert cold.check_prefix("raw", "cold", "raw") == "cold/"
    assert cold.check_prefix("archive", "replays", "raw") == "replays/"  # another bucket is fine


def test_keys():
    assert cold.cold_key("cold", 7) == "cold/replays/7.dem.zst"
    assert cold.manifest_key("cold/") == "cold/manifest.jsonl"
    assert cold.log_key("cold/", "manual__x") == "cold/restore_log/manual__x.jsonl"
    assert cold.hot_key(7) == "replays/7.dem"




def dem(match_id, size=200_000):
    # random bytes so the archive stays bigger than one upload part and truncation hits the data
    return DEM_MAGIC + random.Random(match_id).randbytes(size - len(DEM_MAGIC))


def archive(s3, match_id, end_time, prefix="cold/", body=None, seed_run="a"):
    blob = zstandard.ZstdCompressor().compress(dem(match_id)) if body is None else body
    s3.objects[(COLD, cold.cold_key(prefix, match_id))] = blob
    return cold.manifest_entry(match_id, match_id, end_time - 600, 600, len(blob),
                               hashlib.sha256(blob).hexdigest(), seed_run)


def seeded(s3, ends, prefix="cold/", runs=None):
    entries = [archive(s3, i + 1, end, prefix, seed_run=(runs or "a" * len(ends))[i]) for i, end in enumerate(ends)]
    s3.objects[(COLD, cold.manifest_key(prefix))] = cold.dump_manifest(entries).encode()
    return entries


def run(s3, clock, **kw):
    args = dict(count=10, speed=1.0, rate_per_min=2.0, spacing="uniform", start_after_match_id=None,
                cold_bucket=COLD, cold_prefix="cold/", hot_bucket=HOT, run_id="manual__1")
    return cold.restore(s3, **{**args, **kw}, now=clock.now, mono=clock.mono, sleep=clock.sleep)


def log_rows(s3, run_id="manual__1"):
    return [json.loads(line) for line in s3.objects[(COLD, cold.log_key("cold/", run_id))].decode().splitlines()]


def schedule(plan_ids, *, release=lambda e: (1, 2), is_hot=lambda m: False, write_log=lambda rows: None,
             clock=None, gap=0.0):
    clock = clock or Clock()
    plan = [({"match_id": i, "end_time": i}, n * gap) for n, i in enumerate(plan_ids)]
    return cold.release_schedule(plan, release=release, is_hot=is_hot, write_log=write_log,
                                 fields={"run_id": "r", "speed": 1, "rate_per_min": 2, "spacing": "uniform"},
                                 now=clock.now, mono=clock.mono, sleep=clock.sleep)


def test_release_object_writes_the_whole_decompressed_replay():
    s3 = FakeS3()
    e = archive(s3, 5, 1000)
    n_in, n_out = cold.release_object(s3, COLD, cold.cold_key("cold/", 5), HOT, "replays/5.dem", e["sha256"])
    assert s3.objects[(HOT, "replays/5.dem")] == dem(5)
    assert (n_in, n_out) == (e["bytes"], len(dem(5)))


def test_release_object_checks_a_body_read_in_one_go():
    s3 = FakeS3(part=-1)
    e = archive(s3, 5, 1000)
    cold.release_object(s3, COLD, cold.cold_key("cold/", 5), HOT, "replays/5.dem", e["sha256"])
    with pytest.raises(PermanentError, match="sha256"):
        cold.release_object(s3, COLD, cold.cold_key("cold/", 5), HOT, "replays/6.dem", "0" * 64)
    assert (HOT, "replays/6.dem") not in s3.objects


@pytest.mark.parametrize("body", [zstandard.ZstdCompressor().compress(b"<html>not a replay</html>"),
                                  b"not zstd at all"])
def test_release_object_rejects_what_is_not_a_replay(body):
    s3 = FakeS3()
    e = archive(s3, 5, 1000, body=body)
    with pytest.raises(PermanentError, match="not a Dota 2 replay"):
        cold.release_object(s3, COLD, cold.cold_key("cold/", 5), HOT, "replays/5.dem", e["sha256"])
    assert (HOT, "replays/5.dem") not in s3.objects


def test_release_object_never_uploads_a_truncated_archive():
    s3 = FakeS3()
    e = archive(s3, 5, 1000)
    key = cold.cold_key("cold/", 5)
    s3.objects[(COLD, key)] = s3.objects[(COLD, key)][:-40]
    with pytest.raises(PermanentError, match="sha256"):
        cold.release_object(s3, COLD, key, HOT, "replays/5.dem", e["sha256"])
    assert (HOT, "replays/5.dem") not in s3.objects


class RecordingS3:
    """A real boto3 client whose API calls are answered in memory, so s3transfer's own multipart,
    single-PUT and abort logic runs without a network."""

    def __init__(self, monkeypatch, objects):
        import boto3

        self.client = boto3.client("s3", endpoint_url="http://127.0.0.1:9", region_name="us-east-1",
                                   aws_access_key_id="test", aws_secret_access_key="test")
        self.objects, self.calls, self.parts = objects, [], {}
        monkeypatch.setattr(self.client, "_make_api_call", self.api)

    def api(self, op, params):
        self.calls.append(op)
        if op == "GetObject":
            return {"Body": io.BytesIO(self.objects[params["Key"]])}
        if op == "PutObject":
            self.objects[params["Key"]] = params["Body"].read()
            return {"ETag": '"put"'}
        if op == "CreateMultipartUpload":
            return {"UploadId": "u1"}
        if op == "UploadPart":
            self.parts[params["PartNumber"]] = params["Body"].read()
            return {"ETag": f'"{params["PartNumber"]}"'}
        if op == "CompleteMultipartUpload":
            self.objects[params["Key"]] = b"".join(self.parts[n] for n in sorted(self.parts))
            return {}
        if op == "AbortMultipartUpload":
            return {}
        raise AssertionError(f"unexpected {op}")


@pytest.mark.parametrize("size, ops", [(16 * 1024 * 1024, {"CreateMultipartUpload", "CompleteMultipartUpload"}),
                                       (3 * 1024 * 1024, {"PutObject"})])
def test_release_object_through_s3transfer(monkeypatch, size, ops):
    replay = dem(1, size)  # 16 MiB is exactly two default parts, so the end comes as an empty read
    blob = zstandard.ZstdCompressor().compress(replay)
    s3 = RecordingS3(monkeypatch, {"cold/replays/1.dem.zst": blob})
    sha = hashlib.sha256(blob).hexdigest()
    assert cold.release_object(s3.client, COLD, "cold/replays/1.dem.zst", HOT, "replays/1.dem", sha) == (len(blob), size)
    assert s3.objects["replays/1.dem"] == replay
    assert ops <= set(s3.calls)


def test_release_object_aborts_the_multipart_upload_on_a_bad_checksum(monkeypatch):
    blob = zstandard.ZstdCompressor().compress(dem(1, 20 * 1024 * 1024))
    s3 = RecordingS3(monkeypatch, {"cold/replays/1.dem.zst": blob})
    with pytest.raises(PermanentError, match="sha256"):
        cold.release_object(s3.client, COLD, "cold/replays/1.dem.zst", HOT, "replays/1.dem", "0" * 64)
    assert "AbortMultipartUpload" in s3.calls and "CompleteMultipartUpload" not in s3.calls
    assert "replays/1.dem" not in s3.objects


def test_in_pipeline_counts_hot_replays_and_parsed_markers():
    s3 = FakeS3()
    s3.objects[(HOT, "replays/1.dem")] = b""
    s3.objects[(HOT, "replays/99.dem")] = b""
    s3.objects[(HOT, "parsed/_complete/match_id=2/part-0.parquet")] = b""
    s3.objects[(HOT, "parsed/match/match_id=3/part-0.parquet")] = b""  # tables without the marker: not parsed
    assert cold.in_pipeline(s3, HOT, [1, 2, 3, 4]) == {1, 2}


def test_restore_releases_on_schedule_and_logs_each_release():
    s3, clock = FakeS3(), Clock()
    entries = seeded(s3, [1_000, 1_100, 1_400])
    rows = run(s3, clock, count=3, speed=2.0, spacing="event")
    assert [s3.objects[(HOT, f"replays/{i}.dem")] == dem(i) for i in (1, 2, 3)] == [True] * 3
    # mean rate 2 * 2 = 4 a minute: 2 gaps over 30 s, split 1:3 like the real 100 s and 300 s gaps
    assert [r["scheduled_offset_s"] for r in rows] == [0, 7.5, 30]
    assert clock.sleeps == pytest.approx([7.5, 22.5])
    assert log_rows(s3) == rows
    first = rows[0]
    assert list(first) == list(cold.LOG_FIELDS)
    assert first["status"] == "released" and first["run_id"] == "manual__1"
    assert (first["speed"], first["rate_per_min"], first["spacing"]) == (2.0, 2.0, "event")
    assert first["end_time"] == entries[0]["end_time"]
    assert first["bytes_in"] == entries[0]["bytes"] and first["bytes_out"] == len(dem(1))
    assert first["scheduled_at"] == first["started_at"] == cold.utc_iso(1_800_000_000.0)
    assert rows[2]["released_at"] == rows[2]["finished_at"] == cold.utc_iso(1_800_000_030.0)
    assert cold.raise_if_failed(rows) == {"released": 3, "skipped": 0, "failed": 0}


def test_back_to_back_runs_take_fresh_matches_without_waiting_for_used_ones():
    s3 = FakeS3()
    seeded(s3, [1_000, 1_100, 1_200, 1_300])
    assert [r["match_id"] for r in run(s3, Clock(), count=2)] == [1, 2]
    clock = Clock()
    second = run(s3, clock, count=2, speed=2.0, run_id="manual__2")
    assert [(r["match_id"], r["status"]) for r in second] == [(3, "released"), (4, "released")]
    assert clock.sleeps == [15]
    with pytest.raises(PermanentError, match="no fresh replays.*4 already hot or parsed"):
        run(s3, Clock(), run_id="manual__3")


def test_restore_leaves_out_parsed_matches_even_when_their_replay_is_gone():
    s3 = FakeS3()
    seeded(s3, [1_000, 1_100, 1_200])
    s3.objects[(HOT, "parsed/_complete/match_id=1/part-0.parquet")] = b""
    assert [r["match_id"] for r in run(s3, Clock())] == [2, 3]


def test_restore_stops_at_the_end_of_a_seed_run():
    s3 = FakeS3()
    seeded(s3, [1_000, 1_100, 900_000, 900_100], runs="aabb")
    clock = Clock()
    rows = run(s3, clock, count=4, spacing="event")
    assert [r["match_id"] for r in rows] == [1, 2]
    assert clock.sleeps == [30]  # the gap to run b would have taken the whole schedule
    assert [r["match_id"] for r in run(s3, Clock(), run_id="manual__2")] == [3, 4]


def test_a_failed_replay_is_logged_the_rest_run_and_the_task_fails_at_the_end():
    s3, clock = FakeS3(), Clock()
    seeded(s3, [1_000, 1_100, 1_200])
    del s3.objects[(COLD, cold.cold_key("cold/", 2))]
    rows = run(s3, clock)
    assert [r["status"] for r in rows] == ["released", "failed", "released"]
    assert "NoSuchKey" in rows[1]["error"]
    assert rows[1]["started_at"] and rows[1]["finished_at"] and rows[1]["released_at"] is None
    assert (HOT, "replays/2.dem") not in s3.objects
    assert log_rows(s3) == rows
    with pytest.raises(PermanentError, match=r"1 of 3 .*\[2\]"):
        cold.raise_if_failed(rows)


def test_a_replay_that_turned_hot_after_the_pick_is_skipped():
    rows = schedule([1, 2], is_hot=lambda m: m == 1)
    assert [r["status"] for r in rows] == ["skipped", "released"]
    assert rows[0]["started_at"] is None and rows[0]["released_at"] is None and rows[0]["finished_at"]
    assert rows[0]["bytes_in"] == rows[0]["bytes_out"] == 0


def test_a_release_slower_than_its_gap_does_not_sleep():
    clock = Clock()

    def slow(entry):
        clock.advance(100)
        return 1, 2

    rows = schedule([0, 1, 2], release=slow, clock=clock, gap=30)
    assert clock.sleeps == [] and [r["status"] for r in rows] == ["released"] * 3


def test_waits_follow_the_monotonic_clock_when_the_wall_clock_steps():
    clock = Clock()

    def release(entry):
        if entry["match_id"] == 0:
            clock.jump(-3600)  # wall clock set back an hour mid-run
        return 1, 2

    rows = schedule([0, 1, 2], release=release, clock=clock, gap=30)
    assert clock.sleeps == [30, 30]
    assert rows[2]["scheduled_at"] == cold.utc_iso(1_800_000_060.0)  # from the wall time at the start


def test_the_replay_in_flight_is_in_the_log_during_its_upload():
    writes = []

    def write_log(rows):
        writes.append([(r["match_id"], r["status"], r["released_at"]) for r in rows])

    def release(entry):
        assert writes[-1][-1] == (entry["match_id"], "in_flight", None)
        return 1, 2

    schedule([1, 2], release=release, write_log=write_log)
    assert [w[-1][1] for w in writes] == ["in_flight", "released", "in_flight", "released"]


def test_a_timeout_mid_upload_logs_that_replay_as_interrupted():
    writes = []

    class Timeout(BaseException):  # like Airflow's task timeout
        pass

    def release(entry):
        if entry["match_id"] == 2:
            raise Timeout()
        return 1, 2

    with pytest.raises(Timeout):
        schedule([1, 2, 3], release=release, write_log=lambda rows: writes.append([(r["match_id"], r["status"]) for r in rows]))
    assert writes[-1] == [(1, "released"), (2, "interrupted")]


def test_a_failed_log_write_mid_run_does_not_stop_the_run():
    calls = []

    def write_log(rows):
        calls.append(len(rows))
        if len(calls) == 1:
            raise OSError("garage hiccup")

    assert len(schedule([1, 2], write_log=write_log)) == 2
    assert calls == [1, 1, 2, 2]  # no extra write once a later one has succeeded


def test_a_run_whose_log_cannot_be_written_fails():
    def write_log(rows):
        raise OSError("garage down")

    with pytest.raises(OSError):
        schedule([1], write_log=write_log)


def test_restore_takes_entries_after_start_after_match_id():
    s3 = FakeS3()
    seeded(s3, [1_000, 1_100, 1_200])
    rows = run(s3, Clock(), count=5, start_after_match_id=1)
    assert [r["match_id"] for r in rows] == [2, 3]


def test_restore_refuses_an_empty_manifest_a_bad_prefix_and_a_schedule_too_long_to_finish():
    s3 = FakeS3()
    with pytest.raises(PermanentError, match="no fresh replays"):
        run(s3, Clock())
    with pytest.raises(PermanentError, match="overlaps"):
        run(s3, Clock(), cold_bucket=HOT, cold_prefix="")
    seeded(s3, [1_000, 1_100, 1_200])
    with pytest.raises(PermanentError, match="may not finish"):
        run(s3, Clock(), rate_per_min=0.001)
    assert s3.puts == []  # nothing released, no log


def test_check_replay_accepts_one_complete_frame():
    blob = zstandard.ZstdCompressor().compress(dem(1))
    assert cold.check_replay(io.BytesIO(blob)) == (hashlib.sha256(blob).hexdigest(), len(blob), len(dem(1)))


@pytest.mark.parametrize("blob, problem", [
    (zstandard.ZstdCompressor().compress(dem(1))[:-10], "truncated"),
    (zstandard.ZstdCompressor().compress(dem(1)) * 2, "trailing"),
    (zstandard.ZstdCompressor().compress(b"<html></html>"), "not a Dota 2 replay"),
    (b"plain text", "not valid zstd"),
])
def test_check_replay_rejects_damaged_or_foreign_files(blob, problem):
    with pytest.raises(PermanentError, match=problem):
        cold.check_replay(io.BytesIO(blob))
