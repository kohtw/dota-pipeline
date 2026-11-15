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
    assert cold.plan_key("cold/", "manual__x") == "cold/restore_log/manual__x/_plan.json"
    assert cold.row_key("cold", "manual__x", 7) == "cold/restore_log/manual__x/7.json"
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


def plan(s3, clock=None, **kw):
    args = dict(count=10, speed=1.0, rate_per_min=2.0, spacing="uniform", start_after_match_id=None,
                cold_bucket=COLD, cold_prefix="cold/", hot_bucket=HOT, run_id="manual__1")
    return cold.plan_restore(s3, **{**args, **kw}, now=(clock or Clock()).now)


def release_all(s3, planned, clock=None):
    clock = clock or Clock()
    return [cold.release_item(s3, item, hot_bucket=HOT, now=clock.now) for item in planned["items"]]


def stored_json(s3, key):
    return json.loads(s3.objects[(COLD, key)])


def row_in_log(s3, match_id, run_id="manual__1"):
    return stored_json(s3, cold.row_key("cold/", run_id, match_id))


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


def test_plan_fixes_t0_and_each_release_time_and_saves_the_plan():
    s3, clock = FakeS3(), Clock()
    entries = seeded(s3, [1_000, 1_100, 1_400])
    planned = plan(s3, clock, count=3, speed=2.0, spacing="event")
    t0 = 1_800_000_000.0 + cold.LEAD_S
    assert planned["t0"] == cold.utc_iso(t0) and planned["planned_at"] == cold.utc_iso(1_800_000_000.0)
    # mean rate 2 * 2 = 4 a minute: 2 gaps over 30 s, split 1:3 like the real 100 s and 300 s gaps
    assert [i["scheduled_offset_s"] for i in planned["items"]] == [0, 7.5, 30]
    assert [i["scheduled_at"] for i in planned["items"]] == [cold.utc_iso(t0 + o) for o in (0, 7.5, 30)]
    first = planned["items"][0]
    assert first == {"run_id": "manual__1", "speed": 2.0, "rate_per_min": 2.0, "spacing": "event",
                    "cold_bucket": COLD, "cold_prefix": "cold/", "match_id": 1, "seed_run": "a",
                    "end_time": entries[0]["end_time"], "sha256": entries[0]["sha256"],
                    "scheduled_offset_s": 0.0, "scheduled_at": cold.utc_iso(t0)}
    assert stored_json(s3, "cold/restore_log/manual__1/_plan.json") == planned
    assert (HOT, "replays/1.dem") not in s3.objects  # planning releases nothing


def test_back_to_back_runs_take_fresh_matches():
    s3 = FakeS3()
    seeded(s3, [1_000, 1_100, 1_200, 1_300])
    first = plan(s3, count=2)
    assert ids(first["items"]) == [1, 2]
    release_all(s3, first)
    second = plan(s3, count=2, speed=2.0, run_id="manual__2")
    assert ids(second["items"]) == [3, 4]
    assert [i["scheduled_offset_s"] for i in second["items"]] == [0, 15]  # used matches take no time
    release_all(s3, second)
    with pytest.raises(PermanentError, match="no fresh replays.*4 already hot or parsed"):
        plan(s3, run_id="manual__3")


def test_plan_leaves_out_parsed_matches_even_when_their_replay_is_gone():
    s3 = FakeS3()
    seeded(s3, [1_000, 1_100, 1_200])
    s3.objects[(HOT, "parsed/_complete/match_id=1/part-0.parquet")] = b""
    assert ids(plan(s3)["items"]) == [2, 3]


def test_plan_stops_at_the_end_of_a_seed_run():
    s3 = FakeS3()
    seeded(s3, [1_000, 1_100, 900_000, 900_100], runs="aabb")
    planned = plan(s3, count=4, spacing="event")
    assert ids(planned["items"]) == [1, 2]
    assert planned["items"][1]["scheduled_offset_s"] == 30  # the gap to run b would have taken it all
    release_all(s3, planned)
    assert ids(plan(s3, run_id="manual__2")["items"]) == [3, 4]


def test_plan_takes_entries_after_start_after_match_id():
    s3 = FakeS3()
    seeded(s3, [1_000, 1_100, 1_200])
    assert ids(plan(s3, count=5, start_after_match_id=1)["items"]) == [2, 3]


def test_plan_refuses_an_empty_manifest_a_bad_prefix_and_a_schedule_too_long():
    s3 = FakeS3()
    with pytest.raises(PermanentError, match="no fresh replays"):
        plan(s3)
    with pytest.raises(PermanentError, match="overlaps"):
        plan(s3, cold_bucket=HOT, cold_prefix="")
    seeded(s3, [1_000, 1_100, 1_200])
    with pytest.raises(PermanentError, match="longer than 24 h"):
        plan(s3, rate_per_min=0.001)
    assert s3.puts == []  # no plan saved


class SlowS3(FakeS3):
    """Uploads take two seconds of the test clock, and each upload can check the log as it stands."""

    def __init__(self, clock, during_upload=None):
        super().__init__()
        self.clock, self.during_upload = clock, during_upload

    def upload_fileobj(self, f, bucket, key):
        if self.during_upload:
            self.during_upload(self)
        self.clock.advance(2)
        super().upload_fileobj(f, bucket, key)


def test_a_release_writes_its_own_log_row():
    clock = Clock()
    s3 = SlowS3(clock)
    entries = seeded(s3, [1_000, 1_400])
    planned = plan(s3, clock, spacing="event")
    clock.advance(cold.LEAD_S)
    row = cold.release_item(s3, planned["items"][0], hot_bucket=HOT, now=clock.now)
    assert s3.objects[(HOT, "replays/1.dem")] == dem(1)
    assert list(row) == list(cold.LOG_FIELDS)
    assert row_in_log(s3, 1) == row
    assert row["status"] == "released" and row["seed_run"] == "a" and row["run_id"] == "manual__1"
    assert (row["speed"], row["rate_per_min"], row["spacing"]) == (1.0, 2.0, "event")
    assert row["end_time"] == entries[0]["end_time"]
    assert row["bytes_in"] == entries[0]["bytes"] and row["bytes_out"] == len(dem(1))
    assert row["scheduled_at"] == row["started_at"] == cold.utc_iso(1_800_000_000.0 + cold.LEAD_S)
    assert row["released_at"] == row["finished_at"] == cold.utc_iso(1_800_000_002.0 + cold.LEAD_S)


def test_the_log_row_says_in_flight_during_the_upload():
    seen = []
    clock = Clock()
    s3 = SlowS3(clock, during_upload=lambda s3: seen.append(row_in_log(s3, 1)))
    seeded(s3, [1_000])
    cold.release_item(s3, plan(s3)["items"][0], hot_bucket=HOT, now=clock.now)
    assert seen[0]["status"] == "in_flight" and seen[0]["started_at"] and seen[0]["released_at"] is None
    assert row_in_log(s3, 1)["status"] == "released"


def test_a_replay_already_hot_is_skipped_without_an_upload():
    s3 = FakeS3()
    seeded(s3, [1_000, 1_100])
    planned = plan(s3)
    s3.objects[(HOT, "replays/1.dem")] = b"already here"
    row = cold.release_item(s3, planned["items"][0], hot_bucket=HOT)
    assert row["status"] == "skipped" and s3.objects[(HOT, "replays/1.dem")] == b"already here"
    assert row["started_at"] is None and row["released_at"] is None and row["finished_at"]
    assert row["bytes_in"] == row["bytes_out"] == 0
    assert row_in_log(s3, 1) == row


def test_a_failed_release_is_logged_and_leaves_no_hot_replay():
    s3 = FakeS3()
    seeded(s3, [1_000, 1_100])
    planned = plan(s3)
    del s3.objects[(COLD, cold.cold_key("cold/", 1))]
    row = cold.release_item(s3, planned["items"][0], hot_bucket=HOT)
    assert row["status"] == "failed" and "NoSuchKey" in row["error"]
    assert row["started_at"] and row["finished_at"] and row["released_at"] is None
    assert (HOT, "replays/1.dem") not in s3.objects
    assert row_in_log(s3, 1) == row
    assert cold.release_item(s3, planned["items"][1], hot_bucket=HOT)["status"] == "released"


def test_a_timeout_mid_upload_logs_the_replay_as_interrupted():
    class Timeout(BaseException):  # like Airflow's task timeout
        pass

    def stop(s3):
        raise Timeout()

    s3 = SlowS3(Clock(), during_upload=stop)
    seeded(s3, [1_000])
    with pytest.raises(Timeout):
        cold.release_item(s3, plan(s3)["items"][0], hot_bucket=HOT)
    row = row_in_log(s3, 1)
    assert row["status"] == "interrupted" and row["finished_at"] and row["released_at"] is None


def test_read_log_returns_the_plan_and_rows_in_schedule_order():
    s3 = FakeS3()
    seeded(s3, [1_000, 1_100, 1_200])
    planned = plan(s3)
    for item in reversed(planned["items"]):
        cold.release_item(s3, item, hot_bucket=HOT)
    got_plan, rows = cold.read_log(s3, COLD, "cold/", "manual__1")
    assert got_plan == planned
    assert [(r["match_id"], r["status"]) for r in rows] == [(1, "released"), (2, "released"), (3, "released")]
    assert cold.read_log(s3, COLD, "cold/", "manual__2") == (None, [])


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
