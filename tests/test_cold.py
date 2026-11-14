import hashlib
import io
import json
import random

import pytest
import zstandard
from botocore.exceptions import ClientError

from dota import cold
from dota.errors import PermanentError
from dota.storage import DEM_MAGIC

HOT, COLD = "raw", "archive"


# ---- schedule ----

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


# ---- manifest ----

def entry(match_id, end_time, seq=None, sha="x", nbytes=1):
    return cold.manifest_entry(match_id, seq or match_id, end_time - 100, 100, nbytes, sha)


def test_manifest_entry_end_time_is_start_plus_duration():
    e = cold.manifest_entry(1, 2, 1_000, 2_400, 37, "ab")
    assert e == {"match_id": 1, "match_seq_num": 2, "start_time": 1_000, "duration": 2_400,
                 "end_time": 3_400, "bytes": 37, "sha256": "ab"}


def test_manifest_round_trips_and_ignores_blank_lines():
    entries = [entry(2, 500), entry(1, 900)]
    text = cold.dump_manifest(entries)
    assert len(text.splitlines()) == 2
    assert cold.parse_manifest(text + "\n\n") == entries
    assert list(json.loads(text.splitlines()[0])) == list(cold.MANIFEST_FIELDS)


def test_merge_manifest_has_no_duplicates_and_new_entries_win():
    old = [entry(1, 100, sha="old"), entry(2, 200)]
    merged = cold.merge_manifest(old, [entry(1, 100, sha="new"), entry(3, 50)])
    assert [e["match_id"] for e in merged] == [3, 1, 2]  # end_time order
    assert next(e for e in merged if e["match_id"] == 1)["sha256"] == "new"


def test_pick_takes_the_next_entries_in_end_time_order():
    entries = [entry(1, 300), entry(2, 100), entry(3, 200), entry(4, 200)]
    assert [e["match_id"] for e in cold.pick(entries, 3)] == [2, 3, 4]
    assert [e["match_id"] for e in cold.pick(entries, 5, start_after_match_id=3)] == [4, 1]
    with pytest.raises(PermanentError):
        cold.pick(entries, 1, start_after_match_id=99)


def test_keys():
    assert cold.cold_key("cold", 7) == "cold/replays/7.dem.zst"
    assert cold.manifest_key("cold/") == "cold/manifest.jsonl"
    assert cold.log_key("cold/", "manual__x") == "cold/restore_log/manual__x.jsonl"
    assert cold.hot_key(7) == "replays/7.dem"


# ---- fakes ----

class FakeS3:
    """Enough of a boto3 S3 client. upload_fileobj reads in parts like a multipart upload and
    stores nothing when a read raises."""

    def __init__(self, part=64 * 1024):
        self.objects: dict[tuple[str, str], bytes] = {}
        self.part = part
        self.puts: list[str] = []

    def _missing(self, op):
        return ClientError({"Error": {"Code": "404" if op == "HeadObject" else "NoSuchKey"}}, op)

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise self._missing("GetObject")
        data = self.objects[(Bucket, Key)]
        return {"Body": io.BytesIO(data), "ContentLength": len(data)}

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise self._missing("HeadObject")
        return {}

    def put_object(self, Bucket, Key, Body=b""):
        self.objects[(Bucket, Key)] = Body.read() if hasattr(Body, "read") else Body
        self.puts.append(Key)

    def upload_fileobj(self, f, bucket, key):
        parts = []
        while chunk := f.read(self.part):
            parts.append(chunk)
        self.objects[(bucket, key)] = b"".join(parts)

    def delete_object(self, Bucket, Key):
        self.objects.pop((Bucket, Key), None)


class Clock:
    def __init__(self, t=1_800_000_000.0, cost=0.0):
        self.t, self.cost, self.sleeps = t, cost, []

    def now(self):
        return self.t

    def sleep(self, s):
        self.sleeps.append(s)
        self.t += s


def dem(match_id):
    # random bytes so the archive stays bigger than one upload part and truncation hits the data
    return DEM_MAGIC + random.Random(match_id).randbytes(200_000)


def archive(s3, match_id, end_time, prefix="cold/", body=None):
    blob = zstandard.ZstdCompressor().compress(dem(match_id)) if body is None else body
    s3.objects[(COLD, cold.cold_key(prefix, match_id))] = blob
    return cold.manifest_entry(match_id, match_id, end_time - 600, 600, len(blob), hashlib.sha256(blob).hexdigest())


def seeded(s3, ends, prefix="cold/"):
    entries = [archive(s3, i + 1, end, prefix) for i, end in enumerate(ends)]
    s3.objects[(COLD, cold.manifest_key(prefix))] = cold.dump_manifest(entries).encode()
    return entries


def run(s3, clock, **kw):
    args = dict(count=10, speed=1.0, rate_per_min=2.0, spacing="uniform", start_after_match_id=None,
                cold_bucket=COLD, cold_prefix="cold/", hot_bucket=HOT, run_id="manual__1")
    return cold.restore(s3, **{**args, **kw}, now=clock.now, sleep=clock.sleep)


def log_rows(s3, run_id="manual__1"):
    return [json.loads(line) for line in s3.objects[(COLD, cold.log_key("cold/", run_id))].decode().splitlines()]


# ---- release ----

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
    assert set(cold.LOG_FIELDS) <= set(first)
    assert first["status"] == "released" and first["speed"] == 2.0 and first["spacing"] == "event"
    assert first["end_time"] == entries[0]["end_time"]
    assert first["bytes_in"] == entries[0]["bytes"] and first["bytes_out"] == len(dem(1))
    assert first["scheduled_at"] == cold.utc_iso(1_800_000_000.0)
    assert rows[2]["released_at"] == cold.utc_iso(1_800_000_030.0)
    assert cold.raise_if_failed(rows) == {"released": 3, "skipped": 0, "failed": 0}


def test_restore_skips_replays_already_hot():
    s3, clock = FakeS3(), Clock()
    seeded(s3, [1_000, 1_100])
    s3.objects[(HOT, "replays/1.dem")] = b"already here"
    rows = run(s3, clock)
    assert [r["status"] for r in rows] == ["skipped", "released"]
    assert s3.objects[(HOT, "replays/1.dem")] == b"already here"
    assert rows[0]["bytes_in"] == rows[0]["bytes_out"] == 0
    again = run(s3, Clock(), run_id="manual__2")
    assert [r["status"] for r in again] == ["skipped", "skipped"]


def test_a_failed_replay_is_logged_the_rest_run_and_the_task_fails_at_the_end():
    s3, clock = FakeS3(), Clock()
    seeded(s3, [1_000, 1_100, 1_200])
    del s3.objects[(COLD, cold.cold_key("cold/", 2))]
    rows = run(s3, clock)
    assert [r["status"] for r in rows] == ["released", "failed", "released"]
    assert "NoSuchKey" in rows[1]["error"] and rows[1]["released_at"]
    assert (HOT, "replays/2.dem") not in s3.objects
    assert log_rows(s3) == rows
    with pytest.raises(PermanentError, match=r"1 of 3 .*\[2\]"):
        cold.raise_if_failed(rows)


def test_a_release_slower_than_its_gap_does_not_sleep():
    clock = Clock()

    def slow(entry):
        clock.t += 100
        return 1, 2

    plan = [({"match_id": i, "end_time": i}, i * 30.0) for i in range(3)]
    rows = cold.release_schedule(plan, release=slow, is_hot=lambda m: False, write_log=lambda r: None,
                                 speed=1, spacing="uniform", now=clock.now, sleep=clock.sleep)
    assert clock.sleeps == [] and [r["status"] for r in rows] == ["released"] * 3


def test_log_is_written_after_every_release_and_when_the_run_stops():
    clock, writes = Clock(), []

    class Timeout(BaseException):  # like Airflow's task timeout
        pass

    def release(entry):
        if entry["match_id"] == 2:
            raise Timeout()
        return 1, 2

    plan = [({"match_id": i, "end_time": i}, 0.0) for i in range(1, 4)]
    with pytest.raises(Timeout):
        cold.release_schedule(plan, release=release, is_hot=lambda m: False,
                              write_log=lambda rows: writes.append([r["match_id"] for r in rows]),
                              speed=1, spacing="uniform", now=clock.now, sleep=clock.sleep)
    assert writes == [[1], [1]]


def test_a_failed_log_write_mid_run_does_not_stop_the_run():
    calls = []

    def write_log(rows):
        calls.append(len(rows))
        if len(calls) == 1:
            raise OSError("garage hiccup")

    plan = [({"match_id": i, "end_time": i}, 0.0) for i in range(2)]
    rows = cold.release_schedule(plan, release=lambda e: (1, 1), is_hot=lambda m: False, write_log=write_log,
                                 speed=1, spacing="uniform", now=Clock().now, sleep=Clock().sleep)
    assert len(rows) == 2 and calls == [1, 2, 2]


def test_restore_takes_entries_after_start_after_match_id():
    s3 = FakeS3()
    seeded(s3, [1_000, 1_100, 1_200])
    rows = run(s3, Clock(), count=5, start_after_match_id=1)
    assert [r["match_id"] for r in rows] == [2, 3]


def test_restore_refuses_an_empty_manifest_and_a_schedule_too_long_to_finish():
    s3 = FakeS3()
    with pytest.raises(PermanentError, match="no replays"):
        run(s3, Clock())
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
