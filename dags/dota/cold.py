"""Cold store: compressed replays archived in a bucket on the pipeline's S3 endpoint (Garage by
default; the S3_* settings pick another S3-compatible store), released back into the hot replays/
prefix on a schedule by the cold_restore DAG (docs/cold-storage.md).

Layout under a prefix (default cold/ in the raw bucket):
  {prefix}replays/{match_id}.dem.zst    one replay, zstd-compressed as Valve serves it
  {prefix}manifest.jsonl                one JSON line per match (MANIFEST_FIELDS)
  {prefix}restore_log/{run_id}.jsonl    one JSON line per release, written by cold_restore

This is an archive for reprocessing and replay/burst tests, not an ingest path: live replays
still come from the API through dota_daily_download.

Manifest and scheduling helpers need only the standard library, so the DAG file can import this
module at parse time and the host tests run without Airflow. boto3, zstandard and dota.storage
are imported inside the functions that use them.
"""

import contextlib
import datetime as dt
import hashlib
import io
import json
import time
from typing import Callable, Iterable

from dota.errors import PermanentError, redact

COLD_PREFIX = "cold/"
SPACINGS = ("event", "uniform")
MANIFEST_FIELDS = ("match_id", "match_seq_num", "start_time", "duration", "end_time", "bytes", "sha256",
                   "seed_run")
LOG_FIELDS = ("match_id", "end_time", "scheduled_offset_s", "scheduled_at", "started_at", "released_at",
              "finished_at", "bytes_in", "bytes_out", "status", "run_id", "speed", "rate_per_min", "spacing")
# prefixes the pipeline sweeps; cold objects there would be read as hot data
PIPELINE_PREFIXES = ("replays", "parsed", "clean", "steam_raw")
MAX_RUN_S = 12 * 3600  # the restore task's execution_timeout
PER_OBJECT_S = 10  # allowance per replay (about 0.5 s each on local Garage) when checking a schedule fits
MAX_ERROR_CHARS = 500


def norm_prefix(prefix: str) -> str:
    return prefix if not prefix or prefix.endswith("/") else prefix + "/"


def check_prefix(cold_bucket: str, prefix: str, hot_bucket: str) -> str:
    """The normalised prefix, or PermanentError if cold objects would land in the hot data."""
    prefix = norm_prefix(prefix)
    if cold_bucket == hot_bucket and (not prefix or prefix.split("/", 1)[0] in PIPELINE_PREFIXES):
        raise PermanentError(f"cold prefix {prefix!r} in bucket {cold_bucket} overlaps the pipeline's own prefixes")
    return prefix


def cold_key(prefix: str, match_id: int) -> str:
    return f"{norm_prefix(prefix)}replays/{match_id}.dem.zst"


def manifest_key(prefix: str) -> str:
    return f"{norm_prefix(prefix)}manifest.jsonl"


def log_key(prefix: str, run_id: str) -> str:
    return f"{norm_prefix(prefix)}restore_log/{run_id}.jsonl"


def hot_key(match_id: int) -> str:
    return f"replays/{match_id}.dem"  # the key dota_daily_download writes


def manifest_entry(match_id: int, match_seq_num: int, start_time: int, duration: int,
                   nbytes: int, sha256: str, seed_run: str | None = None) -> dict:
    return {"match_id": int(match_id), "match_seq_num": int(match_seq_num), "start_time": int(start_time),
            "duration": int(duration), "end_time": int(start_time) + int(duration), "bytes": int(nbytes),
            "sha256": sha256, "seed_run": seed_run}


def by_end(entries: Iterable[dict]) -> list[dict]:
    return sorted(entries, key=lambda e: (e["end_time"], e["match_id"]))


def parse_manifest(text: str) -> list[dict]:
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def merge_manifest(existing: Iterable[dict], new: Iterable[dict]) -> list[dict]:
    """One entry per match_id, a new entry replacing an old one, in end_time order."""
    merged = {e["match_id"]: e for e in existing}
    merged.update({e["match_id"]: e for e in new})
    return by_end(merged.values())


def dump_manifest(entries: Iterable[dict]) -> str:
    return "".join(json.dumps({k: e.get(k) for k in MANIFEST_FIELDS}) + "\n" for e in by_end(entries))


def _missing(error: Exception) -> bool:
    code = getattr(error, "response", {}).get("Error", {}).get("Code")
    return code in ("404", "NoSuchKey", "NotFound")


def read_manifest(client, bucket: str, prefix: str) -> list[dict]:
    from botocore.exceptions import ClientError

    try:
        body = client.get_object(Bucket=bucket, Key=manifest_key(prefix))["Body"].read()
    except ClientError as e:
        if _missing(e):
            return []
        raise
    return parse_manifest(body.decode())


def add_to_manifest(client, bucket: str, prefix: str, new: Iterable[dict]) -> list[dict]:
    """Read, merge and rewrite the manifest. Not safe against two writers at once; seeding is a
    one-off run by hand."""
    merged = merge_manifest(read_manifest(client, bucket, prefix), new)
    client.put_object(Bucket=bucket, Key=manifest_key(prefix), Body=dump_manifest(merged).encode())
    return merged


def list_keys(client, bucket: str, prefix: str) -> list[str]:
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys += [obj["Key"] for obj in page.get("Contents", [])]
    return keys


def in_pipeline(client, hot_bucket: str, match_ids: Iterable[int]) -> set[int]:
    """Of match_ids, those with a hot replay or a parsed _complete marker, read the way
    parse_pending reads them. Releasing one of these again would not be parsed again."""
    from dota import contract

    ids = set(match_ids)
    hot = set(contract.unparsed(list_keys(client, hot_bucket, "replays/"), []))
    unparsed = contract.unparsed([hot_key(i) for i in ids],
                                 list_keys(client, hot_bucket, f"parsed/{contract.COMPLETE}/"))
    return (hot & ids) | (ids - set(unparsed))


def pick(entries: Iterable[dict], count: int, start_after_match_id: int | None = None,
         done: set[int] = frozenset()) -> list[dict]:
    """Up to `count` entries in end_time order, after start_after_match_id when given, leaving out
    those in `done`. All come from the seed run of the first one, so the real gaps between them
    are not stretched by a hole between two seed runs."""
    ordered = by_end(entries)
    if start_after_match_id is not None:
        ids = [e["match_id"] for e in ordered]
        if start_after_match_id not in ids:
            raise PermanentError(f"match {start_after_match_id} is not in the cold manifest")
        ordered = ordered[ids.index(start_after_match_id) + 1:]
    fresh = [e for e in ordered if e["match_id"] not in done]
    if not fresh:
        return []
    run = fresh[0].get("seed_run")
    return [e for e in fresh if e.get("seed_run") == run][:count]


def offsets(end_times: list[int], rate_per_min: float, speed: float = 1.0, spacing: str = "event") -> list[float]:
    """Seconds from the start of the run at which each replay is released.

    uniform: one every 1 / (rate_per_min * speed) minutes.
    event:   the real gaps between end times, scaled so the mean rate is rate_per_min * speed.
    """
    if rate_per_min <= 0 or speed <= 0:
        raise PermanentError("rate_per_min and speed must be positive")
    if spacing not in SPACINGS:
        raise PermanentError(f"spacing must be one of {SPACINGS}, not {spacing!r}")
    if any(b < a for a, b in zip(end_times, end_times[1:])):
        raise PermanentError("end times must be in order")
    gap_s = 60 / (rate_per_min * speed)
    n = len(end_times)
    span = end_times[-1] - end_times[0] if n else 0
    # matches that all ended in the same second have no gaps to keep, so fall back to uniform
    if spacing == "uniform" or n < 2 or span == 0:
        return [i * gap_s for i in range(n)]
    return [(t - end_times[0]) / span * (n - 1) * gap_s for t in end_times]


class _Reader:
    """Read-only wrapper that counts (and optionally hashes) what passes through, and runs
    at_eof once when the wrapped stream ends. at_eof raising makes the upload fail. Wrap only
    readers that return short reads at the end alone, such as io.BufferedReader."""

    def __init__(self, raw, digest=None, at_eof: Callable[[], None] | None = None):
        self.raw, self.digest, self.at_eof, self.n = raw, digest, at_eof, 0

    def read(self, size: int | None = -1) -> bytes:
        chunk = self.raw.read(size)
        self.n += len(chunk)
        if self.digest is not None:
            self.digest.update(chunk)
        # a buffered read only comes back short at the end; boto3 sends a small body after one
        # short read without asking again, so waiting for an empty read would miss it
        if self.at_eof is not None and (size is None or size < 0 or len(chunk) < size):
            check, self.at_eof = self.at_eof, None
            check()
        return chunk

    def readable(self) -> bool:
        return True


def exists(client, bucket: str, key: str) -> bool:
    from botocore.exceptions import ClientError

    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if _missing(e):
            return False
        raise


def release_object(client, cold_bucket: str, src_key: str, hot_bucket: str, dst_key: str,
                   sha256: str) -> tuple[int, int]:
    """Stream one cold .dem.zst into the hot bucket as a decompressed .dem, the same way
    storage.upload_replay does: the header must be a Dota 2 replay, and the multipart upload only
    appears once complete. Returns (compressed bytes read, replay bytes written)."""
    import zstandard

    from dota.storage import DEM_MAGIC

    with contextlib.closing(client.get_object(Bucket=cold_bucket, Key=src_key)["Body"]) as body:
        src = _Reader(body, hashlib.sha256())

        def check_source() -> None:
            # zstd's stream reader returns a truncated frame without an error, so the checksum is
            # what catches a damaged archive. Checking here, before the last read returns, aborts
            # the upload instead of leaving a bad replay in the hot prefix.
            while src.read(1 << 20):
                pass
            if src.digest.hexdigest() != sha256:
                raise PermanentError(f"{src_key} does not match its manifest sha256")

        with zstandard.ZstdDecompressor().stream_reader(src) as dem:
            replay = io.BufferedReader(dem)
            try:
                head = replay.peek(len(DEM_MAGIC))[:len(DEM_MAGIC)]
            except zstandard.ZstdError:
                head = b""
            if head != DEM_MAGIC:
                raise PermanentError(f"{src_key} is not a Dota 2 replay")
            out = _Reader(replay, at_eof=check_source)
            client.upload_fileobj(out, hot_bucket, dst_key)
    return src.n, out.n


def check_replay(f) -> tuple[str, int, int]:
    """(sha256, compressed bytes, replay bytes) of a .dem.zst file object. Raises PermanentError
    unless it is one complete zstd frame holding a Dota 2 replay."""
    import zstandard

    from dota.storage import DEM_MAGIC

    digest, d = hashlib.sha256(), zstandard.ZstdDecompressor().decompressobj()
    n_in = n_out = 0
    head = b""
    try:
        while chunk := f.read(1 << 20):
            digest.update(chunk)
            n_in += len(chunk)
            out = d.decompress(chunk)
            if len(head) < len(DEM_MAGIC):
                head += out[:len(DEM_MAGIC) - len(head)]
            n_out += len(out)
    except zstandard.ZstdError as e:
        raise PermanentError(f"not valid zstd: {e}") from None
    if not d.eof or d.unused_data:
        raise PermanentError("not a single complete zstd frame (truncated or trailing data)")
    if head != DEM_MAGIC:
        raise PermanentError("not a Dota 2 replay")
    return digest.hexdigest(), n_in, n_out


def utc_iso(t: float) -> str:
    return dt.datetime.fromtimestamp(t, dt.timezone.utc).isoformat(timespec="milliseconds")


def dump_log(rows: Iterable[dict]) -> str:
    return "".join(json.dumps(r) + "\n" for r in rows)


def release_schedule(plan: list[tuple[dict, float]], *, release: Callable[[dict], tuple[int, int]],
                     is_hot: Callable[[int], bool], write_log: Callable[[list[dict]], None],
                     fields: dict, now: Callable[[], float] = time.time,
                     mono: Callable[[], float] = time.monotonic,
                     sleep: Callable[[float], None] = time.sleep) -> list[dict]:
    """Release each (entry, offset_s) at its offset from the start, one at a time, adding `fields`
    (run_id, speed, ...) to every log row. Waits run on the monotonic clock so a wall-clock step
    cannot bunch releases; times in the log are wall-clock UTC.

    A replay that became hot since it was picked is skipped; a failed one is recorded and the rest
    still run. The log is rewritten before each upload (the replay as in_flight) and after it, so
    if the task is killed mid-upload the log still names the replay. Airflow's task timeout is a
    BaseException, so it stops the loop, and the replay in flight is logged as interrupted."""
    rows: list[dict] = []
    current: dict | None = None
    dirty = finished = False
    start_wall, start_mono = now(), mono()

    def flush() -> None:
        nonlocal dirty
        try:
            write_log(rows + ([current] if current else []))
            dirty = False
        except Exception as e:  # noqa: BLE001
            dirty = True
            print(f"could not write restore log: {type(e).__name__}: {e}")

    try:
        for entry, offset in plan:
            if (wait := start_mono + offset - mono()) > 0:
                sleep(wait)
            row = {"match_id": entry["match_id"], "end_time": entry["end_time"],
                   "scheduled_offset_s": round(offset, 3), "scheduled_at": utc_iso(start_wall + offset),
                   "started_at": None, "released_at": None, "finished_at": None,
                   "bytes_in": 0, "bytes_out": 0, "status": "in_flight", **fields}
            try:
                if is_hot(entry["match_id"]):
                    row["status"] = "skipped"
                else:
                    row["started_at"] = utc_iso(now())
                    current = row
                    flush()
                    row["bytes_in"], row["bytes_out"] = release(entry)
                    row["status"], row["released_at"] = "released", utc_iso(now())
            except Exception as e:  # noqa: BLE001
                row["status"] = "failed"
                row["error"] = redact(f"{type(e).__name__}: {e}")[:MAX_ERROR_CHARS]
            row["finished_at"] = utc_iso(now())
            rows.append(row)
            current = None
            print(f"match {row['match_id']}: {row['status']} at +{mono() - start_mono:.1f}s "
                  f"(scheduled +{offset:.1f}s){' ' + row['error'] if 'error' in row else ''}")
            flush()
        finished = True
    finally:
        if current is not None:
            current["status"], current["finished_at"] = "interrupted", utc_iso(now())
            rows.append(current)
            dirty = True
        if dirty:
            try:
                write_log(rows)
            except Exception:
                # after a clean finish a missing log is the run's failure; while another error is
                # on its way out, it must not hide that one
                if finished:
                    raise
                print("could not write restore log after the run stopped")
    return rows


def restore(client, *, count: int, speed: float, rate_per_min: float, spacing: str,
            start_after_match_id: int | None, cold_bucket: str, cold_prefix: str, hot_bucket: str,
            run_id: str, now: Callable[[], float] = time.time, mono: Callable[[], float] = time.monotonic,
            sleep: Callable[[float], None] = time.sleep, max_run_s: float = MAX_RUN_S) -> list[dict]:
    """Release the next `count` fresh manifest entries into the hot bucket on schedule; returns
    the log rows. Fresh means not hot and not parsed, so consecutive runs take new matches."""
    prefix = check_prefix(cold_bucket, cold_prefix, hot_bucket)
    entries = read_manifest(client, cold_bucket, prefix)
    done = in_pipeline(client, hot_bucket, [e["match_id"] for e in entries])
    chosen = pick(entries, count, start_after_match_id, done)
    if not chosen:
        raise PermanentError(f"no fresh replays in s3://{cold_bucket}/{manifest_key(prefix)}: "
                             f"{len(entries)} in the manifest, {len(done)} already hot or parsed")
    if len(chosen) < count:
        print(f"only {len(chosen)} of {count} replays left in seed run {chosen[0].get('seed_run')}; "
              "releasing those")
    offs = offsets([e["end_time"] for e in chosen], rate_per_min, speed, spacing)
    if offs[-1] + len(chosen) * PER_OBJECT_S > max_run_s:
        raise PermanentError(f"schedule of {offs[-1] / 60:.0f} min for {len(chosen)} replays may not finish "
                             f"within {max_run_s / 3600:.0f} h; raise speed or lower count")
    key = log_key(prefix, run_id)
    print(f"releasing {len(chosen)} replays over {offs[-1] / 60:.1f} min ({spacing}, speed {speed}), "
          f"matches {chosen[0]['match_id']}..{chosen[-1]['match_id']}; log s3://{cold_bucket}/{key}")
    return release_schedule(
        list(zip(chosen, offs)),
        release=lambda e: release_object(client, cold_bucket, cold_key(prefix, e["match_id"]),
                                         hot_bucket, hot_key(e["match_id"]), e["sha256"]),
        is_hot=lambda match_id: exists(client, hot_bucket, hot_key(match_id)),
        write_log=lambda rows: client.put_object(Bucket=cold_bucket, Key=key, Body=dump_log(rows).encode()),
        fields={"run_id": run_id, "speed": speed, "rate_per_min": rate_per_min, "spacing": spacing},
        now=now, mono=mono, sleep=sleep)


def status_counts(rows: Iterable[dict]) -> dict[str, int]:
    counts = {"released": 0, "skipped": 0, "failed": 0}
    for r in rows:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    return counts


def raise_if_failed(rows: list[dict]) -> dict[str, int]:
    """The status counts, or PermanentError naming the matches that failed."""
    counts = status_counts(rows)
    if counts["failed"]:
        failed = [r["match_id"] for r in rows if r["status"] == "failed"]
        raise PermanentError(f"{counts['failed']} of {len(rows)} replays failed to restore: {failed}")
    return counts
