"""Cold store: compressed replays archived in a bucket on the pipeline's S3 endpoint (Garage by
default; the S3_* settings pick another S3-compatible store), released back into the hot replays/
prefix on a schedule by the cold_restore DAG (docs/cold-storage.md).

Layout under a prefix (default cold/ in the raw bucket):
  {prefix}replays/{match_id}.dem.zst    one replay, zstd-compressed as Valve serves it
  {prefix}manifest.jsonl                one JSON line per match (MANIFEST_FIELDS)
  {prefix}restore_log/{run_id}/_plan.json         a restore run's plan, written by cold_restore
  {prefix}restore_log/{run_id}/{match_id}.json    one release's log row

This is an archive for reprocessing and replay/burst tests, not an ingest path: live replays
still come from the API through dota_daily_download and download_pending.

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
LOG_FIELDS = ("run_id", "match_id", "seed_run", "end_time", "scheduled_offset_s", "scheduled_at", "started_at",
              "released_at", "finished_at", "bytes_in", "bytes_out", "status", "speed", "spacing", "rate_per_min")
# prefixes the pipeline sweeps; cold objects there would be read as hot data
PIPELINE_PREFIXES = ("replays", "parsed", "clean", "steam_raw")
# time from planning to the first release, so every wait task has deferred before its replay is due
LEAD_S = 30
MAX_PLAN_S = 24 * 3600  # longest schedule a run accepts; the wait tasks time out a little after it
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


def hot_key(match_id: int) -> str:
    return f"replays/{match_id}.dem"  # the key download_pending writes


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


def log_prefix(prefix: str, run_id: str) -> str:
    return f"{norm_prefix(prefix)}restore_log/{run_id}/"


def plan_key(prefix: str, run_id: str) -> str:
    return log_prefix(prefix, run_id) + "_plan.json"


def row_key(prefix: str, run_id: str, match_id: int) -> str:
    return log_prefix(prefix, run_id) + f"{match_id}.json"


def plan_restore(client, *, count: int, speed: float, rate_per_min: float, spacing: str,
                 start_after_match_id: int | None, cold_bucket: str, cold_prefix: str, hot_bucket: str,
                 run_id: str, now: Callable[[], float] = time.time, lead_s: float = LEAD_S) -> dict:
    """Pick the next `count` fresh manifest entries (not hot, not parsed, one seed run), fix the
    run's start T0 = now + lead_s and each replay's scheduled_at, and save the plan as
    {prefix}restore_log/{run_id}/_plan.json. Returns the plan; its items are what each release
    needs."""
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
    if lead_s + offs[-1] > MAX_PLAN_S:
        raise PermanentError(f"schedule of {offs[-1] / 3600:.1f} h for {len(chosen)} replays is longer than "
                             f"{MAX_PLAN_S / 3600:.0f} h; raise speed or lower count")
    planned_at = now()
    t0 = planned_at + lead_s
    run = {"run_id": run_id, "speed": speed, "rate_per_min": rate_per_min, "spacing": spacing,
           "cold_bucket": cold_bucket, "cold_prefix": prefix}
    items = [{**run, "match_id": e["match_id"], "seed_run": e.get("seed_run"), "end_time": e["end_time"],
              "sha256": e["sha256"], "scheduled_offset_s": round(o, 3), "scheduled_at": utc_iso(t0 + o)}
             for e, o in zip(chosen, offs)]
    plan = {**run, "count": count, "start_after_match_id": start_after_match_id, "hot_bucket": hot_bucket,
            "planned_at": utc_iso(planned_at), "t0": utc_iso(t0), "items": items}
    key = plan_key(prefix, run_id)
    client.put_object(Bucket=cold_bucket, Key=key, Body=json.dumps(plan, indent=1).encode())
    print(f"planned {len(items)} releases from {plan['t0']} over {offs[-1] / 60:.1f} min ({spacing}, "
          f"speed {speed}), matches {items[0]['match_id']}..{items[-1]['match_id']}; s3://{cold_bucket}/{key}")
    return plan


def release_item(client, item: dict, *, hot_bucket: str, now: Callable[[], float] = time.time) -> dict:
    """Release one planned replay and record it in its own log object, written as in_flight before
    the upload and again with the outcome, so a release killed mid-upload still shows up.
    Returns the final row: released, skipped (already hot), or failed."""
    match_id = item["match_id"]
    bucket, key = item["cold_bucket"], row_key(item["cold_prefix"], item["run_id"], match_id)
    row = {"run_id": item["run_id"], "match_id": match_id, "seed_run": item.get("seed_run"),
           "end_time": item["end_time"], "scheduled_offset_s": item["scheduled_offset_s"],
           "scheduled_at": item["scheduled_at"], "started_at": None, "released_at": None, "finished_at": None,
           "bytes_in": 0, "bytes_out": 0, "status": "in_flight", "speed": item["speed"],
           "spacing": item["spacing"], "rate_per_min": item["rate_per_min"]}

    def write() -> None:
        client.put_object(Bucket=bucket, Key=key, Body=json.dumps(row).encode())

    try:
        if exists(client, hot_bucket, hot_key(match_id)):
            row["status"] = "skipped"
        else:
            row["started_at"] = utc_iso(now())
            write()
            row["bytes_in"], row["bytes_out"] = release_object(
                client, bucket, cold_key(item["cold_prefix"], match_id), hot_bucket, hot_key(match_id), item["sha256"])
            row["status"], row["released_at"] = "released", utc_iso(now())
    except Exception as e:  # noqa: BLE001
        row["status"] = "failed"
        row["error"] = redact(f"{type(e).__name__}: {e}")[:MAX_ERROR_CHARS]
    except BaseException:  # Airflow's task timeout or a kill signal
        row["status"], row["finished_at"] = "interrupted", utc_iso(now())
        with contextlib.suppress(Exception):
            write()
        raise
    row["finished_at"] = utc_iso(now())
    write()
    return row


def read_log(client, bucket: str, prefix: str, run_id: str) -> tuple[dict | None, list[dict]]:
    """A restore run's plan and its log rows in schedule order."""
    plan, rows = None, []
    for key in list_keys(client, bucket, log_prefix(prefix, run_id)):
        doc = json.loads(client.get_object(Bucket=bucket, Key=key)["Body"].read())
        if key.endswith("/_plan.json"):
            plan = doc
        else:
            rows.append(doc)
    return plan, sorted(rows, key=lambda r: (r["scheduled_offset_s"], r["match_id"]))
