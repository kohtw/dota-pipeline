"""
One-time seed: copy a run of real matches from the collector's Drive stockpile into the cold store
(docs/cold-storage.md). Ingest stays API -> Garage; this only fills the archive that cold_restore
releases from.

Picks --count matches that the collector uploaded (status 'uploaded' in its SQLite state), in
match_seq_num order from --start-seq (default: just after the newest match already in the
manifest, else the oldest upload). Valve numbers matches about in the order they end, so the gaps
between their end times are real, but only between the matches the collector happened to sample
and upload. Every match of one invocation shares a seed_run id in the manifest, and a restore never
mixes seed runs. Matches skipped or failed are listed at the end. Each match's start_time
and duration come from its saved Steam page (seq_{start}.json.gz, the page with the largest start at
or below the match's seq); a match missing from its page is skipped with a warning. Replays are
downloaded in batches with rclone, checked (one complete zstd frame holding a Dota 2 replay),
uploaded unchanged as {prefix}replays/{match_id}.dem.zst, and added to {prefix}manifest.jsonl.

Run from the repo root on the host, with the stack up (Garage S3 on 127.0.0.1:3900):
  S3_ACCESS_KEY=... S3_SECRET_KEY=... uv run --no-project --with boto3 --with zstandard \\
      --with requests python downloader/seed_cold_store.py --count 30 [--dry-run]
S3_ENDPOINT defaults to http://127.0.0.1:3900. The local stack's keys are the x-s3-creds in
docker-compose.yml.
"""

import argparse
import bisect
import datetime as dt
import gzip
import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "dags"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dota import cold  # noqa: E402
from dota.config import BUCKET  # noqa: E402
from dota.errors import PermanentError  # noqa: E402

BATCH = 20  # replays per rclone download, about 750 MB of staging at a time
PAGE = re.compile(r"seq_(\d+)\.json\.gz")

log = logging.getLogger("seed_cold_store")


def parse_lsf(lines) -> dict[str, int | None]:
    """name -> size from `rclone lsf --format ps` output (name;size), or plain names."""
    out = {}
    for line in lines:
        line = line.strip()
        if not line:
            continue
        name, _, rest = line.partition(";")
        size = rest.split(";", 1)[0]
        out[name] = int(size) if size.isdigit() else None
    return out


def page_starts(names) -> list[int]:
    return sorted(int(m.group(1)) for n in names if (m := PAGE.fullmatch(n)))


def page_for(seq: int, starts: list[int]) -> int | None:
    """The page holding seq: the largest start_seq at or below it."""
    i = bisect.bisect_right(starts, seq)
    return starts[i - 1] if i else None


def candidates(rows: list[dict], start_seq: int, exclude=()) -> list[dict]:
    """Uploaded matches from start_seq on, in match_seq_num order, minus those already seeded."""
    exclude = set(exclude)
    return sorted((r for r in rows if r["match_seq_num"] >= start_seq and r["match_id"] not in exclude),
                  key=lambda r: r["match_seq_num"])


def default_start_seq(rows: list[dict], manifest: list[dict]) -> int:
    if manifest:
        return max(e["match_seq_num"] for e in manifest) + 1
    return min(r["match_seq_num"] for r in rows)


def page_times(page: dict) -> dict[int, dict]:
    """match_id -> the fields the manifest needs, from one GetMatchHistoryBySequenceNum page."""
    matches = page.get("result", page).get("matches", [])
    return {m["match_id"]: {"match_seq_num": m["match_seq_num"], "start_time": m.get("start_time"),
                            "duration": m.get("duration")} for m in matches}


def uploaded_rows(db: Path) -> list[dict]:
    with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
        cur = conn.execute("SELECT match_id, match_seq_num FROM matches "
                           "WHERE status = 'uploaded' AND match_seq_num IS NOT NULL ORDER BY match_seq_num")
        return [{"match_id": m, "match_seq_num": s} for m, s in cur]


def rclone_lsf(remote: str, folder: str) -> dict[str, int | None]:
    log.info("listing %s/%s (this takes a minute on Drive)", remote, folder)
    r = subprocess.run(["rclone", "lsf", "--files-only", "--format", "ps", f"{remote}/{folder}"],
                       capture_output=True, text=True)
    if r.returncode:
        sys.exit(f"rclone lsf {folder} failed: {r.stderr.strip()[-500:]}")
    return parse_lsf(r.stdout.splitlines())


def rclone_fetch(remote: str, paths: list[str], dest: Path) -> None:
    """Copy remote-relative paths into dest, keeping their folders."""
    listing = dest / "files-from.txt"
    listing.write_text("".join(p + "\n" for p in paths))
    # without --no-traverse rclone lists the whole source folder first, a minute for steam/
    r = subprocess.run(["rclone", "copy", remote, str(dest), "--files-from", str(listing),
                        "--no-traverse", "--transfers", "4"], capture_output=True, text=True)
    listing.unlink()
    if r.returncode:
        sys.exit(f"rclone copy failed: {r.stderr.strip()[-500:]}")


def resolve(cands: list[dict], count: int, replays: dict, starts: list[int], remote: str,
            staging: Path) -> tuple[list[dict], list[tuple[dict, str]]]:
    """Walk the candidates in order until `count` have a replay on the remote and a Steam
    start_time and duration, fetching only the pages needed. Returns (chosen, [(row, reason)])."""
    chosen, skipped, times, fetched, i = [], [], {}, set(), 0
    while len(chosen) < count and i < len(cands):
        chunk = cands[i:i + count - len(chosen)]
        i += len(chunk)
        usable = []
        for r in chunk:
            if f"{r['match_id']}.dem.zst" not in replays:
                skipped.append((r, "no replay on the remote"))
            elif (start := page_for(r["match_seq_num"], starts)) is None:
                skipped.append((r, "no Steam page at or below its seq"))
            else:
                usable.append((r, start))
        need = sorted({start for _, start in usable} - fetched)
        if need:
            rclone_fetch(remote, [f"steam/seq_{s}.json.gz" for s in need], staging)
            for s in need:
                path = staging / "steam" / f"seq_{s}.json.gz"
                with gzip.open(path) as f:
                    times.update(page_times(json.load(f)))
                path.unlink()
            fetched.update(need)
        for r, start in usable:
            t = times.get(r["match_id"])
            if not t or not t["start_time"] or not t["duration"] or t["duration"] <= 0:
                skipped.append((r, f"not in Steam page seq_{start} with a duration"))
                continue
            chosen.append({"match_id": r["match_id"], "match_seq_num": r["match_seq_num"],
                           "start_time": t["start_time"], "duration": t["duration"],
                           "end_time": t["start_time"] + t["duration"],
                           "remote_bytes": replays[f"{r['match_id']}.dem.zst"]})
    for r, reason in skipped:
        log.warning("skip %s (seq %s): %s", r["match_id"], r["match_seq_num"], reason)
    return chosen, skipped


def print_selection(chosen: list[dict]) -> None:
    print(f"{'match_id':>11} {'seq':>11} {'end_time (UTC)':>20} {'duration':>8} {'gap_s':>6} {'MB':>7}")
    prev = None
    for c in chosen:
        end = cold.utc_iso(c["end_time"])[:19].replace("T", " ")
        gap = "" if prev is None else c["end_time"] - prev
        mb = f"{c['remote_bytes'] / 1e6:.1f}" if c["remote_bytes"] else "?"
        print(f"{c['match_id']:>11} {c['match_seq_num']:>11} {end:>20} {c['duration']:>8} {gap!s:>6} {mb:>7}")
        prev = c["end_time"]
    total = sum(c["remote_bytes"] or 0 for c in chosen)
    print(f"{len(chosen)} matches, {total / 1e9:.2f} GB compressed")


def seed(chosen: list[dict], client, bucket: str, prefix: str, remote: str, staging: Path,
         seed_run: str) -> list[dict]:
    """Download, check, upload and record each chosen match. Returns the ones that failed."""
    failed = []
    for b in range(0, len(chosen), BATCH):
        batch = chosen[b:b + BATCH]
        rclone_fetch(remote, [f"replays/{c['match_id']}.dem.zst" for c in batch], staging)
        entries = []
        for c in batch:
            path = staging / "replays" / f"{c['match_id']}.dem.zst"
            try:
                with open(path, "rb") as f:
                    sha256, n_in, n_out = cold.check_replay(f)
                # one PUT, not a multipart upload: Garage reports a multipart object's composite
                # CRC32 as a whole-object one, so boto3 then rejects every GET of it
                with open(path, "rb") as f:
                    client.put_object(Bucket=bucket, Key=cold.cold_key(prefix, c["match_id"]), Body=f)
            except Exception as e:  # noqa: BLE001
                log.error("match %s not seeded: %s: %s", c["match_id"], type(e).__name__, e)
                failed.append(c)
                continue
            finally:
                path.unlink(missing_ok=True)
            entries.append(cold.manifest_entry(c["match_id"], c["match_seq_num"], c["start_time"],
                                               c["duration"], n_in, sha256, seed_run))
            log.info("seeded %s (%.1f MB, %.1f MB as .dem)", c["match_id"], n_in / 1e6, n_out / 1e6)
        if entries:
            merged = cold.add_to_manifest(client, bucket, prefix, entries)
            log.info("manifest now has %d matches", len(merged))
    return failed


def main(argv=None) -> int:
    import downloader  # the collector's config: DRIVE_DEST and DOWNLOADER_HOME

    cfg = downloader.load_config()
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--count", type=int, default=30)
    ap.add_argument("--start-seq", type=int, help="first match_seq_num to consider")
    ap.add_argument("--remote", default=cfg.drive_dest, help="rclone path holding steam/ and replays/")
    ap.add_argument("--state-db", type=Path, default=cfg.db, help="the collector's SQLite state")
    ap.add_argument("--bucket", default=BUCKET, help="cold store bucket")
    ap.add_argument("--prefix", default=cold.COLD_PREFIX, help="cold store prefix")
    ap.add_argument("--staging", type=Path, help="parent folder for a temporary staging folder")
    ap.add_argument("--keep-staging", action="store_true")
    ap.add_argument("--steam-listing", type=Path, help="saved `rclone lsf` of steam/, instead of listing")
    ap.add_argument("--replay-listing", type=Path, help="saved `rclone lsf --format ps` of replays/")
    ap.add_argument("--dry-run", action="store_true",
                    help="print the selection and sizes; fetches the small Steam pages but no replays and writes nothing")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        prefix = cold.check_prefix(args.bucket, args.prefix, BUCKET)
    except PermanentError as e:
        sys.exit(str(e))

    os.environ.setdefault("S3_ENDPOINT", "http://127.0.0.1:3900")
    if not args.dry_run and not (os.environ.get("S3_ACCESS_KEY") and os.environ.get("S3_SECRET_KEY")):
        sys.exit("set S3_ACCESS_KEY and S3_SECRET_KEY (the local stack's are in docker-compose.yml)")
    from dota import storage

    client = storage.client() if os.environ.get("S3_ACCESS_KEY") else None
    try:
        manifest = cold.read_manifest(client, args.bucket, prefix) if client else []
    except Exception as e:  # noqa: BLE001
        if not args.dry_run:
            raise
        log.warning("could not read the manifest (%s); assuming it is empty", type(e).__name__)
        manifest = []

    rows = uploaded_rows(args.state_db)
    if not rows:
        sys.exit(f"no uploaded matches in {args.state_db}")
    start_seq = args.start_seq if args.start_seq is not None else default_start_seq(rows, manifest)
    cands = candidates(rows, start_seq, {e["match_id"] for e in manifest})
    log.info("%d uploaded matches from seq %d not yet seeded", len(cands), start_seq)

    def listing(path, folder):
        return parse_lsf(path.read_text().splitlines()) if path else rclone_lsf(args.remote, folder)

    replays = listing(args.replay_listing, "replays")
    starts = page_starts(listing(args.steam_listing, "steam"))

    if args.staging:
        args.staging.mkdir(parents=True, exist_ok=True)
    # always a fresh folder of our own, so removing it never touches anything else
    staging = Path(tempfile.mkdtemp(prefix="cold-seed-", dir=args.staging))
    try:
        chosen, skipped = resolve(cands, args.count, replays, starts, args.remote, staging)
        print_selection(chosen)
        if len(chosen) < args.count:
            log.warning("only %d of %d matches usable", len(chosen), args.count)
        if args.dry_run or not chosen:
            return 0
        seed_run = "seed-" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        failed = seed(chosen, client, args.bucket, prefix, args.remote, staging, seed_run)
        print(f"seeded {len(chosen) - len(failed)} of {len(chosen)} into s3://{args.bucket}/{prefix} "
              f"as {seed_run}")
        # later default runs start after the newest seeded seq, so these stay out unless asked for
        holes = sorted([(r["match_seq_num"], r["match_id"], reason) for r, reason in skipped]
                       + [(c["match_seq_num"], c["match_id"], "failed") for c in failed])
        for seq, match_id, reason in holes:
            print(f"  not seeded: {match_id} (seq {seq}): {reason}")
        return 1 if failed else 0
    finally:
        if args.keep_staging:
            print(f"staging kept at {staging}")
        else:
            shutil.rmtree(staging, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
