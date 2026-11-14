import gzip
import json
import sqlite3
import sys
from pathlib import Path

import pytest
import zstandard

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "downloader"))

import seed_cold_store as seed  # noqa: E402
from dota import cold  # noqa: E402
from dota.storage import DEM_MAGIC  # noqa: E402


def test_parse_lsf_reads_names_and_sizes():
    assert seed.parse_lsf(["1.dem.zst;37000", "2.dem.zst;5;2026-11-07 18:57:06", "seq_3.json.gz", ""]) == {
        "1.dem.zst": 37000, "2.dem.zst": 5, "seq_3.json.gz": None}


def test_page_for_is_the_largest_start_at_or_below_the_seq():
    starts = seed.page_starts(["seq_300.json.gz", "seq_100.json.gz", "callback_9.json.gz", "seq_200.json.gz"])
    assert starts == [100, 200, 300]
    assert seed.page_for(100, starts) == 100
    assert seed.page_for(199, starts) == 100
    assert seed.page_for(250, starts) == 200
    assert seed.page_for(10_000, starts) == 300
    assert seed.page_for(99, starts) is None


def rows(*seqs):
    return [{"match_id": 1000 + s, "match_seq_num": s, "start_time": 0, "bytes": 1} for s in seqs]


def test_candidates_are_consecutive_uploads_from_the_start_seq():
    got = seed.candidates(rows(50, 10, 30, 20, 40), start_seq=20)
    assert [r["match_seq_num"] for r in got] == [20, 30, 40, 50]
    got = seed.candidates(rows(10, 20, 30, 40), start_seq=15, exclude={1030})
    assert [r["match_seq_num"] for r in got] == [20, 40]


def test_default_start_seq_follows_the_manifest():
    assert seed.default_start_seq(rows(10, 20), []) == 10
    assert seed.default_start_seq(rows(10, 20), [{"match_seq_num": 15}, {"match_seq_num": 12}]) == 16


def test_page_times_reads_a_steam_page():
    page = {"result": {"status": 1, "matches": [
        {"match_id": 7, "match_seq_num": 70, "start_time": 1000, "duration": 2400, "players": []}]}}
    assert seed.page_times(page) == {7: {"match_seq_num": 70, "start_time": 1000, "duration": 2400}}


def test_uploaded_rows_reads_only_uploaded_matches(tmp_path):
    db = tmp_path / "state.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE matches (match_id INTEGER PRIMARY KEY, match_seq_num INTEGER, "
                     "start_time INTEGER, status TEXT NOT NULL, bytes INTEGER, updated REAL NOT NULL)")
        conn.executemany("INSERT INTO matches VALUES (?, ?, ?, ?, ?, 0)",
                         [(1, 30, 0, "uploaded", 5), (2, 10, 0, "uploaded", 6), (3, 20, 0, "failed", None)])
    assert [r["match_id"] for r in seed.uploaded_rows(db)] == [2, 1]


class FakeRemote:
    """Stands in for rclone: serves steam pages and replays from memory into the staging folder."""

    def __init__(self, files: dict[str, bytes]):
        self.files, self.fetches = files, []

    def fetch(self, remote, paths, dest):
        self.fetches.append(list(paths))
        for p in paths:
            (dest / p).parent.mkdir(parents=True, exist_ok=True)
            (dest / p).write_bytes(self.files[p])


def steam_page(*matches):
    return gzip.compress(json.dumps({"result": {"matches": [
        {"match_id": m, "match_seq_num": s, "start_time": t, "duration": d} for m, s, t, d in matches]}}).encode())


def test_resolve_skips_matches_without_a_replay_or_a_duration_and_fills_up(tmp_path, monkeypatch, caplog):
    remote = FakeRemote({
        "steam/seq_100.json.gz": steam_page((1, 100, 1000, 2000), (3, 102, 1100, 1900)),  # 2 missing from page
        "steam/seq_200.json.gz": steam_page((4, 200, 1300, 0), (5, 201, 1400, 1700)),  # 4 has no duration
    })
    monkeypatch.setattr(seed, "rclone_fetch", remote.fetch)
    cands = [{"match_id": m, "match_seq_num": s} for m, s in [(1, 100), (2, 101), (3, 102), (4, 200), (5, 201), (6, 202)]]
    replays = {f"{m}.dem.zst": 10 * m for m in (1, 2, 3, 4, 5)}  # 6 has no replay
    chosen = seed.resolve(cands, 3, replays, [100, 200], "remote:", tmp_path)
    assert [c["match_id"] for c in chosen] == [1, 3, 5]
    assert chosen[0] == {"match_id": 1, "match_seq_num": 100, "start_time": 1000, "duration": 2000,
                         "end_time": 3000, "remote_bytes": 10}
    assert remote.fetches == [["steam/seq_100.json.gz"], ["steam/seq_200.json.gz"]]  # each page once
    assert "skip 2" in caplog.text and "skip 4" in caplog.text
    assert list(tmp_path.rglob("*.gz")) == []


def test_seed_uploads_checked_replays_unchanged_and_merges_the_manifest(tmp_path, monkeypatch):
    from test_cold import COLD, FakeS3

    good = zstandard.ZstdCompressor().compress(DEM_MAGIC + b"x" * 10_000)
    bad = zstandard.ZstdCompressor().compress(b"<html></html>")
    remote = FakeRemote({"replays/1.dem.zst": good, "replays/2.dem.zst": bad})
    monkeypatch.setattr(seed, "rclone_fetch", remote.fetch)
    s3 = FakeS3()
    s3.upload_file = lambda path, bucket, key: s3.objects.update({(bucket, key): Path(path).read_bytes()})
    old = cold.manifest_entry(9, 9, 0, 10, 1, "old")
    s3.objects[(COLD, "cold/manifest.jsonl")] = cold.dump_manifest([old]).encode()

    chosen = [{"match_id": m, "match_seq_num": m, "start_time": 100, "duration": 50, "end_time": 150}
              for m in (1, 2)]
    assert seed.seed(chosen, s3, COLD, "cold/", "remote:", tmp_path) == 1
    assert s3.objects[(COLD, "cold/replays/1.dem.zst")] == good
    assert (COLD, "cold/replays/2.dem.zst") not in s3.objects
    manifest = cold.parse_manifest(s3.objects[(COLD, "cold/manifest.jsonl")].decode())
    assert [e["match_id"] for e in manifest] == [9, 1]
    assert manifest[1]["bytes"] == len(good) and manifest[1]["end_time"] == 150
    assert list(tmp_path.rglob("*.zst")) == []  # staging emptied as it goes
