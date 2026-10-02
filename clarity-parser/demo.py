"""
Demo: download a Dota 2 replay and parse it with clarity, timing each stage.

Stages:
  1. look up replay URL (OpenDota /api/matches/{id})   -- skipped if the file exists
  2. download from Valve's replay CDN
  3. decompress (Valve now serves zstd despite the .bz2 extension; detect by magic bytes)
  4. run clarity example processors, each a separate JVM, output -> out/<name>.txt

Usage:
  python demo.py                       # default match, info+matchend+combatlog
  python demo.py --match 9024842732
  python demo.py --examples info combatlog position   # position emits ~1 GB of text

Requires: Java 17, `pip install zstandard requests`, and `classpath.txt`
(generated once with: ./gradlew -q -I printcp.init.gradle printCp).
"""

import argparse
import bz2
import os
import shutil
import subprocess
import time

import requests
import zstandard

HERE = os.path.dirname(os.path.abspath(__file__))
REPLAY_DIR = os.path.join(HERE, "replays")
OUT_DIR = os.path.join(HERE, "out")

ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
BZ2_MAGIC = b"BZh"


def timed(label):
    """Context manager printing how long a stage took."""
    class T:
        def __enter__(self):
            self.t = time.perf_counter()
            return self

        def __exit__(self, *exc):
            self.elapsed = time.perf_counter() - self.t
            print(f"  [{label}] {self.elapsed:.2f}s")
    return T()


def mb(path):
    return os.path.getsize(path) / 1e6


def fetch_replay(match_id):
    os.makedirs(REPLAY_DIR, exist_ok=True)
    compressed = os.path.join(REPLAY_DIR, f"{match_id}.dem.bz2")
    if os.path.exists(compressed):
        print(f"  using cached {compressed} ({mb(compressed):.1f} MB)")
        return compressed

    with timed("lookup replay url"):
        d = requests.get(f"https://api.opendota.com/api/matches/{match_id}", timeout=30).json()
        url = d.get("replay_url")
    if not url:
        raise SystemExit(f"no replay_url for {match_id} (expired or not yet available)")
    print(f"  {url}")

    with timed("download"):
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(compressed, "wb") as f:
                shutil.copyfileobj(r.raw, f, length=1 << 20)
    print(f"  downloaded {mb(compressed):.1f} MB")
    return compressed


def decompress(compressed):
    dem = compressed.removesuffix(".bz2")
    if os.path.exists(dem):
        print(f"  using cached {dem} ({mb(dem):.1f} MB)")
        return dem

    with open(compressed, "rb") as f:
        magic = f.read(4)
    with timed("decompress"):
        with open(compressed, "rb") as src, open(dem, "wb") as dst:
            if magic.startswith(ZSTD_MAGIC):
                zstandard.ZstdDecompressor().copy_stream(src, dst)
            elif magic.startswith(BZ2_MAGIC):
                shutil.copyfileobj(bz2.BZ2File(src), dst, length=1 << 20)
            else:
                raise SystemExit(f"unknown compression, magic={magic!r}")
    kind = "zstd" if magic.startswith(ZSTD_MAGIC) else "bzip2"
    print(f"  {kind}: {mb(compressed):.1f} MB -> {mb(dem):.1f} MB")
    return dem


def run_example(name, dem, cp):
    os.makedirs(OUT_DIR, exist_ok=True)
    out = os.path.join(OUT_DIR, f"{name}.txt")
    cmd = ["java", "-Xmx4g", "-cp", cp, f"skadistats.clarity.examples.{name}.Main", dem]
    t = time.perf_counter()
    with open(out, "w", encoding="utf-8") as f:
        rc = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT).returncode
    elapsed = time.perf_counter() - t
    with open(out, encoding="utf-8", errors="replace") as f:
        lines = sum(1 for _ in f)
    return rc, elapsed, lines, mb(out), out


def preview(path, n=8):
    """Print the first n lines plus a few from the middle (where gameplay happens)."""
    with open(path, encoding="utf-8", errors="replace") as f:
        lines = f.readlines()
    print("    " + "    ".join(lines[:n]).rstrip())
    if len(lines) > 3 * n:
        mid = len(lines) // 2
        print("    ...")
        print("    " + "    ".join(lines[mid:mid + n]).rstrip())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--match", type=int, default=9024732472)
    ap.add_argument("--examples", nargs="+", default=["info", "matchend", "combatlog"])
    ap.add_argument("--no-preview", action="store_true")
    args = ap.parse_args()

    with open(os.path.join(HERE, "classpath.txt")) as f:
        cp = f.read().strip()

    print(f"== match {args.match}")
    dem = decompress(fetch_replay(args.match))

    results = []
    for name in args.examples:
        print(f"\n== clarity: {name}")
        rc, elapsed, lines, size, out = run_example(name, dem, cp)
        results.append((name, rc, elapsed, lines, size))
        print(f"  rc={rc}  {elapsed:.2f}s  {lines:,} lines  {size:.1f} MB -> {out}")
        if not args.no_preview:
            preview(out)

    print(f"\n== summary (replay {mb(dem):.0f} MB uncompressed)")
    print(f"  {'example':<12}{'rc':>3}{'seconds':>10}{'lines':>12}{'MB out':>9}")
    for name, rc, elapsed, lines, size in results:
        print(f"  {name:<12}{rc:>3}{elapsed:>10.2f}{lines:>12,}{size:>9.1f}")


if __name__ == "__main__":
    main()
