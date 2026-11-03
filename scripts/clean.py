"""
Clean one match: raw/parsed/{match_id}/* -> clean/{table}/match_id={match_id}/part.parquet

Stub: the clean tables aren't agreed yet. See docs/data-contract.md for the raw inputs.
"""

import argparse

DEFAULT_MATCH = 9024732472


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--match", type=int, default=DEFAULT_MATCH)
    ap.parse_args()
    raise NotImplementedError("clean tables not defined yet")


if __name__ == "__main__":
    main()
