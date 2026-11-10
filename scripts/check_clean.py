"""
Check one match's clean output against the contract, without Airflow.

Same check the DAG's clean task runs (dota.clean_contract), against the Garage bucket named by
S3_ENDPOINT / S3_ACCESS_KEY / S3_SECRET_KEY. Exits non-zero with the reason if the output is bad.

  python3 scripts/check_clean.py <match_id>
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dags"))  # no Airflow install needed


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validate one match's clean output against the contract.")
    p.add_argument("match_id", type=int, help="numeric Dota match id")
    return p.parse_args(argv)


def format_counts(counts: dict[str, int]) -> str:
    width = max((len(n) for n in counts), default=0)
    return "\n".join(f"  {name:<{width}}  {rows:>8}" for name, rows in counts.items())


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    from dota import clean_contract, storage  # lazy: --help works without boto3/S3 env

    try:
        counts = clean_contract.check_parsed(args.match_id, storage.read, storage.list_keys)
    except clean_contract.CleanContractError as e:
        print(f"FAIL match {args.match_id}: {e}", file=sys.stderr)
        return 1
    print(f"OK match {args.match_id}: rows per clean table")
    print(format_counts(counts))
    return 0


if __name__ == "__main__":
    sys.exit(main())
