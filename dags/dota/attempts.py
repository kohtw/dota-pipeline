"""Counts failed sweep runs per match, so a replay that crashes the parser or the clean job every
time stops being picked up after MAX_ATTEMPTS runs, while a one-off crash still retries next run.

Layout: {stage}/_failed/match_id={id}/{run_id}, one empty object per failed run (stage is parsed
or clean). Deleting a match's keys under that prefix makes the sweep retry it."""

import re

MAX_ATTEMPTS = 3
FAILED = "_failed"
# 137 is a kill for memory, which Airflow retries; clean's 2 is a rejection, quarantined by _REJECTED
NOT_CRASHES = {"parsed": {0, 137}, "clean": {0, 2, 137}}


def counts_as_crash(exit_code: int | None, stage: str) -> bool:
    """True when the stage's container ran and failed for a reason of its own. No exit code means
    it never ran (missing image, docker-proxy down), which is not the match's fault."""
    return exit_code is not None and exit_code not in NOT_CRASHES[stage]


def failure_key(stage: str, match_id: int, run_id: str) -> str:
    """The failure marker of one match in one run. There is one key per run id, so a failure
    retried within the same run counts as one attempt."""
    safe_run_id = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)  # run ids hold ':' and '+'
    return f"{stage}/{FAILED}/match_id={match_id}/{safe_run_id}"


def exhausted(failed_keys: list[str], stage: str) -> set[int]:
    """Match ids with at least MAX_ATTEMPTS distinct failure keys in `stage`."""
    runs: dict[int, set[str]] = {}
    for key in failed_keys:
        m = re.fullmatch(rf"{re.escape(stage)}/{FAILED}/match_id=(\d+)/([^/]+)", key)
        if m:
            runs.setdefault(int(m.group(1)), set()).add(m.group(2))
    return {match_id for match_id, names in runs.items() if len(names) >= MAX_ATTEMPTS}
