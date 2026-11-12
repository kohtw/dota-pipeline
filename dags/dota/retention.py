"""Raw `.dem` retention. Modes: keep (default), delete. `archive` is documented but not built:
Airflow has no rclone or Drive credentials, a copy inside Garage frees nothing, and the
downloader already keeps replays/{id}.dem.zst on Drive."""

from collections.abc import Callable

from dota import clean_contract, warehouse


def dem_key(match_id: int) -> str:
    return f"replays/{match_id}.dem"


def eligible(match_id: int, read: Callable[[str], bytes],
             list_keys: Callable[[str], list[str]], exists: Callable[[str], bool]) -> bool:
    """True only when the match is loaded (its _loaded marker exists) and its clean output
    passes the clean contract. The contract check runs in another DAG and leaves no record,
    so it is re-checked here."""
    if not exists(warehouse.loaded_marker_key(match_id)):
        return False
    try:
        clean_contract.check_parsed(match_id, read, list_keys)
    except clean_contract.CleanContractError:
        return False
    return True


def apply_retention(match_id: int, mode: str, *, exists: Callable[[str], bool],
                    delete_key: Callable[[str], None]) -> str:
    """Applies the retention mode to one match's .dem. Returns "kept", "deleted" or "absent"
    (no .dem in Garage). The caller must have checked `eligible` first."""
    mode = mode.strip().lower()
    if mode == "archive":
        raise NotImplementedError("dem_retention=archive is not built")
    if mode not in ("keep", "delete"):
        raise ValueError(f"unknown dem_retention mode: {mode!r} (expected keep or delete)")
    if mode == "keep":
        return "kept"
    key = dem_key(match_id)
    if not exists(key):
        return "absent"
    delete_key(key)
    return "deleted"
