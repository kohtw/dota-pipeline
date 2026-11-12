from dota import attempts

RUN_ID = "manual__2026-11-12T10:15:00.123456+00:00"


def test_max_attempts():
    assert attempts.MAX_ATTEMPTS == 3


def test_failure_key_is_stable_and_key_safe_for_run_ids_with_colon_and_plus():
    key = attempts.failure_key("parsed", 7, RUN_ID)
    assert key == attempts.failure_key("parsed", 7, RUN_ID)
    assert key.startswith("parsed/_failed/match_id=7/")
    name = key.removeprefix("parsed/_failed/match_id=7/")
    assert name and ":" not in name and "+" not in name and "/" not in name


def test_failure_key_differs_per_run_and_stage():
    other_run = RUN_ID.replace("10:15", "11:15")
    assert attempts.failure_key("parsed", 7, RUN_ID) != attempts.failure_key("parsed", 7, other_run)
    assert attempts.failure_key("clean", 7, RUN_ID).startswith("clean/_failed/match_id=7/")


def test_exhausted_counts_distinct_keys_per_match():
    runs = [f"scheduled__2026-11-12T0{h}:00:00+00:00" for h in range(3)]
    keys = [attempts.failure_key("parsed", 1, r) for r in runs]
    keys += [keys[0], keys[1]]  # repeated listing of the same key is one attempt
    keys += [attempts.failure_key("parsed", 2, r) for r in runs[:2]]
    assert attempts.exhausted(keys, "parsed") == {1}


def test_exhausted_ignores_other_stages():
    runs = [f"scheduled__2026-11-12T0{h}:00:00+00:00" for h in range(3)]
    keys = [attempts.failure_key("clean", 1, r) for r in runs]
    assert attempts.exhausted(keys, "parsed") == set()
    assert attempts.exhausted(keys, "clean") == {1}
