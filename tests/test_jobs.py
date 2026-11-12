import pytest

from dota.jobs import should_retry


@pytest.mark.parametrize("state, expected", [
    ({"OOMKilled": True, "ExitCode": 137}, True),
    ({"OOMKilled": False, "ExitCode": 137}, True),
    ({"ExitCode": 2}, False),
    ({"ExitCode": 1}, False),
    ({}, False),
])
def test_should_retry_only_when_killed_for_memory(state, expected):
    assert should_retry(state) is expected
