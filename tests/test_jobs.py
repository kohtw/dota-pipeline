"""should_retry, and JobOperator.execute against stubs of the Docker provider and a fake Docker client."""

import importlib
import logging
import sys
import types

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


class ContainerFailed(Exception):
    pass


class FailNoRetry(Exception):
    pass


class DockerAPIError(Exception):
    pass


class StubDockerOperator:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.container = None
        self.cli = None
        self.log = logging.getLogger("test_jobs")
        self.run = None

    def execute(self, context):
        return self.run(self)


class FakeCli:
    def __init__(self, state=None, inspect_error=None, remove_error=None):
        self.state, self.inspect_error, self.remove_error = state or {}, inspect_error, remove_error
        self.removed = []

    def inspect_container(self, container_id):
        if self.inspect_error:
            raise self.inspect_error
        return {"State": self.state}

    def remove_container(self, container_id):
        self.removed.append(container_id)
        if self.remove_error:
            raise self.remove_error


class FakeTI:
    def __init__(self):
        self.pushed = {}

    def xcom_push(self, key, value):
        self.pushed[key] = value


def _module(name, **attrs):
    module = types.ModuleType(name)
    module.__dict__.update(attrs)
    return module


@pytest.fixture
def jobs(monkeypatch):
    stubs = {
        "airflow": _module("airflow"),
        "airflow.providers": _module("airflow.providers"),
        "airflow.providers.docker": _module("airflow.providers.docker"),
        "airflow.providers.docker.exceptions": _module(
            "airflow.providers.docker.exceptions", DockerContainerFailedException=ContainerFailed),
        "airflow.providers.docker.operators": _module("airflow.providers.docker.operators"),
        "airflow.providers.docker.operators.docker": _module(
            "airflow.providers.docker.operators.docker", DockerOperator=StubDockerOperator),
        "airflow.sdk": _module("airflow.sdk"),
        "airflow.sdk.exceptions": _module("airflow.sdk.exceptions", AirflowFailException=FailNoRetry),
    }
    for name, module in stubs.items():
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.delitem(sys.modules, "dota.jobs", raising=False)
    yield importlib.import_module("dota.jobs")
    sys.modules.pop("dota.jobs", None)


def operator(jobs, cli, run):
    op = jobs.JobOperator(task_id="parse_replay", image="dota-parser")
    op.cli, op.run = cli, run
    return op


def exits(op):
    op.container = {"Id": "c1"}
    raise ContainerFailed("Docker container failed")


def succeeds(op):
    op.container = {"Id": "c1"}
    return "done"


def test_a_kill_for_memory_reraises_the_original_exception(jobs):
    cli, ti = FakeCli({"OOMKilled": True, "ExitCode": 137}), FakeTI()
    with pytest.raises(ContainerFailed):
        operator(jobs, cli, exits).execute({"ti": ti})
    assert ti.pushed == {jobs.EXIT_CODE_KEY: 137}
    assert cli.removed == ["c1"]


def test_exit_1_fails_without_retry_and_pushes_the_exit_code(jobs):
    cli, ti = FakeCli({"OOMKilled": False, "ExitCode": 1}), FakeTI()
    with pytest.raises(FailNoRetry, match="exited with code 1"):
        operator(jobs, cli, exits).execute({"ti": ti})
    assert ti.pushed == {jobs.EXIT_CODE_KEY: 1}
    assert cli.removed == ["c1"]


def test_an_inspect_failure_is_not_retried_and_pushes_no_exit_code(jobs):
    cli, ti = FakeCli(inspect_error=DockerAPIError("proxy down")), FakeTI()
    with pytest.raises(FailNoRetry, match="unknown"):
        operator(jobs, cli, exits).execute({"ti": ti})
    assert ti.pushed == {}
    assert cli.removed == ["c1"]


def test_success_returns_the_result_and_removes_the_container(jobs):
    cli, ti = FakeCli(), FakeTI()
    assert operator(jobs, cli, succeeds).execute({"ti": ti}) == "done"
    assert ti.pushed == {}
    assert cli.removed == ["c1"]


def test_a_container_that_never_started_pushes_nothing_and_removes_nothing(jobs):
    def no_image(op):
        raise DockerAPIError("image not found")

    cli, ti = FakeCli(), FakeTI()
    with pytest.raises(DockerAPIError):
        operator(jobs, cli, no_image).execute({"ti": ti})
    assert ti.pushed == {}
    assert cli.removed == []


@pytest.mark.parametrize("remove_error", [DockerAPIError("gone"), ConnectionError("proxy down")])
def test_a_failed_removal_does_not_change_a_success(jobs, remove_error):
    cli = FakeCli(remove_error=remove_error)
    assert operator(jobs, cli, succeeds).execute({"ti": FakeTI()}) == "done"


@pytest.mark.parametrize("remove_error", [DockerAPIError("gone"), ConnectionError("proxy down")])
def test_a_failed_removal_does_not_change_a_failure(jobs, remove_error):
    cli = FakeCli({"ExitCode": 1}, remove_error=remove_error)
    with pytest.raises(FailNoRetry):
        operator(jobs, cli, exits).execute({"ti": FakeTI()})


def test_auto_remove_is_rejected(jobs):
    with pytest.raises(TypeError):
        jobs.JobOperator(task_id="parse_replay", image="dota-parser", auto_remove="success")
