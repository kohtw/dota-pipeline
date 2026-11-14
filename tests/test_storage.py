import sys
from unittest.mock import MagicMock, patch

import pytest
import requests

# boto3/botocore are Airflow-image dependencies; stub them when absent so host runs can import storage
for _name in ("boto3", "botocore", "botocore.config", "botocore.exceptions"):
    try:
        __import__(_name)
    except ModuleNotFoundError:
        sys.modules[_name] = MagicMock()

from dota import storage  # noqa: E402
from dota.config import BUCKET  # noqa: E402
from dota.errors import PermanentError  # noqa: E402


def test_delete_key_deletes_object_from_bucket():
    fake = MagicMock()
    with patch.object(storage, "client", return_value=fake):
        storage.delete_key("replays/1.dem")
    fake.delete_object.assert_called_once_with(Bucket=BUCKET, Key="replays/1.dem")


class Resp:
    def __init__(self, status):
        self.status_code, self.ok = status, status < 400

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_upload_replay_connection_error_is_redacted_and_not_chained(monkeypatch):
    def get(*a, **k):
        raise requests.ConnectionError("Max retries exceeded with url: /570/1_2.dem.bz2?key=SECRET123")

    monkeypatch.setattr(storage.requests, "get", get)
    with pytest.raises(RuntimeError) as err:
        storage.upload_replay("http://replay1.valve.net/570/1_2.dem.bz2", "replays/1.dem")
    assert "SECRET123" not in str(err.value)
    assert err.value.__cause__ is None
    assert err.value.__suppress_context__


def test_upload_replay_404_is_permanent(monkeypatch):
    monkeypatch.setattr(storage.requests, "get", lambda *a, **k: Resp(404))
    with pytest.raises(PermanentError):
        storage.upload_replay("http://replay1.valve.net/570/1_2.dem.bz2", "replays/1.dem")


def test_upload_replay_503_is_retryable(monkeypatch):
    monkeypatch.setattr(storage.requests, "get", lambda *a, **k: Resp(503))
    with pytest.raises(RuntimeError) as err:
        storage.upload_replay("http://replay1.valve.net/570/1_2.dem.bz2", "replays/1.dem")
    assert not isinstance(err.value, PermanentError)
