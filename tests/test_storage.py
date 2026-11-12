import sys
from unittest.mock import MagicMock, patch

# boto3/botocore are Airflow-image dependencies; stub them when absent so host runs can import storage
for _name in ("boto3", "botocore", "botocore.config", "botocore.exceptions"):
    try:
        __import__(_name)
    except ModuleNotFoundError:
        sys.modules[_name] = MagicMock()

from dota import storage  # noqa: E402
from dota.config import BUCKET  # noqa: E402


def test_delete_key_deletes_object_from_bucket():
    fake = MagicMock()
    with patch.object(storage, "client", return_value=fake):
        storage.delete_key("replays/1.dem")
    fake.delete_object.assert_called_once_with(Bucket=BUCKET, Key="replays/1.dem")
