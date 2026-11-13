"""Error types and helpers shared by the HTTP clients and the alert callback."""

import re

try:
    from airflow.sdk.exceptions import AirflowFailException as _Base
except ImportError:  # host tests run without Airflow
    _Base = Exception

_KEY = re.compile(r"key=[^&\"'\s]*")


class PermanentError(_Base):
    """A failure that retrying cannot fix, such as HTTP 4xx. Airflow fails the task without retrying."""


def redact(text: str) -> str:
    """Replace the value of any key= query parameter in text."""
    return _KEY.sub("key=<redacted>", text)


def is_transient_status(status: int) -> bool:
    """True for HTTP statuses worth retrying: 429 and 5xx."""
    return status == 429 or 500 <= status <= 599
