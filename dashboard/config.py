"""Read and validate the analytics warehouse settings used by the dashboard."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from os import environ


class DashboardConfigurationError(ValueError):
    """The dashboard's warehouse settings are missing or invalid."""


@dataclass(frozen=True)
class WarehouseConfig:
    """Store validated connection settings without exposing the password in repr."""

    host: str
    port: int
    dbname: str
    user: str
    password: str = field(repr=False)

    def __post_init__(self) -> None:
        """Reject incomplete settings before a database connection is attempted."""
        for variable, value in (
            ("WAREHOUSE_HOST", self.host), ("WAREHOUSE_DB", self.dbname),
            ("WAREHOUSE_USER", self.user), ("WAREHOUSE_PASSWORD", self.password),
        ):
            if not isinstance(value, str) or not value.strip():
                raise DashboardConfigurationError(f"Set {variable} before connecting to the warehouse.")
        if type(self.port) is not int or not 1 <= self.port <= 65535:
            raise DashboardConfigurationError("WAREHOUSE_PORT must be an integer between 1 and 65535.")


def load_warehouse_config(settings: Mapping[str, str] | None = None) -> WarehouseConfig:
    """Load WAREHOUSE_* values from the process environment or supplied settings."""
    values = environ if settings is None else settings
    try:
        port = int(values.get("WAREHOUSE_PORT", "5433"))
    except ValueError:
        # Report the setting name rather than echoing an untrusted configuration value.
        raise DashboardConfigurationError(
            "WAREHOUSE_PORT must be an integer between 1 and 65535."
        ) from None

    # Host defaults follow Compose's published port; containers explicitly use warehouse:5432.
    return WarehouseConfig(
        host=values.get("WAREHOUSE_HOST", "localhost").strip(),
        port=port,
        dbname=values.get("WAREHOUSE_DB", "warehouse").strip(),
        user=values.get("WAREHOUSE_USER", "warehouse").strip(),
        # Password whitespace is preserved because it can be part of a valid credential.
        password=values.get("WAREHOUSE_PASSWORD", ""),
    )
