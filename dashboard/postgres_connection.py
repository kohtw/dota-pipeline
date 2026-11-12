"""Open short-lived, read-only connections to the dashboard's Postgres warehouse."""

from contextlib import contextmanager
from typing import TYPE_CHECKING, Iterator

from config import WarehouseConfig

if TYPE_CHECKING:
    from psycopg2.extensions import connection as PostgresConnection


CONNECT_TIMEOUT_SECONDS = 5
STATEMENT_TIMEOUT_MS = 15_000


class DashboardDatabaseError(Exception):
    """The configured dashboard database is unavailable or incomplete."""


@contextmanager
def open_postgres_database(config: WarehouseConfig) -> Iterator["PostgresConnection"]:
    """Open a read-only transaction, translate driver errors, and always close it."""
    try:
        import psycopg2
    except ImportError as error:
        raise DashboardDatabaseError(
            "Install the dashboard dependencies with `python -m pip install -r dashboard/requirements.txt`."
        ) from error

    connection = None
    try:
        connection = psycopg2.connect(
            host=config.host, port=config.port, dbname=config.dbname,
            user=config.user, password=config.password,
            connect_timeout=CONNECT_TIMEOUT_SECONDS,
            options=f"-c statement_timeout={STATEMENT_TIMEOUT_MS}",
            application_name="dota-dashboard",
        )
        # Read-only mode applies to the first transaction, before any loader runs SQL.
        connection.set_session(readonly=True, autocommit=False)
        with connection:
            yield connection
    except (psycopg2.errors.UndefinedTable, psycopg2.errors.InvalidSchemaName,
            psycopg2.errors.UndefinedColumn) as error:
        raise DashboardDatabaseError(
            "The warehouse dashboard marts are missing or out of date. Run the pipeline's dbt build."
        ) from error
    except psycopg2.errors.InsufficientPrivilege as error:
        raise DashboardDatabaseError(
            "The configured warehouse account does not have permission to read the dashboard marts."
        ) from error
    except psycopg2.errors.QueryCanceled as error:
        raise DashboardDatabaseError(
            "The warehouse query was cancelled or exceeded its time limit. Try again."
        ) from error
    except (psycopg2.errors.InvalidPassword, psycopg2.errors.InvalidAuthorizationSpecification) as error:
        raise DashboardDatabaseError(
            "Postgres rejected the warehouse login. Check WAREHOUSE_USER and WAREHOUSE_PASSWORD."
        ) from error
    except psycopg2.OperationalError as error:
        # Connect-time failures may omit SQLSTATE, so the fallback also covers rejected credentials.
        # Driver messages can contain connection details; the UI receives a fixed message.
        raise DashboardDatabaseError(
            "Could not connect to or communicate with Postgres. Check WAREHOUSE_* settings "
            "including the login credentials, and that the warehouse is running."
        ) from error
    except psycopg2.Error as error:
        raise DashboardDatabaseError("Could not read the Postgres dashboard marts.") from error
    finally:
        # Psycopg's transaction context commits or rolls back, but does not close the connection.
        if connection is not None:
            connection.close()
