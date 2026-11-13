"""Offline checks for dashboard warehouse settings and connection lifecycle."""

from importlib import import_module
import builtins
from pathlib import Path
from unittest.mock import MagicMock

import psycopg2
import pytest


@pytest.fixture
def warehouse_modules(monkeypatch):
    """Import dashboard modules using the same module path as the Streamlit entrypoint."""
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "dashboard"))
    return import_module("config"), import_module("postgres_connection")


def test_config_defaults_match_the_host_warehouse_port(warehouse_modules):
    config, _ = warehouse_modules
    settings = config.load_warehouse_config({"WAREHOUSE_PASSWORD": "local-secret"})

    assert (settings.host, settings.port, settings.dbname, settings.user) == (
        "localhost", 5433, "warehouse", "warehouse",
    )
    assert "local-secret" not in repr(settings)
    assert "local-secret" not in str(settings)


def test_config_reads_environment_and_preserves_password_whitespace(warehouse_modules, monkeypatch):
    config, _ = warehouse_modules
    for key, value in {
        "WAREHOUSE_HOST": " warehouse ", "WAREHOUSE_PORT": "5432",
        "WAREHOUSE_DB": " analytics ", "WAREHOUSE_USER": " reader ",
        "WAREHOUSE_PASSWORD": " secret with spaces ",
    }.items():
        monkeypatch.setenv(key, value)

    settings = config.load_warehouse_config()

    assert (settings.host, settings.port, settings.dbname, settings.user) == (
        "warehouse", 5432, "analytics", "reader",
    )
    assert settings.password == " secret with spaces "


@pytest.mark.parametrize("variable", [
    "WAREHOUSE_HOST", "WAREHOUSE_DB", "WAREHOUSE_USER", "WAREHOUSE_PASSWORD",
])
def test_config_rejects_blank_required_settings(warehouse_modules, variable):
    config, _ = warehouse_modules
    settings = {"WAREHOUSE_PASSWORD": "local-secret", variable: "   "}

    with pytest.raises(config.DashboardConfigurationError, match=variable):
        config.load_warehouse_config(settings)


def test_config_requires_an_explicit_password(warehouse_modules):
    config, _ = warehouse_modules
    with pytest.raises(config.DashboardConfigurationError, match="WAREHOUSE_PASSWORD"):
        config.load_warehouse_config({})


@pytest.mark.parametrize("port", ["", "not-a-port", "5433.5", "0", "-1", "65536"])
def test_config_rejects_invalid_ports_without_echoing_values(warehouse_modules, port):
    config, _ = warehouse_modules
    with pytest.raises(config.DashboardConfigurationError) as caught:
        config.load_warehouse_config({"WAREHOUSE_PASSWORD": "local-secret", "WAREHOUSE_PORT": port})
    assert str(caught.value) == "WAREHOUSE_PORT must be an integer between 1 and 65535."


@pytest.fixture
def connection_setup(warehouse_modules, monkeypatch):
    """Keep connection tests independent of Docker and real credentials."""
    config, database = warehouse_modules
    settings = config.load_warehouse_config({"WAREHOUSE_PASSWORD": "private-test-password"})
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connect = MagicMock(return_value=connection)
    monkeypatch.setattr(psycopg2, "connect", connect)
    return database, settings, connection, connect


def test_connection_sets_limits_and_closes_after_success(connection_setup):
    database, settings, connection, connect = connection_setup

    with database.open_postgres_database(settings) as opened:
        assert opened is connection
        connection.set_session.assert_called_once_with(readonly=True, autocommit=False)
        connection.close.assert_not_called()

    connect.assert_called_once_with(
        host="localhost", port=5433, dbname="warehouse", user="warehouse",
        password="private-test-password", connect_timeout=5,
        options="-c statement_timeout=15000", application_name="dota-dashboard",
    )
    connection.__exit__.assert_called_once_with(None, None, None)
    connection.close.assert_called_once_with()


@pytest.mark.parametrize(("error_type", "message"), [
    (psycopg2.errors.UndefinedTable, "missing or out of date"),
    (psycopg2.errors.UndefinedColumn, "missing or out of date"),
    (psycopg2.errors.InvalidSchemaName, "missing or out of date"),
    (psycopg2.errors.InsufficientPrivilege, "permission"),
    (psycopg2.errors.QueryCanceled, "time limit"),
    (psycopg2.errors.InvalidPassword, "rejected the warehouse login"),
    (psycopg2.errors.InvalidAuthorizationSpecification, "rejected the warehouse login"),
    (psycopg2.OperationalError, "communicate with Postgres"),
    (psycopg2.DatabaseError, "Could not read"),
])
def test_query_errors_are_sanitized_and_connections_close(connection_setup, error_type, message):
    database, settings, connection, _ = connection_setup
    original = error_type("driver details: private-test-password")

    with pytest.raises(database.DashboardDatabaseError, match=message) as caught:
        with database.open_postgres_database(settings):
            raise original

    assert "private-test-password" not in str(caught.value)
    assert caught.value.__cause__ is original
    # The transaction context receives the original error so it can roll back.
    assert connection.__exit__.call_args.args[0] is error_type
    connection.close.assert_called_once_with()


def test_connection_failure_does_not_yield_or_expose_credentials(connection_setup):
    database, settings, connection, connect = connection_setup
    connect.side_effect = psycopg2.OperationalError("private-test-password")

    with pytest.raises(database.DashboardDatabaseError) as caught:
        with database.open_postgres_database(settings):
            pytest.fail("A failed connection must not reach the loader")

    assert "private-test-password" not in str(caught.value)
    connection.__enter__.assert_not_called()


def test_session_setup_failure_closes_the_connection(connection_setup):
    database, settings, connection, _ = connection_setup
    connection.set_session.side_effect = psycopg2.OperationalError("session unavailable")

    with pytest.raises(database.DashboardDatabaseError):
        with database.open_postgres_database(settings):
            pytest.fail("An unconfigured session must not reach the loader")

    connection.close.assert_called_once_with()


def test_non_database_errors_propagate_after_cleanup(connection_setup):
    database, settings, connection, _ = connection_setup
    with pytest.raises(ValueError, match="invalid record"):
        with database.open_postgres_database(settings):
            raise ValueError("invalid record")
    connection.close.assert_called_once_with()


def test_missing_driver_reports_installation_command(warehouse_modules, monkeypatch):
    config, database = warehouse_modules
    settings = config.load_warehouse_config({"WAREHOUSE_PASSWORD": "private-test-password"})
    original_import = builtins.__import__

    def import_without_driver(name, *args, **kwargs):
        """Simulate a fresh environment before dashboard dependencies are installed."""
        if name == "psycopg2":
            raise ImportError("Postgres driver unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_without_driver)
    with pytest.raises(database.DashboardDatabaseError, match="dashboard/requirements.txt"):
        with database.open_postgres_database(settings):
            pytest.fail("A missing driver must not reach the loader")
