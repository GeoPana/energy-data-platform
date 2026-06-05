"""Small read-only SQL Server access layer for the API."""

from __future__ import annotations

from typing import Any, Iterable, Sequence

from api.config import get_settings


class WarehouseQueryError(RuntimeError):
    """Raised when the API cannot execute a warehouse query."""


def get_connection():
    try:
        import pyodbc
    except ImportError as exc:
        raise WarehouseQueryError("pyodbc is not installed. Update the Conda environment first.") from exc

    try:
        return pyodbc.connect(get_settings().sql_server.connection_string, timeout=10)
    except Exception as exc:
        raise WarehouseQueryError(f"Could not connect to SQL Server warehouse: {exc}") from exc


def rows_to_dicts(cursor: Any) -> list[dict[str, Any]]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def execute_query(sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
    """Execute a parameterized read-only query and return dictionaries."""
    normalized = sql.lstrip().lower()
    if not normalized.startswith("select") and not normalized.startswith("with"):
        raise WarehouseQueryError("Only read-only SELECT queries are allowed.")

    connection = get_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(sql, tuple(params or ()))
        return rows_to_dicts(cursor)
    except Exception as exc:
        raise WarehouseQueryError(f"Warehouse query failed: {exc}") from exc
    finally:
        connection.close()


def execute_single(sql: str, params: Iterable[Any] | None = None) -> dict[str, Any] | None:
    rows = execute_query(sql, tuple(params or ()))
    return rows[0] if rows else None
