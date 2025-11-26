"""Database instrumentation helpers for autotel."""

import re
import time
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any

from opentelemetry import trace
from opentelemetry.trace import StatusCode

from .context import TraceContext


def instrument_database(
    db: Any,
    *,
    db_system: str,
    db_name: str | None = None,
    slow_threshold_ms: int = 500,
) -> Any:
    """
    Runtime instrumentation for database clients.

    Supports:
    - SQLAlchemy (wraps execute/query methods)
    - MongoDB (wraps find/insert/update/delete)
    - asyncpg (wraps fetch/execute)

    Example:
        >>> db = instrument_database(
        ...     SQLAlchemy(...),
        ...     db_system='postgresql',
        ...     db_name='myapp',
        ... )
    """
    # Wrap common methods
    if hasattr(db, "execute"):
        original_execute = db.execute
        db.execute = _wrap_db_method(
            original_execute,
            db_system=db_system,
            db_name=db_name,
            operation="EXECUTE",
            slow_threshold_ms=slow_threshold_ms,
        )

    if hasattr(db, "query"):
        original_query = db.query
        db.query = _wrap_db_method(
            original_query,
            db_system=db_system,
            db_name=db_name,
            operation="SELECT",
            slow_threshold_ms=slow_threshold_ms,
        )

    # MongoDB-specific
    for method_name in ["find", "insert_one", "update_one", "delete_one"]:
        if hasattr(db, method_name):
            original_method = getattr(db, method_name)
            operation = method_name.split("_")[0].upper()
            setattr(
                db,
                method_name,
                _wrap_db_method(
                    original_method,
                    db_system=db_system,
                    db_name=db_name,
                    operation=operation,
                    slow_threshold_ms=slow_threshold_ms,
                ),
            )

    return db


def _wrap_db_method(
    method: Callable[..., Any],
    db_system: str,
    db_name: str | None,
    operation: str,
    slow_threshold_ms: int,
) -> Callable[..., Any]:
    """Wrap database method with tracing."""

    async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
        tracer = trace.get_tracer(__name__)

        # Infer table from SQL query (heuristic)
        table = None
        query = None
        if args and isinstance(args[0], str):
            query = args[0]
            table = _extract_table_name(query)

        span_name = f"{operation} {table}" if table else operation

        with tracer.start_as_current_span(span_name) as span:
            span.set_attribute("db.system", db_system)
            if db_name:
                span.set_attribute("db.name", db_name)
            span.set_attribute("db.operation", operation)
            if table:
                span.set_attribute("db.sql.table", table)
            if query:
                # Sanitize query (remove values, keep structure)
                span.set_attribute("db.statement", _sanitize_sql(query))

            start = time.time()
            try:
                result = await method(*args, **kwargs)

                # Record result count
                if hasattr(result, "__len__"):
                    span.set_attribute("db.rows_affected", len(result))

                duration_ms = (time.time() - start) * 1000
                span.set_attribute("db.duration_ms", duration_ms)
                if duration_ms > slow_threshold_ms:
                    span.set_attribute("db.slow_query", True)

                return result
            except Exception as e:
                span.record_exception(e)
                span.set_status(StatusCode.ERROR, str(e))
                raise

    def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
        tracer = trace.get_tracer(__name__)

        # Infer table from SQL query
        table = None
        query = None
        if args and isinstance(args[0], str):
            query = args[0]
            table = _extract_table_name(query)

        span_name = f"{operation} {table}" if table else operation

        with tracer.start_as_current_span(span_name) as span:
            span.set_attribute("db.system", db_system)
            if db_name:
                span.set_attribute("db.name", db_name)
            span.set_attribute("db.operation", operation)
            if table:
                span.set_attribute("db.sql.table", table)
            if query:
                span.set_attribute("db.statement", _sanitize_sql(query))

            start = time.time()
            try:
                result = method(*args, **kwargs)

                # Record result count
                if hasattr(result, "__len__"):
                    span.set_attribute("db.rows_affected", len(result))

                duration_ms = (time.time() - start) * 1000
                span.set_attribute("db.duration_ms", duration_ms)
                if duration_ms > slow_threshold_ms:
                    span.set_attribute("db.slow_query", True)

                return result
            except Exception as e:
                span.record_exception(e)
                span.set_status(StatusCode.ERROR, str(e))
                raise

    # Return appropriate wrapper
    import inspect

    if inspect.iscoroutinefunction(method):
        return async_wrapper
    return sync_wrapper


@contextmanager
def trace_db_query(operation: str, table: str, db_system: str) -> Any:
    """
    Manual database query tracing.

    Example:
        >>> with trace_db_query("SELECT", "users", "postgresql") as ctx:
        ...     ctx.set_attribute("db.statement", query)
        ...     result = await db.execute(query)
    """
    tracer = trace.get_tracer(__name__)
    span_name = f"{operation} {table}"

    with tracer.start_as_current_span(span_name) as span:
        span.set_attribute("db.system", db_system)
        span.set_attribute("db.operation", operation)
        span.set_attribute("db.sql.table", table)
        yield TraceContext(span)


def _extract_table_name(query: str) -> str | None:
    """Extract table name from SQL query."""
    # Simple regex for SELECT/INSERT/UPDATE/DELETE
    patterns = [
        r"FROM\s+([^\s,;]+)",  # SELECT
        r"INSERT INTO\s+([^\s(;]+)",  # INSERT
        r"UPDATE\s+([^\s;]+)",  # UPDATE
        r"DELETE FROM\s+([^\s;]+)",  # DELETE
    ]

    for pattern in patterns:
        match = re.search(pattern, query, re.IGNORECASE)
        if match:
            return match.group(1).strip('`"[]')

    return None


def _sanitize_sql(query: str) -> str:
    """
    Sanitize SQL query to remove PII while keeping structure.

    Examples:
        "SELECT * FROM users WHERE email = 'user@example.com'"
        -> "SELECT * FROM users WHERE email = ?"
    """
    # Replace string literals with ?
    sanitized = re.sub(r"'[^']*'", "?", query)
    # Replace numbers with ?
    sanitized = re.sub(r"\b\d+\b", "?", sanitized)
    return sanitized
