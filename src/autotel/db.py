"""Database instrumentation helpers for autotel."""

import inspect
import re
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from opentelemetry import trace
from opentelemetry.trace import SpanKind, StatusCode

from .context import TraceContext
from .instruments import measured, record_db_operation


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


@contextmanager
def _db_span(
    query: str | None, db_system: str, db_name: str | None, operation: str, slow_threshold_ms: int
) -> Iterator[Callable[[Any], None]]:
    """CLIENT span for one call. Yields a function that records the call's result."""
    table = _extract_table_name(query) if query else None
    span_name = f"{operation} {table}" if table else operation
    tracer = trace.get_tracer(__name__)
    labels = {
        "db.system.name": db_system,
        "db.namespace": db_name,
        "db.operation.name": operation,
        "db.collection.name": table,
    }
    with (
        tracer.start_as_current_span(span_name, kind=SpanKind.CLIENT) as span,
        measured(record_db_operation, labels),
    ):
        span.set_attributes({key: value for key, value in labels.items() if value is not None})
        if query:
            span.set_attribute("db.query.text", sanitize_query(query, db_system))

        start = time.perf_counter()

        def finish(result: Any) -> None:
            if hasattr(result, "__len__"):
                span.set_attribute("db.response.returned_rows", len(result))
            if (time.perf_counter() - start) * 1000 > slow_threshold_ms:
                span.set_attribute("db.slow_query", True)

        try:
            yield finish
        except Exception as e:
            span.set_attribute("error.type", type(e).__qualname__)
            span.record_exception(e)
            span.set_status(StatusCode.ERROR, str(e))
            raise


def _wrap_db_method(
    method: Callable[..., Any],
    db_system: str,
    db_name: str | None,
    operation: str,
    slow_threshold_ms: int,
) -> Callable[..., Any]:
    """Wrap database method with tracing."""

    def query_of(args: tuple[Any, ...]) -> str | None:
        return args[0] if args and isinstance(args[0], str) else None

    async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
        with _db_span(query_of(args), db_system, db_name, operation, slow_threshold_ms) as finish:
            result = await method(*args, **kwargs)
            finish(result)
            return result

    def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
        with _db_span(query_of(args), db_system, db_name, operation, slow_threshold_ms) as finish:
            result = method(*args, **kwargs)
            finish(result)
            return result

    return async_wrapper if inspect.iscoroutinefunction(method) else sync_wrapper


@contextmanager
def trace_db_query(
    operation: str, table: str, db_system: str, db_name: str | None = None
) -> Iterator[TraceContext]:
    """
    Manual database query tracing.

    Example:
        >>> with trace_db_query("SELECT", "users", "postgresql") as ctx:
        ...     ctx.set_query_text(query)
        ...     result = await db.execute(query)
    """
    tracer = trace.get_tracer(__name__)
    labels = {
        "db.system.name": db_system,
        "db.namespace": db_name,
        "db.operation.name": operation,
        "db.collection.name": table,
    }
    with (
        tracer.start_as_current_span(f"{operation} {table}", kind=SpanKind.CLIENT) as span,
        measured(record_db_operation, labels),
    ):
        span.set_attributes({key: value for key, value in labels.items() if value is not None})
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


_MYSQL = frozenset({"mysql", "mariadb"})
_POSTGRES = frozenset({"postgresql", "postgres"})
_QUOTED_IDENTIFIERS = _POSTGRES | {
    "sqlite",
    "oracle",
    "mssql",
    "microsoft.sql_server",
    "db2",
    "ibm.db2",
}
_DOLLAR_TAG = re.compile(r"\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$")


def _quoted_end(sql: str, start: int, quote: str, escapes: bool) -> int | None:
    """Index of the quote closing the literal at ``start``, or None if it never closes."""
    i = start + 1
    while i < len(sql):
        if escapes and sql[i] == "\\":
            i += 2
        elif sql[i] != quote:
            i += 1
        elif sql[i + 1 : i + 2] == quote:
            i += 2
        else:
            return i
    return None


def _is_word(char: str) -> bool:
    return char.isalnum() or char in "_$:"


def sanitize_query(sql: str, system: str = "") -> str:
    """
    Replace literals in SQL with ``?`` so ``db.query.text`` can't leak data.

    ``system`` is the ``db.system.name`` value and picks the dialect's quoting rules.
    Fails closed: an unterminated string turns the rest of the query into ``?``.

    Examples:
        "SELECT * FROM users WHERE email = 'user@example.com'"
        -> "SELECT * FROM users WHERE email = ?"
    """
    mysql = system in _MYSQL
    postgres = system in _POSTGRES
    identifiers = system in _QUOTED_IDENTIFIERS
    out: list[str] = []
    i = 0
    while i < len(sql):
        char = sql[i]
        previous = out[-1][-1:] if out else ""
        after = sql[i + 1 : i + 2]
        end: int | None = i
        if char == "'":
            # E'...' (Postgres) opts into backslash escapes; drop the prefix too.
            before = out[-2][-1:] if len(out) > 1 else ""
            escaped = postgres and previous in ("E", "e") and not _is_word(before)
            if escaped:
                out.pop()
            end = _quoted_end(sql, i, "'", mysql or escaped)
            out.append("?")
        elif char == '"' and not identifiers:
            end = _quoted_end(sql, i, '"', mysql)
            out.append("?")
        elif char == '"' or (char == "`" and mysql):
            end = _quoted_end(sql, i, char, False)
            out.append(sql[i : end + 1] if end is not None else "?")
        elif char == "$" and not mysql and not _is_word(previous) and not after.isdigit():
            tag = _DOLLAR_TAG.match(sql, i)
            if tag is None:
                out.append(char)
            else:
                close = sql.find(tag.group(), tag.end())
                end = close + len(tag.group()) - 1 if close != -1 else None
                out.append("?")
        elif (char == "-" and after == "-") or (char == "#" and mysql):
            newline = sql.find("\n", i)
            end = len(sql) - 1 if newline == -1 else newline - 1
        elif char == "/" and after == "*":
            close = sql.find("*/", i + 2)
            end = close + 1 if close != -1 else None
            out.append(" ")
        elif char.isascii() and char.isdigit() and not _is_word(previous):
            last = i
            while sql[last + 1 : last + 2].isalnum() or sql[last + 1 : last + 2] == ".":
                last += 1
            end = last
            out.append("?")
        else:
            out.append(char)
        if end is None:
            break
        i = end + 1
    return " ".join("".join(out).split())
