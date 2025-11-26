"""Tests for database instrumentation helpers."""

from typing import Any

import pytest

from autotel import init
from autotel.db import instrument_database, trace_db_query
from autotel.exporters import InMemorySpanExporter
from autotel.processors import SimpleSpanProcessor


@pytest.fixture
def exporter() -> Any:
    """Create in-memory exporter for testing."""
    exp = InMemorySpanExporter()
    init(service="test", span_processor=SimpleSpanProcessor(exp))
    return exp


def test_trace_db_query(exporter: Any) -> None:
    """Test manual database query tracing."""
    with trace_db_query("SELECT", "users", "postgresql") as ctx:
        ctx.set_attribute("db.statement", "SELECT * FROM users WHERE id = ?")

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "SELECT users"
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("db.system") == "postgresql"
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("db.operation") == "SELECT"
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("db.sql.table") == "users"


def test_extract_table_name() -> None:
    """Test SQL table name extraction."""
    from autotel.db import _extract_table_name

    assert _extract_table_name("SELECT * FROM users") == "users"
    assert _extract_table_name("INSERT INTO orders VALUES (...)") == "orders"
    assert _extract_table_name("UPDATE products SET ...") == "products"
    assert _extract_table_name("DELETE FROM carts WHERE ...") == "carts"


def test_sanitize_sql() -> None:
    """Test SQL query sanitization."""
    from autotel.db import _sanitize_sql

    query = "SELECT * FROM users WHERE email = 'user@example.com' AND id = 123"
    sanitized = _sanitize_sql(query)
    assert "user@example.com" not in sanitized
    assert "123" not in sanitized
    assert "?" in sanitized


def test_instrument_database_sync(exporter: Any) -> None:
    """Test database instrumentation for sync methods."""

    class MockDB:
        def query(self: Any, sql: str) -> list[dict[str, int]]:  # noqa: ARG002
            """Mock query method."""
            return [{"id": 1}, {"id": 2}]

    db = MockDB()
    instrumented_db = instrument_database(db, db_system="postgresql", db_name="test")

    result = instrumented_db.query("SELECT * FROM users")

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("db.system") == "postgresql"
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("db.name") == "test"
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("db.operation") == "SELECT"
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("db.rows_affected") == 2
    assert result == [{"id": 1}, {"id": 2}]


@pytest.mark.asyncio
async def test_instrument_database_async(exporter: Any) -> None:
    """Test database instrumentation for async methods."""

    class MockDB:
        async def query(self: Any, sql: str) -> list[dict[str, Any]]:  # noqa: ARG002
            """Mock async query method."""
            return [{"id": "1"}, {"id": "2"}]

    db = MockDB()
    instrumented_db = instrument_database(db, db_system="postgresql", db_name="test")

    result = await instrumented_db.query("SELECT * FROM users")

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("db.system") == "postgresql"
    assert spans[0].attributes is not None
    assert spans[0].attributes.get("db.operation") == "SELECT"
    assert result == [{"id": "1"}, {"id": "2"}]
