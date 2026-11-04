"""Integration tests for the gRPC interceptors, over a real local server."""

import asyncio
from collections.abc import AsyncIterator, Iterator
from concurrent import futures
from typing import Any

import grpc
import pytest
from opentelemetry import trace
from opentelemetry.trace import SpanKind, StatusCode

from autotel import InMemorySpanExporter, SimpleSpanProcessor, init
from autotel.integrations.grpc import (
    _peer_attributes,
    aio_server_interceptor,
    server_interceptor,
)

TRACE_ID = "0af7651916cd43dd8448eb211c80319c"
TRACEPARENT = f"00-{TRACE_ID}-b7ad6b7169203331-01"


def _bytes(value: bytes) -> bytes:
    return value


def unary(request: bytes, context: Any) -> bytes:
    with trace.get_tracer("test").start_as_current_span("inner"):
        return request


def not_found(request: bytes, context: Any) -> bytes:
    context.abort(grpc.StatusCode.NOT_FOUND, "nope")
    raise AssertionError


def internal(request: bytes, context: Any) -> bytes:
    raise RuntimeError("boom")


def stream(request: bytes, context: Any) -> Iterator[bytes]:
    yield b"a"
    yield b"b"


def handlers(unary_fn: Any = unary, stream_fn: Any = stream, not_found_fn: Any = not_found) -> Any:
    return grpc.method_handlers_generic_handler(
        "test.Echo",
        {
            "Unary": grpc.unary_unary_rpc_method_handler(unary_fn, _bytes, _bytes),
            "NotFound": grpc.unary_unary_rpc_method_handler(not_found_fn, _bytes, _bytes),
            "Internal": grpc.unary_unary_rpc_method_handler(internal, _bytes, _bytes),
            "Stream": grpc.unary_stream_rpc_method_handler(stream_fn, _bytes, _bytes),
        },
    )


@pytest.fixture
def exporter() -> Any:
    exp = InMemorySpanExporter()
    init(service="test-grpc", span_processor=SimpleSpanProcessor(exp))
    return exp


@pytest.fixture
def channel(exporter: Any) -> Iterator[grpc.Channel]:
    server = grpc.server(futures.ThreadPoolExecutor(2), interceptors=[server_interceptor()])
    server.add_generic_rpc_handlers((handlers(),))
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    with grpc.insecure_channel(f"127.0.0.1:{port}") as open_channel:
        yield open_channel
    server.stop(None)


def call(channel: grpc.Channel, method: str, **kwargs: Any) -> Any:
    return channel.unary_unary(f"/test.Echo/{method}", _bytes, _bytes)(b"x", **kwargs)


def server_span(exporter: Any) -> Any:
    (span,) = [s for s in exporter.get_finished_spans() if s.kind is SpanKind.SERVER]
    return span


def test_unary_joins_the_caller_and_parents_handler_spans(channel: Any, exporter: Any) -> None:
    assert call(channel, "Unary", metadata=(("traceparent", TRACEPARENT),)) == b"x"

    span = server_span(exporter)
    inner = next(s for s in exporter.get_finished_spans() if s.name == "inner")
    assert span.name == "test.Echo/Unary"
    assert format(span.context.trace_id, "032x") == TRACE_ID
    assert inner.parent.span_id == span.context.span_id
    assert span.attributes["rpc.system.name"] == "grpc"
    assert span.attributes["rpc.method"] == "test.Echo/Unary"
    assert span.attributes["rpc.status_code"] == "OK"
    assert span.attributes["client.address"] == "127.0.0.1"


def test_client_fault_is_not_a_span_error(channel: Any, exporter: Any) -> None:
    with pytest.raises(grpc.RpcError):
        call(channel, "NotFound")

    span = server_span(exporter)
    assert span.attributes["rpc.status_code"] == "NOT_FOUND"
    assert span.status.status_code is not StatusCode.ERROR
    assert "error.type" not in span.attributes


def test_server_fault_is_a_span_error(channel: Any, exporter: Any) -> None:
    with pytest.raises(grpc.RpcError):
        call(channel, "Internal")

    span = server_span(exporter)
    assert span.attributes["rpc.status_code"] == "UNKNOWN"
    assert span.attributes["error.type"] == "UNKNOWN"
    assert span.status.status_code is StatusCode.ERROR


def test_span_stays_open_for_the_whole_stream(channel: Any, exporter: Any) -> None:
    responses = channel.unary_stream("/test.Echo/Stream", _bytes, _bytes)(b"x")

    first = next(responses)
    assert first == b"a"
    assert list(responses) == [b"b"]

    assert server_span(exporter).attributes["rpc.status_code"] == "OK"


def test_peer_parsing() -> None:
    assert _peer_attributes("ipv6:[::1]:50051")["client.address"] == "::1"
    assert _peer_attributes("ipv4:10.0.0.1:50051")["network.peer.port"] == 50051
    assert _peer_attributes("unix:/tmp/socket") == {}


def test_unknown_method_stays_out_of_span_names(channel: Any, exporter: Any) -> None:
    with pytest.raises(grpc.RpcError):
        channel.unary_unary("/made.Up/Path", _bytes, _bytes)(b"x")

    span = server_span(exporter)
    assert span.name == "grpc"
    assert span.attributes["rpc.method"] == "_OTHER"
    assert span.attributes["rpc.method_original"] == "made.Up/Path"


async def test_aio_server(exporter: Any) -> None:
    async def aio_unary(request: bytes, context: Any) -> bytes:
        with trace.get_tracer("test").start_as_current_span("inner"):
            return request

    async def aio_stream(request: bytes, context: Any) -> AsyncIterator[bytes]:
        yield b"a"
        yield b"b"

    async def aio_not_found(request: bytes, context: Any) -> bytes:
        await context.abort(grpc.StatusCode.NOT_FOUND, "nope")

    server = grpc.aio.server(interceptors=[aio_server_interceptor()])
    server.add_generic_rpc_handlers((handlers(aio_unary, aio_stream, aio_not_found),))
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
        assert await channel.unary_unary("/test.Echo/Unary", _bytes, _bytes)(b"x") == b"x"
        items = [i async for i in channel.unary_stream("/test.Echo/Stream", _bytes, _bytes)(b"x")]
        with pytest.raises(grpc.aio.AioRpcError):
            await channel.unary_unary("/test.Echo/NotFound", _bytes, _bytes)(b"x")
    await server.stop(None)

    assert items == [b"a", b"b"]
    spans = {s.name: s for s in exporter.get_finished_spans() if s.kind is SpanKind.SERVER}
    assert spans["test.Echo/Unary"].attributes["rpc.status_code"] == "OK"
    assert spans["test.Echo/Stream"].attributes["rpc.status_code"] == "OK"
    assert spans["test.Echo/NotFound"].attributes["rpc.status_code"] == "NOT_FOUND"
    inner = next(s for s in exporter.get_finished_spans() if s.name == "inner")
    assert inner.parent.span_id == spans["test.Echo/Unary"].context.span_id


async def test_aio_sync_handler_gets_a_reduced_context_and_still_works(exporter: Any) -> None:
    server = grpc.aio.server(
        futures.ThreadPoolExecutor(1),
        interceptors=[aio_server_interceptor()],
    )
    server.add_generic_rpc_handlers((handlers(),))
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
        assert await channel.unary_unary("/test.Echo/Unary", _bytes, _bytes)(b"x") == b"x"
    await server.stop(None)

    assert server_span(exporter).attributes["rpc.status_code"] == "OK"


async def test_aio_client_hang_up_ends_the_span(exporter: Any) -> None:
    started = asyncio.Event()

    async def hang(request: bytes, context: Any) -> bytes:
        started.set()
        await asyncio.sleep(30)
        return request

    server = grpc.aio.server(interceptors=[aio_server_interceptor()])
    server.add_generic_rpc_handlers((handlers(hang),))
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
        pending = channel.unary_unary("/test.Echo/Unary", _bytes, _bytes)(b"x")
        await started.wait()
        pending.cancel()
        await asyncio.sleep(0.2)
    await server.stop(None)

    assert server_span(exporter).attributes["rpc.status_code"] == "CANCELLED"
