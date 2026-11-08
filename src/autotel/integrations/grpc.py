"""gRPC server interceptors: SERVER spans and ``rpc.server.call.duration``.

Example:
    >>> server = grpc.server(pool, interceptors=[server_interceptor()])
    >>> server = grpc.aio.server(interceptors=[aio_server_interceptor()])
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import contextmanager
from time import perf_counter
from typing import Any

import grpc
from grpc import aio
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.trace import SpanKind, StatusCode

from ..http import extract_trace_context
from ..instruments import record_rpc_server
from ._server import _forwarded_client

_OTHER = "_OTHER"
# The codes the gRPC conventions count as a server's fault.
_SERVER_FAULTS = frozenset(
    {
        grpc.StatusCode.UNKNOWN,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.UNIMPLEMENTED,
        grpc.StatusCode.INTERNAL,
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DATA_LOSS,
    }
)


def _peer_attributes(peer: str | None) -> dict[str, Any]:
    """Parse gRPC's ``ipv4:1.2.3.4:50051`` and ``ipv6:[::1]:50051``."""
    kind, _, rest = (peer or "").partition(":")
    if kind not in ("ipv4", "ipv6"):
        return {}
    host, _, port = rest.rpartition(":")
    host = host.strip("[]")
    return {
        "client.address": host,
        "network.peer.address": host,
        "network.peer.port": int(port) if port.isdigit() else None,
    }


class _Call:
    """One RPC's span, ended once."""

    def __init__(self, method: str, metadata: Any, trust_forwarded_for: bool) -> None:
        self.method = method
        self.metadata = {str(key).lower(): value for key, value in metadata or ()}
        self.trust_forwarded_for = trust_forwarded_for
        self.started = perf_counter()
        self.finished = False
        self.parent_context = extract_trace_context(self._text_metadata())
        self.span = trace.get_tracer("autotel").start_span(
            method,
            context=self.parent_context,
            kind=SpanKind.SERVER,
            attributes={"rpc.system.name": "grpc"},
        )

    def _text_metadata(self) -> dict[str, str]:
        return {key: value for key, value in self.metadata.items() if isinstance(value, str)}

    def set_peer(self, peer: str | None) -> None:
        attributes = _peer_attributes(peer)
        if self.trust_forwarded_for:
            forwarded = _forwarded_client(self._text_metadata().get("x-forwarded-for"))
            if forwarded:
                attributes["client.address"] = forwarded
        self.span.set_attributes(
            {key: value for key, value in attributes.items() if value is not None}
        )

    @contextmanager
    def active(self) -> Iterator[None]:
        token = otel_context.attach(trace.set_span_in_context(self.span, self.parent_context))
        try:
            yield
        finally:
            otel_context.detach(token)

    def finish(self, code: grpc.StatusCode, error: BaseException | None = None) -> None:
        if self.finished:
            return
        self.finished = True
        status = code.name
        failed = code in _SERVER_FAULTS
        labels: dict[str, Any] = {"rpc.system.name": "grpc", "rpc.status_code": status}
        self.span.set_attribute("rpc.status_code", status)
        if error is not None and failed:
            self.span.record_exception(error)
        if failed:
            self.span.set_attribute("error.type", status)
            self.span.set_status(StatusCode.ERROR, status)
            labels["error.type"] = status
        # Clients choose the path, so an unknown one stays out of span names and series.
        method = self.method
        if code is grpc.StatusCode.UNIMPLEMENTED:
            self.span.update_name("grpc")
            self.span.set_attribute("rpc.method_original", method)
            method = _OTHER
        self.span.set_attribute("rpc.method", method)
        record_rpc_server(perf_counter() - self.started, {**labels, "rpc.method": method})
        self.span.end()


def _peer(context: Any) -> str | None:
    """A sync handler on an aio server gets a context with no ``peer()``."""
    peer = getattr(context, "peer", None)
    return peer() if peer else None


def _code(context: Any) -> grpc.StatusCode:
    """The status the handler set. A sync handler on an aio server has no ``code()``."""
    read = getattr(context, "code", None)
    code = read() if read else None
    return code if isinstance(code, grpc.StatusCode) else grpc.StatusCode.OK


def _failure(context: Any, code_of_exception: grpc.StatusCode) -> grpc.StatusCode:
    """``abort`` sets a code then raises; any other exception reads as UNKNOWN."""
    code = _code(context)
    return code if code is not grpc.StatusCode.OK else code_of_exception


_VARIANTS: tuple[tuple[str, Callable[..., Any]], ...] = (
    ("unary_unary", grpc.unary_unary_rpc_method_handler),
    ("unary_stream", grpc.unary_stream_rpc_method_handler),
    ("stream_unary", grpc.stream_unary_rpc_method_handler),
    ("stream_stream", grpc.stream_stream_rpc_method_handler),
)


def _wrap_handler(handler: Any, new_call: Callable[[], _Call]) -> Any:
    for name, build in _VARIANTS:
        behavior = getattr(handler, name)
        if behavior is None:
            continue
        streaming = handler.response_streaming
        if inspect.isasyncgenfunction(behavior):
            wrapped = _async_stream(behavior, new_call)
        elif inspect.iscoroutinefunction(behavior):
            wrapped = _async_unary(behavior, new_call)
        elif streaming:
            wrapped = _sync_stream(behavior, new_call)
        else:
            wrapped = _sync_unary(behavior, new_call)
        return build(
            wrapped,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
    return handler


def _sync_unary(behavior: Callable[..., Any], new_call: Callable[[], _Call]) -> Any:
    def wrapped(request: Any, context: Any) -> Any:
        call = new_call()
        call.set_peer(_peer(context))
        try:
            with call.active():
                response = behavior(request, context)
        except Exception as error:
            call.finish(_failure(context, grpc.StatusCode.UNKNOWN), error)
            raise
        call.finish(_code(context))
        return response

    return wrapped


def _sync_stream(behavior: Callable[..., Any], new_call: Callable[[], _Call]) -> Any:
    def wrapped(request: Any, context: Any) -> Iterator[Any]:
        call = new_call()
        call.set_peer(_peer(context))
        try:
            with call.active():
                responses = iter(behavior(request, context))
            while True:
                with call.active():
                    try:
                        item = next(responses)
                    except StopIteration:
                        break
                yield item
        except GeneratorExit:
            call.finish(grpc.StatusCode.CANCELLED)
            raise
        except Exception as error:
            call.finish(_failure(context, grpc.StatusCode.UNKNOWN), error)
            raise
        call.finish(_code(context))

    return wrapped


def _async_unary(behavior: Callable[..., Any], new_call: Callable[[], _Call]) -> Any:
    async def wrapped(request: Any, context: Any) -> Any:
        call = new_call()
        call.set_peer(_peer(context))
        try:
            with call.active():
                response = await behavior(request, context)
        except asyncio.CancelledError:
            call.finish(grpc.StatusCode.CANCELLED)
            raise
        except Exception as error:
            call.finish(_failure(context, grpc.StatusCode.UNKNOWN), error)
            raise
        call.finish(_code(context))
        return response

    return wrapped


def _async_stream(behavior: Callable[..., Any], new_call: Callable[[], _Call]) -> Any:
    async def wrapped(request: Any, context: Any) -> AsyncIterator[Any]:
        call = new_call()
        call.set_peer(_peer(context))
        try:
            responses = behavior(request, context).__aiter__()
            while True:
                with call.active():
                    try:
                        item = await responses.__anext__()
                    except StopAsyncIteration:
                        break
                yield item
        except (GeneratorExit, asyncio.CancelledError):
            call.finish(grpc.StatusCode.CANCELLED)
            raise
        except Exception as error:
            call.finish(_failure(context, grpc.StatusCode.UNKNOWN), error)
            raise
        call.finish(_code(context))

    return wrapped


class _Interceptor(grpc.ServerInterceptor):  # type: ignore[misc]
    def __init__(self, trust_forwarded_for: bool) -> None:
        self.trust_forwarded_for = trust_forwarded_for

    def _traced(self, handler: Any, details: Any) -> Any:
        method = details.method.lstrip("/")

        def new_call() -> _Call:
            return _Call(method, details.invocation_metadata, self.trust_forwarded_for)

        if handler is None:
            new_call().finish(grpc.StatusCode.UNIMPLEMENTED)
            return None
        return _wrap_handler(handler, new_call)

    def intercept_service(self, continuation: Any, handler_call_details: Any) -> Any:
        return self._traced(continuation(handler_call_details), handler_call_details)


class _AioInterceptor(_Interceptor, aio.ServerInterceptor):  # type: ignore[misc]
    async def intercept_service(self, continuation: Any, handler_call_details: Any) -> Any:
        return self._traced(await continuation(handler_call_details), handler_call_details)


def server_interceptor(*, trust_forwarded_for: bool = False) -> grpc.ServerInterceptor:
    """Interceptor for ``grpc.server``."""
    return _Interceptor(trust_forwarded_for)


def aio_server_interceptor(*, trust_forwarded_for: bool = False) -> aio.ServerInterceptor:
    """Interceptor for ``grpc.aio.server``."""
    return _AioInterceptor(trust_forwarded_for)
