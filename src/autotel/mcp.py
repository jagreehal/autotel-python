"""MCP instrumentation helpers for distributed tracing."""

from __future__ import annotations

import inspect
import json
from collections.abc import Callable, Mapping, MutableMapping
from dataclasses import dataclass
from types import MethodType
from typing import Any, TypedDict

from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.propagate import extract, inject
from opentelemetry.trace import StatusCode

TraceAttributes = Mapping[str, str | int | float | bool]


class McpTraceMeta(TypedDict, total=False):
    """Metadata field for W3C Trace Context propagation."""

    traceparent: str
    tracestate: str
    baggage: str


@dataclass
class McpInstrumentationConfig:
    """Configuration options for MCP instrumentation."""

    capture_args: bool = True
    capture_results: bool = False
    capture_errors: bool = True
    custom_attributes: Callable[[dict[str, Any]], TraceAttributes] | None = None


DEFAULT_CONFIG = McpInstrumentationConfig()


def inject_otel_context_to_meta(ctx: otel_context.Context | None = None) -> McpTraceMeta:
    """
    Inject OpenTelemetry context into an MCP-compatible _meta structure.

    Returns only the fields that are present to avoid sending empty values.
    """
    carrier: dict[str, str] = {}
    inject(carrier, context=ctx)

    meta: McpTraceMeta = {}
    for key in ("traceparent", "tracestate", "baggage"):
        value = carrier.get(key)
        if isinstance(value, str) and value:
            meta[key] = value
    return meta


def extract_otel_context_from_meta(meta: Mapping[str, Any] | None = None) -> otel_context.Context:
    """
    Extract an OpenTelemetry context from an MCP _meta field.

    Falls back to the active context when no trace information is present.
    """
    if not meta or not isinstance(meta, Mapping):
        return otel_context.get_current()

    carrier: dict[str, str] = {}
    for key in ("traceparent", "tracestate", "baggage"):
        value = meta.get(key)
        if isinstance(value, str) and value:
            carrier[key] = value

    if not carrier:
        return otel_context.get_current()

    return extract(carrier, context=otel_context.get_current())


def activate_trace_context(meta: Mapping[str, Any] | None = None) -> otel_context.Context:
    """Extract and return a trace context from _meta for activation."""
    return extract_otel_context_from_meta(meta)


def instrument_mcp_client(
    client: Any,
    config: McpInstrumentationConfig | None = None,
) -> Any:
    """
    Instrument an MCP client to trace outbound calls and inject context.

    Supports both camelCase and snake_case method names:
    - call_tool / callTool
    - read_resource / readResource
    - get_prompt / getPrompt
    """
    cfg = _merge_config(config)

    _wrap_client_method(client, ("call_tool", "callTool"), "call_tool", cfg, allow_string_name=True)
    _wrap_client_method(client, ("read_resource", "readResource"), "read_resource", cfg)
    _wrap_client_method(client, ("get_prompt", "getPrompt"), "get_prompt", cfg)

    client.__autotel_mcp_client__ = True
    return client


def instrument_mcp_server(
    server: Any,
    config: McpInstrumentationConfig | None = None,
) -> Any:
    """
    Instrument an MCP server to trace incoming requests and extract context.

    Supports both camelCase and snake_case registration methods:
    - register_tool / registerTool
    - register_resource / registerResource
    - register_prompt / registerPrompt
    """
    cfg = _merge_config(config)

    _wrap_server_registration(server, ("register_tool", "registerTool"), "tool", cfg)
    _wrap_server_registration(server, ("register_resource", "registerResource"), "resource", cfg)
    _wrap_server_registration(server, ("register_prompt", "registerPrompt"), "prompt", cfg)

    server.__autotel_mcp_server__ = True
    return server


def enable_mcp_auto_instrumentation(logger: Any | None = None) -> None:
    """
    Best-effort auto-instrumentation for popular MCP server/client classes.

    If popular MCP packages are installed, patch their Client/Server classes so
    that new instances are wrapped with autotel instrumentation. This is
    intentionally transport-agnostic (stdio, HTTP, SSE).
    """
    global _MCP_PATCHED
    if _MCP_PATCHED:
        return

    _MCP_PATCHED = True

    try:
        import importlib

        modules: list[Any] = []
        for name in (
            "mcp",
            "mcp.server",
            "mcp.client",
            # fastmcp / agents.mcp (used by fastmcp-distributed-tracing examples)
            "agents.mcp",
            "fastmcp",
            "fastmcp.client",
            "fastmcp.server",
        ):
            try:
                modules.append(importlib.import_module(name))
            except Exception:
                continue

        for module in modules:
            _patch_mcp_class(
                module,
                ("Client", "MCPClient"),
                instrument_mcp_client,
            )
            _patch_mcp_class(
                module,
                ("Server", "MCPServer", "MCPServerStdio"),
                instrument_mcp_server,
            )
    except Exception as exc:  # pragma: no cover - defensive guardrail
        if logger and hasattr(logger, "warning"):
            logger.warning(
                "autotel: failed to enable MCP auto-instrumentation",
                exc_info=exc,
            )


# Internal state
_MCP_PATCHED = False


def _patch_mcp_class(
    module: Any, names: tuple[str, ...], instrumenter: Callable[[Any], Any]
) -> None:
    """Patch MCP classes to instrument instances after construction."""
    for name in names:
        cls = getattr(module, name, None)
        if cls is None or not inspect.isclass(cls):
            continue

        if getattr(cls, "__autotel_mcp_patched__", False):
            continue

        original_init = cls.__init__

        def patched_init(
            self: Any,
            *args: Any,
            __original_init: Callable[..., Any] = original_init,
            **kwargs: Any,
        ) -> None:
            __original_init(self, *args, **kwargs)
            instrumenter(self)

        patched_init.__signature__ = getattr(original_init, "__signature__", None)  # type: ignore[attr-defined]
        cls.__init__ = patched_init
        cls.__autotel_mcp_patched__ = True


def _merge_config(config: McpInstrumentationConfig | None) -> McpInstrumentationConfig:
    """Merge user config with defaults."""
    if config is None:
        return McpInstrumentationConfig(
            capture_args=DEFAULT_CONFIG.capture_args,
            capture_results=DEFAULT_CONFIG.capture_results,
            capture_errors=DEFAULT_CONFIG.capture_errors,
            custom_attributes=None,
        )

    return McpInstrumentationConfig(
        capture_args=config.capture_args,
        capture_results=config.capture_results,
        capture_errors=config.capture_errors,
        custom_attributes=config.custom_attributes,
    )


def _wrap_client_method(
    client: Any,
    names: tuple[str, ...],
    operation: str,
    config: McpInstrumentationConfig,
    *,
    allow_string_name: bool = False,
) -> None:
    """Wrap a client method if it exists."""
    for name in names:
        original = getattr(client, name, None)
        if original is None or not callable(original):
            continue

        if getattr(original, "__autotel_mcp_wrapped__", False):
            return

        is_async = inspect.iscoroutinefunction(original)
        wrapped = _make_client_wrapper(original, operation, config, allow_string_name, is_async)
        wrapped.__autotel_mcp_wrapped__ = True  # type: ignore[attr-defined]

        setattr(client, name, MethodType(wrapped, client))
        return


def _make_client_wrapper(
    original: Callable[..., Any],
    operation: str,
    config: McpInstrumentationConfig,
    allow_string_name: bool,
    is_async: bool,
) -> Callable[..., Any]:
    """Create a client wrapper preserving sync/async behaviour."""
    tracer = trace.get_tracer(__name__)

    async def async_wrapper(_self: Any, *args: Any, **kwargs: Any) -> Any:
        span_name, name, arg_payload, new_args, new_kwargs = _prepare_client_call(
            operation,
            args,
            kwargs,
            allow_string_name=allow_string_name,
        )

        with tracer.start_as_current_span(span_name) as span:
            _apply_client_attributes(span, operation, name, arg_payload, config)
            try:
                result = await original(*new_args, **new_kwargs)
                _apply_client_results(span, result, name, arg_payload, config)
                span.set_status(StatusCode.OK)
                return result
            except Exception as exc:
                _record_client_error(span, exc, config)
                raise

    def sync_wrapper(_self: Any, *args: Any, **kwargs: Any) -> Any:
        span_name, name, arg_payload, new_args, new_kwargs = _prepare_client_call(
            operation,
            args,
            kwargs,
            allow_string_name=allow_string_name,
        )

        with tracer.start_as_current_span(span_name) as span:
            _apply_client_attributes(span, operation, name, arg_payload, config)
            try:
                result = original(*new_args, **new_kwargs)
                _apply_client_results(span, result, name, arg_payload, config)
                span.set_status(StatusCode.OK)
                return result
            except Exception as exc:
                _record_client_error(span, exc, config)
                raise

    return async_wrapper if is_async else sync_wrapper


def _prepare_client_call(
    operation: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    allow_string_name: bool,
) -> tuple[str, str, Any, list[Any], dict[str, Any]]:
    """Prepare client call arguments with injected _meta."""
    meta = inject_otel_context_to_meta()
    args_list = list(args)
    kwargs_copy = dict(kwargs)

    name = kwargs_copy.get("name") if isinstance(kwargs_copy.get("name"), str) else None
    arg_payload: Any = kwargs_copy.get("arguments")

    if allow_string_name and args_list and isinstance(args_list[0], str):
        name = name or args_list[0]
        if len(args_list) > 1 and isinstance(args_list[1], MutableMapping):
            arguments_dict = dict(args_list[1])
            arg_payload = arg_payload or arguments_dict
            arguments_dict["_meta"] = _merge_meta(arguments_dict.get("_meta"), meta)
            args_list[1] = arguments_dict
        else:
            kwargs_copy["_meta"] = _merge_meta(kwargs_copy.get("_meta"), meta)
    elif args_list and isinstance(args_list[0], MutableMapping):
        params = dict(args_list[0])
        name = params.get("name") if isinstance(params.get("name"), str) else name
        arg_payload = params.get("arguments", arg_payload)
        params["_meta"] = _merge_meta(params.get("_meta"), meta)
        args_list[0] = params
    else:
        kwargs_copy["_meta"] = _merge_meta(kwargs_copy.get("_meta"), meta)

    resolved_name = name or "unknown"
    span_name = f"mcp.client.{operation}.{resolved_name}"
    return span_name, resolved_name, arg_payload, args_list, kwargs_copy


def _apply_client_attributes(
    span: trace.Span,
    operation: str,
    name: str,
    args: Any,
    config: McpInstrumentationConfig,
) -> None:
    attrs: dict[str, str | int | float | bool] = {
        "mcp.client.operation": operation,
        "mcp.client.name": name,
    }

    if config.capture_args and args is not None:
        attrs["mcp.client.args"] = _safe_serialize(args)

    _set_attributes(span, attrs)

    if config.custom_attributes:
        custom = config.custom_attributes({"type": "client", "name": name, "args": args})
        _set_attributes(span, custom)


def _apply_client_results(
    span: trace.Span,
    result: Any,
    name: str,
    args: Any,
    config: McpInstrumentationConfig,
) -> None:
    if config.capture_results and result is not None:
        span.set_attribute("mcp.client.result", _safe_serialize(result))

    if config.custom_attributes:
        custom = config.custom_attributes(
            {"type": "client", "name": name, "args": args, "result": result},
        )
        _set_attributes(span, custom)


def _record_client_error(
    span: trace.Span, exc: Exception, config: McpInstrumentationConfig
) -> None:
    if config.capture_errors:
        span.record_exception(exc)
        span.set_status(StatusCode.ERROR, str(exc))


def _wrap_server_registration(
    server: Any,
    names: tuple[str, ...],
    kind: str,
    config: McpInstrumentationConfig,
) -> None:
    """Wrap MCP server registration methods."""
    for name in names:
        original = getattr(server, name, None)
        if original is None or not callable(original):
            continue

        if getattr(original, "__autotel_mcp_wrapped__", False):
            return

        is_async = inspect.iscoroutinefunction(original)
        wrapped = _make_server_registration_wrapper(original, kind, config, is_async)
        wrapped.__autotel_mcp_wrapped__ = True  # type: ignore[attr-defined]

        setattr(server, name, MethodType(wrapped, server))
        return


def _make_server_registration_wrapper(
    original: Callable[..., Any],
    kind: str,
    config: McpInstrumentationConfig,
    is_async: bool,
) -> Callable[..., Any]:
    """Wrap register_* calls to wrap handlers with tracing."""
    tracer = trace.get_tracer(__name__)

    async def async_registration(_self: Any, *args: Any, **kwargs: Any) -> Any:
        _, wrapped_args, wrapped_kwargs = _wrap_handler_in_registration(
            kind,
            args,
            kwargs,
            tracer,
            config,
        )
        return await original(*wrapped_args, **wrapped_kwargs)

    def sync_registration(_self: Any, *args: Any, **kwargs: Any) -> Any:
        _, wrapped_args, wrapped_kwargs = _wrap_handler_in_registration(
            kind,
            args,
            kwargs,
            tracer,
            config,
        )
        return original(*wrapped_args, **wrapped_kwargs)

    return async_registration if is_async else sync_registration


def _wrap_handler_in_registration(
    kind: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    tracer: trace.Tracer,
    config: McpInstrumentationConfig,
) -> tuple[str, list[Any], dict[str, Any]]:
    args_list = list(args)
    kwargs_copy = dict(kwargs)

    name = _infer_registration_name(args_list, kwargs_copy)
    handler, handler_index, handler_key = _find_handler(args_list, kwargs_copy)

    if handler is None:
        return name, args_list, kwargs_copy

    wrapped_handler = _wrap_handler(kind, name, handler, tracer, config)

    if handler_index is not None:
        args_list[handler_index] = wrapped_handler
    elif handler_key:
        kwargs_copy[handler_key] = wrapped_handler

    return name, args_list, kwargs_copy


def _wrap_handler(
    kind: str,
    name: str,
    handler: Callable[..., Any],
    tracer: trace.Tracer,
    config: McpInstrumentationConfig,
) -> Callable[..., Any]:
    """Wrap tool/resource/prompt handlers to extract context and trace execution."""
    is_async = inspect.iscoroutinefunction(handler)

    async def async_handler(*args: Any, **kwargs: Any) -> Any:
        meta = _extract_meta(args, kwargs)
        parent_context = extract_otel_context_from_meta(meta)
        token = otel_context.attach(parent_context)
        try:
            with tracer.start_as_current_span(f"mcp.server.{kind}.{name}") as span:
                _apply_server_attributes(span, kind, name, args, config)
                try:
                    result = await handler(*args, **kwargs)
                    _apply_server_results(span, kind, name, args, result, config)
                    span.set_status(StatusCode.OK)
                    return result
                except Exception as exc:
                    _record_server_error(span, exc, config)
                    raise
        finally:
            otel_context.detach(token)

    def sync_handler(*args: Any, **kwargs: Any) -> Any:
        meta = _extract_meta(args, kwargs)
        parent_context = extract_otel_context_from_meta(meta)
        token = otel_context.attach(parent_context)
        try:
            with tracer.start_as_current_span(f"mcp.server.{kind}.{name}") as span:
                _apply_server_attributes(span, kind, name, args, config)
                try:
                    result = handler(*args, **kwargs)
                    _apply_server_results(span, kind, name, args, result, config)
                    span.set_status(StatusCode.OK)
                    return result
                except Exception as exc:
                    _record_server_error(span, exc, config)
                    raise
        finally:
            otel_context.detach(token)

    return async_handler if is_async else sync_handler


def _apply_server_attributes(
    span: trace.Span,
    kind: str,
    name: str,
    args: tuple[Any, ...],
    config: McpInstrumentationConfig,
) -> None:
    attrs: dict[str, str | int | float | bool] = {
        "mcp.type": kind,
        f"mcp.{kind}.name": name,
    }

    if config.capture_args and args:
        attrs[f"mcp.{kind}.args"] = _safe_serialize(args[0] if len(args) == 1 else args)

    _set_attributes(span, attrs)

    if config.custom_attributes:
        custom = config.custom_attributes(
            {"type": kind, "name": name, "args": args[0] if args else None}
        )
        _set_attributes(span, custom)


def _apply_server_results(
    span: trace.Span,
    kind: str,
    name: str,
    args: tuple[Any, ...],
    result: Any,
    config: McpInstrumentationConfig,
) -> None:
    if config.capture_results and result is not None:
        span.set_attribute(f"mcp.{kind}.result", _safe_serialize(result))

    if config.custom_attributes:
        custom = config.custom_attributes(
            {"type": kind, "name": name, "args": args[0] if args else None, "result": result},
        )
        _set_attributes(span, custom)


def _record_server_error(
    span: trace.Span, exc: Exception, config: McpInstrumentationConfig
) -> None:
    if config.capture_errors:
        span.record_exception(exc)
        span.set_status(StatusCode.ERROR, str(exc))


def _extract_meta(args: tuple[Any, ...], kwargs: dict[str, Any]) -> Mapping[str, Any] | None:
    """Find a _meta mapping in args/kwargs."""
    meta = kwargs.get("_meta")
    if isinstance(meta, Mapping):
        return meta

    for arg in reversed(args):
        if isinstance(arg, Mapping):
            candidate = arg.get("_meta") if isinstance(arg.get("_meta"), Mapping) else None
            if candidate:
                return dict(candidate)
        if arg.get("_meta") is None and "_meta" in arg:
            return None
    return None


def _find_handler(
    args: list[Any],
    kwargs: dict[str, Any],
) -> tuple[Callable[..., Any] | None, int | None, str | None]:
    """Locate a handler callable in args or kwargs."""
    for key in ("handler", "read_callback", "callback"):
        candidate = kwargs.get(key)
        if callable(candidate):
            return candidate, None, key

    for index in range(len(args) - 1, -1, -1):
        if callable(args[index]):
            return args[index], index, None

    return None, None, None


def _infer_registration_name(args: list[Any], kwargs: dict[str, Any]) -> str:
    """Infer a tool/resource/prompt name from registration arguments."""
    name = kwargs.get("name")
    if isinstance(name, str):
        return name

    if args and isinstance(args[0], str):
        return args[0]

    for key in ("uri", "resource", "prompt"):
        candidate = kwargs.get(key)
        if isinstance(candidate, str):
            return candidate

    return "unknown"


def _merge_meta(existing: Any, meta: Mapping[str, Any]) -> dict[str, Any]:
    """Merge existing _meta with injected trace context."""
    base: dict[str, Any] = dict(existing) if isinstance(existing, Mapping) else {}
    for key, value in meta.items():
        base[key] = value
    return base


def _safe_serialize(value: Any) -> str:
    """Safely serialize values for span attributes."""
    try:
        return json.dumps(value)
    except (TypeError, ValueError):
        return "<serialization-failed>"


def _set_attributes(span: trace.Span, attrs: Mapping[str, Any]) -> None:
    """Set attributes on a span, ignoring None values."""
    for key, value in attrs.items():
        if value is None:
            continue
        span.set_attribute(key, value)
