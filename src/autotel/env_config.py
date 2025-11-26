"""Environment configuration resolver for OpenTelemetry standard environment variables."""

import os
from typing import Literal, TypedDict


class OtelEnvVars(TypedDict, total=False):
    """Standard OpenTelemetry environment variables."""

    OTEL_SERVICE_NAME: str
    OTEL_EXPORTER_OTLP_ENDPOINT: str
    OTEL_EXPORTER_OTLP_HEADERS: str
    OTEL_RESOURCE_ATTRIBUTES: str
    OTEL_EXPORTER_OTLP_PROTOCOL: Literal["http/protobuf", "grpc"]


class EnvConfig(TypedDict, total=False):
    """Environment-resolved configuration."""

    service: str
    endpoint: str
    protocol: Literal["http", "grpc"]
    headers: dict[str, str]
    resource_attributes: dict[str, str]


def resolve_otel_env() -> OtelEnvVars:
    """
    Resolve OpenTelemetry environment variables.

    Returns:
        Dictionary of resolved environment variables
    """
    env_vars: OtelEnvVars = {}

    if service_name := os.getenv("OTEL_SERVICE_NAME"):
        env_vars["OTEL_SERVICE_NAME"] = service_name

    if endpoint := os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"):
        env_vars["OTEL_EXPORTER_OTLP_ENDPOINT"] = endpoint

    if headers := os.getenv("OTEL_EXPORTER_OTLP_HEADERS"):
        env_vars["OTEL_EXPORTER_OTLP_HEADERS"] = headers

    if resource_attrs := os.getenv("OTEL_RESOURCE_ATTRIBUTES"):
        env_vars["OTEL_RESOURCE_ATTRIBUTES"] = resource_attrs

    if (protocol := os.getenv("OTEL_EXPORTER_OTLP_PROTOCOL")) and protocol in (
        "http/protobuf",
        "grpc",
        "http",
    ):
        env_vars["OTEL_EXPORTER_OTLP_PROTOCOL"] = protocol  # type: ignore[typeddict-item]

    return env_vars


def parse_resource_attributes(input_str: str | None) -> dict[str, str]:
    """
    Parse OTEL_RESOURCE_ATTRIBUTES from comma-separated key=value pairs.

    Example: "service.version=1.0.0,deployment.environment=production"

    Args:
        input_str: Comma-separated key=value pairs

    Returns:
        Dictionary of parsed attributes
    """
    if not input_str or not input_str.strip():
        return {}

    attributes: dict[str, str] = {}
    pairs = input_str.split(",")

    for pair in pairs:
        trimmed_pair = pair.strip()
        if not trimmed_pair:
            continue

        if "=" not in trimmed_pair:
            # Invalid format, skip this pair
            continue

        key, _, value = trimmed_pair.partition("=")
        key = key.strip()
        value = value.strip()

        if key and value:
            attributes[key] = value

    return attributes


def parse_otlp_headers(input_str: str | None) -> dict[str, str]:
    """
    Parse OTEL_EXPORTER_OTLP_HEADERS from comma-separated key=value pairs.

    Example: "api-key=secret123,x-custom-header=value"

    Args:
        input_str: Comma-separated key=value pairs

    Returns:
        Dictionary of parsed headers
    """
    if not input_str or not input_str.strip():
        return {}

    headers: dict[str, str] = {}
    pairs = input_str.split(",")

    for pair in pairs:
        trimmed_pair = pair.strip()
        if not trimmed_pair:
            continue

        if "=" not in trimmed_pair:
            # Invalid format, skip this pair
            continue

        key, _, value = trimmed_pair.partition("=")
        key = key.strip()
        value = value.strip()

        if key and value:
            headers[key] = value

    return headers


def env_to_config(env: OtelEnvVars) -> EnvConfig:
    """
    Convert resolved environment variables to config.

    Args:
        env: Resolved environment variables

    Returns:
        Configuration dictionary
    """
    config: EnvConfig = {}

    if "OTEL_SERVICE_NAME" in env:
        config["service"] = env["OTEL_SERVICE_NAME"]

    if "OTEL_EXPORTER_OTLP_ENDPOINT" in env:
        config["endpoint"] = env["OTEL_EXPORTER_OTLP_ENDPOINT"]

    if "OTEL_EXPORTER_OTLP_PROTOCOL" in env:
        protocol = env["OTEL_EXPORTER_OTLP_PROTOCOL"]
        # Normalize protocol value
        if protocol in ("http/protobuf", "http"):
            config["protocol"] = "http"
        elif protocol == "grpc":
            config["protocol"] = "grpc"

    if "OTEL_EXPORTER_OTLP_HEADERS" in env:
        config["headers"] = parse_otlp_headers(env["OTEL_EXPORTER_OTLP_HEADERS"])

    if "OTEL_RESOURCE_ATTRIBUTES" in env:
        resource_attrs = parse_resource_attributes(env["OTEL_RESOURCE_ATTRIBUTES"])
        if resource_attrs:
            config["resource_attributes"] = resource_attrs

    return config


def resolve_config_from_env() -> EnvConfig:
    """
    Main function to resolve config from environment variables.

    Reads standard OpenTelemetry environment variables and returns
    a configuration dictionary that can be merged with explicit config.

    Returns:
        Configuration dictionary from environment variables
    """
    env = resolve_otel_env()
    return env_to_config(env)
