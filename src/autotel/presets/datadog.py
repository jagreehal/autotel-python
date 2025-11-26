"""Datadog preset configuration."""

from typing import Any


def datadog_preset(
    api_key: str,
    *,
    site: str = "datadoghq.com",
    service: str | None = None,
    environment: str | None = None,
) -> dict[str, Any]:
    """
    Create Datadog preset configuration.

    Configures autotel for Datadog APM with:
    - OTLP HTTP exporter pointing to Datadog endpoint
    - API key authentication
    - Resource attributes for Datadog

    Args:
        api_key: Datadog API key
        site: Datadog site (default: datadoghq.com)
        service: Service name (optional, can be set in init())
        environment: Environment name (optional)

    Returns:
        Dictionary with configuration for init()

    Example:
        >>> from autotel import init
        >>> from autotel.presets import datadog_preset
        >>>
        >>> init(
        ...     service="my-app",
        ...     preset=datadog_preset(
        ...         api_key="dd_api_key",
        ...         site="datadoghq.com",
        ...     )
        ... )
    """
    endpoint = f"https://trace-intake.{site}"
    headers = {
        "DD-API-KEY": api_key,
    }

    resource_attributes = {}
    if service:
        resource_attributes["service.name"] = service
    if environment:
        resource_attributes["deployment.environment"] = environment

    return {
        "endpoint": endpoint,
        "protocol": "http",
        "insecure": False,
        "headers": headers,
        "resource_attributes": resource_attributes,
    }
