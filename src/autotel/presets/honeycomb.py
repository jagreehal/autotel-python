"""Honeycomb preset configuration."""

from typing import Any


def honeycomb_preset(
    api_key: str,
    *,
    dataset: str = "production",
    service: str | None = None,
    environment: str | None = None,
) -> dict[str, Any]:
    """
    Create Honeycomb preset configuration.

    Configures autotel for Honeycomb with:
    - OTLP HTTP exporter pointing to Honeycomb endpoint
    - API key authentication
    - Dataset configuration

    Args:
        api_key: Honeycomb API key
        dataset: Dataset name (default: production)
        service: Service name (optional, can be set in init())
        environment: Environment name (optional)

    Returns:
        Dictionary with configuration for init()

    Example:
        >>> from autotel import init
        >>> from autotel.presets import honeycomb_preset
        >>>
        >>> init(
        ...     service="my-app",
        ...     preset=honeycomb_preset(
        ...         api_key="hny_api_key",
        ...         dataset="production",
        ...     )
        ... )
    """
    endpoint = "https://api.honeycomb.io"
    headers = {
        "x-honeycomb-team": api_key,
        "x-honeycomb-dataset": dataset,
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
