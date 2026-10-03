"""Langfuse preset configuration (OTLP ingest)."""

from __future__ import annotations

import base64
from typing import Any


def langfuse_preset(
    public_key: str,
    secret_key: str,
    *,
    host: str = "https://cloud.langfuse.com",
    service: str | None = None,
    environment: str | None = None,
) -> dict[str, Any]:
    """
    Create Langfuse preset configuration.

    Points OTLP HTTP at Langfuse's public OTEL endpoint with Basic auth
    (``public_key:secret_key``).

    Args:
        public_key: Langfuse public key (``pk-lf-...``).
        secret_key: Langfuse secret key (``sk-lf-...``).
        host: Langfuse host, e.g. ``https://cloud.langfuse.com``,
            ``https://us.cloud.langfuse.com``, or a self-hosted base URL.
        service: Optional service name resource attribute.
        environment: Optional deployment environment resource attribute.

    Returns:
        Dictionary with configuration for ``init(preset=...)``.

    Example:
        >>> from autotel import init
        >>> from autotel.presets import langfuse_preset
        >>>
        >>> init(
        ...     service="agent-demo",
        ...     preset=langfuse_preset(
        ...         public_key="pk-lf-...",
        ...         secret_key="sk-lf-...",
        ...     ),
        ... )
    """
    if not public_key or not secret_key:
        raise ValueError("Langfuse public_key and secret_key are required.")

    base = host.rstrip("/")
    token = base64.b64encode(f"{public_key}:{secret_key}".encode()).decode()

    resource_attributes: dict[str, str] = {}
    if service:
        resource_attributes["service.name"] = service
    if environment:
        resource_attributes["deployment.environment"] = environment

    return {
        "endpoint": f"{base}/api/public/otel",
        "protocol": "http",
        "insecure": base.startswith("http://"),
        "headers": {"Authorization": f"Basic {token}"},
        "resource_attributes": resource_attributes,
    }
