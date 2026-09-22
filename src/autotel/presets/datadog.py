"""Datadog preset configuration.

Direct OTLP ingest (no Agent, no Collector), `dd-api-key` auth, and
`dd-otlp-source: llmobs` so `gen_ai.*` spans reach Agent Observability as well
as APM.
"""

from typing import Any


def datadog_preset(
    api_key: str | None = None,
    *,
    site: str = "datadoghq.com",
    service: str | None = None,
    environment: str | None = None,
    version: str | None = None,
    enable_logs: bool = False,
    llmobs: bool = True,
    use_agent: bool = False,
    agent_host: str = "localhost",
    agent_port: int = 4318,
) -> dict[str, Any]:
    """
    Create Datadog preset configuration.

    Args:
        api_key: Datadog API key. Required unless ``use_agent`` is True.
        site: Datadog site: ``datadoghq.com`` (default), ``datadoghq.eu``,
            ``us3.datadoghq.com``, ``us5.datadoghq.com``, ``ap1.datadoghq.com``
            or ``ddog-gov.com``.
        service: Service name (also settable on ``init()``).
        environment: Deployment environment, e.g. ``production``.
        version: Service version, for Datadog deployment tracking.
        enable_logs: Export logs over OTLP to Datadog Logs.
        llmobs: Send ``dd-otlp-source: llmobs`` so ``gen_ai.*`` spans also reach
            Agent Observability. Ignored in ``use_agent`` mode, where the Agent
            decides routing. Default True; traces reach APM either way.
        use_agent: Send to a local Datadog Agent (7.35+, OTLP enabled) instead
            of Datadog's cloud ingest. No API key needed.
        agent_host: Agent hostname when ``use_agent`` is True.
        agent_port: Agent OTLP HTTP port when ``use_agent`` is True.

    Returns:
        Dictionary for ``init(preset=...)``.

    Example:
        >>> from autotel import init
        >>> from autotel.presets import datadog_preset
        >>>
        >>> init(
        ...     service="my-app",
        ...     preset=datadog_preset(
        ...         api_key="dd_api_key",
        ...         site="datadoghq.eu",
        ...         enable_logs=True,
        ...     ),
        ... )
    """
    if not use_agent and not api_key:
        raise ValueError(
            "Datadog API key is required for direct cloud ingestion. "
            "Either pass api_key or set use_agent=True to use a local Datadog Agent."
        )

    resource_attributes: dict[str, str] = {}
    if service:
        resource_attributes["service.name"] = service
    if environment:
        resource_attributes["deployment.environment"] = environment
    if version:
        resource_attributes["service.version"] = version

    if use_agent:
        # The Agent authenticates on the app's behalf, so no headers.
        return {
            "endpoint": f"http://{agent_host}:{agent_port}",
            "protocol": "http",
            "insecure": True,
            "headers": {},
            "resource_attributes": resource_attributes,
            "logs": enable_logs,
        }

    # Base URL only: the SDK appends /v1/traces, /v1/metrics, /v1/logs.
    headers = {"dd-api-key": api_key or ""}
    if llmobs:
        headers["dd-otlp-source"] = "llmobs"

    return {
        "endpoint": f"https://otlp.{site}",
        "protocol": "http",
        "insecure": False,
        "headers": headers,
        "resource_attributes": resource_attributes,
        "logs": enable_logs,
    }
