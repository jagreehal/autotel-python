"""OpenLLMetry integration for LLM observability."""

import logging

logger = logging.getLogger(__name__)


def configure_openllmetry(
    *,
    api_endpoint: str = "https://api.traceloop.com",
    api_key: str | None = None,
    enabled: bool = True,
) -> None:
    """
    Configure OpenLLMetry integration.

    Auto-instruments LLM SDKs:
    - OpenAI SDK
    - Anthropic SDK
    - LangChain
    - LlamaIndex

    Args:
        api_endpoint: Traceloop API endpoint
        api_key: Traceloop API key
        enabled: Whether to enable OpenLLMetry

    Example:
        >>> from autotel.openllmetry import configure_openllmetry
        >>> configure_openllmetry(
        ...     api_endpoint="https://api.traceloop.com",
        ...     api_key="your_api_key",
        ... )
    """
    if not enabled:
        logger.info("OpenLLMetry integration disabled")
        return

    try:
        import traceloop  # type: ignore[import-not-found]
    except ImportError:
        logger.warning("traceloop not installed. Install with: pip install traceloop")
        return

    try:
        # Configure Traceloop
        traceloop.configure(
            api_endpoint=api_endpoint,
            api_key=api_key,
        )

        # Get current tracer provider and reuse it
        from opentelemetry import trace

        tracer_provider = trace.get_tracer_provider()
        if tracer_provider:
            traceloop.set_tracer_provider(tracer_provider)

        logger.info("OpenLLMetry integration configured")
    except Exception as e:
        logger.error(f"Failed to configure OpenLLMetry: {e}", exc_info=True)
