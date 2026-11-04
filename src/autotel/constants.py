"""Shared constants for autotel."""

# Baggage key that marks a request for force-keep / debug sampling.
AUTOTEL_DEBUG_BAGGAGE_KEY = "autotel.debug"

# Span attributes: a verdict was made, and whether it keeps the trace.
AUTOTEL_SAMPLING_TAIL_EVALUATED = "autotel.sampling.tail.evaluated"
AUTOTEL_SAMPLING_TAIL_KEEP = "autotel.sampling.tail.keep"
