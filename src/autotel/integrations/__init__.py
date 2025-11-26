"""Framework integrations for autotel.

Imports are lazy to avoid requiring all framework dependencies.
Only the framework you actually use needs to be installed.
"""

__all__ = [
    "FastAPIautotelMiddleware",
    "DjangoautotelMiddleware",
    "init_autotel",
]


def __getattr__(name: str) -> object:
    """Lazy import framework integrations to avoid requiring all dependencies."""
    if name == "FastAPIautotelMiddleware":
        from .fastapi import autotelMiddleware as _fastapi_middleware

        return _fastapi_middleware
    if name == "DjangoautotelMiddleware":
        from .django import autotelMiddleware as _django_middleware

        return _django_middleware
    if name == "init_autotel":
        from .flask import init_autotel as _flask_init

        return _flask_init
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
