"""Langfuse preset: OTLP endpoint and Basic auth."""

import base64

import pytest

from autotel.presets import langfuse_preset


def test_langfuse_preset_endpoint_and_basic_auth() -> None:
    config = langfuse_preset(
        public_key="pk-lf-test",
        secret_key="sk-lf-test",
        service="agent-demo",
        environment="dev",
    )

    assert config["endpoint"] == "https://cloud.langfuse.com/api/public/otel"
    assert config["protocol"] == "http"
    assert config["insecure"] is False
    expected = base64.b64encode(b"pk-lf-test:sk-lf-test").decode()
    assert config["headers"]["Authorization"] == f"Basic {expected}"
    assert config["resource_attributes"]["service.name"] == "agent-demo"
    assert config["resource_attributes"]["deployment.environment"] == "dev"


def test_langfuse_preset_self_hosted_http() -> None:
    config = langfuse_preset(
        public_key="pk",
        secret_key="sk",
        host="http://langfuse.local",
    )
    assert config["endpoint"] == "http://langfuse.local/api/public/otel"
    assert config["insecure"] is True


def test_langfuse_preset_requires_keys() -> None:
    with pytest.raises(ValueError, match="public_key and secret_key"):
        langfuse_preset(public_key="", secret_key="sk")
