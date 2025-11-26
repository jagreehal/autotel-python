"""Configuration validation and helpers for autotel."""

from typing import Literal

from pydantic import BaseModel, Field, field_validator


class autotelConfig(BaseModel):
    """Configuration model for autotel initialization."""

    service: str = Field(..., description="Service name")
    endpoint: str = Field(default="http://localhost:4318", description="OTLP endpoint")
    protocol: Literal["http", "grpc"] = Field(default="http", description="OTLP protocol")
    insecure: bool = Field(default=True, description="Allow insecure connections")
    service_version: str | None = Field(default=None, description="Service version")
    environment: str | None = Field(default=None, description="Deployment environment")
    batch_timeout: int = Field(default=5000, ge=100, le=60000, description="Batch timeout in ms")
    max_queue_size: int = Field(default=2048, ge=1, le=100000, description="Max queue size")
    max_export_batch_size: int = Field(
        default=512, ge=1, le=10000, description="Max export batch size"
    )

    @field_validator("endpoint")
    @classmethod
    def validate_endpoint(cls, v: str) -> str:
        """Validate endpoint URL."""
        if not v.startswith(("http://", "https://", "grpc://")):
            raise ValueError("endpoint must start with http://, https://, or grpc://")
        return v

    @field_validator("service")
    @classmethod
    def validate_service(cls, v: str) -> str:
        """Validate service name."""
        if not v or not v.strip():
            raise ValueError("service name cannot be empty")
        return v.strip()
