from typing import Any
import pydantic


class _ConfigModel(pydantic.BaseModel):
    """Model for configuration settings."""

    max_urls: int = 100
    wait_time: int = pydantic.Field(default=5, ge=3, le=60)
    iteration_wait_time: int = pydantic.Field(default=10, ge=30, le=120)
    pagination: int = pydantic.Field(default=0, ge=0, le=100)


class SettingsModel(pydantic.BaseModel):
    """Model for application settings."""

    conf: _ConfigModel


class UrlsModel(pydantic.BaseModel):
    """Model for managing URLs."""

    done_urls: list[str] = pydantic.Field(default_factory=list)
    pending_urls: list[str] = pydantic.Field(default_factory=list)


class DataModel(pydantic.BaseModel):
    """Main data model combining settings and URLs."""

    results: list[dict[str, Any]]
    total_results: int
    page: int
    per_page: int
    total_pages: int
