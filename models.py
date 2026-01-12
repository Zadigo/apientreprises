from typing import Any
from dataclasses import dataclass, field
import pydantic


class _ConfigModel(pydantic.BaseModel):
    """Model for configuration settings."""

    max_urls: int = 100
    search_url: str
    wait_time: int = pydantic.Field(default=5, ge=3, le=60)
    iteration_wait_time: int = pydantic.Field(default=10, ge=20, le=120)
    per_page: int = pydantic.Field(default=10, ge=10, le=100)


class SettingsModel(pydantic.BaseModel):
    """Model for application settings."""

    conf: _ConfigModel


@dataclass
class UrlsModel:
    """Model for managing URLs."""

    done_urls: list[str] = field(default_factory=list)
    pending_urls: list[str] = field(default_factory=list)


class DataModel(pydantic.BaseModel):
    """Main data model combining settings and URLs."""

    results: list[dict[str, Any]]
    total_results: int
    page: int
    per_page: int
    total_pages: int
