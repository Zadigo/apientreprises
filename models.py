from typing import Annotated
import pydantic


class _ConfigModel(pydantic.BaseModel):
    max_urls: int = 100
    wait_time: int = pydantic.Field(default=5, ge=3, le=60)
    iteration_wait_time: int = pydantic.Field(default=10, ge=30, le=120)
    pagination: int = pydantic.Field(default=0, ge=0, le=100)
    


class SettingsModel(pydantic.BaseModel):
    conf: _ConfigModel


class UrlsModel(pydantic.BaseModel):
    done_urls: list[str] = []
    pending_urls: list[str] = []
