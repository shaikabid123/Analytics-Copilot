from pydantic import BaseModel


class LiveResponse(BaseModel):
    status: str
    service: str
    version: str


class ReadyResponse(BaseModel):
    status: str
    database: str
    schema_revision: str | None = None
    detail: str | None = None
