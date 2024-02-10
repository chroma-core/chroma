from typing import Optional
from uuid import UUID
from starlette.responses import JSONResponse

from chromadb.errors import ChromaError, InvalidUUIDError


def fastapi_json_response(
    error: ChromaError, *, trace_id: Optional[str] = None
) -> JSONResponse:
    content = {"error": error.name(), "message": error.message()}
    headers = {}
    if trace_id:
        content["trace-id"] = trace_id
        headers["Trace-Id"] = trace_id
    return JSONResponse(content=content, status_code=error.code(), headers=headers)


def string_to_uuid(uuid_str: str) -> UUID:
    try:
        return UUID(uuid_str)
    except ValueError:
        raise InvalidUUIDError(f"Could not parse {uuid_str} as a UUID")
