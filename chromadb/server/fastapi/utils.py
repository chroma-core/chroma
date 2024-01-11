from starlette.responses import JSONResponse

from chromadb.errors import ChromaError


def fastapi_json_response(error: ChromaError) -> JSONResponse:
    return JSONResponse(
        content={"error": error.name(), "message": error.message()},
        status_code=error.code(),
    )
