"""HTTP routes for stream lifecycle management.

Exposes the public API to start, stop and list candle ingestion streams.
Each route translates an HTTP request into a Supervisor command and
returns a wire-friendly StreamResponse built from the core StreamInfo.

Errors raised by the supervisor are mapped to standard HTTP statuses:
409 Conflict when the stream is already active, 404 Not Found when it
does not exist.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, status

from src.api.schemas import StreamCreateRequest, StreamResponse
from src.core.events import StreamInfo, StreamKey, StreamStatus
from src.core.stream_supervisor import (
    StreamAlreadyActiveError,
    StreamNotFoundError,
    Supervisor,
)

router: APIRouter = APIRouter(
    prefix="/streams",
    tags=["streams"],
)


def _supervisor(request: Request) -> Supervisor:
    """Return the supervisor stored on the FastAPI app state.

    Parameters
    ----------
    request : Request
        Current request; used to reach ``app.state``.

    Returns
    -------
    Supervisor
        The shared supervisor instance.
    """
    supervisor: Supervisor = request.app.state.supervisor
    return supervisor


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=StreamResponse,
    summary="Activate a (symbol, interval) stream",
)
async def create_stream(
    body: StreamCreateRequest,
    request: Request,
) -> StreamResponse:
    """Start ingesting candles for ``(symbol, interval)``.

    Parameters
    ----------
    body : StreamCreateRequest
        Request body with ``symbol`` and ``interval``.
    request : Request
        Used to reach the shared supervisor.

    Returns
    -------
    StreamResponse
        Info of the newly activated stream.

    Raises
    ------
    HTTPException
        409 Conflict if a stream with this key is already active.
    """
    key: StreamKey = StreamKey(symbol=body.symbol, interval=body.interval)
    supervisor: Supervisor = _supervisor(request)
    try:
        info: StreamInfo = await supervisor.start(key)
    except StreamAlreadyActiveError as error:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error
    response: StreamResponse = StreamResponse.from_info(info)
    return response


@router.delete(
    "/{symbol}/{interval}",
    response_model=StreamResponse,
    summary="Stop a (symbol, interval) stream",
)
async def delete_stream(symbol: str, interval: str, request: Request) -> StreamResponse:
    """Stop ingesting for ``(symbol, interval)``.

    Parameters
    ----------
    symbol : str
        Symbol path segment, normalized to uppercase.
    interval : str
        Interval path segment.
    request : Request
        Used to reach the shared supervisor.

    Returns
    -------
    StreamResponse
        Final info of the stopped stream.

    Raises
    ------
    HTTPException
        404 Not Found if the stream is not active.
    """
    key: StreamKey = StreamKey(symbol=symbol.upper(), interval=interval)
    supervisor: Supervisor = _supervisor(request)
    try:
        info: StreamInfo = await supervisor.stop(key)
    except StreamNotFoundError as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(error)) from error
    response: StreamResponse = StreamResponse.from_info(info)
    return response


@router.get(
    "",
    response_model=list[StreamResponse],
    summary="List registered streams",
)
async def list_streams(
    request: Request,
    status_filter: StreamStatus | None = None,
) -> list[StreamResponse]:
    """Return the registry contents.

    Parameters
    ----------
    request : Request
        Used to reach the shared supervisor.
    status_filter : StreamStatus | None
        Optional query param ``status_filter`` to restrict the list.

    Returns
    -------
    list[StreamResponse]
        All matching streams.
    """
    supervisor: Supervisor = _supervisor(request)
    infos: list[StreamInfo] = await supervisor.list(status_filter)
    responses: list[StreamResponse] = [StreamResponse.from_info(info) for info in infos]
    return responses
