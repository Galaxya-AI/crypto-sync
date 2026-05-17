"""Integration tests for /streams routes — happy path + errors.

Builds the FastAPI app with FakeStorage / FakeMarket injected on app.state
so we exercise the routes through TestClient without spinning up MariaDB.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.routes_streams import router as streams_router
from src.core.stream_supervisor import Supervisor
from _helpers import FakeMarket, FakeStorage


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch) -> FastAPI:
    """Build a minimal FastAPI app with the streams router and a fake supervisor."""
    monkeypatch.setenv("BINANCE_API_KEY", "test")
    monkeypatch.setenv("BINANCE_SECRET_KEY", "test")
    monkeypatch.setenv("DB_PASSWORD", "test")

    from src.config import get_settings  # local import after env vars set

    get_settings.cache_clear()

    fastapi_app: FastAPI = FastAPI()
    fastapi_app.include_router(streams_router)
    fastapi_app.state.supervisor = Supervisor(
        market=FakeMarket(),
        storage=FakeStorage(),
    )
    return fastapi_app


def test_post_creates_stream(app: FastAPI) -> None:
    """POST /streams with a valid body returns 201 Created with the stream info."""
    with TestClient(app) as client:
        response = client.post(
            "/streams",
            json={"symbol": "BTCUSDT", "interval": "1m"},
        )
    assert response.status_code == 201
    body = response.json()
    assert body["symbol"] == "BTCUSDT"
    assert body["interval"] == "1m"
    assert body["status"] == "active"


def test_post_duplicate_returns_409(app: FastAPI) -> None:
    """Activating the same stream twice returns 409 Conflict."""
    payload: dict[str, str] = {"symbol": "BTCUSDT", "interval": "1m"}
    with TestClient(app) as client:
        first = client.post("/streams", json=payload)
        second = client.post("/streams", json=payload)
    assert first.status_code == 201
    assert second.status_code == 409


def test_delete_unknown_returns_404(app: FastAPI) -> None:
    """Stopping a stream that was never started returns 404."""
    with TestClient(app) as client:
        response = client.delete("/streams/BTCUSDT/1m")
    assert response.status_code == 404
