"""Tests for dust and precision fields on position API responses."""

from collections.abc import AsyncGenerator
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError

from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository_types import PositionRow
from snapper.messaging.schemas.data import PositionData
from snapper.server.app import create_app
from snapper.server.app import get_repository_dependency
from snapper.server.position_cycle_routes import PositionCycleData
from snapper.server.position_display import build_position_payloads
from snapper.server.position_display import load_position_spec_steps
from snapper.server.position_display import position_data_from_row

NOW = datetime(2026, 11, 23, 12, 0, tzinfo=UTC)
_THRESHOLD = Decimal("0.50")


async def _noop_lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    """Disable application lifespan for endpoint-only tests."""
    yield


class _SpecRepo:
    """Repository double that serves instrument specs by id."""

    def __init__(self, specs: dict[str, object]) -> None:
        """Store specs and record the ids that were read."""
        self.specs = specs
        self.calls: list[str] = []

    async def get_instrument_spec(self, instrument_public_id: str, as_of: datetime) -> object:
        """Return the configured spec, or None when this id was not seeded."""
        del as_of
        self.calls.append(instrument_public_id)
        return self.specs.get(instrument_public_id)

    async def get_positions(self, **_kwargs: Any) -> list[PositionRow]:
        """Unused by the loader tests."""
        return []


def _row(**overrides: object) -> PositionRow:
    """Build one position projection row."""
    base: dict[str, object] = {
        "public_id": "pos-1",
        "timestamp": NOW,
        "session_id": "s1",
        "sequence_id": 1,
        "instrument": "XRP-EUR",
        "instrument_public_id": "inst-xrp",
        "exchange": "kraken",
        "mode": "live",
        "quantity": 9.99999993923e-9,
        "average_price": 1.5,
        "unrealized_pnl": -1.2830999922e-9,
        "realized_pnl": 0.0,
        "mark_price": 1.5,
        "marked_at": NOW,
        "source_venue_event_id": None,
        "position_cycle_public_id": "cycle-1",
        "wallet_public_id": "wallet-1",
    }
    base.update(overrides)
    return base


def _client(repo: _SpecRepo) -> TestClient:
    """Return an admin client bound to the spec repository double."""
    app = create_app()
    app.router.lifespan_context = _noop_lifespan

    def skip_csrf() -> None:
        return None

    def skip_auth() -> AuthPrincipal:
        return AuthPrincipal(username="admin", role=UserRole.ADMIN)

    app.dependency_overrides[validate_csrf_token] = skip_csrf
    app.dependency_overrides[require_authentication] = skip_auth
    app.dependency_overrides[get_repository_dependency] = lambda: repo
    return TestClient(app)


def test_dust_flag_stays_optional_in_the_json_schema() -> None:
    """Generated clients may omit is_dust because the server default fills it.

    Given: the position and open-cycle response models,
    When: their JSON schemas are built,
    Then: is_dust has no schema default and is not required.
    """
    for model in (PositionData, PositionCycleData):
        schema = model.model_json_schema()
        assert "default" not in schema["properties"]["is_dust"]
        assert "is_dust" not in schema["required"]


def test_incomplete_position_row_fails_schema_validation() -> None:
    """A row without a quantity cannot become a position payload.

    Given: an empty position row,
    When: it is projected into PositionData,
    Then: schema validation fails after the dust fields are attached.
    """
    with pytest.raises(ValidationError):
        position_data_from_row({}, None, _THRESHOLD)


async def test_spec_reads_skip_blanks_and_duplicate_ids() -> None:
    """Instrument specs are loaded once per distinct non-blank id.

    Given: a blank id and the same instrument id twice,
    When: display steps are loaded,
    Then: the blank id is skipped and the instrument is read once.
    """
    repo = _SpecRepo({"inst-xrp": {"lot_size": 1e-8, "tick_size": 1e-5, "min_order_size": 1.65}})
    loaded = await load_position_spec_steps(repo, ["", "inst-xrp", "inst-xrp"], NOW)
    assert repo.calls == ["inst-xrp"]
    assert loaded["inst-xrp"].lot_size == 1e-8
    assert loaded["inst-xrp"].min_order_size == 1.65


async def test_non_mapping_spec_leaves_precision_empty() -> None:
    """A spec payload that is not a mapping contributes no steps.

    Given: the repository returns a string instead of a spec row,
    When: display steps are loaded,
    Then: lot, tick, and minimum are all absent.
    """
    repo = _SpecRepo({"inst-xrp": "not-a-spec"})
    loaded = await load_position_spec_steps(repo, ["inst-xrp"], NOW)
    assert loaded["inst-xrp"].lot_size is None
    assert loaded["inst-xrp"].tick_size is None
    assert loaded["inst-xrp"].min_order_size is None


async def test_non_string_instrument_id_skips_the_spec_read() -> None:
    """A row whose instrument id is not text does not query the spec.

    Given: a position row with a numeric instrument id,
    When: the position payload is built,
    Then: no spec is read and the notional rule still classifies the row.
    """
    repo = _SpecRepo({})
    payloads = await build_position_payloads(
        repo,
        [_row(instrument_public_id=5, quantity=1.0, mark_price=0.49)],
        NOW,
    )
    assert repo.calls == []
    assert payloads[0].is_dust is True
    assert payloads[0].quantity_decimals is None
    assert payloads[0].price_decimals is None


def test_positions_endpoint_reports_xrp_dust_and_precision() -> None:
    """GET /positions returns the XRP dust example on the response payload.

    Given: a Kraken XRP position whose stored quantity is 9.99999993923e-9,
    When: GET /api/positions is called,
    Then: is_dust is true and quantity_decimals is 8.
    """
    repo = _SpecRepo({"inst-xrp": {"lot_size": 1e-8, "tick_size": 1e-5, "min_order_size": 1.65}})
    repo.get_positions = _positions_returning([_row()])
    response = _client(repo).get("/api/positions")
    assert response.status_code == 200
    item = response.json()["payload"][0]
    assert item["is_dust"] is True
    assert item["quantity_decimals"] == 8
    assert item["price_decimals"] == 5
    assert repo.calls == ["inst-xrp"]


def test_positions_endpoint_uses_notional_when_the_spec_is_missing() -> None:
    """A position with no spec is dust only from its quote notional.

    Given: a 1-unit position marked at 0.10 and no instrument spec,
    When: GET /api/positions is called,
    Then: is_dust is true and both precision fields are null.
    """
    repo = _SpecRepo({})
    repo.get_positions = _positions_returning(
        [_row(quantity=1.0, mark_price=0.10, instrument="DUST-EUR")]
    )
    response = _client(repo).get("/api/positions")
    assert response.status_code == 200
    item = response.json()["payload"][0]
    assert item["is_dust"] is True
    assert item["quantity_decimals"] is None
    assert item["price_decimals"] is None


def test_positions_endpoint_keeps_the_threshold_boundary() -> None:
    """A notional equal to the threshold is not dust.

    Given: a 1-unit position marked at 0.50 and no venue minimum,
    When: GET /api/positions is called,
    Then: is_dust is false.
    """
    repo = _SpecRepo({"inst-xrp": None})
    repo.get_positions = _positions_returning([_row(quantity=1.0, mark_price=0.50)])
    response = _client(repo).get("/api/positions")
    assert response.status_code == 200
    assert response.json()["payload"][0]["is_dust"] is False


def test_open_cycles_report_precision_and_peak_dust() -> None:
    """GET /position-cycles/open classifies the cycle peak against the spec.

    Given: an open cycle whose peak quantity is below the venue minimum,
    When: GET /api/position-cycles/open is called,
    Then: the cycle is dust and carries the instrument precision.
    """
    repo = _SpecRepo({"inst-xrp": {"lot_size": 1e-8, "tick_size": 0.01, "min_order_size": 1.65}})

    async def get_cycles(**_kwargs: Any) -> list[dict[str, object]]:
        return [
            {
                "public_id": "cycle-1",
                "timestamp": NOW,
                "session_id": "s1",
                "sequence_id": 1,
                "instrument_public_id": "inst-xrp",
                "exchange": "kraken",
                "mode": "live",
                "shard_key": "kraken.XRP-EUR.live.wallet-1",
                "wallet_public_id": "wallet-1",
                "operator_public_id": None,
                "direction": "long",
                "max_qty": 1e-8,
                "status": "open",
                "opened_at": NOW,
                "closed_at": None,
                "opening_command_public_id": None,
                "closing_command_public_id": None,
            }
        ]

    repo.get_all_open_position_cycles = get_cycles
    app = create_app()
    app.router.lifespan_context = _noop_lifespan

    def skip_csrf() -> None:
        return None

    def skip_auth() -> AuthPrincipal:
        return AuthPrincipal(
            username="admin",
            role=UserRole.ADMIN,
            permissions=[Permission.READ_POSITION_CYCLES.value, Permission.READ_ALL_SCOPES.value],
        )

    app.dependency_overrides[validate_csrf_token] = skip_csrf
    app.dependency_overrides[require_authentication] = skip_auth
    app.dependency_overrides[get_repository_dependency] = lambda: repo
    response = TestClient(app).get("/api/position-cycles/open")
    assert response.status_code == 200
    item = response.json()["payload"][0]
    assert item["is_dust"] is True
    assert item["quantity_decimals"] == 8
    assert item["price_decimals"] == 2


def _positions_returning(rows: list[PositionRow]) -> Any:
    """Build an async get_positions implementation for the repository double."""

    async def get_positions(**_kwargs: Any) -> list[PositionRow]:
        return rows

    return get_positions
