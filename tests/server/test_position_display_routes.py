"""Tests for dust and precision fields on position API responses."""

from collections.abc import AsyncGenerator
from collections.abc import Mapping
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from decimal import Decimal

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError

from snapper.application.portfolio.fx_rates import currency_pair_key
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository_types import PositionCycleRow
from snapper.data.repository_types import PositionRow
from snapper.messaging.schemas.data import PositionData
from snapper.server import position_display as position_display_module
from snapper.server.app import create_app
from snapper.server.app import get_repository_dependency
from snapper.server.position_cycle_routes import PositionCycleData
from snapper.server.position_display import build_position_payloads
from snapper.server.position_display import load_position_quote_prices
from snapper.server.position_display import load_position_spec_steps
from snapper.server.position_display import load_usd_per_quote
from snapper.server.position_display import position_data_from_row

NOW = datetime(2026, 11, 23, 12, 0, tzinfo=UTC)
_THRESHOLD = Decimal("0.50")


async def _noop_lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    """Disable application lifespan for endpoint-only tests."""
    yield


class _SpecRepo:
    """Repository double that serves one batched instrument-fact read."""

    def __init__(self, facts: dict[str, object]) -> None:
        """Store facts and record each batched id list."""
        self.facts = facts
        self.calls: list[tuple[str, ...]] = []
        self.position_rows: list[PositionRow] = []
        self.cycles: list[PositionCycleRow] = []

    async def get_position_display_facts(
        self,
        instrument_public_ids: Sequence[str],
        as_of: datetime,
    ) -> list[object]:
        """Return one fact per requested id, preserving non-mapping fixtures."""
        del as_of
        self.calls.append(tuple(instrument_public_ids))
        rows: list[object] = []
        for instrument_id in instrument_public_ids:
            if instrument_id not in self.facts:
                continue
            fact = self.facts[instrument_id]
            if isinstance(fact, Mapping):
                stamped = dict(fact)
                stamped.setdefault("instrument_public_id", instrument_id)
                rows.append(stamped)
            else:
                rows.append(fact)
        return rows

    async def get_positions(
        self,
        as_of: datetime,
        wallet_public_ids: list[str] | None = None,
        current_marks: bool = False,
    ) -> list[PositionRow]:
        """Return the configured position rows."""
        del as_of, wallet_public_ids, current_marks
        return self.position_rows

    async def get_all_open_position_cycles(
        self,
        as_of: datetime,
        opened_before: datetime | None = None,
    ) -> list[PositionCycleRow]:
        """Return the configured open cycles."""
        del as_of, opened_before
        return self.cycles


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


def _client(repo: _SpecRepo, principal: AuthPrincipal | None = None) -> TestClient:
    """Return an admin client bound to the spec repository double."""
    app = create_app()
    app.router.lifespan_context = _noop_lifespan

    def skip_csrf() -> None:
        return None

    def skip_auth() -> AuthPrincipal:
        return principal or AuthPrincipal(username="admin", role=UserRole.ADMIN)

    app.dependency_overrides[validate_csrf_token] = skip_csrf
    app.dependency_overrides[require_authentication] = skip_auth
    app.dependency_overrides[get_repository_dependency] = lambda: repo
    return TestClient(app)


def _shared_display_schema(schema: dict[str, object]) -> dict[str, object]:
    """Return the shared display component embedded in a payload schema."""
    defs = schema.get("$defs")
    if not isinstance(defs, dict):
        raise AssertionError("display component is missing")
    component = defs.get("PositionDisplayFields")
    if not isinstance(component, dict):
        raise AssertionError("PositionDisplayFields is missing")
    return component


def _required_names(schema: dict[str, object]) -> set[str]:
    """Collect required names from a schema and its allOf members."""
    names: set[str] = set()
    required = schema.get("required")
    if isinstance(required, list):
        names.update(name for name in required if isinstance(name, str))
    members = schema.get("allOf")
    if isinstance(members, list):
        for member in members:
            if not isinstance(member, dict):
                continue
            nested = member.get("required")
            if isinstance(nested, list):
                names.update(name for name in nested if isinstance(name, str))
    return names


def test_dust_flag_stays_optional_in_the_json_schema() -> None:
    """Generated clients may omit is_dust because the server default fills it.

    Given: the position and open-cycle response models,
    When: their JSON schemas are built,
    Then: both reference one display component, is_dust has no schema
        default, and is_dust is not required.
    """
    for model in (PositionData, PositionCycleData):
        schema = model.model_json_schema()
        component = _shared_display_schema(schema)
        properties = component["properties"]
        if not isinstance(properties, dict):
            raise AssertionError("display properties are missing")
        is_dust = properties["is_dust"]
        if not isinstance(is_dust, dict):
            raise AssertionError("is_dust schema is missing")
        assert "default" not in is_dust
        assert "is_dust" not in _required_names(schema)
        members = schema["allOf"]
        if not isinstance(members, list) or not isinstance(members[0], dict):
            raise AssertionError("display allOf is missing")
        assert members[0]["$ref"] == "#/$defs/PositionDisplayFields"
        remainder = members[1]
        if not isinstance(remainder, dict):
            raise AssertionError("payload fields are missing")
        remainder_properties = remainder["properties"]
        if not isinstance(remainder_properties, dict):
            raise AssertionError("payload properties are missing")
        assert "is_dust" not in remainder_properties
        assert "quantity_decimals" not in remainder_properties
        assert "price_decimals" not in remainder_properties


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
    assert repo.calls == [("inst-xrp",)]
    assert loaded["inst-xrp"].lot_size == 1e-8
    assert loaded["inst-xrp"].min_order_size == 1.65


async def test_fact_without_an_instrument_id_is_skipped() -> None:
    """A fact row with no instrument id cannot be joined to a position.

    Given: fact rows whose instrument id is missing, blank, or not text,
    When: display steps are loaded,
    Then: none of those rows are returned.
    """

    class _BareFacts:
        async def get_position_display_facts(
            self,
            instrument_public_ids: Sequence[str],
            as_of: datetime,
        ) -> list[dict[str, object]]:
            del instrument_public_ids, as_of
            return [
                {"quote": "USD"},
                {"quote": ""},
                {"instrument_public_id": "", "quote": "USD"},
                {"instrument_public_id": 5, "quote": "USD"},
                {"instrument_public_id": "inst-xrp", "quote": 1, "lot_size": 0.01},
            ]

    loaded = await load_position_spec_steps(_BareFacts(), ["inst-xrp"], NOW)
    assert list(loaded) == ["inst-xrp"]
    assert loaded["inst-xrp"].quote_currency is None
    assert loaded["inst-xrp"].lot_size == 0.01


async def test_blank_ids_do_not_query() -> None:
    """An empty instrument set does not read specs or FX rates.

    Given: only blank instrument ids,
    When: display steps are loaded,
    Then: the repository is not called and the result is empty.
    """
    repo = _SpecRepo({})
    loaded = await load_position_spec_steps(repo, ["", ""], NOW)
    assert repo.calls == []
    assert loaded == {}


async def test_non_mapping_spec_is_ignored() -> None:
    """A fact row that is not a mapping contributes no steps.

    Given: the repository returns a string instead of a fact row,
    When: display steps are loaded,
    Then: that instrument is absent.
    """
    repo = _SpecRepo({"inst-xrp": "not-a-spec"})
    loaded = await load_position_spec_steps(repo, ["inst-xrp"], NOW)
    assert "inst-xrp" not in loaded


async def test_non_string_instrument_id_skips_the_spec_read() -> None:
    """A row whose instrument id is not text does not query the spec.

    Given: a position row with a numeric instrument id and a small mark,
    When: the position payload is built,
    Then: no spec is read and the notional rule does not guess a currency.
    """
    repo = _SpecRepo({})
    payloads = await build_position_payloads(
        repo,
        [_row(instrument_public_id=5, quantity=1.0, mark_price=0.49)],
        NOW,
    )
    assert repo.calls == []
    assert payloads[0].is_dust is False
    assert payloads[0].quantity_decimals is None
    assert payloads[0].price_decimals is None


def test_positions_endpoint_reports_xrp_dust_and_precision() -> None:
    """GET /positions returns the XRP dust example on the response payload.

    Given: a Kraken XRP position whose stored quantity is 9.99999993923e-9,
    When: GET /api/positions is called,
    Then: is_dust is true and quantity_decimals is 8.
    """
    repo = _SpecRepo({"inst-xrp": {"lot_size": 1e-8, "tick_size": 1e-5, "min_order_size": 1.65}})
    repo.position_rows = [_row()]
    response = _client(repo).get("/api/positions")
    assert response.status_code == 200
    item = response.json()["payload"][0]
    assert item["is_dust"] is True
    assert item["quantity_decimals"] == 8
    assert item["price_decimals"] == 5
    assert repo.calls == [("inst-xrp",)]


def test_positions_endpoint_uses_notional_when_the_spec_is_missing() -> None:
    """A position with no spec is dust only from its USD notional.

    Given: a 1-unit USD position marked at 0.10 and no instrument spec,
    When: GET /api/positions is called,
    Then: is_dust is true and both precision fields are null.
    """
    repo = _SpecRepo({"inst-xrp": {"quote": "USD", "exchange": "kraken", "base": "DUST"}})
    repo.position_rows = [_row(quantity=1.0, mark_price=0.10, instrument="DUST-USD")]
    response = _client(repo).get("/api/positions")
    assert response.status_code == 200
    item = response.json()["payload"][0]
    assert item["is_dust"] is True
    assert item["quantity_decimals"] is None
    assert item["price_decimals"] is None


def test_positions_endpoint_keeps_the_threshold_boundary() -> None:
    """A USD notional equal to the threshold is not dust.

    Given: a 1-unit USD position marked at 0.50 and no venue minimum,
    When: GET /api/positions is called,
    Then: is_dust is false.
    """
    repo = _SpecRepo({"inst-xrp": {"quote": "USD", "exchange": "kraken", "base": "XRP"}})
    repo.position_rows = [_row(quantity=1.0, mark_price=0.50)]
    response = _client(repo).get("/api/positions")
    assert response.status_code == 200
    assert response.json()["payload"][0]["is_dust"] is False


def test_position_payload_keeps_stale_mark_without_using_it_for_dust() -> None:
    """Expose the stale price to the UI while skipping it for dust classification.

    Given: a stale mark below the dust threshold and no usable entry price,
    When: the positions endpoint builds its payload,
    Then: the mark, evidence time and stale flag survive and the position is visible.
    """
    repo = _SpecRepo({"inst-xrp": {"quote": "USD", "exchange": "kraken", "base": "XRP"}})
    repo.position_rows = [
        _row(
            quantity=1.0,
            mark_price=0.10,
            average_price=None,
            mark_stale=True,
            mark_as_of=NOW,
        )
    ]
    response = _client(repo).get("/api/positions")
    assert response.status_code == 200
    item = response.json()["payload"][0]
    assert item["mark_price"] == 0.10
    assert datetime.fromisoformat(item["mark_as_of"]) == NOW
    assert item["mark_stale"] is True
    assert item["is_dust"] is False


def test_open_cycles_report_precision_and_peak_dust() -> None:
    """GET /position-cycles/open classifies the cycle peak against the spec.

    Given: an open cycle whose peak quantity is below the venue minimum,
    When: GET /api/position-cycles/open is called,
    Then: the cycle is dust and carries the instrument precision.
    """
    repo = _SpecRepo({"inst-xrp": {"lot_size": 1e-8, "tick_size": 0.01, "min_order_size": 1.65}})
    repo.cycles = [_cycle_row(max_qty=1e-8)]
    item = _open_cycles(repo)[0]
    assert item["is_dust"] is True
    assert item["quantity_decimals"] == 8
    assert item["price_decimals"] == 2


def test_open_cycle_agrees_with_the_position_price() -> None:
    """An open cycle uses the same mark as the matching position.

    Given: a 1-unit USD position marked at 0.10 and a cycle with the same peak,
    When: both list routes are called,
    Then: both payloads are dust, and an unmatched cycle is not.
    """
    repo = _SpecRepo({"inst-xrp": {"quote": "USD", "exchange": "kraken", "base": "XRP"}})
    repo.position_rows = [_row(quantity=1.0, mark_price=0.10)]
    repo.cycles = [
        _cycle_row(max_qty=1.0),
        _cycle_row(public_id="cycle-2", wallet_public_id="wallet-2", max_qty=1.0),
    ]
    position = _client(repo).get("/api/positions").json()["payload"][0]
    cycles = {item["wallet_public_id"]: item for item in _open_cycles(repo)}
    assert position["is_dust"] is True
    assert cycles["wallet-1"]["is_dust"] is True
    assert cycles["wallet-2"]["is_dust"] is False


def test_open_cycle_falls_back_to_the_position_average_price() -> None:
    """A non-positive mark does not hide the position's entry price.

    Given: a position whose mark is zero and whose entry notional is 0.10 USD,
    When: the matching cycle is listed,
    Then: the cycle is dust from that entry price.
    """
    repo = _SpecRepo({"inst-xrp": {"quote": "USD", "exchange": "kraken", "base": "XRP"}})
    repo.position_rows = [_row(quantity=1.0, mark_price=0.0, average_price=0.10)]
    repo.cycles = [_cycle_row(max_qty=1.0)]
    assert _open_cycles(repo)[0]["is_dust"] is True


def test_positions_endpoint_prices_a_known_futures_contract() -> None:
    """A linear futures multiplier changes the USD notional.

    Given: one contract with multiplier 10 marked at 0.10 USD,
    When: GET /api/positions is called,
    Then: the 1.00 USD contract notional is not dust.
    """
    repo = _SpecRepo(
        {
            "inst-xrp": {
                "exchange": "kraken_futures",
                "base": "BTC",
                "quote": "USD",
                "instrument_kind": "perpetual",
                "quantity_unit": "contract_count",
                "native_order_quantity_unit": "contract_count",
                "contract_size": Decimal("10"),
                "contract_payoff_model": "linear",
                "contract_value_currency": "BTC",
                "settlement_currency": "USD",
            }
        }
    )
    repo.position_rows = [_row(quantity=1.0, mark_price=0.10, exchange="kraken_futures")]
    response = _client(repo).get("/api/positions")
    assert response.status_code == 200
    assert response.json()["payload"][0]["is_dust"] is False


def test_positions_endpoint_skips_notional_without_an_fx_rate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-USD quote with no portfolio rate keeps only the minimum rule.

    Given: a 0.10 EUR position and an FX load that finds no rate,
    When: GET /api/positions is called,
    Then: is_dust is false.
    """

    async def no_rate(
        repo: object,
        quotes: Sequence[str],
        as_of: datetime,
    ) -> dict[str, Decimal]:
        del repo, quotes, as_of
        return {"USD": Decimal(1)}

    monkeypatch.setattr(position_display_module, "load_usd_per_quote", no_rate)
    repo = _SpecRepo({"inst-xrp": {"quote": "EUR", "exchange": "kraken", "base": "XRP"}})
    repo.position_rows = [_row(quantity=1.0, mark_price=0.10, instrument="XRP-EUR")]
    response = _client(repo).get("/api/positions")
    assert response.status_code == 200
    assert response.json()["payload"][0]["is_dust"] is False


def test_positions_endpoint_converts_eur_with_the_supplied_rate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The position route scales the quote notional by the USD rate.

    Given: one unit marked at 0.20 EUR and a rate of 2 USD per EUR,
    When: GET /api/positions is called,
    Then: the 0.40 USD notional is dust.
    """

    async def eur_rate(
        repo: object,
        quotes: Sequence[str],
        as_of: datetime,
    ) -> dict[str, Decimal]:
        del repo, quotes, as_of
        return {"USD": Decimal(1), "EUR": Decimal(2)}

    monkeypatch.setattr(position_display_module, "load_usd_per_quote", eur_rate)
    repo = _SpecRepo({"inst-xrp": {"quote": "EUR", "exchange": "kraken", "base": "XRP"}})
    repo.position_rows = [_row(quantity=1.0, mark_price=0.20, instrument="XRP-EUR")]
    response = _client(repo).get("/api/positions")
    assert response.status_code == 200
    assert response.json()["payload"][0]["is_dust"] is True


async def test_usd_rates_use_the_portfolio_fx_conversion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-USD quotes are converted by the P&L candle converter.

    Given: a pinned EUR-USD close of 1.25 at the request minute,
    When: USD rates are loaded,
    Then: EUR maps to that close and a quote with no plane is omitted.
    """
    minute = NOW.replace(second=0, microsecond=0)
    plane = ("EUR", "USD", "walutomat")
    fx_rates = {(*plane, minute): 1.25}
    venues = {currency_pair_key("EUR", "USD"): plane}
    seen: dict[str, datetime] = {}

    async def evidence(
        repo: object,
        currencies: frozenset[str],
        start: datetime,
        end: datetime,
        as_of: datetime,
    ) -> tuple[object, object, object]:
        del repo
        seen["start"] = start
        seen["end"] = end
        seen["as_of"] = as_of
        assert currencies == frozenset({"EUR", "BTC"})
        return fx_rates, venues, {}

    monkeypatch.setattr(position_display_module, "load_basket_fiat_evidence", evidence)
    priced_at = NOW.replace(second=30)
    rates = await load_usd_per_quote(_SpecRepo({}), ["USD", "EUR", "BTC", ""], priced_at)
    assert rates["USD"] == Decimal(1)
    assert rates["EUR"] == Decimal("1.25")
    assert "BTC" not in rates
    assert seen == {"start": minute, "end": minute, "as_of": priced_at}


async def test_usd_quotes_do_not_read_fx_candles(monkeypatch: pytest.MonkeyPatch) -> None:
    """USD identity does not discover a forex plane.

    Given: only a USD quote,
    When: USD rates are loaded,
    Then: the rate is 1 and the candle loader is not called.
    """

    async def evidence(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("USD does not need a candle")

    monkeypatch.setattr(position_display_module, "load_basket_fiat_evidence", evidence)
    assert await load_usd_per_quote(_SpecRepo({}), ["USD", ""], NOW) == {"USD": Decimal(1)}


async def test_unusable_fx_conversion_is_omitted(monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing or non-positive conversion result is not stored as a rate.

    Given: the converter returns None, zero, and then a non-finite value,
    When: USD rates are loaded for EUR,
    Then: EUR is omitted every time.
    """
    results = iter((None, 0.0, float("nan")))

    def convert(*_args: object) -> float | None:
        return next(results)

    async def evidence(
        *_args: object, **_kwargs: object
    ) -> tuple[dict[str, float], dict[str, str], dict[str, str]]:
        return {}, {}, {}

    monkeypatch.setattr(position_display_module, "load_basket_fiat_evidence", evidence)
    monkeypatch.setattr(position_display_module, "convert_amount", convert)
    assert "EUR" not in await load_usd_per_quote(_SpecRepo({}), ["EUR"], NOW)
    assert "EUR" not in await load_usd_per_quote(_SpecRepo({}), ["EUR"], NOW)
    assert "EUR" not in await load_usd_per_quote(_SpecRepo({}), ["EUR"], NOW)


async def test_position_prices_ignore_unusable_rows() -> None:
    """Price matching keeps only complete position identities.

    Given: a non-list payload, a string row, and rows missing an identity part,
    When: position prices are loaded,
    Then: only the complete row is returned.
    """

    class _Prices:
        def __init__(self, payload: object) -> None:
            self.payload = payload

        async def get_positions(
            self,
            as_of: datetime,
            wallet_public_ids: list[str] | None = None,
            current_marks: bool = False,
        ) -> object:
            del as_of, wallet_public_ids, current_marks
            return self.payload

    missing = await load_position_quote_prices(_Prices("nope"), NOW)
    assert missing == {}
    assert await load_position_quote_prices(_Prices(5), NOW) == {}
    rows: list[object] = [
        "row",
        {"instrument_public_id": "", "exchange": "kraken", "mode": "live", "wallet_public_id": "w"},
        {"instrument_public_id": "inst", "exchange": "", "mode": "live", "wallet_public_id": "w"},
        {"instrument_public_id": "inst", "exchange": 1, "mode": "live", "wallet_public_id": "w"},
        {"instrument_public_id": "inst", "exchange": "kraken", "mode": "", "wallet_public_id": "w"},
        {"instrument_public_id": "inst", "exchange": "kraken", "mode": 1, "wallet_public_id": "w"},
        {
            "instrument_public_id": "inst",
            "exchange": "kraken",
            "mode": "live",
            "wallet_public_id": "",
        },
        {"instrument_public_id": 1, "exchange": "kraken", "mode": "live", "wallet_public_id": "w"},
        {"instrument_public_id": "inst-xrp", "exchange": "kraken", "mode": "live"},
        {
            "instrument_public_id": "inst-xrp",
            "exchange": "kraken",
            "mode": "live",
            "wallet_public_id": "wallet-1",
            "mark_price": 0.10,
            "average_price": True,
        },
    ]
    prices = await load_position_quote_prices(_Prices(rows), NOW)
    assert prices == {("inst-xrp", "kraken", "live", "wallet-1"): (0.10, None)}


def _cycle_principal() -> AuthPrincipal:
    """Return an admin who can read every open cycle."""
    return AuthPrincipal(
        username="admin",
        role=UserRole.ADMIN,
        permissions=[Permission.READ_POSITION_CYCLES.value, Permission.READ_ALL_SCOPES.value],
    )


def _open_cycles(repo: _SpecRepo) -> list[dict[str, object]]:
    """Call the open-cycle route and return its payload."""
    response = _client(repo, _cycle_principal()).get("/api/position-cycles/open")
    assert response.status_code == 200
    payload: list[dict[str, object]] = response.json()["payload"]
    return payload


def _cycle_row(**overrides: object) -> PositionCycleRow:
    """Build one open-cycle projection."""
    base: dict[str, object] = {
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
        "max_qty": 1.0,
        "status": "open",
        "opened_at": NOW,
        "closed_at": None,
        "opening_command_public_id": None,
        "closing_command_public_id": None,
    }
    base.update(overrides)
    return base
