"""Repository tests for persisted Phase-5B portfolio equity/drawdown samples."""

import json
import math
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import PropertyMock
from unittest.mock import patch
from uuid import uuid7

import pytest
from sqlalchemy import create_engine
from sqlalchemy import select

from snapper.application.portfolio.pnl_anchor_identity import portfolio_pnl_anchor_public_id
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import PortfolioPnlPoint
from snapper.data.models import VenueAccountObservation
from snapper.data.repository import PortfolioPnlSampleConflictError
from snapper.data.repository import PortfolioPnlSampleQuery
from snapper.data.repository import PortfolioPnlSampleScope
from snapper.data.repository import PortfolioPnlSampleScopeError
from snapper.data.repository import PortfolioPnlSampleSupersedeError
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PNL_SAMPLE_CALC_VERSION
from snapper.data.repository_types import PNL_SAMPLE_NEVER_PERSIST_REASONS
from snapper.data.repository_types import PNL_SAMPLE_REASON_CODES
from snapper.data.repository_types import PortfolioPnlSampleRow

_T0 = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)
_M1 = _T0 + timedelta(minutes=1)
_M2 = _T0 + timedelta(minutes=2)
_M3 = _T0 + timedelta(minutes=3)
_M4 = _T0 + timedelta(minutes=4)
_M5 = _T0 + timedelta(minutes=5)
_WALLET = "0000face-0000-7000-8000-0000000000a1"
_OTHER_WALLET = "0000face-0000-7000-8000-0000000000b2"
_ANCHOR_PUBLIC_ID = portfolio_pnl_anchor_public_id(_WALLET, "live", "USD")
_EPOCH = "00000000-0000-7000-8000-000000000102"
_OTHER_EPOCH = "00000000-0000-7000-8000-000000000188"
_SESSION = "00000000-0000-7000-8000-000000000103"
_ORIG = "22222222-0000-7000-8000-000000000002"

_VALID_VALUATION = {
    "kind": "crypto_candle",
    "orientation": "crypto",
    "currency": "BTC",
    "base": "BTC",
    "quote": "USD",
    "exchange": "kraken",
    "rate": 65000.0,
    "close": 65000.0,
    "candle": {
        "candle_id": 42,
        "candle_public_id": "cndl-1",
        "candle_timestamp": "2026-07-20T12:00:00+00:00",
        "candle_open_at": "2026-07-20T11:59:00+00:00",
        "instrument_public_id": "inst-btc",
        "native_symbol": "BTC/USD",
    },
}
_VALID_OBSERVATION = {
    "observation_public_id": "obs-1",
    "exchange": "kraken",
    "balance_observed_at": "2026-07-20T12:00:30+00:00",
}
_VALID_COVERAGE = {
    "leveraged_inventory_excluded": False,
    "non_finite_position_excluded": False,
    "venue_scope": "spot_only",
    "external_flows_adjusted": False,
}


def _complete_audit(
    valuation: list[object] | None = None,
    observations: list[object] | None = None,
    coverage: object | None = None,
    **extra: object,
) -> str:
    """Serialize one complete-row audit envelope with A3/A5 provenance records."""
    envelope: dict[str, object] = {
        "valuation": [dict(_VALID_VALUATION)] if valuation is None else valuation,
        "observations": [dict(_VALID_OBSERVATION)] if observations is None else observations,
        "coverage": dict(_VALID_COVERAGE) if coverage is None else coverage,
    }
    envelope.update(extra)
    return json.dumps(envelope)


def _incomplete_audit(reason_codes: object = ("missing_mark",)) -> str:
    """Serialize one incomplete-row audit envelope with a reason-code list."""
    codes = list(reason_codes) if isinstance(reason_codes, tuple) else reason_codes
    diagnostics = (
        [
            {"stage": "test", "cause": "test_cause", "reason_code": code}
            for code in codes
            if isinstance(code, str)
        ]
        if isinstance(codes, list)
        else []
    )
    return json.dumps(
        {
            "valuation": [],
            "observations": [],
            "reason_codes": codes,
            "diagnostics": diagnostics,
            "coverage": dict(_VALID_COVERAGE),
        }
    )


_COMPLETE_AUDIT_JSON = _complete_audit()


def _complete_sample(
    point_time: datetime = _M1,
    public_id: str | None = None,
    timestamp: datetime | None = None,
) -> PortfolioPnlSampleRow:
    """Build one canonical complete Phase-5B sample carrying every field."""
    return {
        "public_id": public_id if public_id is not None else str(uuid7()),
        "session_id": _SESSION,
        "sequence_id": 10,
        "timestamp": timestamp if timestamp is not None else point_time,
        "wallet_public_id": _WALLET,
        "mode": "live",
        "valuation_ccy": "USD",
        "point_time": point_time,
        "point_kind": "sample",
        "epoch_public_id": _EPOCH,
        "calc_version": PNL_SAMPLE_CALC_VERSION,
        "valuation_status": "complete",
        "realized_pnl": 12.5,
        "fee_pnl": -0.75,
        "accrual_pnl": 0.2,
        "external_flow_adjustment": 0.0,
        "unrealized_pnl": 33.0,
        "cash_usd": 1000.0,
        "position_value_usd": 250.0,
        "drawdown": 0.1,
        "mark_source": "finalized_1m_candle_close",
        "mark_time": point_time,
        "audit_json": _COMPLETE_AUDIT_JSON,
        "watermarks_json": "{}",
    }


def _incomplete_sample(
    point_time: datetime = _M1,
    public_id: str | None = None,
    reasons: tuple[str, ...] = ("missing_mark",),
) -> PortfolioPnlSampleRow:
    """Build one canonical incomplete sample with a reason-code list."""
    return {
        "public_id": public_id if public_id is not None else str(uuid7()),
        "session_id": _SESSION,
        "sequence_id": 11,
        "timestamp": point_time,
        "wallet_public_id": _WALLET,
        "mode": "live",
        "valuation_ccy": "USD",
        "point_time": point_time,
        "point_kind": "sample",
        "epoch_public_id": _EPOCH,
        "calc_version": PNL_SAMPLE_CALC_VERSION,
        "valuation_status": "incomplete",
        "realized_pnl": 12.5,
        "fee_pnl": -0.75,
        "accrual_pnl": 0.2,
        "external_flow_adjustment": 0.0,
        "unrealized_pnl": None,
        "cash_usd": None,
        "position_value_usd": None,
        "drawdown": None,
        "mark_source": None,
        "mark_time": None,
        "audit_json": _incomplete_audit(reasons),
        "watermarks_json": "{}",
    }


def _mutate(base: PortfolioPnlSampleRow, **overrides: object) -> PortfolioPnlSampleRow:
    """Overlay a deliberately invalid runtime payload for guard tests."""
    return cast(PortfolioPnlSampleRow, {**base, **overrides})


def _scope(
    epoch: str = _EPOCH,
    anchor_point_time: datetime = _T0,
    wallet: str = _WALLET,
    mode: str = "live",
) -> PortfolioPnlSampleScope:
    """Build one write scope pinning a sample chunk to its anchor."""
    return PortfolioPnlSampleScope(
        wallet_public_id=wallet,
        mode=mode,
        valuation_ccy="USD",
        epoch_public_id=epoch,
        anchor_point_time=anchor_point_time,
    )


def _query(epoch: str = _EPOCH, calc: str = PNL_SAMPLE_CALC_VERSION) -> PortfolioPnlSampleQuery:
    """Build one read query pinning the exact-predicate sample reads."""
    return PortfolioPnlSampleQuery(
        wallet_public_id=_WALLET,
        mode="live",
        valuation_ccy="USD",
        epoch_public_id=epoch,
        calc_version=calc,
    )


def _anchor_orm(epoch: str = _EPOCH, point_time: datetime = _T0) -> PortfolioPnlPoint:
    """Build the durable activation anchor the sample writers verify against."""
    return PortfolioPnlPoint(
        public_id=_ANCHOR_PUBLIC_ID,
        session_id=_SESSION,
        sequence_id=1,
        timestamp=point_time,
        wallet_public_id=_WALLET,
        mode="live",
        valuation_ccy="USD",
        point_time=point_time,
        point_kind="anchor",
        epoch_public_id=epoch,
        calc_version="5A.13",
        valuation_status="complete",
        realized_pnl=0.0,
        fee_pnl=0.0,
        accrual_pnl=0.0,
        external_flow_adjustment=0.0,
        unrealized_pnl=5.0,
        mark_source="finalized_1m",
        mark_time=point_time,
        opening_basket_json="{}",
        known_to=KNOWN_TO_MAX,
    )


def _observation(
    exchange: str = "kraken",
    timestamp: datetime = _M1,
    balance_status: str = "observed",
    balances_json: str | None = "[]",
    error: str | None = None,
) -> VenueAccountObservation:
    """Build one append-only observation attempt row for the temporal read."""
    observed = balance_status == "observed"
    return VenueAccountObservation(
        wallet_public_id=_WALLET,
        exchange=exchange,
        mode="live",
        attempt_status="observed" if observed else "error",
        balance_status=balance_status,
        position_status="not_applicable" if observed else "error",
        balances_json=balances_json,
        open_positions_json=None,
        balance_observed_at=timestamp - timedelta(seconds=30) if observed else None,
        position_observed_at=None,
        error=error,
        session_id=_SESSION,
        sequence_id=1,
        timestamp=timestamp,
        known_to=KNOWN_TO_MAX,
    )


@pytest.fixture
async def bare_repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create an isolated repository with the tables but no activation anchor."""
    db_path = tmp_path / "pnl-sample.db"
    schema_engine = create_engine(f"sqlite:///{db_path}")
    PortfolioPnlPoint.__table__.create(schema_engine)
    VenueAccountObservation.__table__.create(schema_engine)
    schema_engine.dispose()
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    try:
        yield repo
    finally:
        await repo.engine.dispose()


@pytest.fixture
async def repository(
    bare_repository: SQLAlchemyRepository,
) -> AsyncIterator[SQLAlchemyRepository]:
    """Yield a repository whose scope already carries its verified USD anchor."""
    async with bare_repository.session() as s:
        s.add(_anchor_orm())
        await s.commit()
    yield bare_repository


async def _active_samples(repository: SQLAlchemyRepository) -> list[PortfolioPnlPoint]:
    """Return every currently active sample ORM row for assertions."""
    async with repository.session() as s:
        return list(
            (
                await s.execute(
                    select(PortfolioPnlPoint).where(
                        PortfolioPnlPoint.point_kind == "sample",
                        PortfolioPnlPoint.known_to == KNOWN_TO_MAX,
                    )
                )
            )
            .scalars()
            .all()
        )


def test_validator_accepts_a_canonical_complete_sample() -> None:
    """A fully valued complete sample passes the combined-status truth table."""
    SQLAlchemyRepository._validate_portfolio_pnl_sample(_complete_sample(), _scope())


def test_validator_accepts_a_canonical_incomplete_sample() -> None:
    """A null-valued incomplete sample with a reason code is accepted."""
    SQLAlchemyRepository._validate_portfolio_pnl_sample(_incomplete_sample(), _scope())


def test_complete_sample_refuses_a_diagnostics_key() -> None:
    """Complete rows refuse diagnostics even when the list is empty."""
    sample = _complete_sample()
    audit = json.loads(sample["audit_json"])
    audit["diagnostics"] = []
    sample["audit_json"] = json.dumps(audit)
    s5778_value_1 = _scope()
    with pytest.raises(ValueError, match="must carry no diagnostics"):
        SQLAlchemyRepository._validate_portfolio_pnl_sample(sample, s5778_value_1)


@pytest.mark.parametrize(
    "record",
    [
        "not-an-object",
        {"stage": "", "cause": "cause", "reason_code": "missing_mark"},
        {"stage": "stage", "cause": "cause"},
        {"stage": "stage", "cause": "cause", "reason_code": "unknown"},
        {
            "stage": "stage",
            "cause": "cause",
            "reason_code": "missing_mark",
            "exchange": "",
        },
    ],
)
def test_diagnostic_record_rejects_invalid_shape(record: object) -> None:
    """Diagnostic records require canonical codes and nonempty string fields."""
    sample = _incomplete_sample()
    audit = json.loads(sample["audit_json"])
    audit["diagnostics"] = [record]
    sample["audit_json"] = json.dumps(audit)
    s5778_value_1 = _scope()
    with pytest.raises(ValueError):
        SQLAlchemyRepository._validate_portfolio_pnl_sample(sample, s5778_value_1)


def test_diagnostics_must_explain_exactly_the_reason_codes() -> None:
    """The diagnostic code set equals the persisted reason-code set."""
    sample = _incomplete_sample()
    audit = json.loads(sample["audit_json"])
    audit["diagnostics"] = []
    sample["audit_json"] = json.dumps(audit)
    s5778_value_1 = _scope()
    with pytest.raises(ValueError, match="must explain exactly"):
        SQLAlchemyRepository._validate_portfolio_pnl_sample(sample, s5778_value_1)


def test_diagnostics_are_capped() -> None:
    """The DAL refuses an oversized diagnostics list."""
    sample = _incomplete_sample()
    audit = json.loads(sample["audit_json"])
    audit["diagnostics"] = [
        {"stage": "test", "cause": str(index), "reason_code": "missing_mark"} for index in range(65)
    ]
    sample["audit_json"] = json.dumps(audit)
    s5778_value_1 = _scope()
    with pytest.raises(ValueError, match="exceed the maximum"):
        SQLAlchemyRepository._validate_portfolio_pnl_sample(sample, s5778_value_1)


def test_diagnostic_record_accepts_an_unknown_cause() -> None:
    """Stage and cause remain deliberately open vocabularies."""
    sample = _incomplete_sample()
    audit = json.loads(sample["audit_json"])
    audit["diagnostics"][0]["cause"] = "future_upstream_taxonomy"
    sample["audit_json"] = json.dumps(audit)
    SQLAlchemyRepository._validate_portfolio_pnl_sample(sample, _scope())


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        pytest.param({"point_kind": "anchor"}, "point_kind must be 'sample'", id="point_kind"),
        pytest.param({"mode": "paper"}, "mode must be 'live'", id="paper_mode"),
        pytest.param({"valuation_ccy": 123}, "must be a string", id="ccy_not_string"),
        pytest.param({"valuation_ccy": "usd"}, "must be 'USD'", id="ccy_not_normalized"),
        pytest.param({"valuation_ccy": "EUR"}, "must be 'USD'", id="ccy_not_usd"),
        pytest.param({"valuation_ccy": "US"}, "three-letter", id="ccy_bad_length"),
        pytest.param({"point_time": _T0}, "strictly after the anchor t0", id="t0_collision"),
        pytest.param(
            {"point_time": _T0 - timedelta(minutes=1)},
            "strictly after the anchor t0",
            id="before_t0",
        ),
        pytest.param({"epoch_public_id": _OTHER_EPOCH}, "epoch_public_id", id="wrong_epoch"),
        pytest.param({"calc_version": "5B.0"}, "calc_version", id="wrong_calc_version"),
        pytest.param({"realized_pnl": math.inf}, "realized_pnl must be finite", id="realized_inf"),
        pytest.param({"fee_pnl": math.nan}, "fee_pnl must be finite", id="fee_nan"),
        pytest.param({"accrual_pnl": math.inf}, "accrual_pnl must be finite", id="accrual_inf"),
        pytest.param(
            {"external_flow_adjustment": math.inf},
            "external_flow_adjustment must be finite",
            id="flow_inf",
        ),
        pytest.param(
            {"external_flow_adjustment": 5.0},
            "external_flow_adjustment must be 0.0",
            id="flow_nonzero",
        ),
        pytest.param(
            {"valuation_status": "partial"}, "must be 'complete' or 'incomplete'", id="bad_status"
        ),
        pytest.param(
            {"unrealized_pnl": None}, "unrealized_pnl must be finite", id="unrealized_none"
        ),
        pytest.param(
            {"unrealized_pnl": math.inf}, "unrealized_pnl must be finite", id="unrealized_inf"
        ),
        pytest.param({"mark_source": None}, "mark_source must be nonempty", id="mark_source_none"),
        pytest.param({"mark_source": "  "}, "mark_source must be nonempty", id="mark_source_blank"),
        pytest.param({"mark_time": None}, "mark_time must be present", id="mark_time_none"),
        pytest.param({"cash_usd": None}, "cash_usd must be finite", id="cash_none"),
        pytest.param({"cash_usd": math.inf}, "cash_usd must be finite", id="cash_inf"),
        pytest.param(
            {"position_value_usd": None},
            "position_value_usd must be finite",
            id="position_none",
        ),
        pytest.param(
            {"position_value_usd": math.inf},
            "position_value_usd must be finite",
            id="position_inf",
        ),
        pytest.param({"drawdown": None}, "drawdown must be finite within", id="drawdown_none"),
        pytest.param({"drawdown": math.inf}, "drawdown must be finite within", id="drawdown_inf"),
        pytest.param({"drawdown": 1.5}, "drawdown must be finite within", id="drawdown_high"),
        pytest.param({"drawdown": -0.1}, "drawdown must be finite within", id="drawdown_low"),
        pytest.param(
            {"point_time": datetime(2026, 7, 20, 12, 1, 30, tzinfo=UTC)},
            "aligned to a minute",
            id="non_grid_seconds",
        ),
        pytest.param(
            {"point_time": _M1.replace(microsecond=1)},
            "aligned to a minute",
            id="non_grid_micros",
        ),
        pytest.param(
            {"point_time": datetime(2026, 7, 20, 12, 1)},
            "must be UTC",
            id="naive_point_time",
        ),
        pytest.param({"calc_version": "5B.9"}, "sample constant", id="wrong_constant_calc"),
        pytest.param(
            {"audit_json": _complete_audit(reason_codes=["missing_mark"])},
            "must carry no reason codes",
            id="complete_with_reason",
        ),
        pytest.param(
            {
                "audit_json": json.dumps(
                    {"valuation": {}, "observations": [dict(_VALID_OBSERVATION)]}
                )
            },
            "valuation must be a present list",
            id="valuation_not_list",
        ),
        pytest.param(
            {"audit_json": json.dumps({"valuation": [dict(_VALID_VALUATION)], "observations": {}})},
            "observations must be a present list",
            id="observations_not_list",
        ),
        pytest.param(
            {"audit_json": json.dumps({"valuation": [dict(_VALID_VALUATION)], "extra": 1})},
            "unknown top-level keys",
            id="unknown_top_key",
        ),
        pytest.param({"audit_json": ""}, "audit_json must be a nonempty string", id="audit_empty"),
        pytest.param({"audit_json": "not json{"}, "must contain valid JSON", id="audit_bad_json"),
        pytest.param({"audit_json": "[1, 2]"}, "must contain a JSON object", id="audit_not_object"),
    ],
)
def test_validator_rejects_complete_row_shapes(overrides: dict[str, object], message: str) -> None:
    """Every complete-branch and scope guard rejects its malformed field."""
    s5778_value_1 = _mutate(_complete_sample(), **overrides)
    s5778_value_2 = _scope()
    with pytest.raises(ValueError, match=message):
        SQLAlchemyRepository._validate_portfolio_pnl_sample(s5778_value_1, s5778_value_2)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        pytest.param({"unrealized_pnl": 1.0}, "unrealized_pnl must be null", id="unrealized_set"),
        pytest.param({"mark_source": "x"}, "mark_source must be null", id="mark_source_set"),
        pytest.param({"mark_time": _M1}, "mark_time must be null", id="mark_time_set"),
        pytest.param({"cash_usd": 1.0}, "cash_usd must be null", id="cash_set"),
        pytest.param(
            {"position_value_usd": 1.0}, "position_value_usd must be null", id="position_set"
        ),
        pytest.param({"drawdown": 0.1}, "drawdown must be null", id="drawdown_set"),
        pytest.param(
            {"audit_json": json.dumps({"reason_codes": ["missing_mark"]})},
            "valuation must be a present list",
            id="incomplete_missing_valuation_key",
        ),
        pytest.param(
            {"audit_json": _incomplete_audit([])}, "at least one reason code", id="reasons_empty"
        ),
        pytest.param(
            {"audit_json": _incomplete_audit({})},
            "reason_codes must be a list",
            id="reasons_not_list",
        ),
        pytest.param(
            {"audit_json": _incomplete_audit([1])},
            "reason_codes must be strings",
            id="reasons_not_str",
        ),
        pytest.param(
            {"audit_json": _incomplete_audit(["nope"])},
            "must be canonical",
            id="reasons_unknown",
        ),
        pytest.param(
            {"audit_json": _incomplete_audit(["missing_mark", "missing_mark"])},
            "must not repeat",
            id="reasons_duplicate",
        ),
        pytest.param(
            {"audit_json": _incomplete_audit(["pnl_untrusted"])},
            "must never be persisted",
            id="reasons_untrusted",
        ),
        pytest.param(
            {
                "audit_json": json.dumps(
                    {
                        "valuation": [{"kind": "bogus"}],
                        "observations": [],
                        "reason_codes": ["missing_mark"],
                    }
                )
            },
            "kind is not canonical",
            id="incomplete_invalid_valuation_record",
        ),
    ],
)
def test_validator_rejects_incomplete_row_shapes(
    overrides: dict[str, object], message: str
) -> None:
    """Every incomplete-branch guard rejects its malformed field."""
    s5778_value_1 = _mutate(_incomplete_sample(), **overrides)
    s5778_value_2 = _scope()
    with pytest.raises(ValueError, match=message):
        SQLAlchemyRepository._validate_portfolio_pnl_sample(s5778_value_1, s5778_value_2)


@pytest.mark.parametrize(
    "scope",
    [
        pytest.param(_scope(wallet=_OTHER_WALLET), id="wallet"),
        pytest.param(_scope(mode="paper"), id="mode"),
        pytest.param(
            PortfolioPnlSampleScope(_WALLET, "live", "EUR", _EPOCH, _T0),
            id="valuation_ccy",
        ),
    ],
)
def test_validator_rejects_scope_mismatch(scope: PortfolioPnlSampleScope) -> None:
    """A sample whose identity diverges from its write scope is refused."""
    s5778_value_1 = _complete_sample()
    with pytest.raises(ValueError, match="does not match its write scope"):
        SQLAlchemyRepository._validate_portfolio_pnl_sample(s5778_value_1, scope)


@pytest.mark.parametrize(
    ("audit", "message"),
    [
        pytest.param(
            {"valuation": [123], "observations": [dict(_VALID_OBSERVATION)]},
            "valuation record must be an object",
            id="valuation_not_object",
        ),
        pytest.param(
            {"valuation": [{**_VALID_VALUATION, "kind": "bogus"}]},
            "kind is not canonical",
            id="bad_kind",
        ),
        pytest.param(
            {"valuation": [{k: v for k, v in _VALID_VALUATION.items() if k != "base"}]},
            "valuation record base must be a nonempty string",
            id="missing_base",
        ),
        pytest.param(
            {"valuation": [{k: v for k, v in _VALID_VALUATION.items() if k != "rate"}]},
            "rate must be a finite number",
            id="missing_rate",
        ),
        pytest.param(
            {"valuation": [{**_VALID_VALUATION, "rate": "many"}]},
            "rate must be a finite number",
            id="non_numeric_rate",
        ),
        pytest.param(
            {"valuation": [{k: v for k, v in _VALID_VALUATION.items() if k != "close"}]},
            "close must be a finite number",
            id="missing_close",
        ),
        pytest.param(
            {"valuation": [{k: v for k, v in _VALID_VALUATION.items() if k != "candle"}]},
            "must carry a candle identity",
            id="priced_missing_candle",
        ),
        pytest.param(
            {"valuation": [{**_VALID_VALUATION, "kind": "identity", "orientation": "identity"}]},
            "identity valuation record must carry no candle",
            id="identity_with_candle",
        ),
        pytest.param(
            {"valuation": [{k: v for k, v in _VALID_VALUATION.items() if k != "currency"}]},
            "currency must be a nonempty string",
            id="missing_currency",
        ),
        pytest.param(
            {"valuation": [{**_VALID_VALUATION, "orientation": "sideways"}]},
            "orientation is not canonical",
            id="bad_orientation",
        ),
        pytest.param(
            {
                "valuation": [
                    {
                        **_VALID_VALUATION,
                        "candle": {
                            k: v
                            for k, v in cast(dict[str, object], _VALID_VALUATION["candle"]).items()
                            if k != "candle_open_at"
                        },
                    }
                ]
            },
            "candle_open_at must be a nonempty string",
            id="candle_missing_open_at",
        ),
        pytest.param(
            {
                "valuation": [
                    {
                        **_VALID_VALUATION,
                        "candle": {"candle_public_id": "x", "candle_timestamp": "t"},
                    }
                ]
            },
            "candle_id must be an integer",
            id="candle_missing_id",
        ),
        pytest.param(
            {
                "valuation": [
                    {**_VALID_VALUATION, "candle": {"candle_id": 1, "candle_timestamp": "t"}}
                ]
            },
            "candle_public_id must be a nonempty string",
            id="candle_missing_public_id",
        ),
        pytest.param(
            {
                "valuation": [
                    {**_VALID_VALUATION, "candle": {"candle_id": 1, "candle_public_id": "x"}}
                ]
            },
            "candle_timestamp must be a nonempty string",
            id="candle_missing_timestamp",
        ),
        pytest.param(
            {"observations": [789]},
            "observation record must be an object",
            id="observation_not_object",
        ),
        pytest.param(
            {
                "observations": [
                    {k: v for k, v in _VALID_OBSERVATION.items() if k != "observation_public_id"}
                ]
            },
            "observation record observation_public_id must be a nonempty string",
            id="observation_missing_public_id",
        ),
        pytest.param(
            {"observations": []}, "observations must be non-empty", id="observations_empty"
        ),
        pytest.param(
            {"valuation": []}, "valuation must be non-empty", id="valuation_empty_nonzero"
        ),
    ],
)
def test_validator_rejects_complete_audit_records(audit: dict[str, object], message: str) -> None:
    """The complete-row audit enforces the A3/A5 per-record schema (M1)."""
    envelope = {
        "valuation": audit.get("valuation", [dict(_VALID_VALUATION)]),
        "observations": audit.get("observations", [dict(_VALID_OBSERVATION)]),
    }
    s5778_value_1 = _mutate(_complete_sample(), audit_json=json.dumps(envelope))
    s5778_value_2 = _scope()
    with pytest.raises(ValueError, match=message):
        SQLAlchemyRepository._validate_portfolio_pnl_sample(s5778_value_1, s5778_value_2)


def test_validator_accepts_empty_basket_zero_equity_complete() -> None:
    """A zero-equity complete sample may carry an empty valuation list."""
    row = _mutate(
        _complete_sample(),
        cash_usd=0.0,
        position_value_usd=0.0,
        drawdown=0.0,
        audit_json=_complete_audit(valuation=[]),
    )
    SQLAlchemyRepository._validate_portfolio_pnl_sample(row, _scope())


def test_validator_accepts_an_identity_valuation_record() -> None:
    """A USD identity leg is priced without a candle version identity."""
    identity = {
        "kind": "identity",
        "orientation": "identity",
        "currency": "USD",
        "base": "USD",
        "quote": "USD",
        "exchange": "kraken",
        "rate": 1.0,
        "close": 1.0,
    }
    row = _mutate(_complete_sample(), audit_json=_complete_audit(valuation=[identity]))
    SQLAlchemyRepository._validate_portfolio_pnl_sample(row, _scope())


def test_validator_rejects_non_finite_audit_numbers() -> None:
    """An audit envelope carrying a non-finite number is refused."""
    poisoned = _mutate(
        _complete_sample(),
        audit_json='{"valuation": [{"rate": 1e400}], "observations": []}',
    )
    s5778_value_1 = _scope()
    with pytest.raises(ValueError, match="must be finite"):
        SQLAlchemyRepository._validate_portfolio_pnl_sample(poisoned, s5778_value_1)


async def test_writer_inserts_complete_and_incomplete_through_db_checks(
    repository: SQLAlchemyRepository,
) -> None:
    """A validator-accepted row also satisfies every portfolio_pnl_points CHECK."""
    result = await repository.record_portfolio_pnl_samples(
        [_complete_sample(_M1), _incomplete_sample(_M2)], _scope()
    )
    assert result == {"inserted": (_M1, _M2), "already_present": (), "conflicts": ()}
    stored = await repository.get_portfolio_pnl_samples(_query(), _M1, _M2)
    assert [row["point_time"] for row in stored] == [_M1, _M2]
    assert [row["valuation_status"] for row in stored] == ["complete", "incomplete"]
    assert stored[0]["audit_json"] == _COMPLETE_AUDIT_JSON


async def test_writer_empty_chunk_returns_empty_result(
    repository: SQLAlchemyRepository,
) -> None:
    """An empty catch-up chunk performs no write and reports nothing."""
    result = await repository.record_portfolio_pnl_samples([], _scope())
    assert result == {"inserted": (), "already_present": (), "conflicts": ()}


async def test_writer_rejects_a_repeated_minute_in_one_chunk(
    repository: SQLAlchemyRepository,
) -> None:
    """A chunk repeating a minute is refused before any row is written."""
    s5778_value_1 = _complete_sample(_M1)
    s5778_value_2 = _incomplete_sample(_M1)
    s5778_value_3 = _scope()
    with pytest.raises(ValueError, match="must not repeat a minute"):
        await repository.record_portfolio_pnl_samples([s5778_value_1, s5778_value_2], s5778_value_3)
    assert await _active_samples(repository) == []


async def test_writer_rejects_the_whole_chunk_on_one_invalid_row(
    repository: SQLAlchemyRepository,
) -> None:
    """One invalid row rolls back the whole chunk (batch atomicity)."""
    invalid = _mutate(_complete_sample(_M2), drawdown=2.0)
    s5778_value_1 = _complete_sample(_M1)
    s5778_value_2 = _scope()
    with pytest.raises(ValueError, match="drawdown must be finite within"):
        await repository.record_portfolio_pnl_samples([s5778_value_1, invalid], s5778_value_2)
    assert await _active_samples(repository) == []


async def test_writer_winner_idempotency_skips_an_identical_active_row(
    repository: SQLAlchemyRepository,
) -> None:
    """Re-recording a byte-identical minute is an idempotent success skip."""
    public_id = "11111111-0000-7000-8000-000000000001"
    first = await repository.record_portfolio_pnl_samples(
        [_complete_sample(_M1, public_id=public_id)], _scope()
    )
    assert first["inserted"] == (_M1,)
    second = await repository.record_portfolio_pnl_samples(
        [_complete_sample(_M1, public_id=public_id)], _scope()
    )
    assert second == {"inserted": (), "already_present": (_M1,), "conflicts": ()}
    assert len(await _active_samples(repository)) == 1


async def test_writer_reports_a_different_active_row_as_conflict(
    repository: SQLAlchemyRepository,
) -> None:
    """A different active row for the minute is a conflict, never overwritten."""
    await repository.record_portfolio_pnl_samples([_complete_sample(_M1)], _scope())
    changed = _mutate(_complete_sample(_M1), cash_usd=9999.0)
    result = await repository.record_portfolio_pnl_samples([changed], _scope())
    assert result == {"inserted": (), "already_present": (), "conflicts": (_M1,)}
    stored = await repository.get_portfolio_pnl_samples(_query(), _M1, _M1)
    assert stored[0]["cash_usd"] == 1000.0


async def test_derived_suffix_reconciliation_supersedes_a_complete_sample(
    repository: SQLAlchemyRepository,
) -> None:
    """A changed complete suffix minute remains supersedable."""
    original = _complete_sample(_M1, public_id="22222222-0000-7000-8000-000000000002")
    await repository.record_portfolio_pnl_samples([original], _scope())
    replacement = _mutate(
        _complete_sample(_M1),
        realized_pnl=99.0,
        timestamp=_M1 + timedelta(minutes=3),
    )
    superseded = await repository.supersede_portfolio_pnl_sample(
        _scope(),
        replacement,
        derived_suffix_reconciliation=True,
        expected_public_id=original["public_id"],
    )
    assert superseded["realized_pnl"] == 99.0
    assert superseded["public_id"] == original["public_id"]
    active = await _active_samples(repository)
    assert len(active) == 1
    assert active[0].realized_pnl == 99.0


async def test_derived_suffix_reconciliation_supersedes_an_incomplete_sample(
    repository: SQLAlchemyRepository,
) -> None:
    """A changed incomplete suffix minute remains supersedable."""
    await repository.record_portfolio_pnl_samples(
        [_incomplete_sample(_M1, public_id=_ORIG, reasons=("missing_mark",))], _scope()
    )
    healed = await repository.supersede_portfolio_pnl_sample(
        _scope(),
        _mutate(_complete_sample(_M1), timestamp=_M1 + timedelta(minutes=5)),
        derived_suffix_reconciliation=True,
        expected_public_id=_ORIG,
    )
    assert healed["valuation_status"] == "complete"


async def test_supersede_refuses_a_non_derived_write(
    repository: SQLAlchemyRepository,
) -> None:
    """The money writer accepts only derived suffix reconciliations."""
    await repository.record_portfolio_pnl_samples(
        [_complete_sample(_M1, public_id=_ORIG)], _scope()
    )
    s5778_value_1 = _scope()
    s5778_value_2 = _mutate(_complete_sample(_M1), timestamp=_M1 + timedelta(minutes=5))
    with pytest.raises(
        PortfolioPnlSampleSupersedeError, match="requires a derived suffix reconciliation"
    ):
        await repository.supersede_portfolio_pnl_sample(
            s5778_value_1,
            s5778_value_2,
            derived_suffix_reconciliation=False,
            expected_public_id=_ORIG,
        )


@pytest.mark.parametrize("code", sorted(PNL_SAMPLE_REASON_CODES - PNL_SAMPLE_NEVER_PERSIST_REASONS))
async def test_incomplete_sample_accepts_every_canonical_code(
    repository: SQLAlchemyRepository, code: str
) -> None:
    """Every canonical, persistable reason code is writable by the validator.

    Parametrized off the shared declaration, so a code added to the contract but
    unreachable through the writer fails here instead of at the first production
    row that tries to carry it.
    """
    await repository.record_portfolio_pnl_samples(
        [_incomplete_sample(_M1, reasons=(code,))], _scope()
    )
    rows = await repository.get_portfolio_pnl_samples(_query(), _M1, _M1)
    assert json.loads(rows[0]["audit_json"])["reason_codes"] == [code]


async def test_supersede_refuses_when_no_active_sample_exists(
    repository: SQLAlchemyRepository,
) -> None:
    """Superseding a minute with no active sample is refused."""
    s5778_value_1 = _scope()
    s5778_value_2 = _complete_sample(_M1)
    with pytest.raises(PortfolioPnlSampleSupersedeError, match="no active row"):
        await repository.supersede_portfolio_pnl_sample(
            s5778_value_1,
            s5778_value_2,
            derived_suffix_reconciliation=True,
            expected_public_id=_ORIG,
        )


async def test_supersede_refuses_a_scope_epoch_the_anchor_disowns(
    repository: SQLAlchemyRepository,
) -> None:
    """A supersede scope whose epoch the DB anchor disowns is refused (B1)."""
    await repository.record_portfolio_pnl_samples([_complete_sample(_M1)], _scope())
    other_scope = _scope(epoch=_OTHER_EPOCH)
    replacement = _mutate(_complete_sample(_M1), epoch_public_id=_OTHER_EPOCH)
    with pytest.raises(PortfolioPnlSampleScopeError, match="does not match the active anchor"):
        await repository.supersede_portfolio_pnl_sample(
            other_scope,
            replacement,
            derived_suffix_reconciliation=True,
            expected_public_id=_ORIG,
        )


async def test_supersede_refuses_a_cross_epoch_active_row(
    repository: SQLAlchemyRepository,
) -> None:
    """Even with a valid scope, a stray active row from another epoch is refused."""
    async with repository.session() as s:
        s.add(
            PortfolioPnlPoint(
                **SQLAlchemyRepository._portfolio_pnl_sample_orm_kwargs(
                    _mutate(_complete_sample(_M1), public_id=_ORIG, epoch_public_id=_OTHER_EPOCH)
                ),
                known_to=KNOWN_TO_MAX,
            )
        )
        await s.commit()
    s5778_value_1 = _scope()
    s5778_value_2 = _mutate(_complete_sample(_M1), timestamp=_M1 + timedelta(minutes=5))
    with pytest.raises(PortfolioPnlSampleSupersedeError, match="within the same epoch"):
        await repository.supersede_portfolio_pnl_sample(
            s5778_value_1,
            s5778_value_2,
            derived_suffix_reconciliation=True,
            expected_public_id=_ORIG,
        )


async def test_supersede_validates_the_replacement(
    repository: SQLAlchemyRepository,
) -> None:
    """The replacement row is validated before any active row is touched."""
    await repository.record_portfolio_pnl_samples([_complete_sample(_M1)], _scope())
    s5778_value_1 = _scope()
    s5778_value_2 = _mutate(_complete_sample(_M1), drawdown=3.0)
    with pytest.raises(ValueError, match="drawdown must be finite within"):
        await repository.supersede_portfolio_pnl_sample(
            s5778_value_1,
            s5778_value_2,
            derived_suffix_reconciliation=True,
            expected_public_id=_ORIG,
        )


async def test_writer_refuses_a_scope_without_an_active_anchor(
    bare_repository: SQLAlchemyRepository,
) -> None:
    """A scope whose anchor was never created cannot be sampled (B1)."""
    s5778_value_1 = _complete_sample(_M1)
    s5778_value_2 = _scope()
    with pytest.raises(PortfolioPnlSampleScopeError, match="no active anchor"):
        await bare_repository.record_portfolio_pnl_samples([s5778_value_1], s5778_value_2)
    async with bare_repository.session() as s:
        assert (await s.execute(select(PortfolioPnlPoint))).scalars().all() == []


async def test_supersede_refuses_a_scope_without_an_active_anchor(
    bare_repository: SQLAlchemyRepository,
) -> None:
    """Supersede also refuses an unsampleable scope before touching a row."""
    s5778_value_1 = _scope()
    s5778_value_2 = _complete_sample(_M1)
    with pytest.raises(PortfolioPnlSampleScopeError, match="no active anchor"):
        await bare_repository.supersede_portfolio_pnl_sample(
            s5778_value_1,
            s5778_value_2,
            derived_suffix_reconciliation=True,
            expected_public_id=_ORIG,
        )


async def test_writer_refuses_a_scope_epoch_the_anchor_disowns(
    repository: SQLAlchemyRepository,
) -> None:
    """A scope epoch the persisted anchor does not carry cannot fork the series."""
    scope = _scope(epoch=_OTHER_EPOCH)
    row = _mutate(_complete_sample(_M1), epoch_public_id=_OTHER_EPOCH)
    with pytest.raises(PortfolioPnlSampleScopeError, match="does not match the active anchor"):
        await repository.record_portfolio_pnl_samples([row], scope)
    assert await _active_samples(repository) == []


async def test_writer_refuses_a_scope_t0_the_anchor_disowns(
    repository: SQLAlchemyRepository,
) -> None:
    """A scope t0 that disagrees with the persisted anchor is refused."""
    scope = _scope(anchor_point_time=_T0 - timedelta(minutes=1))
    s5778_value_1 = _complete_sample(_M1)
    with pytest.raises(PortfolioPnlSampleScopeError, match="does not match the active anchor"):
        await repository.record_portfolio_pnl_samples([s5778_value_1], scope)
    assert await _active_samples(repository) == []


async def _seed_read_fixture(repository: SQLAlchemyRepository) -> None:
    """Seed the anchored scope with in-scope, off-epoch and off-calc rows.

    The activation anchor at ``_T0`` is supplied by the ``repository`` fixture and
    serves as the wrong-``point_kind`` exclusion for the R4 read predicates.
    """
    await repository.record_portfolio_pnl_samples(
        [_complete_sample(_M1), _incomplete_sample(_M2), _complete_sample(_M3)], _scope()
    )
    async with repository.session() as s:
        s.add_all(
            [
                PortfolioPnlPoint(
                    **SQLAlchemyRepository._portfolio_pnl_sample_orm_kwargs(
                        _mutate(_complete_sample(_M4), epoch_public_id=_OTHER_EPOCH)
                    ),
                    known_to=KNOWN_TO_MAX,
                ),
                PortfolioPnlPoint(
                    **SQLAlchemyRepository._portfolio_pnl_sample_orm_kwargs(
                        _mutate(_complete_sample(_M5), calc_version="5B.9")
                    ),
                    known_to=KNOWN_TO_MAX,
                ),
            ]
        )
        await s.commit()


async def test_range_read_applies_the_r4_predicate_set(
    repository: SQLAlchemyRepository,
) -> None:
    """The range read excludes anchor, wrong epoch, wrong calc and off-window rows."""
    await _seed_read_fixture(repository)
    windowed = await repository.get_portfolio_pnl_samples(_query(), _T0, _M5)
    assert [row["point_time"] for row in windowed] == [_M1, _M2, _M3]
    assert all(row["epoch_public_id"] == _EPOCH for row in windowed)
    assert all(row["calc_version"] == PNL_SAMPLE_CALC_VERSION for row in windowed)
    assert all(row["point_kind"] == "sample" for row in windowed)
    clipped = await repository.get_portfolio_pnl_samples(_query(), _M2, _M2)
    assert [row["point_time"] for row in clipped] == [_M2]


async def test_range_read_status_filter_selects_one_plane(
    repository: SQLAlchemyRepository,
) -> None:
    """The optional status filter narrows to one valuation plane."""
    await _seed_read_fixture(repository)
    complete = await repository.get_portfolio_pnl_samples(_query(), _M1, _M3, status="complete")
    assert [row["point_time"] for row in complete] == [_M1, _M3]
    incomplete = await repository.get_portfolio_pnl_samples(_query(), _M1, _M3, status="incomplete")
    assert [row["point_time"] for row in incomplete] == [_M2]


async def test_range_read_excludes_a_superseded_row(
    repository: SQLAlchemyRepository,
) -> None:
    """A superseded (closed) row never appears in the active range read."""
    await repository.record_portfolio_pnl_samples(
        [_complete_sample(_M1, public_id=_ORIG)], _scope()
    )
    await repository.supersede_portfolio_pnl_sample(
        _scope(),
        _mutate(_complete_sample(_M1), realized_pnl=42.0, timestamp=_M1 + timedelta(minutes=3)),
        derived_suffix_reconciliation=True,
        expected_public_id=_ORIG,
    )
    rows = await repository.get_portfolio_pnl_samples(_query(), _M1, _M1)
    assert [row["realized_pnl"] for row in rows] == [42.0]


async def test_peak_read_maximizes_complete_equity_only(
    repository: SQLAlchemyRepository,
) -> None:
    """Peak equity is the max cash+position over complete samples only."""
    await repository.record_portfolio_pnl_samples(
        [
            _mutate(_complete_sample(_M1), cash_usd=100.0, position_value_usd=50.0),
            _incomplete_sample(_M2),
            _mutate(_complete_sample(_M3), cash_usd=400.0, position_value_usd=25.0),
        ],
        _scope(),
    )
    assert await repository.get_portfolio_pnl_sample_peak(_query(), before=_M5) == 425.0


async def test_peak_read_excludes_superseded_rows(
    repository: SQLAlchemyRepository,
) -> None:
    """A superseded high-equity row never advances the re-derived peak."""
    await repository.record_portfolio_pnl_samples(
        [_mutate(_complete_sample(_M1), public_id=_ORIG, cash_usd=1000.0, position_value_usd=0.0)],
        _scope(),
    )
    await repository.supersede_portfolio_pnl_sample(
        _scope(),
        _mutate(
            _complete_sample(_M1),
            cash_usd=10.0,
            position_value_usd=0.0,
            timestamp=_M1 + timedelta(minutes=3),
        ),
        derived_suffix_reconciliation=True,
        expected_public_id=_ORIG,
    )
    assert await repository.get_portfolio_pnl_sample_peak(_query(), before=_M5) == 10.0


async def test_peak_read_returns_none_without_a_complete_sample(
    repository: SQLAlchemyRepository,
) -> None:
    """An epoch with no complete sample has no peak."""
    await repository.record_portfolio_pnl_samples([_incomplete_sample(_M1)], _scope())
    assert await repository.get_portfolio_pnl_sample_peak(_query(), before=_M5) is None


async def test_latest_sample_progress_read(
    repository: SQLAlchemyRepository,
) -> None:
    """The progress read returns the last active sample of any status."""
    await repository.record_portfolio_pnl_samples(
        [_complete_sample(_M1), _incomplete_sample(_M3)], _scope()
    )
    latest = await repository.get_latest_portfolio_pnl_sample(_query())
    assert latest is not None
    assert latest["point_time"] == _M3


async def test_latest_sample_returns_none_when_empty(
    repository: SQLAlchemyRepository,
) -> None:
    """A scope with no sample yet has no durable progress."""
    assert await repository.get_latest_portfolio_pnl_sample(_query()) is None


async def test_unpinned_active_sample_reads_support_version_transition(
    repository: SQLAlchemyRepository,
) -> None:
    """Unpinned transition reads expose only rows in the requested epoch."""
    await repository.record_portfolio_pnl_samples(
        [_complete_sample(_M1), _incomplete_sample(_M3)], _scope()
    )
    foreign = _complete_sample(_M5)
    foreign["epoch_public_id"] = _OTHER_EPOCH
    async with repository.session() as s:
        s.add(
            PortfolioPnlPoint(
                **repository._portfolio_pnl_sample_orm_kwargs(foreign),
                known_to=KNOWN_TO_MAX,
            )
        )
        await s.commit()
    latest = await repository.get_latest_active_portfolio_pnl_sample_any_version(_scope())
    rows = await repository.get_active_portfolio_pnl_samples_any_version(_scope(), _M1, _M5)
    assert latest is not None
    assert latest["point_time"] == _M3
    assert [row["point_time"] for row in rows] == [_M1, _M3]
    assert all(row["epoch_public_id"] == _EPOCH for row in rows)


async def test_observation_read_boundary_selects_the_latest_by_bus_time(
    repository: SQLAlchemyRepository,
) -> None:
    """The knowledge cut is exactly the grid instant M, boundary M-eps/M/M+eps."""
    epsilon = timedelta(microseconds=1)
    async with repository.session() as s:
        s.add_all(
            [
                _observation("kraken", _M1 - epsilon),
                _observation("kraken", _M2),
            ]
        )
        await s.commit()
    before = await repository.get_venue_account_observation_attempts_at(
        _WALLET, ["kraken"], "live", _M1 - epsilon - epsilon
    )
    assert before == {}
    at_first = await repository.get_venue_account_observation_attempts_at(
        _WALLET, ["kraken"], "live", _M1 - epsilon
    )
    assert at_first["kraken"]["timestamp"] == _M1 - epsilon
    before_second = await repository.get_venue_account_observation_attempts_at(
        _WALLET, ["kraken"], "live", _M2 - epsilon
    )
    assert before_second["kraken"]["timestamp"] == _M1 - epsilon
    at_second = await repository.get_venue_account_observation_attempts_at(
        _WALLET, ["kraken"], "live", _M2
    )
    assert at_second["kraken"]["timestamp"] == _M2


async def test_observation_read_returns_a_later_failed_attempt_verbatim(
    repository: SQLAlchemyRepository,
) -> None:
    """A later error attempt is returned AS-IS, never skipped for an older success."""
    async with repository.session() as s:
        s.add_all(
            [
                _observation("kraken", _M1, balance_status="observed", balances_json="[]"),
                _observation(
                    "kraken", _M2, balance_status="error", balances_json=None, error="timeout"
                ),
            ]
        )
        await s.commit()
    result = await repository.get_venue_account_observation_attempts_at(
        _WALLET, ["kraken"], "live", _M2
    )
    assert result["kraken"]["balance_status"] == "error"
    assert result["kraken"]["error"] == "timeout"
    assert result["kraken"]["balances_json"] is None


async def test_observation_read_tie_break_prefers_the_higher_id(
    repository: SQLAlchemyRepository,
) -> None:
    """Same-timestamp attempts are tie-broken deterministically by row id."""
    async with repository.session() as s:
        s.add(_observation("kraken", _M1, balances_json='[{"currency":"BTC"}]'))
        await s.commit()
        s.add(_observation("kraken", _M1, balances_json='[{"currency":"ETH"}]'))
        await s.commit()
    result = await repository.get_venue_account_observation_attempts_at(
        _WALLET, ["kraken"], "live", _M1
    )
    assert result["kraken"]["balances_json"] == '[{"currency":"ETH"}]'


async def test_observation_read_isolates_exchanges_and_deduplicates(
    repository: SQLAlchemyRepository,
) -> None:
    """Each exchange resolves independently and a repeated request collapses."""
    async with repository.session() as s:
        s.add_all(
            [
                _observation("kraken", _M1),
                _observation("zonda", _M2),
            ]
        )
        await s.commit()
    result = await repository.get_venue_account_observation_attempts_at(
        _WALLET, ["kraken", "zonda", "kraken", "binance"], "live", _M2
    )
    assert set(result) == {"kraken", "zonda"}
    assert result["kraken"]["timestamp"] == _M1
    assert result["zonda"]["timestamp"] == _M2


async def test_observation_read_empty_exchanges_returns_nothing(
    repository: SQLAlchemyRepository,
) -> None:
    """No requested exchange yields an empty basket map."""
    result = await repository.get_venue_account_observation_attempts_at(_WALLET, [], "live", _M2)
    assert result == {}


async def test_sample_write_transaction_takes_the_postgres_advisory_lock(
    repository: SQLAlchemyRepository,
) -> None:
    """The Postgres path fences per scope without a table share lock."""
    statements: list[str] = []

    async def execute(statement: object, parameters: object = None) -> AsyncMock:
        """Record every statement the periodic writer issues."""
        del parameters
        statements.append(str(statement))
        return AsyncMock()

    session = AsyncMock()
    session.execute = AsyncMock(side_effect=execute)
    with patch.object(
        SQLAlchemyRepository,
        "dialect_name",
        new_callable=PropertyMock,
        return_value="postgresql",
    ):
        await repository._begin_portfolio_pnl_sample_write_transaction(session, _scope())
    expected_lock = (
        "SELECT pg_advisory_xact_lock(hashtext('portfolio_pnl_sample'), hashtext(:scope))"
    )
    assert statements == [
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        expected_lock,
    ]
    assert not any("LOCK TABLE" in statement for statement in statements)


async def test_sample_write_transaction_refuses_an_unknown_dialect(
    repository: SQLAlchemyRepository,
) -> None:
    """A new backend never silently skips the periodic writer's lock."""
    session = AsyncMock()
    with patch.object(
        SQLAlchemyRepository,
        "dialect_name",
        new_callable=PropertyMock,
        return_value="unknown",
    ), pytest.raises(NotImplementedError, match="sample write transaction"):
        await repository._begin_portfolio_pnl_sample_write_transaction(session, _scope())


async def test_collision_read_short_circuits_on_no_minutes(
    repository: SQLAlchemyRepository,
) -> None:
    """The by-minute collision read never issues an empty IN query."""
    async with repository.session() as s:
        assert await repository._read_active_portfolio_pnl_samples_by_minute(s, _scope(), []) == {}


@pytest.mark.parametrize(
    ("watermarks_json", "message"),
    [
        ("", "watermarks_json must be a nonempty string"),
        ("{bad", "must contain valid JSON"),
        ("[]", "must be a canonical watermark map"),
        ('{"kraken":0}', "must be a canonical watermark map"),
        ('{"kraken": 5}', "must be canonically serialized"),
    ],
)
def test_validator_rejects_bad_watermarks(watermarks_json: str, message: str) -> None:
    """Every sample requires a present, valid, canonical watermark map (B2)."""
    s5778_value_1 = _mutate(_complete_sample(), watermarks_json=watermarks_json)
    s5778_value_2 = _scope()
    with pytest.raises(ValueError, match=message):
        SQLAlchemyRepository._validate_portfolio_pnl_sample(s5778_value_1, s5778_value_2)


def test_validator_accepts_a_nonempty_watermark_map() -> None:
    """A canonical per-exchange watermark map is accepted."""
    SQLAlchemyRepository._validate_portfolio_pnl_sample(
        _mutate(_complete_sample(), watermarks_json='{"kraken":7}'), _scope()
    )


@pytest.mark.parametrize(
    ("coverage", "message"),
    [
        (789, "coverage must be a present object"),
        (
            {k: v for k, v in _VALID_COVERAGE.items() if k != "leveraged_inventory_excluded"},
            "leveraged_inventory_excluded must be a boolean",
        ),
        (
            {**_VALID_COVERAGE, "non_finite_position_excluded": "no"},
            "non_finite_position_excluded must be a boolean",
        ),
        ({**_VALID_COVERAGE, "venue_scope": "all"}, "venue_scope must be 'spot_only'"),
        (
            {**_VALID_COVERAGE, "external_flows_adjusted": True},
            "external_flows_adjusted must be false",
        ),
    ],
)
def test_validator_rejects_bad_coverage(coverage: object, message: str) -> None:
    """The self-describing coverage disclosure enforces its schema (A4)."""
    s5778_value_1 = _mutate(_complete_sample(), audit_json=_complete_audit(coverage=coverage))
    s5778_value_2 = _scope()
    with pytest.raises(ValueError, match=message):
        SQLAlchemyRepository._validate_portfolio_pnl_sample(s5778_value_1, s5778_value_2)


def test_validator_accepts_a_disclosed_leveraged_exclusion() -> None:
    """A complete sample may disclose an excluded leveraged position (A4)."""
    coverage = {**_VALID_COVERAGE, "leveraged_inventory_excluded": True}
    SQLAlchemyRepository._validate_portfolio_pnl_sample(
        _mutate(_complete_sample(), audit_json=_complete_audit(coverage=coverage)), _scope()
    )


async def test_supersede_conflict_on_a_stale_public_id(
    repository: SQLAlchemyRepository,
) -> None:
    """A supersede whose expected public_id no longer matches loses the CAS (B3)."""
    await repository.record_portfolio_pnl_samples(
        [_complete_sample(_M1, public_id=_ORIG)], _scope()
    )
    s5778_value_1 = _scope()
    s5778_value_2 = _mutate(_complete_sample(_M1), timestamp=_M1 + timedelta(minutes=3))
    with pytest.raises(PortfolioPnlSampleConflictError, match="lost its optimistic CAS"):
        await repository.supersede_portfolio_pnl_sample(
            s5778_value_1,
            s5778_value_2,
            derived_suffix_reconciliation=True,
            expected_public_id="99999999-0000-7000-8000-000000000009",
        )


async def test_peak_read_is_causal_before_the_bound(
    repository: SQLAlchemyRepository,
) -> None:
    """The peak reads only complete samples strictly before the bound (A1)."""
    await repository.record_portfolio_pnl_samples(
        [
            _mutate(_complete_sample(_M1), cash_usd=100.0, position_value_usd=0.0),
            _mutate(_complete_sample(_M3), cash_usd=500.0, position_value_usd=0.0),
        ],
        _scope(),
    )
    assert await repository.get_portfolio_pnl_sample_peak(_query(), before=_M2) == 100.0
    assert await repository.get_portfolio_pnl_sample_peak(_query(), before=_M5) == 500.0


async def test_retract_closes_the_active_row_without_a_successor(
    repository: SQLAlchemyRepository,
) -> None:
    """A retract closes the active sample leaving honest absence (N1)."""
    await repository.record_portfolio_pnl_samples(
        [_complete_sample(_M1, public_id=_ORIG)], _scope()
    )
    await repository.retract_portfolio_pnl_sample(
        _scope(), _M1, expected_public_id=_ORIG, bus_time=_M1 + timedelta(minutes=5)
    )
    assert await _active_samples(repository) == []


async def test_retract_drops_the_row_from_the_causal_peak(
    repository: SQLAlchemyRepository,
) -> None:
    """A retracted high-equity minute no longer feeds the re-derived peak (N1)."""
    await repository.record_portfolio_pnl_samples(
        [
            _mutate(
                _complete_sample(_M1, public_id=_ORIG), cash_usd=10000.0, position_value_usd=0.0
            ),
            _mutate(_complete_sample(_M2), cash_usd=200.0, position_value_usd=0.0),
        ],
        _scope(),
    )
    await repository.retract_portfolio_pnl_sample(
        _scope(), _M1, expected_public_id=_ORIG, bus_time=_M1 + timedelta(minutes=5)
    )
    assert await repository.get_portfolio_pnl_sample_peak(_query(), before=_M5) == 200.0


async def test_retract_refuses_the_anchor(repository: SQLAlchemyRepository) -> None:
    """Retracting the anchor minute is refused (N1)."""
    s5778_value_1 = _scope()
    with pytest.raises(PortfolioPnlSampleSupersedeError, match="must not target the anchor"):
        await repository.retract_portfolio_pnl_sample(
            s5778_value_1, _T0, expected_public_id=_ORIG, bus_time=_M1
        )


async def test_retract_refuses_when_no_active_row(repository: SQLAlchemyRepository) -> None:
    """Retracting a minute with no active sample is refused (N1)."""
    s5778_value_1 = _scope()
    with pytest.raises(PortfolioPnlSampleSupersedeError, match="no active row"):
        await repository.retract_portfolio_pnl_sample(
            s5778_value_1, _M1, expected_public_id=_ORIG, bus_time=_M2
        )


async def test_retract_conflict_on_a_stale_public_id(
    repository: SQLAlchemyRepository,
) -> None:
    """A retract whose expected public_id no longer matches loses the CAS (N1/B3)."""
    await repository.record_portfolio_pnl_samples(
        [_complete_sample(_M1, public_id=_ORIG)], _scope()
    )
    s5778_value_1 = _scope()
    s5778_value_2 = timedelta(minutes=5)
    with pytest.raises(PortfolioPnlSampleConflictError, match="retract lost its optimistic CAS"):
        await repository.retract_portfolio_pnl_sample(
            s5778_value_1,
            _M1,
            expected_public_id="99999999-0000-7000-8000-000000000009",
            bus_time=_M1 + s5778_value_2,
        )


async def test_retract_refuses_a_scope_without_an_active_anchor(
    bare_repository: SQLAlchemyRepository,
) -> None:
    """Retract also refuses an unsampleable scope before touching a row (N1)."""
    s5778_value_1 = _scope()
    with pytest.raises(PortfolioPnlSampleScopeError, match="no active anchor"):
        await bare_repository.retract_portfolio_pnl_sample(
            s5778_value_1, _M1, expected_public_id=_ORIG, bus_time=_M2
        )
