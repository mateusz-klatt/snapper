"""Tests for the venue account-state read-surface mapping (PnL Phase 3).

Pins the fail-closed contract of
:func:`snapper.application.portfolio.account_view.build_portfolio_account_state`
and its helpers (``_finite_number``, ``_parse_balances``, ``_parse_positions``,
``_row_is_coherent``), plus the three Phase 3 payload schemas
(:class:`AccountBalanceEntry`, :class:`AccountPositionEntry`,
:class:`PortfolioAccountState`).

The mapper hardens truth at read time along three axes and marks the WHOLE
state ``corrupt`` (clearing BOTH balances and open_positions, never
authoritative) on any violation:

* STRICT parsing — every balance/position number must be a finite, non-bool
  int/float (a ``bool``, a numeric STRING, or ``inf``/``nan`` is corrupt),
  a currency/symbol must be a non-empty string, a position side must be
  ``buy``/``sell``, and a position timestamp must be an ISO string.
* COHERENCE — an ``observed`` balance/positions component must carry its
  payload AND its observation clock, an ``observed`` roll-up must carry an
  authority window, and a payload and its source-observation id are
  inseparable (both-null or both-non-null); ``_row_is_coherent`` re-checks
  these independently of the DB CHECKs, so an incoherent row is corrupt even
  when its JSON parses.
* STALENESS/CLOCK — a coherent, well-parsed row still derives to ``stale``
  past its authority window and ``clock_error`` on a future-dated clock, and
  is authoritative only when the effective status is exactly ``observed``.
"""

import json
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

from snapper.application.portfolio.account_status import EFFECTIVE_CLOCK_ERROR
from snapper.application.portfolio.account_status import EFFECTIVE_STALE
from snapper.application.portfolio.account_view import EFFECTIVE_CORRUPT
from snapper.application.portfolio.account_view import build_portfolio_account_state
from snapper.data.repository_types import VenueAccountStateRow

_NOW = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
_PAST = _NOW - timedelta(hours=1)
_FUTURE = _NOW + timedelta(hours=1)

_POS1_TS = "2026-07-13T11:00:00+00:00"
_POS2_TS = "2026-07-13T11:05:00+00:00"

_VALID_BALANCES_JSON = json.dumps(
    [
        {"currency": "BTC", "total": 1.5, "free": 1.0, "used": 0.5},
        {"currency": "USD_collateral_value", "total": 1000.0, "free": None, "used": None},
    ]
)
_VALID_POSITIONS_JSON = json.dumps(
    [
        {
            "symbol": "PF_XBTUSD",
            "side": "buy",
            "size": 2.0,
            "entry_price": 50000.0,
            "mark_price": 50500.0,
            "unrealized_pnl": 1000.0,
            "unrealized_funding": -5.0,
            "timestamp": _POS1_TS,
        },
        {
            "symbol": "PF_ETHUSD",
            "side": "sell",
            "size": 3.0,
            "entry_price": 3000.0,
            "mark_price": 2950.0,
            "unrealized_pnl": 150.0,
            "unrealized_funding": 2.5,
            "timestamp": _POS2_TS,
        },
    ]
)


def _make_row(
    *,
    wallet_public_id: str = "wal-1",
    exchange: str = "kraken",
    mode: str = "live",
    sync_status: str = "observed",
    balance_status: str = "observed",
    position_status: str = "observed",
    valuation_status: str = "native_only",
    balances_json: str | None = _VALID_BALANCES_JSON,
    open_positions_json: str | None = _VALID_POSITIONS_JSON,
    balance_observed_at: datetime | None = _PAST,
    position_observed_at: datetime | None = _PAST,
    current_attempt_observation_id: int = 5,
    balance_payload_source_observation_id: int | None = 5,
    position_payload_source_observation_id: int | None = 5,
    authoritative_until: datetime | None = _FUTURE,
    error: str | None = None,
    public_id: str = "vas-1",
    timestamp: datetime = _NOW,
    session_id: str = "sess-1",
    sequence_id: int = 7,
) -> VenueAccountStateRow:
    """Build a full, COHERENT ``venue_account_states`` row for the mapper.

    Defaults describe a fresh ``observed`` row that satisfies every coherence
    invariant (observed balance + positions each carry a non-null payload, an
    observation clock, and a payload-source id EQUAL to
    ``current_attempt_observation_id`` so the fresh-source check passes; the
    observed roll-up carries an authority window) with well-formed,
    strictly-valid payloads, so each test varies only the fields it cares about
    and the happy path derives to ``observed``.

    Args:
        wallet_public_id: Owning wallet identity.
        exchange: Venue the account is held on.
        mode: Trading mode label (``live``/``paper``).
        sync_status: Raw stored attempt outcome.
        balance_status: Per-component balance outcome.
        position_status: Per-component positions outcome.
        valuation_status: Valuation label (``native_only`` in Phase 3).
        balances_json: Stored balances payload, or ``None`` when never stored.
        open_positions_json: Stored positions payload, or ``None`` when never
            stored.
        balance_observed_at: When the balance was last observed.
        position_observed_at: When positions were last observed.
        current_attempt_observation_id: Latest observation attempt id.
        balance_payload_source_observation_id: Observation whose balances are
            shown.
        position_payload_source_observation_id: Observation whose positions
            are shown.
        authoritative_until: Instant past which an observed row is stale.
        error: Last error detail, or ``None``.
        public_id: Envelope public id.
        timestamp: Envelope bus timestamp.
        session_id: Envelope session id.
        sequence_id: Envelope sequence id.

    Returns:
        The assembled row dict.
    """
    return VenueAccountStateRow(
        wallet_public_id=wallet_public_id,
        exchange=exchange,
        mode=mode,
        sync_status=sync_status,
        balance_status=balance_status,
        position_status=position_status,
        valuation_status=valuation_status,
        balances_json=balances_json,
        open_positions_json=open_positions_json,
        balance_observed_at=balance_observed_at,
        position_observed_at=position_observed_at,
        current_attempt_observation_id=current_attempt_observation_id,
        balance_payload_source_observation_id=balance_payload_source_observation_id,
        position_payload_source_observation_id=position_payload_source_observation_id,
        authoritative_until=authoritative_until,
        error=error,
        public_id=public_id,
        timestamp=timestamp,
        session_id=session_id,
        sequence_id=sequence_id,
    )


def _balances_json(**overrides: object) -> str:
    """Serialize a single-entry balances payload, applying field overrides.

    Args:
        overrides: Field values layered over a valid ``BTC`` balance entry.

    Returns:
        The JSON string for a one-element balances list.
    """
    entry: dict[str, object] = {"currency": "BTC", "total": 1.0}
    entry.update(overrides)
    return json.dumps([entry])


def _positions_json(**overrides: object) -> str:
    """Serialize a single-entry positions payload, applying field overrides.

    Args:
        overrides: Field values layered over a valid ``buy`` position entry.

    Returns:
        The JSON string for a one-element positions list.
    """
    entry: dict[str, object] = {
        "symbol": "PF_XBTUSD",
        "side": "buy",
        "size": 1.0,
        "entry_price": 1.0,
        "mark_price": 1.0,
        "unrealized_pnl": 1.0,
        "unrealized_funding": 1.0,
        "timestamp": _POS1_TS,
    }
    entry.update(overrides)
    return json.dumps([entry])


_BALANCES_STRICT_CORRUPT = [
    pytest.param(_balances_json(total=True), id="total_bool"),
    pytest.param(_balances_json(total="1.0"), id="total_numeric_string"),
    pytest.param(_balances_json(total=float("inf")), id="total_infinity"),
    pytest.param('[{"currency": "BTC"}]', id="total_missing"),
    pytest.param(_balances_json(free=True), id="free_bool"),
    pytest.param(_balances_json(free=float("inf")), id="free_infinity"),
    pytest.param(_balances_json(used=True), id="used_bool"),
    pytest.param(_balances_json(currency=None), id="currency_null"),
    pytest.param(_balances_json(currency=""), id="currency_empty"),
    pytest.param(_balances_json(currency=123), id="currency_non_string"),
]

_POSITIONS_STRICT_CORRUPT = [
    pytest.param(_positions_json(side="flat"), id="side_unknown"),
    pytest.param(_positions_json(symbol=None), id="symbol_null"),
    pytest.param(_positions_json(symbol=""), id="symbol_empty"),
    pytest.param(_positions_json(symbol=123), id="symbol_non_string"),
    pytest.param(_positions_json(timestamp=123), id="timestamp_non_string"),
    pytest.param(_positions_json(timestamp="not-a-date"), id="timestamp_bad_iso"),
    pytest.param(_positions_json(size=float("inf")), id="size_infinity"),
    pytest.param(_positions_json(size=True), id="size_bool"),
    pytest.param(_positions_json(entry_price="5"), id="entry_price_numeric_string"),
]

_INCOHERENT_ROWS = [
    pytest.param(
        lambda: _make_row(balances_json=None),
        id="observed_balance_without_json",
    ),
    pytest.param(
        lambda: _make_row(balance_observed_at=None),
        id="observed_balance_without_clock",
    ),
    pytest.param(
        lambda: _make_row(open_positions_json=None),
        id="observed_position_without_json",
    ),
    pytest.param(
        lambda: _make_row(position_observed_at=None),
        id="observed_position_without_clock",
    ),
    pytest.param(
        lambda: _make_row(authoritative_until=None),
        id="observed_sync_without_window",
    ),
    pytest.param(
        lambda: _make_row(balance_payload_source_observation_id=None),
        id="balance_json_without_source",
    ),
    pytest.param(
        lambda: _make_row(
            sync_status="unsupported",
            balance_status="unsupported",
            balances_json=None,
            balance_observed_at=None,
            balance_payload_source_observation_id=40,
        ),
        id="balance_source_without_json",
    ),
    pytest.param(
        lambda: _make_row(position_payload_source_observation_id=None),
        id="position_json_without_source",
    ),
    pytest.param(
        lambda: _make_row(
            sync_status="unsupported",
            position_status="unsupported",
            open_positions_json=None,
            position_observed_at=None,
            position_payload_source_observation_id=41,
        ),
        id="position_source_without_json",
    ),
    pytest.param(
        lambda: _make_row(
            sync_status="observed",
            balance_status="unsupported",
            balances_json=None,
            balance_observed_at=None,
            balance_payload_source_observation_id=None,
        ),
        id="observed_rollup_with_unsupported_balance",
    ),
    pytest.param(
        lambda: _make_row(
            sync_status="observed",
            balance_status="error",
            balances_json=None,
            balance_observed_at=None,
            balance_payload_source_observation_id=None,
        ),
        id="observed_rollup_with_error_balance",
    ),
    pytest.param(
        lambda: _make_row(
            sync_status="observed",
            balance_status="simulated",
            balances_json=None,
            balance_observed_at=None,
            balance_payload_source_observation_id=None,
        ),
        id="observed_rollup_with_simulated_balance",
    ),
    pytest.param(
        lambda: _make_row(
            sync_status="observed",
            position_status="error",
            open_positions_json=None,
            position_observed_at=None,
            position_payload_source_observation_id=None,
        ),
        id="observed_rollup_with_error_positions",
    ),
    pytest.param(
        lambda: _make_row(
            sync_status="observed",
            position_status="unsupported",
            open_positions_json=None,
            position_observed_at=None,
            position_payload_source_observation_id=None,
        ),
        id="observed_rollup_with_unsupported_positions",
    ),
    pytest.param(
        lambda: _make_row(
            sync_status="simulated",
            balance_status="simulated",
            position_status="simulated",
            mode="live",
            authoritative_until=None,
        ),
        id="simulated_sync_on_live_row",
    ),
    pytest.param(
        lambda: _make_row(
            sync_status="unsupported",
            balance_status="simulated",
            position_status="unsupported",
            mode="live",
            open_positions_json=None,
            position_observed_at=None,
            position_payload_source_observation_id=None,
            authoritative_until=None,
        ),
        id="simulated_balance_on_live_row",
    ),
    pytest.param(
        lambda: _make_row(balance_payload_source_observation_id=9),
        id="observed_balance_source_not_current_attempt",
    ),
    pytest.param(
        lambda: _make_row(
            sync_status="simulated",
            balance_status="simulated",
            position_status="simulated",
            mode="paper",
            balance_payload_source_observation_id=9,
            open_positions_json=None,
            position_observed_at=None,
            position_payload_source_observation_id=None,
            authoritative_until=None,
        ),
        id="simulated_balance_source_not_current_attempt",
    ),
    pytest.param(
        lambda: _make_row(position_payload_source_observation_id=9),
        id="observed_position_source_not_current_attempt",
    ),
    pytest.param(
        lambda: _make_row(valuation_status="usd"),
        id="valuation_not_native_only",
    ),
    pytest.param(
        lambda: _make_row(balance_status="unsupported", balances_json="[]"),
        id="unsupported_balance_with_payload",
    ),
    pytest.param(
        lambda: _make_row(position_status="not_applicable", open_positions_json="[]"),
        id="not_applicable_positions_with_payload",
    ),
    pytest.param(
        lambda: _make_row(position_status="unsupported", open_positions_json="[]"),
        id="unsupported_positions_with_payload",
    ),
    pytest.param(
        lambda: _make_row(
            balance_status="unsupported",
            balances_json=None,
            balance_payload_source_observation_id=None,
            balance_observed_at=_PAST,
        ),
        id="unsupported_balance_with_lingering_observed_at",
    ),
    pytest.param(
        lambda: _make_row(
            position_status="not_applicable",
            open_positions_json=None,
            position_payload_source_observation_id=None,
            position_observed_at=_PAST,
        ),
        id="not_applicable_positions_with_lingering_observed_at",
    ),
]


def test_observed_fresh_is_authoritative_and_payloads_present() -> None:
    """A fresh, coherent observed row is served as authoritative live truth.

    Given: a default row (coherent observed balance + positions, open authority
        window, well-formed strictly-valid payloads),
    When: the row is mapped,
    Then: the effective status is ``observed``, is_authoritative is True, and
        both payloads parse into their two typed entries.
    """
    result = build_portfolio_account_state(_make_row(), _NOW)
    assert result.effective_status == "observed"
    assert result.is_authoritative is True
    assert result.balances is not None
    assert result.open_positions is not None
    assert len(result.balances) == 2
    assert len(result.open_positions) == 2


def test_observed_past_authoritative_window_is_stale_not_authoritative() -> None:
    """A coherent observed row whose authority window has elapsed is stale.

    Given: a coherent observed row with sane past clocks but an
        authoritative_until BEFORE now (window closed, still non-null so it
        stays coherent),
    When: the row is mapped,
    Then: the effective status is ``stale`` and is_authoritative is False.
    """
    result = build_portfolio_account_state(_make_row(authoritative_until=_PAST), _NOW)
    assert result.effective_status == EFFECTIVE_STALE
    assert result.is_authoritative is False


def test_observed_stale_retains_parsed_payloads() -> None:
    """The mapper clears payloads only on corruption, never on staleness.

    Given: a stale coherent observed row (elapsed but non-null window) whose
        payloads are well-formed,
    When: the row is mapped,
    Then: the effective status is ``stale`` yet the parsed balances/positions
        remain populated — payload masking for a non-fresh row is a higher
        layer's responsibility; the mapper only nulls payloads on ``corrupt``.
    """
    result = build_portfolio_account_state(_make_row(authoritative_until=_PAST), _NOW)
    assert result.effective_status == EFFECTIVE_STALE
    assert result.balances is not None
    assert result.open_positions is not None


def test_future_balance_clock_is_clock_error_not_authoritative() -> None:
    """A future-dated balance clock is never trusted, payloads aside.

    Given: a coherent observed row whose balance_observed_at is AFTER now (still
        non-null, so coherent) while its payloads are well-formed,
    When: the row is mapped,
    Then: the effective status is ``clock_error`` and is_authoritative is
        False — the future clock short-circuits before the window check.
    """
    result = build_portfolio_account_state(_make_row(balance_observed_at=_FUTURE), _NOW)
    assert result.effective_status == EFFECTIVE_CLOCK_ERROR
    assert result.is_authoritative is False


def test_simulated_paper_row_survives_verbatim_and_is_not_authoritative() -> None:
    """A simulated paper row keeps its label and is never authoritative.

    Given: a coherent ``simulated`` paper row (paper exchange/mode, simulated
        component statuses so the observed-component invariants do not apply)
        with well-formed payloads and no authority window,
    When: the row is mapped,
    Then: the effective status is ``simulated`` verbatim and is_authoritative
        is False — only ``observed`` is ever authoritative.
    """
    result = build_portfolio_account_state(
        _make_row(
            sync_status="simulated",
            balance_status="simulated",
            position_status="simulated",
            exchange="paper",
            mode="paper",
            authoritative_until=None,
        ),
        _NOW,
    )
    assert result.effective_status == "simulated"
    assert result.is_authoritative is False


def test_unsupported_row_with_no_payloads_survives_verbatim() -> None:
    """An unsupported row with no payloads keeps its label and nulls payloads.

    Given: a coherent ``unsupported`` row (market-data-only venue) whose
        balances and positions payloads were never stored — both JSON, both
        observation clocks, and both payload-source ids are NULL, and every
        component status is non-observed so the coherence invariants hold,
    When: the row is mapped,
    Then: the effective status is ``unsupported`` verbatim, is_authoritative
        is False, and both payloads are None (the not-None guards are skipped).
    """
    result = build_portfolio_account_state(
        _make_row(
            sync_status="unsupported",
            balance_status="unsupported",
            position_status="unsupported",
            balances_json=None,
            open_positions_json=None,
            balance_observed_at=None,
            position_observed_at=None,
            balance_payload_source_observation_id=None,
            position_payload_source_observation_id=None,
            authoritative_until=None,
        ),
        _NOW,
    )
    assert result.effective_status == "unsupported"
    assert result.is_authoritative is False
    assert result.balances is None
    assert result.open_positions is None


def test_error_row_with_no_payloads_survives_verbatim() -> None:
    """An error row keeps its label and is never authoritative.

    Given: a coherent ``error`` row whose payloads were never stored (all JSON,
        clocks and payload-source ids NULL, component statuses non-observed),
    When: the row is mapped,
    Then: the effective status is ``error`` verbatim and is_authoritative is
        False.
    """
    result = build_portfolio_account_state(
        _make_row(
            sync_status="error",
            balance_status="error",
            position_status="error",
            balances_json=None,
            open_positions_json=None,
            balance_observed_at=None,
            position_observed_at=None,
            balance_payload_source_observation_id=None,
            position_payload_source_observation_id=None,
            authoritative_until=None,
            error="venue timeout",
        ),
        _NOW,
    )
    assert result.effective_status == "error"
    assert result.is_authoritative is False


def test_corrupt_overrides_observed_and_clears_both_payloads() -> None:
    """A corrupt balances payload defeats an otherwise-authoritative row.

    Given: a row that would derive to ``observed`` (coherent, sane clocks +
        open window) but whose balances_json is not valid JSON, while its
        open_positions_json IS well-formed,
    When: the row is mapped,
    Then: the effective status is ``corrupt``, is_authoritative is False, and
        BOTH payloads are cleared to None — the parseable positions payload is
        nulled alongside the corrupt balances so nothing corrupt is served.
    """
    result = build_portfolio_account_state(_make_row(balances_json="not json"), _NOW)
    assert result.effective_status == EFFECTIVE_CORRUPT
    assert result.is_authoritative is False
    assert result.balances is None
    assert result.open_positions is None


def test_balances_json_object_root_is_corrupt() -> None:
    """A balances payload whose root is a JSON object (not a list) is corrupt.

    Given: a coherent row whose balances_json is a JSON object ``{"a": 1}``,
    When: the row is mapped,
    Then: the effective status is ``corrupt`` — the not-a-list guard rejects
        it (ValueError arm of the except).
    """
    result = build_portfolio_account_state(_make_row(balances_json='{"a": 1}'), _NOW)
    assert result.effective_status == EFFECTIVE_CORRUPT
    assert result.balances is None


def test_balances_entry_not_object_is_corrupt() -> None:
    """A balances list holding a non-object entry is corrupt.

    Given: a coherent row whose balances_json is ``[123]`` (a scalar where an
        object is required),
    When: the row is mapped,
    Then: the effective status is ``corrupt`` — the entry-not-object guard
        rejects it.
    """
    result = build_portfolio_account_state(_make_row(balances_json="[123]"), _NOW)
    assert result.effective_status == EFFECTIVE_CORRUPT
    assert result.balances is None


def test_balances_invalid_json_is_corrupt() -> None:
    """A balances payload that is not valid JSON is corrupt.

    Given: a coherent row whose balances_json is the non-JSON string
        ``"not json"``,
    When: the row is mapped,
    Then: the effective status is ``corrupt`` — the json.JSONDecodeError arm of
        the except fires.
    """
    result = build_portfolio_account_state(_make_row(balances_json="not json"), _NOW)
    assert result.effective_status == EFFECTIVE_CORRUPT
    assert result.balances is None


@pytest.mark.parametrize("balances_json", _BALANCES_STRICT_CORRUPT)
def test_balances_strict_parse_corruption(balances_json: str) -> None:
    """Strict balance-field validation rejects every non-authoritative value.

    Given: a coherent row whose single balance entry violates one strict rule
        — a bool/numeric-string/infinite ``total`` or ``free``/``used`` (each
        rejected by ``_finite_number``), a missing ``total`` (``.get`` returns
        None), or a null/empty/non-string currency,
    When: the row is mapped,
    Then: the effective status is ``corrupt``, is_authoritative is False, and
        both payloads are cleared to None.
    """
    result = build_portfolio_account_state(_make_row(balances_json=balances_json), _NOW)
    assert result.effective_status == EFFECTIVE_CORRUPT
    assert result.is_authoritative is False
    assert result.balances is None
    assert result.open_positions is None


def test_positions_json_object_root_is_corrupt() -> None:
    """A positions payload whose root is a JSON object is corrupt.

    Given: a coherent row whose open_positions_json is ``{"a": 1}``,
    When: the row is mapped,
    Then: the effective status is ``corrupt`` — the positions not-a-list guard
        rejects it.
    """
    result = build_portfolio_account_state(_make_row(open_positions_json='{"a": 1}'), _NOW)
    assert result.effective_status == EFFECTIVE_CORRUPT
    assert result.open_positions is None


def test_positions_entry_not_object_is_corrupt() -> None:
    """A positions list holding a non-object entry is corrupt.

    Given: a coherent row whose open_positions_json is ``[123]``,
    When: the row is mapped,
    Then: the effective status is ``corrupt`` — the positions entry-not-object
        guard rejects it.
    """
    result = build_portfolio_account_state(_make_row(open_positions_json="[123]"), _NOW)
    assert result.effective_status == EFFECTIVE_CORRUPT
    assert result.open_positions is None


def test_positions_invalid_json_is_corrupt() -> None:
    """A positions payload that is not valid JSON is corrupt.

    Given: a coherent row whose open_positions_json is the non-JSON string
        ``"not json"`` while balances_json is well-formed,
    When: the row is mapped,
    Then: the effective status is ``corrupt`` and both payloads are cleared
        (json.JSONDecodeError arm on the positions parse).
    """
    result = build_portfolio_account_state(_make_row(open_positions_json="not json"), _NOW)
    assert result.effective_status == EFFECTIVE_CORRUPT
    assert result.balances is None
    assert result.open_positions is None


@pytest.mark.parametrize("open_positions_json", _POSITIONS_STRICT_CORRUPT)
def test_positions_strict_parse_corruption(open_positions_json: str) -> None:
    """Strict position-field validation rejects every malformed value.

    Given: a coherent row whose single position entry violates one strict rule
        — an unknown ``side``, a null/empty/non-string ``symbol``, a
        non-string or non-ISO ``timestamp``, or a bool/infinite/numeric-string
        number,
    When: the row is mapped,
    Then: the effective status is ``corrupt``, is_authoritative is False, and
        both payloads are cleared to None.
    """
    result = build_portfolio_account_state(_make_row(open_positions_json=open_positions_json), _NOW)
    assert result.effective_status == EFFECTIVE_CORRUPT
    assert result.is_authoritative is False
    assert result.balances is None
    assert result.open_positions is None


@pytest.mark.parametrize("make_row", _INCOHERENT_ROWS)
def test_incoherent_row_is_corrupt(make_row: Callable[[], VenueAccountStateRow]) -> None:
    """A row that violates a read-time coherence invariant is corrupt.

    Given: a row whose JSON WOULD parse but that breaks one coherence rule —
        a non-``native_only`` valuation_status, a structurally-absent component
        that still carries a payload (an unsupported balance, or
        not_applicable/unsupported positions, with non-null JSON), a ROLL-UP
        mismatch (an observed roll-up whose balance is not observed, or whose
        positions are neither observed nor not_applicable, or a simulated
        status riding a non-paper row), an observed balance/positions component
        missing its payload or its observation clock, an observed roll-up
        missing its authority window, a payload present without its
        source-observation id (or vice versa), or a fresh (observed/simulated)
        component whose payload-source id is NOT this attempt's
        current_attempt_observation_id (a retained/forged payload masquerading
        as fresh),
    When: the row is mapped,
    Then: the effective status is ``corrupt``, is_authoritative is False, and
        both payloads are cleared to None even though parsing itself succeeds.
    """
    result = build_portfolio_account_state(make_row(), _NOW)
    assert result.effective_status == EFFECTIVE_CORRUPT
    assert result.is_authoritative is False
    assert result.balances is None
    assert result.open_positions is None


def test_observed_rollup_with_not_applicable_positions_is_coherent() -> None:
    """An observed roll-up may carry not_applicable positions and stay live.

    Given: a coherent observed row (observed balance with a well-formed payload,
        open authority window) whose position_status is ``not_applicable`` with
        no positions payload, clock, or source id (a venue that reports
        balances but has no position concept),
    When: the row is mapped,
    Then: the roll-up coherence check accepts ``not_applicable`` positions, so
        the effective status is ``observed`` and authoritative, the balances
        are present, and open_positions is None — the row is NOT corrupt.
    """
    result = build_portfolio_account_state(
        _make_row(
            position_status="not_applicable",
            open_positions_json=None,
            position_observed_at=None,
            position_payload_source_observation_id=None,
        ),
        _NOW,
    )
    assert result.effective_status == "observed"
    assert result.is_authoritative is True
    assert result.balances is not None
    assert result.open_positions is None


def test_balances_parse_none_preserving_and_by_value() -> None:
    """Balances parse into typed entries preserving None free/used splits.

    Given: a fresh coherent observed row whose balances carry a first entry
        with concrete free/used and a second collateral-valuation entry with
        NULL free/used,
    When: the row is mapped,
    Then: both entries map by value and the None free/used are preserved
        rather than zero-coerced.
    """
    result = build_portfolio_account_state(_make_row(), _NOW)
    assert result.balances is not None
    first = result.balances[0]
    assert first.currency == "BTC"
    assert first.total == 1.5
    assert first.free == 1.0
    assert first.used == 0.5
    second = result.balances[1]
    assert second.currency == "USD_collateral_value"
    assert second.total == 1000.0
    assert second.free is None
    assert second.used is None


def test_positions_parse_both_sides_by_value() -> None:
    """Positions parse into typed entries for both buy and sell sides.

    Given: a fresh coherent observed row whose positions carry a ``buy`` and a
        ``sell`` entry with finite numbers and isoformat timestamps,
    When: the row is mapped,
    Then: every field of both entries maps by value, including the parsed
        timestamps.
    """
    result = build_portfolio_account_state(_make_row(), _NOW)
    assert result.open_positions is not None
    long_pos = result.open_positions[0]
    assert long_pos.symbol == "PF_XBTUSD"
    assert long_pos.side == "buy"
    assert long_pos.size == 2.0
    assert long_pos.entry_price == 50000.0
    assert long_pos.mark_price == 50500.0
    assert long_pos.unrealized_pnl == 1000.0
    assert long_pos.unrealized_funding == -5.0
    assert long_pos.timestamp == datetime.fromisoformat(_POS1_TS)
    short_pos = result.open_positions[1]
    assert short_pos.symbol == "PF_ETHUSD"
    assert short_pos.side == "sell"
    assert short_pos.size == 3.0
    assert short_pos.timestamp == datetime.fromisoformat(_POS2_TS)


def test_empty_payload_lists_map_to_empty_not_none() -> None:
    """Empty payload lists map to empty lists, not None.

    Given: a fresh coherent observed row whose balances and positions payloads
        are both the empty JSON list ``[]`` (non-null strings, so the row stays
        coherent),
    When: the row is mapped,
    Then: balances and open_positions are empty lists (distinct from the
        never-stored None case) and the row stays authoritative.
    """
    result = build_portfolio_account_state(
        _make_row(balances_json="[]", open_positions_json="[]"), _NOW
    )
    assert result.effective_status == "observed"
    assert result.is_authoritative is True
    assert result.balances == []
    assert result.open_positions == []


def test_row_identity_provenance_and_ids_are_mapped() -> None:
    """Every non-derived row field is carried through onto the response.

    Given: a fresh coherent observed row with distinct identity, provenance,
        and error values (its fresh payload-source ids necessarily equal
        current_attempt_observation_id so the fresh-source check passes),
    When: the row is mapped,
    Then: the wallet identity, exchange/mode, per-component statuses, the
        current-attempt id, both payload-source ids, the error, and the
        envelope fields (public_id/session_id/sequence_id/timestamp) all map
        verbatim, alongside the observation timestamps and authority window.
    """
    row = _make_row(error="transient read failure")
    result = build_portfolio_account_state(row, _NOW)
    assert result.wallet_public_id == "wal-1"
    assert result.exchange == "kraken"
    assert result.mode == "live"
    assert result.sync_status == "observed"
    assert result.balance_status == "observed"
    assert result.position_status == "observed"
    assert result.valuation_status == "native_only"
    assert result.current_attempt_observation_id == 5
    assert result.balance_payload_source_observation_id == 5
    assert result.position_payload_source_observation_id == 5
    assert result.error == "transient read failure"
    assert result.balance_observed_at == _PAST
    assert result.position_observed_at == _PAST
    assert result.authoritative_until == _FUTURE
    assert result.public_id == "vas-1"
    assert result.session_id == "sess-1"
    assert result.sequence_id == 7
    assert result.timestamp == _NOW
    assert result.type == "portfolio_account_state"
