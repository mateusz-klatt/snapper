"""Exact retained REST account profiles, immutable source binding and separate knowledge clocks."""

import base64
import json
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import func
from sqlalchemy import select

from snapper.application.trade.futures_native_accounts import FUTURES_NATIVE_ACCOUNTS_CHANNEL
from snapper.application.trade.futures_native_accounts import FuturesNativeAccountsRequest
from snapper.application.trade.futures_native_accounts import NativeAccountsSourceError
from snapper.application.trade.futures_native_accounts import get_bound_futures_native_accounts
from snapper.application.trade.futures_native_accounts import parse_futures_native_accounts
from snapper.core.contract_accounting_observation import AccountingObservationScope
from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_route_authority import client_selection_digest
from snapper.data.accounting_observation_binding import AccountingObservationBindingError
from snapper.data.contract_accounting_store import capture_accounting_observation
from snapper.data.models import AccountingHoldResolution
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import Execution
from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 10, 23, 11, tzinfo=UTC)
_WIRE = (
    b'{"result":"success","serverTime":"2026-10-23T11:00:00.125Z","accounts":{'
    b'"cash":{"type":"cashAccount","balances":{"USD":12.3400000000000000001}},'
    b'"flex":{"type":"multiCollateralMarginAccount","currencies":{'
    b'"USD":{"quantity":-1.25,"value":999},"XBT":{"quantity":0.003,"value":777}},'
    b'"balanceValue":1e30,"portfolioValue":1e90,"availableMargin":50000},'
    b'"fi_xbtusd":{"type":"marginAccount","currency":"xbt","balances":{"xbt":0.04}}}}'
)


def _id(number: int) -> str:
    """Provide separate durable source, wallet and credential fixture identities."""
    return f"00000000-0000-7000-8000-{number:012d}"


def _selection(**updates: object) -> ExecutionClientSelection:
    """Specify only public actual client provenance, without creating an authority."""
    return ExecutionClientSelection.model_validate(
        {
            "wallet_public_id": _id(101),
            "exchange": "kraken_futures",
            "mode": "live",
            "environment": "production",
            "credential_public_id": _id(102),
            "credential_timestamp": _NOW - timedelta(days=1),
            "public_api_origin": "https://futures.kraken.com",
            **updates,
        }
    )


def _raw(wire: bytes = _WIRE, **updates: object) -> RawAccountingObservation:
    """Retain the whole original native account response and its actual capture clock."""
    selected = _selection()
    raw = RawAccountingObservation(
        scope=AccountingObservationScope(
            wallet_public_id=selected.wallet_public_id,
            exchange=selected.exchange,
            mode=selected.mode,
            environment=selected.environment,
        ),
        route_revision=client_selection_digest(selected),
        credential_public_id=selected.credential_public_id,
        credential_timestamp=selected.credential_timestamp,
        kind="account_cash",
        channel=FUTURES_NATIVE_ACCOUNTS_CHANNEL,
        representation="wire_bytes_base64",
        material=base64.b64encode(wire).decode("ascii"),
        observed_at=_NOW,
    )
    return RawAccountingObservation.model_validate({**raw.model_dump(), **updates})


def _envelope(accounts: object, **updates: object) -> bytes:
    """Author structural source cases as actual JSON bytes without inventing precision evidence."""
    return json.dumps(
        {"result": "success", "serverTime": "2026-10-23T11:00:00Z", "accounts": accounts, **updates}
    ).encode()


def _stamp(number: int = 1, seconds: int = 1) -> AccountingWriteProvenance:
    """Record actual local source knowledge independently of the response server clock."""
    return AccountingWriteProvenance(
        public_id=_id(number),
        session_id=_id(999),
        sequence_id=number,
        timestamp=_NOW + timedelta(seconds=seconds),
    )


def test_native_profiles_preserve_exact_units_without_collateral_valuation() -> None:
    """Same-currency balances remain separate exact quantities.

    Given: three distinct native accounts,
    When: the exact native account parser reads it,
    Then: same-currency balances remain separate exact quantities.
    """
    snapshot = parse_futures_native_accounts(_raw())
    assert snapshot.server_time_text == "2026-10-23T11:00:00.125Z"
    assert snapshot.server_time == _NOW + timedelta(milliseconds=125)
    assert [profile.native_account_key for profile in snapshot.profiles] == [
        "cash",
        "flex",
        "fi_xbtusd",
    ]
    cash, flex, inverse = snapshot.profiles
    assert [(entry.currency, entry.quantity) for entry in cash.quantities] == [
        ("USD", Decimal("12.3400000000000000001"))
    ]
    assert [(entry.currency, entry.quantity) for entry in flex.quantities] == [
        ("USD", Decimal("-1.25")),
        ("XBT", Decimal("0.003")),
    ]
    assert inverse.settlement_currency == "xbt"
    assert [(entry.currency, entry.quantity) for entry in inverse.quantities] == [
        ("xbt", Decimal("0.04"))
    ]
    assert cash.settlement_currency is flex.settlement_currency is None


def test_native_type_and_exact_account_key_do_not_come_from_a_familiar_alias() -> None:
    """Explicit type supplies only its real shape.

    Given: a differently named multi-collateral account,
    When: the exact native account parser reads it,
    Then: explicit type supplies only its real shape.
    """
    raw = _raw(
        _envelope(
            {
                "Other-Account": {
                    "type": "multiCollateralMarginAccount",
                    "currencies": {"usd": {"quantity": 2}},
                }
            }
        )
    )
    profile = parse_futures_native_accounts(raw).profiles[0]
    assert profile.native_account_key == "Other-Account"
    assert profile.quantities[0].currency == "usd"
    assert profile.quantities[0].quantity == Decimal(2)


def test_empty_supported_account_inventory_is_distinct_from_missing_inventory() -> None:
    """No account profile or zero cash namespace is manufactured.

    Given: a successful empty object,
    When: the exact native account parser reads it,
    Then: no account profile or zero cash namespace is manufactured.
    """
    assert parse_futures_native_accounts(_raw(_envelope({}))).profiles == ()
    snapshot = parse_futures_native_accounts(
        _raw(_envelope({"cash": {"type": "cashAccount", "balances": {}}}))
    )
    assert snapshot.profiles[0].quantities == ()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("kind", "unknown"),
        ("channel", "kraken_futures.ws.balances"),
        ("target_json_pointer", "/accounts/flex"),
        ("representation", "parsed_fields_json"),
    ],
)
def test_source_context_must_prove_the_complete_original_response(
    field: str, value: object
) -> None:
    """Native accounts are not inferred from its fields.

    Given: weaker or differently selected material,
    When: the exact native account parser reads it,
    Then: native accounts are not inferred from its fields.
    """
    with pytest.raises(NativeAccountsSourceError, match="original_source_required"):
        parse_futures_native_accounts(_raw(**{field: value}))


def test_another_venue_cannot_supply_futures_account_source() -> None:
    """Actual source venue still controls admission.

    Given: a Spot source carrying a Futures-shaped body,
    When: the exact native account parser reads it,
    Then: actual source venue still controls admission.
    """
    scope = AccountingObservationScope.model_validate(
        {**_raw().scope.model_dump(), "exchange": "kraken"}
    )
    with pytest.raises(NativeAccountsSourceError, match="original_source_required"):
        parse_futures_native_accounts(_raw(scope=scope))


@pytest.mark.parametrize(
    "wire", [b"{", b"\xff", b"[]", b'{"accounts":{},"accounts":{}}', b'{"accounts":{"value":NaN}}']
)
def test_malformed_or_ambiguous_original_json_remains_held(wire: bytes) -> None:
    """Source interpretation never accepts a reconstructed fallback.

    Given: invalid wire JSON,
    When: the exact native account parser reads it,
    Then: source interpretation never accepts a reconstructed fallback.
    """
    with pytest.raises(NativeAccountsSourceError, match="original_json_invalid"):
        parse_futures_native_accounts(_raw(wire))


@pytest.mark.parametrize(
    "updates", [{"result": "error"}, {"result": True}, {"api_key": "synthetic-control-field"}]
)
def test_success_requires_the_actual_non_control_source_envelope(
    updates: dict[str, object],
) -> None:
    """No account inventory is certified.

    Given: an error or authentication-control envelope,
    When: the exact native account parser reads it,
    Then: no account inventory is certified.
    """
    with pytest.raises(NativeAccountsSourceError, match="success_unproven"):
        parse_futures_native_accounts(_raw(_envelope({}, **updates)))


@pytest.mark.parametrize(
    "timestamp", [None, 0, " ", "invalid", "2026-10-23T11:00:00", "2026-10-23T13:00:00+02:00"]
)
def test_missing_invalid_or_non_utc_server_clock_is_refused(timestamp: object) -> None:
    """Source interpretation does not substitute the local clock.

    Given: a malformed provider clock,
    When: the exact native account parser reads it,
    Then: source interpretation does not substitute the local clock.
    """
    with pytest.raises(NativeAccountsSourceError):
        parse_futures_native_accounts(_raw(_envelope({}, serverTime=timestamp)))


@pytest.mark.parametrize(
    "accounts",
    [
        None,
        [],
        {"flex": None},
        {"flex": {"type": "futureUnknownAccount", "currencies": {}}},
        {"cash": {"type": "cashAccount"}},
        {"cash": {"type": "cashAccount", "balances": []}},
        {"cash": {"type": "cashAccount", "balances": {"USD": True}}},
        {"cash": {"type": "cashAccount", "balances": {"USD": " 12.3"}}},
        {"cash": {"type": "cashAccount", "balances": {" ": 1}}},
        {" ": {"type": "cashAccount", "balances": {}}},
        {"flex": {"type": "multiCollateralMarginAccount", "currencies": {"USD": 5}}},
        {"flex": {"type": "multiCollateralMarginAccount", "currencies": {"USD": {"value": 5}}}},
        {"fi_xbtusd": {"type": "marginAccount", "balances": {"xbt": 1}}},
        {"fi_xbtusd": {"type": "marginAccount", "currency": "xbt", "balances": {"pi_xbtusd": 2}}},
    ],
)
def test_malformed_profile_never_disappears_into_a_partial_success(accounts: object) -> None:
    """The whole source inventory remains unproven.

    Given: an unsupported or incomplete native profile,
    When: the exact native account parser reads it,
    Then: the whole source inventory remains unproven.
    """
    with pytest.raises(NativeAccountsSourceError):
        parse_futures_native_accounts(_raw(_envelope(accounts)))


def test_documented_string_balances_keep_contract_components_separate() -> None:
    """The documented legacy mixed balance map does not become fictitious cash.

    Given: Exact decimal strings and contract names beside an explicit settlement currency,
    When: The original REST profile is interpreted,
    Then: Cash quantities stay exact and non-settlement components retain unknown units.
    """
    accounts = {
        "cash": {"type": "cashAccount", "balances": {"xbt": "141.3175679700000000001"}},
        "fi_xbtusd": {
            "type": "marginAccount",
            "currency": "xbt",
            "balances": {"FI_XBTUSD_171215": "50000", "xbt": "141.31756797", "xrp": "0"},
        },
    }
    cash, margin = parse_futures_native_accounts(_raw(_envelope(accounts))).profiles
    assert cash.quantities[0].quantity == Decimal("141.3175679700000000001")
    assert [(entry.currency, entry.quantity) for entry in margin.quantities] == [
        ("xbt", Decimal("141.31756797"))
    ]
    assert [(entry.native_key, entry.quantity) for entry in margin.unclassified_components] == [
        ("FI_XBTUSD_171215", Decimal("50000")),
        ("xrp", Decimal(0)),
    ]


@pytest.mark.parametrize("value", ("NaN", "Infinity", "+1", "01", "", "1e999999999999999999999"))
def test_invalid_legacy_decimal_text_cannot_be_coerced(value: str) -> None:
    """Invalid quoted balances remain unproven instead of acquiring a numeric fallback.

    Given: Nonfinite, ambiguous or unrepresentable native decimal text,
    When: The original cash profile is parsed,
    Then: The entire source inventory is refused.
    """
    accounts = {"cash": {"type": "cashAccount", "balances": {"USD": value}}}
    with pytest.raises(NativeAccountsSourceError, match="decimal_text_invalid"):
        parse_futures_native_accounts(_raw(_envelope(accounts)))


def test_multicollateral_quantity_does_not_borrow_legacy_string_encoding() -> None:
    """The typed multicollateral numeric field retains its documented JSON-number contract.

    Given: A string in a multicollateral currency quantity,
    When: Its exact source is parsed,
    Then: Legacy balance text handling does not silently widen the different schema.
    """
    accounts = {
        "flex": {"type": "multiCollateralMarginAccount", "currencies": {"USD": {"quantity": "1"}}}
    }
    with pytest.raises(NativeAccountsSourceError, match="exact_quantity_required"):
        parse_futures_native_accounts(_raw(_envelope(accounts)))


@pytest.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Use a real isolated immutable raw store and the current complete physical schema."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'native-accounts.db'}")
    await repo.create_all()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


async def test_bound_reader_replays_real_source_without_financial_writes_or_hold_resolution(
    repository: SQLAlchemyRepository,
) -> None:
    """Source replay returns its real clocks and leaves financial state untouched.

    Given: a retained response,
    When: the bound source reader replays it,
    Then: source replay returns its real clocks and leaves financial state untouched.
    """
    captured = await capture_accounting_observation(repository, _raw(), _stamp())
    request = FuturesNativeAccountsRequest(
        raw_receipt_public_id=captured.public_id, client_selection=_selection()
    )
    async with repository.session() as session:
        result = await get_bound_futures_native_accounts(
            session, request, _NOW + timedelta(seconds=2)
        )
        assert result.source_observed_at == _NOW
        assert result.source_recorded_at == _NOW + timedelta(seconds=1)
        assert result.snapshot == parse_futures_native_accounts(_raw())
        for model in (Execution, ContractAccountingEvent, AccountingHoldResolution):
            assert await session.scalar(select(func.count()).select_from(model)) == 0


async def test_demo_source_keeps_its_actual_environment_and_endpoint(
    repository: SQLAlchemyRepository,
) -> None:
    """The bound inventory stays separate from production.

    Given: an actual demo-selected source,
    When: the bound source reader replays it,
    Then: the bound inventory stays separate from production.
    """
    selection = _selection(environment="demo", public_api_origin="https://demo-futures.kraken.com")
    scope = AccountingObservationScope.model_validate(
        {**_raw().scope.model_dump(), "environment": "demo"}
    )
    captured = await capture_accounting_observation(
        repository,
        _raw(scope=scope, route_revision=client_selection_digest(selection)),
        _stamp(),
    )
    request = FuturesNativeAccountsRequest(
        raw_receipt_public_id=captured.public_id, client_selection=selection
    )
    async with repository.session() as session:
        result = await get_bound_futures_native_accounts(
            session, request, _NOW + timedelta(seconds=2)
        )
    assert result.request.client_selection.environment == "demo"
    assert result.request.client_selection.public_api_origin == "https://demo-futures.kraken.com"


@pytest.mark.parametrize(("observed_seconds", "recorded_seconds"), [(0, 3), (3, 1)])
async def test_bound_reader_refuses_source_not_known_at_the_requested_clock(
    repository: SQLAlchemyRepository, observed_seconds: int, recorded_seconds: int
) -> None:
    """Replay cannot certify earlier knowledge.

    Given: a later actual capture or recording clock,
    When: the bound source reader replays it,
    Then: replay cannot certify earlier knowledge.
    """
    captured = await capture_accounting_observation(
        repository,
        _raw(observed_at=_NOW + timedelta(seconds=observed_seconds)),
        _stamp(seconds=recorded_seconds),
    )
    request = FuturesNativeAccountsRequest(
        raw_receipt_public_id=captured.public_id, client_selection=_selection()
    )
    async with repository.session() as session:
        with pytest.raises(NativeAccountsSourceError, match="source_not_yet_known"):
            await get_bound_futures_native_accounts(session, request, _NOW + timedelta(seconds=2))


async def test_bound_reader_requires_an_aware_local_knowledge_clock(
    repository: SQLAlchemyRepository,
) -> None:
    """No implicit local timezone determines source eligibility.

    Given: a naive caller clock,
    When: the bound source reader replays it,
    Then: no implicit local timezone determines source eligibility.
    """
    request = FuturesNativeAccountsRequest(
        raw_receipt_public_id=_id(404), client_selection=_selection()
    )
    async with repository.session() as session:
        with pytest.raises(NativeAccountsSourceError, match="clock_not_aware"):
            await get_bound_futures_native_accounts(session, request, _NOW.replace(tzinfo=None))


@pytest.mark.parametrize(
    "updates",
    [
        {"wallet_public_id": _id(103)},
        {"credential_public_id": _id(104)},
        {"public_api_origin": "https://unproven.example"},
    ],
)
async def test_bound_reader_rejects_a_different_selected_client(
    repository: SQLAlchemyRepository, updates: dict[str, object]
) -> None:
    """The original raw receipt cannot certify its account source.

    Given: another client selection,
    When: the bound source reader replays it,
    Then: the original raw receipt cannot certify its account source.
    """
    captured = await capture_accounting_observation(repository, _raw(), _stamp())
    request = FuturesNativeAccountsRequest(
        raw_receipt_public_id=captured.public_id, client_selection=_selection(**updates)
    )
    async with repository.session() as session:
        with pytest.raises(NativeAccountsSourceError, match="client_selection_mismatch"):
            await get_bound_futures_native_accounts(session, request, _NOW + timedelta(seconds=2))


async def test_unsupported_origin_is_refused_even_when_its_digest_matches_source(
    repository: SQLAlchemyRepository,
) -> None:
    """Digest equality alone is insufficient.

    Given: a self-consistent unsupported public origin,
    When: the bound source reader replays it,
    Then: digest equality alone is insufficient.
    """
    selection = _selection(public_api_origin="https://unproven.example")
    captured = await capture_accounting_observation(
        repository, _raw(route_revision=client_selection_digest(selection)), _stamp()
    )
    request = FuturesNativeAccountsRequest(
        raw_receipt_public_id=captured.public_id, client_selection=selection
    )
    async with repository.session() as session:
        with pytest.raises(NativeAccountsSourceError, match="client_selection_mismatch"):
            await get_bound_futures_native_accounts(session, request, _NOW + timedelta(seconds=2))


async def test_conflicted_raw_identity_is_not_usable_account_source(
    repository: SQLAlchemyRepository,
) -> None:
    """Conflict storage cannot become authority.

    Given: changed bytes under one claimed receipt UUID,
    When: the bound source reader replays it,
    Then: conflict storage cannot become authority.
    """
    await capture_accounting_observation(repository, _raw(), _stamp())
    conflict = await capture_accounting_observation(repository, _raw(_envelope({})), _stamp())
    assert conflict.conflicted
    request = FuturesNativeAccountsRequest(
        raw_receipt_public_id=conflict.public_id, client_selection=_selection()
    )
    async with repository.session() as session:
        with pytest.raises(AccountingObservationBindingError, match="projection_mismatch"):
            await get_bound_futures_native_accounts(session, request, _NOW + timedelta(seconds=2))


async def test_source_capture_cannot_precede_its_selected_credential_revision(
    repository: SQLAlchemyRepository,
) -> None:
    """The earlier capture cannot prove that revision.

    Given: self-consistent future credential metadata,
    When: the bound source reader replays it,
    Then: the earlier capture cannot prove that revision.
    """
    future = _NOW + timedelta(seconds=1)
    selection = _selection(credential_timestamp=future)
    captured = await capture_accounting_observation(
        repository,
        _raw(credential_timestamp=future, route_revision=client_selection_digest(selection)),
        _stamp(),
    )
    request = FuturesNativeAccountsRequest(
        raw_receipt_public_id=captured.public_id, client_selection=selection
    )
    async with repository.session() as session:
        with pytest.raises(NativeAccountsSourceError, match="capture_precedes_credential"):
            await get_bound_futures_native_accounts(session, request, _NOW + timedelta(seconds=2))
