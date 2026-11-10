"""Tests for the accrual retirement CLI."""

import json
from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from click.testing import Result
from typer.testing import CliRunner

from snapper.application.engine.service import TradingEngineService
from snapper.application.engine.trader import TraderCoordinator
from snapper.cli import accrual_retirement
from snapper.cli.app import app
from snapper.data.repository_types import AccrualLedgerRow

_WALLET = "00000000-0000-7000-8000-000000000001"
_INSTRUMENT = "00000000-0000-7000-8000-0000000000b1"
_FIRST = "00000000-0000-7000-8000-0000000000a1"
_SECOND = "00000000-0000-7000-8000-0000000000a2"
_ABSENT = "00000000-0000-7000-8000-0000000000a3"
_BOUNDARY = datetime(2026, 11, 8, 20, 0, tzinfo=UTC)
_TOTAL = "-0.0010250000024345249852042447"
_SCOPE = {"wallet_public_id": _WALLET, "instrument_public_id": _INSTRUMENT, "mode": "live"}


def _row(public_id: str, amount: float, exchange: str = "kraken") -> AccrualLedgerRow:
    """Build one current rollover accrual as the repository returns it."""
    return AccrualLedgerRow(
        public_id=public_id,
        instrument_public_id=_INSTRUMENT,
        mode="live",
        accrual_type="rollover",
        accrued_at=_BOUNDARY,
        amount=amount,
        amount_asset="EUR",
        rate=0.0002,
        notional=3.96,
        position_quantity_at_accrual=4.07,
        exchange=exchange,
        timestamp=_BOUNDARY,
        session_id="session",
        sequence_id=1,
    )


@pytest.fixture
def repository(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Provide a mocked repository holding two current rollover accruals."""
    mocked = MagicMock()
    mocked.get_instrument_spec = AsyncMock(return_value={"funding_type": "spot_margin_rollover"})
    mocked.get_accrual_retirement_candidates = AsyncMock(
        return_value=[_row(_FIRST, -0.001025), _row(_SECOND, -2.4345249852042447e-12)]
    )
    mocked.retire_accruals = AsyncMock(return_value=2)
    monkeypatch.setattr(accrual_retirement, "configured_repository", lambda db_url: mocked)
    monkeypatch.setattr(accrual_retirement, "dispose_repositories", AsyncMock())
    monkeypatch.setattr(
        accrual_retirement,
        "get_bootstrap_settings",
        lambda: MagicMock(db_url="postgresql+asyncpg://operator:secret@prod-db/snapper_prod"),
    )
    return mocked


def _invoke(*arguments: str) -> Result:
    """Run the command for the test wallet and instrument."""
    return CliRunner().invoke(
        app,
        ["retire-accruals", "--wallet", _WALLET, "--instrument", _INSTRUMENT, *arguments],
    )


def test_dry_run_previews_rows_and_total_and_writes_nothing(repository: MagicMock) -> None:
    """The default path is a complete read-only preview.

    Given: Two current accruals and a credential-bearing production URL.
    When: Both are asserted without ``--confirm``.
    Then: The command prints the safe database identity, both rows and their
        exact sum, never exposes credentials, reads the live scope only and
        never calls the writer.
    """
    result = _invoke("--accrual", _FIRST, "--accrual", _SECOND)

    assert result.exit_code == 0, result.stderr
    assert "host=prod-db database=snapper_prod" in result.stdout
    assert "operator" not in result.stdout
    assert "secret" not in result.stdout
    assert "current accrual versions: 2" in result.stdout
    assert f"public_id={_FIRST} accrued_at={_BOUNDARY.isoformat()} type=rollover" in result.stdout
    assert "amount=-2.4345249852042447e-12 asset=EUR exchange=kraken" in result.stdout
    assert f"total: {_TOTAL} EUR" in result.stdout
    assert "set matches" in result.stdout
    assert "DRY RUN: nothing was written" in result.stdout
    repository.get_accrual_retirement_candidates.assert_awaited_once_with(_SCOPE)
    repository.retire_accruals.assert_not_awaited()
    accrual_retirement.dispose_repositories.assert_awaited_once()


def test_confirm_closes_the_previewed_rows_and_prints_a_receipt(repository: MagicMock) -> None:
    """Confirmation writes exactly what the preview showed.

    Given: Two current accruals asserted with their printed total, one id in
        upper case.
    When: The command runs with ``--confirm``.
    Then: The writer receives the live scope, the previewed rows and one
        aware instant, and the last line is a receipt naming both ids, that
        instant and the closed count.
    """
    result = _invoke(
        "--accrual",
        _FIRST.upper(),
        "--accrual",
        _SECOND,
        "--expect-total",
        f"{_TOTAL} EUR",
        "--confirm",
    )

    assert result.exit_code == 0, result.stderr
    assert "DRY RUN" not in result.stdout
    scope, rows, retired_at = repository.retire_accruals.await_args.args
    assert scope == _SCOPE
    assert [row["public_id"] for row in rows] == [_FIRST, _SECOND]
    assert retired_at.tzinfo is UTC
    assert f"closed 2 accrual version(s) at {retired_at.isoformat()}" in result.stdout
    assert json.loads(result.stdout.splitlines()[-1]) == {
        **_SCOPE,
        "retired_public_ids": [_FIRST, _SECOND],
        "known_to": retired_at.isoformat(),
        "count": 2,
    }


@pytest.mark.parametrize(
    ("arguments", "refusal"),
    [
        (["--wallet", "w-1", "--instrument", _INSTRUMENT, "--accrual", _FIRST], "--wallet"),
        (["--wallet", _WALLET, "--instrument", "XRP-EUR", "--accrual", _FIRST], "--instrument"),
        (["--wallet", _WALLET, "--instrument", _INSTRUMENT, "--accrual", "A1"], "--accrual"),
    ],
)
def test_refuses_a_value_that_is_not_a_public_id(
    repository: MagicMock, arguments: list[str], refusal: str
) -> None:
    """A malformed identity is refused before the database is opened.

    Given: One option carrying text that is not a UUID.
    When: The command validates its input.
    Then: It names that option, exits non-zero and reads nothing.
    """
    result = CliRunner().invoke(app, ["retire-accruals", *arguments])

    assert result.exit_code == 1
    assert f"refused: {refusal} is not a public id" in result.stderr
    repository.get_instrument_spec.assert_not_awaited()


def test_refuses_a_repeated_accrual_id(repository: MagicMock) -> None:
    """One row cannot be asserted twice, whatever its spelling.

    Given: The same accrual id in lower and upper case.
    When: The command validates its input.
    Then: It refuses before reading the database.
    """
    result = _invoke("--accrual", _FIRST, "--accrual", _FIRST.upper())

    assert result.exit_code == 1
    assert "refused: an --accrual id is repeated" in result.stderr
    repository.get_instrument_spec.assert_not_awaited()


@pytest.mark.parametrize(
    ("text", "refusal"),
    [
        ("1", "takes an amount and an asset"),
        ("1 EUR now", "takes an amount and an asset"),
        ("one EUR", "amount is not a finite decimal: 'one'"),
        ("NaN EUR", "amount is not a finite decimal: 'NaN'"),
        ("Infinity EUR", "amount is not a finite decimal: 'Infinity'"),
    ],
)
def test_refuses_a_malformed_expected_total(repository: MagicMock, text: str, refusal: str) -> None:
    """The asserted total must be one finite amount and one asset code.

    Given: An ``--expect-total`` value in another shape.
    When: The command validates its input.
    Then: It explains the expected shape and reads nothing.
    """
    result = _invoke("--accrual", _FIRST, "--expect-total", text)

    assert result.exit_code == 1
    assert f"refused: --expect-total {refusal}" in result.stderr
    repository.get_instrument_spec.assert_not_awaited()


@pytest.mark.parametrize(
    "spec",
    [None, {"funding_type": None}, {"funding_type": "perpetual_funding"}],
)
def test_refuses_an_instrument_outside_the_contained_route(
    repository: MagicMock, spec: dict[str, str | None] | None
) -> None:
    """Only spot rollover instruments are served.

    Given: An instrument with no specification or another funding type.
    When: The command checks the route.
    Then: It refuses before reading any accrual and still releases the
        repository.
    """
    repository.get_instrument_spec.return_value = spec

    result = _invoke("--accrual", _FIRST, "--accrual", _SECOND)

    assert result.exit_code == 1
    assert "refused: instrument funding type is" in result.stderr
    assert "only spot_margin_rollover accruals of live kraken" in result.stderr
    repository.get_accrual_retirement_candidates.assert_not_awaited()
    accrual_retirement.dispose_repositories.assert_awaited_once()


def test_refuses_rows_written_for_another_exchange(repository: MagicMock) -> None:
    """A current row from another venue stops the command.

    Given: A scope whose second current accrual belongs to another exchange.
    When: Both rows are asserted and confirmed.
    Then: The command names the foreign row and writes nothing.
    """
    repository.get_accrual_retirement_candidates.return_value = [
        _row(_FIRST, -0.001025),
        _row(_SECOND, -0.002, exchange="kraken_futures"),
    ]

    result = _invoke("--accrual", _FIRST, "--accrual", _SECOND, "--confirm")

    assert result.exit_code == 1
    assert f"refused: rows are not kraken rollover accruals: ['{_SECOND}']" in result.stderr
    repository.retire_accruals.assert_not_awaited()


@pytest.mark.parametrize(
    ("asserted", "difference"),
    [
        ([_FIRST], f"current but not asserted=['{_SECOND}'] asserted but not current=[]"),
        (
            [_FIRST, _SECOND, _ABSENT],
            f"current but not asserted=[] asserted but not current=['{_ABSENT}']",
        ),
    ],
)
def test_refuses_an_asserted_set_that_differs_from_the_current_rows(
    repository: MagicMock, asserted: list[str], difference: str
) -> None:
    """The asserted ids must be exactly the current rows.

    Given: Two current accruals and an assertion naming fewer or more ids.
    When: The command compares the sets under ``--confirm``.
    Then: It prints the rows, names the difference and writes nothing.
    """
    arguments = [value for accrual in asserted for value in ("--accrual", accrual)]

    result = _invoke(*arguments, "--confirm")

    assert result.exit_code == 1
    assert "current accrual versions: 2" in result.stdout
    assert difference in result.stderr
    assert "set matches" not in result.stdout
    repository.retire_accruals.assert_not_awaited()


def test_an_emptied_scope_refuses_every_asserted_id(repository: MagicMock) -> None:
    """A repeat after a successful retirement finds nothing current.

    Given: A scope without current accruals.
    When: A previously retired id is asserted again.
    Then: The preview shows no rows and a zero total, and the assertion is
        refused as not current.
    """
    repository.get_accrual_retirement_candidates.return_value = []

    result = _invoke("--accrual", _FIRST, "--confirm")

    assert result.exit_code == 1
    assert "current accrual versions: 0" in result.stdout
    assert "total: 0 none" in result.stdout
    assert f"asserted but not current=['{_FIRST}']" in result.stderr
    repository.retire_accruals.assert_not_awaited()


@pytest.mark.parametrize("text", ["-0.001 EUR", f"{_TOTAL} USD"])
def test_refuses_an_asserted_total_the_rows_do_not_have(repository: MagicMock, text: str) -> None:
    """A wrong sum or asset stops the command after the set matched.

    Given: Two current accruals and an asserted total with another amount or
        another asset.
    When: The command compares the total under ``--confirm``.
    Then: It prints the actual total and assets and writes nothing.
    """
    result = _invoke("--accrual", _FIRST, "--accrual", _SECOND, "--expect-total", text, "--confirm")

    assert result.exit_code == 1
    assert f"refused: --expect-total {text} differs from the rows" in result.stderr
    assert f"total={_TOTAL} assets=['EUR']" in result.stderr
    repository.retire_accruals.assert_not_awaited()


def test_reports_rows_that_changed_between_preview_and_write(repository: MagicMock) -> None:
    """A concurrent change found by the writer is reported as a refusal.

    Given: A writer that finds different current versions under its lock.
    When: The command confirms the previewed set.
    Then: It prints the writer's detail and the refusal, and no receipt.
    """
    repository.retire_accruals.side_effect = ValueError(
        "accrual versions changed after preview; nothing was written"
    )

    result = _invoke("--accrual", _FIRST, "--accrual", _SECOND, "--confirm")

    assert result.exit_code == 1
    assert "detail: accrual versions changed after preview" in result.stderr
    assert "refused: rows changed concurrently, nothing was written" in result.stderr
    assert "closed" not in result.stdout
    accrual_retirement.dispose_repositories.assert_awaited_once()


def test_the_served_route_is_the_one_the_coordinator_contains() -> None:
    """The command serves exactly the route whose accrual producer is stopped.

    Given: The exchange, mode and funding type this command retires for.
    When: The coordinator is asked whether that route requires native postings.
    Then: It does, so no schedule accrual can reappear after a retirement,
        while the paper sibling of the same route keeps its model.
    """
    live = cast(
        TradingEngineService,
        SimpleNamespace(
            exchange=accrual_retirement.CONTAINED_EXCHANGE,
            mode=accrual_retirement.CONTAINED_MODE,
        ),
    )
    paper = cast(
        TradingEngineService,
        SimpleNamespace(exchange=accrual_retirement.CONTAINED_EXCHANGE, mode="paper"),
    )

    assert TraderCoordinator._requires_native_rollover(
        live, accrual_retirement.CONTAINED_FUNDING_TYPE
    )
    assert not TraderCoordinator._requires_native_rollover(
        paper, accrual_retirement.CONTAINED_FUNDING_TYPE
    )
