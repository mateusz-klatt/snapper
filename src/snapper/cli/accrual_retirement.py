"""Operator CLI for retiring live spot-rollover accruals that no venue charged.

The coordinator once wrote schedule-based rollover accruals for live Kraken spot
positions. That producer is contained: actual live Kraken spot financing costs
now come only from native postings. The rows it left behind are still current,
and any current accrual row keeps its shard uncertified at recovery. This
command closes an explicitly asserted set of those rows by SCD2, so they stop
counting from the closure onward and stay readable for every earlier knowledge
instant. Nothing is deleted and no balancing row is written.
"""

import asyncio
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from decimal import InvalidOperation
from typing import Annotated
from typing import Final
from typing import NoReturn
from typing import cast
from uuid import UUID

import typer
from sqlalchemy.engine import make_url

from snapper.config.settings import get_bootstrap_settings
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import dispose_repositories
from snapper.data.repository import get_repository
from snapper.data.repository_types import AccrualLedgerRow
from snapper.data.repository_types import AccrualRetirementScope

CONTAINED_EXCHANGE: Final[str] = "kraken"
"""Exchange whose live spot rollover producer is contained."""

CONTAINED_MODE: Final[str] = "live"
"""Execution mode of the contained producer; paper and backtest models keep accruing."""

CONTAINED_FUNDING_TYPE: Final[str] = "spot_margin_rollover"
"""Instrument funding type the contained producer served."""

_CONTAINED_ACCRUAL_TYPE: Final[str] = "rollover"
"""Accrual type the contained producer wrote for that funding type."""


@dataclass(frozen=True, slots=True)
class _Retirement:
    """One operator request, validated before the database is opened.

    Attributes:
        scope: Wallet, instrument and mode whose current rows are retired.
        accrual_ids: Asserted public ids, canonical and without repeats.
        expected_total: Asserted sum and asset, when the operator supplied one.
        confirm: Whether the separate confirmation flag was present.
    """

    scope: AccrualRetirementScope
    accrual_ids: tuple[str, ...]
    expected_total: tuple[Decimal, str] | None
    confirm: bool


def _fatal(message: str) -> NoReturn:
    """Print a refusal and terminate the command.

    Args:
        message: Refusal shown to the operator.

    Raises:
        typer.Exit: Always, with a non-zero status.
    """
    typer.echo(message, err=True)
    raise typer.Exit(code=1)


def _database_identity(db_url: str) -> str:
    """Render a credential-free database identity.

    Args:
        db_url: Configured SQLAlchemy database URL.

    Returns:
        Dialect, host and database without user, password, port or query.
    """
    url = make_url(db_url)
    return (
        f"dialect={url.get_backend_name()} "
        f"host={url.host or '(local)'} database={url.database or '(none)'}"
    )


def _repository(db_url: str) -> SQLAlchemyRepository:
    """Resolve the configured repository.

    Args:
        db_url: Configured SQLAlchemy database URL.

    Returns:
        Cached repository for the target database.
    """
    return cast(SQLAlchemyRepository, get_repository(db_url))


def _canonical_id(option: str, value: str) -> str:
    """Return the canonical spelling of one public id.

    Args:
        option: Command-line option that carried the value.
        value: Text supplied by the operator.

    Returns:
        Lower-case hyphenated UUID text.
    """
    try:
        return str(UUID(value))
    except ValueError:
        _fatal(f"refused: {option} is not a public id: {value!r}")


def _asserted_ids(values: Sequence[str]) -> tuple[str, ...]:
    """Canonicalize the asserted accrual ids and refuse a repeated one.

    Args:
        values: Every ``--accrual`` value in command-line order.

    Returns:
        Canonical public ids in command-line order.
    """
    canonical = tuple(_canonical_id("--accrual", value) for value in values)
    if len(set(canonical)) != len(canonical):
        _fatal("refused: an --accrual id is repeated")
    return canonical


def _expected_total(text: str | None) -> tuple[Decimal, str] | None:
    """Parse the optional asserted sum, written as an amount and an asset code.

    Args:
        text: Raw ``--expect-total`` value, or None when it was not supplied.

    Returns:
        Finite amount and asset code, or None when nothing was asserted.
    """
    if text is None:
        return None
    parts = text.split()
    if len(parts) != 2:
        _fatal('refused: --expect-total takes an amount and an asset, for example "0.0031 EUR"')
    try:
        amount = Decimal(parts[0])
    except InvalidOperation:
        amount = Decimal("NaN")
    if not amount.is_finite():
        _fatal(f"refused: --expect-total amount is not a finite decimal: {parts[0]!r}")
    return amount, parts[1]


def _total(rows: Sequence[AccrualLedgerRow]) -> Decimal:
    """Sum stored amounts through their shortest decimal spelling.

    Stored amounts are floats. Their ``str`` spelling round-trips, so the
    printed total can be pasted back as ``--expect-total`` and compares equal.

    Args:
        rows: Current accrual versions of the scope.

    Returns:
        Exact decimal sum of the spelled amounts.
    """
    return sum((Decimal(str(row["amount"])) for row in rows), Decimal(0))


def _echo_rows(rows: Sequence[AccrualLedgerRow]) -> None:
    """Print every current accrual version of the scope and their sum.

    Args:
        rows: Current accrual versions of the scope.
    """
    typer.echo(f"current accrual versions: {len(rows)}")
    for row in rows:
        typer.echo(
            f"  public_id={row['public_id']} accrued_at={row['accrued_at'].isoformat()} "
            f"type={row['accrual_type']} amount={row['amount']!r} asset={row['amount_asset']} "
            f"exchange={row['exchange']} timestamp={row['timestamp'].isoformat()} "
            f"session_id={row['session_id']} sequence_id={row['sequence_id']}"
        )
    assets = sorted({row["amount_asset"] for row in rows})
    typer.echo(f"total: {_total(rows)} {'/'.join(assets) or 'none'}")


def _refuse_foreign_rows(rows: Sequence[AccrualLedgerRow]) -> None:
    """Refuse rows the contained producer could not have written.

    Args:
        rows: Current accrual versions of the scope.
    """
    foreign = [
        row["public_id"]
        for row in rows
        if (row["exchange"], row["accrual_type"]) != (CONTAINED_EXCHANGE, _CONTAINED_ACCRUAL_TYPE)
    ]
    if foreign:
        _fatal(
            f"refused: rows are not {CONTAINED_EXCHANGE} {_CONTAINED_ACCRUAL_TYPE} accruals: "
            f"{foreign}"
        )


def _refuse_set_difference(rows: Sequence[AccrualLedgerRow], asserted: tuple[str, ...]) -> None:
    """Refuse unless the asserted ids are exactly the current rows of the scope.

    Args:
        rows: Current accrual versions of the scope.
        asserted: Canonical public ids the operator named.
    """
    current = {row["public_id"] for row in rows}
    unasserted = sorted(current - set(asserted))
    not_current = sorted(set(asserted) - current)
    if unasserted or not_current:
        _fatal(
            "refused: asserted set differs from the current rows; "
            f"current but not asserted={unasserted} asserted but not current={not_current}"
        )


def _refuse_total_difference(
    rows: Sequence[AccrualLedgerRow], expected: tuple[Decimal, str] | None
) -> None:
    """Refuse when the operator asserted a sum or asset the rows do not have.

    Args:
        rows: Current accrual versions of the scope.
        expected: Asserted sum and asset, or None when nothing was asserted.
    """
    if expected is None:
        return
    amount, asset = expected
    assets = sorted({row["amount_asset"] for row in rows})
    if assets != [asset] or _total(rows) != amount:
        _fatal(
            f"refused: --expect-total {amount} {asset} differs from the rows: "
            f"total={_total(rows)} assets={assets}"
        )


async def _close(
    repository: SQLAlchemyRepository,
    request: _Retirement,
    rows: Sequence[AccrualLedgerRow],
) -> None:
    """Close the previewed rows at one instant and print the receipt.

    Args:
        repository: Repository of the target database.
        request: Validated operator request.
        rows: Versions shown by the preview, which the writer must find again.
    """
    retired_at = datetime.now(UTC)
    try:
        closed = await repository.retire_accruals(request.scope, rows, retired_at)
    except ValueError as error:
        typer.echo(f"detail: {error}", err=True)
        _fatal("refused: rows changed concurrently, nothing was written")
    typer.echo(f"closed {closed} accrual version(s) at {retired_at.isoformat()}")
    receipt: dict[str, str | int | list[str]] = {
        "wallet_public_id": request.scope["wallet_public_id"],
        "instrument_public_id": request.scope["instrument_public_id"],
        "mode": request.scope["mode"],
        "retired_public_ids": [row["public_id"] for row in rows],
        "known_to": retired_at.isoformat(),
        "count": closed,
    }
    typer.echo(json.dumps(receipt, sort_keys=True))


async def _run(db_url: str, request: _Retirement) -> None:
    """Preview the scope's current accruals and optionally close the asserted set.

    Checks run in a fixed order and the first failure refuses without writing:
    the instrument's funding type, the rows' origin, equality of the asserted
    and current sets, then the optional asserted total.

    Args:
        db_url: Configured database URL.
        request: Validated operator request.
    """
    repository = _repository(db_url)
    try:
        spec = await repository.get_instrument_spec(
            request.scope["instrument_public_id"], datetime.now(UTC)
        )
        funding_type = None if spec is None else spec["funding_type"]
        if funding_type != CONTAINED_FUNDING_TYPE:
            _fatal(
                f"refused: instrument funding type is {funding_type!r}; only "
                f"{CONTAINED_FUNDING_TYPE} accruals of {CONTAINED_MODE} {CONTAINED_EXCHANGE} "
                "are retired here"
            )
        rows = await repository.get_accrual_retirement_candidates(request.scope)
        _echo_rows(rows)
        _refuse_foreign_rows(rows)
        _refuse_set_difference(rows, request.accrual_ids)
        _refuse_total_difference(rows, request.expected_total)
        typer.echo("set matches")
        if not request.confirm:
            typer.echo("DRY RUN: nothing was written; add --confirm to close these rows")
            return
        await _close(repository, request, rows)
    finally:
        await dispose_repositories()


def retire_accruals(
    wallet: Annotated[
        str,
        typer.Option("--wallet", help="Wallet public id of the shard"),
    ],
    instrument: Annotated[
        str,
        typer.Option("--instrument", help="Instrument public id of the shard"),
    ],
    accrual: Annotated[
        list[str],
        typer.Option("--accrual", help="Public id of one current accrual to close; repeatable"),
    ],
    expect_total: Annotated[
        str | None,
        typer.Option("--expect-total", help='Asserted sum and asset, for example "0.0031 EUR"'),
    ] = None,
    confirm: Annotated[
        bool,
        typer.Option("--confirm", help="Required separately to perform the SCD2 close"),
    ] = False,
) -> None:
    """Preview or SCD2-close asserted current accruals of one live Kraken spot shard.

    The command serves one case: schedule-based rollover accruals that the
    contained live Kraken spot producer left current. Every ``--accrual`` names
    one row; together they must be exactly the current rows of the wallet and
    instrument in live mode. The default is a read-only preview that prints
    those rows and their sum. ``--confirm`` closes the same rows at one
    instant and prints a JSON receipt; it never widens the selection.

    Args:
        wallet: Wallet public id of the shard.
        instrument: Instrument public id of the shard.
        accrual: Public ids of the current accrual rows to close.
        expect_total: Optional asserted sum and asset of those rows.
        confirm: Separately acknowledge closing the previewed versions.
    """
    request = _Retirement(
        scope=AccrualRetirementScope(
            wallet_public_id=_canonical_id("--wallet", wallet),
            instrument_public_id=_canonical_id("--instrument", instrument),
            mode=CONTAINED_MODE,
        ),
        accrual_ids=_asserted_ids(accrual),
        expected_total=_expected_total(expect_total),
        confirm=confirm,
    )
    db_url = get_bootstrap_settings().db_url
    typer.echo(f"target database: {_database_identity(db_url)}")
    asyncio.run(_run(db_url, request))
