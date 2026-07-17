"""Add immutable execution scope columns and the per-scope commit counter.

Executions gain ``exchange``/``mode`` (the certification scope, previously
resolvable only through SCD2 Order -> Instrument joins) and
``scope_sequence`` (a per-``(wallet_public_id, exchange, mode)`` contiguous
counter assigned under the per-wallet execution fence). The total unique
index ``uq_executions_scope_sequence`` serves the unlocked watermark
capture, the ingest ``max+1`` read, and the replay bundle's range scan,
and doubles as the fail-closed backstop against double allocation and
SCD2 supersede of ledger rows.

Refusal atomicity: SQLite/Alembic offers no transactional DDL, so EVERY
data-dependent validation (offline detection, lineage, target scope
domain, anchor emptiness) runs BEFORE the first DDL statement — a refusal
leaves the schema at exactly revision 0028 and a remediate-and-retry
starts clean instead of colliding with half-applied columns or indexes.
The downgrade applies the same rule: it refuses BEFORE any destructive
DDL while a ``scope_sequence`` anchor exists, because the restored
``execution_id`` watermark unit cannot represent such an anchor.

Backfill policy is fail-closed: rows whose active lineage is missing or
contradictory ABORT the migration (a fabricated scope would poison
certification input), as do rows whose active instrument ``exchange``
would violate the new executions scope CHECK (the legacy instruments
CHECK permits blank values); the operator remediates and re-runs. Alias
UUID wallet spellings are normalized to the canonical form before
numbering (SQLite stores wallet identities verbatim, so raw-text
partitioning would split one logical wallet into several counter
scopes), and the crossed-wallet lineage comparison canonicalizes both
sides for the same reason. Legacy rows receive ``scope_sequence`` as
``ROW_NUMBER()`` per scope ordered by ``id`` (the previous watermark's
own order; no anchors exist anywhere, so no certified range can be
invalidated — the ordering only needs to be deterministic). The spot
anchor ``source_watermark_kind`` CHECK swaps from ``'execution_id'`` to
``'scope_sequence'`` (the anchor table is empty in every deployment;
verified here before any DDL).

Operational: ``executions`` is a live-written table and the deploy shape
(``make migrate-prod`` then restart) stops no executor, so quiescence is
NOT assumed — it is enforced. BOTH dialects take a writer-conflicting
fence as the migration's FIRST statement, before even the first
validation read, and hold it through normalization, DDL, backfill and
the revision stamp: SQLite takes the database write reservation
(``BEGIN IMMEDIATE``) and PostgreSQL takes ``LOCK TABLE executions IN
ACCESS EXCLUSIVE MODE``. A surviving old writer is therefore blocked for
the whole window, and on SQLite one already writing makes the migration
refuse before touching any schema. Without that fence every validation
above is a TOCTOU read — see :func:`_acquire_migration_write_fence` for
the corruptions that follow on each dialect (SQLite alias-split and the
PostgreSQL crossed-wallet false verdict the native ``uuid`` type does
NOT catch). The data-dependent pre-checks require an online database
connection; ``--sql`` offline rendering is refused before any DDL is
emitted (and before the fence) rather than silently skipping the
fail-closed checks.
Revises 0028.
"""

from collections.abc import Sequence
from uuid import UUID

import sqlalchemy as sa
from alembic import op

revision: str = "0029"
down_revision: str | None = "0028"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KNOWN_TO_ACTIVE_PG = "9999-12-31T23:59:59+00:00"
_KNOWN_TO_ACTIVE_SQLITE = "9999-12-31 23:59:59.000000"
_CK_EXECUTIONS_EXCHANGE_LOWER = "exchange = LOWER(exchange) AND LENGTH(TRIM(exchange)) > 0"
_CK_EXECUTIONS_MODE = "mode IN ('live', 'paper')"
_CK_EXECUTIONS_SCOPE_SEQUENCE = "scope_sequence >= 1"
_CK_ANCHOR_WATERMARK_NEW = "source_watermark_kind = 'scope_sequence' AND source_watermark >= 0"
_CK_ANCHOR_WATERMARK_OLD = "source_watermark_kind = 'execution_id' AND source_watermark >= 0"
_LINEAGE_SAMPLE_LIMIT = 5


def _is_sqlite() -> bool:
    """Return whether the migration is executing against SQLite."""
    return op.get_bind().dialect.name == "sqlite"


def _active_literal() -> str:
    """Return the dialect's SCD2 active ``known_to`` sentinel literal."""
    return _KNOWN_TO_ACTIVE_SQLITE if _is_sqlite() else _KNOWN_TO_ACTIVE_PG


def _counter_col() -> sa.types.TypeEngine[int]:
    """Build PostgreSQL BIGINT with SQLite integer-compatible storage."""
    return sa.BigInteger().with_variant(sa.Integer(), "sqlite")


def _wallet_scope_key(value: str) -> str:
    """Canonicalize one wallet identity for scope comparison when possible.

    Mirrors the repository's execution scope wallet key: alias UUID
    spellings must compare (and partition) equal to their canonical form;
    a non-UUID identity keeps its raw spelling.
    """
    try:
        return str(UUID(value))
    except ValueError:
        return value


def _require_online_bind() -> sa.engine.Connection:
    """Return a live connection, refusing offline ``--sql`` rendering.

    The lineage/target-domain pre-checks and the anchor-emptiness assert
    must READ data to fail closed; offline rendering cannot, and skipping
    them silently would let a corrupt ledger receive fabricated scopes.
    Every validation helper calls this FIRST, and every validation runs
    before the first DDL statement, so an offline invocation refuses with
    ZERO rendered output.
    """
    if op.get_context().as_sql:
        raise RuntimeError(
            "migration 0029 requires an online connection: its fail-closed "
            "lineage pre-checks and anchor-emptiness assert read data"
        )
    return op.get_bind()


def _acquire_migration_write_fence() -> None:
    """Take a writer-conflicting table lock BEFORE the first validation read.

    Must be the migration's FIRST statement. ``executions`` is a
    live-written table and the documented quiescence is PROSE, not a
    mechanism — ``make migrate-prod`` stops no executor — so without a
    fence every validation here is a TOCTOU read and an OLD-schema
    (revision 0028) writer can commit a fill after the checks passed.
    Both dialects need the fence; the harms differ by dialect but the
    read-then-write window is the same, which is why the fence must
    precede the FIRST validation rather than merely the normalization.

    SQLite harms — two distinct corruptions:

    * A fill arriving after :func:`_normalize_wallet_aliases` keeps its
      VERBATIM wallet spelling (canonicalization shipped WITH the new
      writer; the deployed 0028 writer stores the caller's spelling as
      given, and SQLite's ``UUIDColumn`` persists it as given). The
      backfill partitions by RAW stored text, so an alias spelling forms
      its own scope and receives ``scope_sequence = 1`` beside the
      canonical row; the TOTAL unique index accepts the two as distinct
      textual scopes, and the canonical watermark capture never sees the
      alias row. The migration SUCCEEDS and a published fill is silently
      missing from certification input — durable corruption, not a crash.
    * A fill arriving after :func:`_abort_on_broken_lineage` may carry
      lineage that check would have refused. The backfill leaves its
      scope NULL, the NOT NULL tightening then fails AFTER the DDL has
      run, and the module's remediate-and-retry contract is wedged
      against half-applied columns.

    SQLite mechanism: ``BEGIN IMMEDIATE`` takes the database-wide write
    reservation up front — the same primitive, for the same
    read-then-write reason, that
    :meth:`Repository._begin_execution_insert_transaction` uses to
    serialize counter allocation across CONNECTIONS. Alembic wraps each
    SQLite migration in one connection transaction (the dialect reports
    no transactional DDL, so the per-migration transaction is the real
    one), so the reservation spans validation, normalization, DDL,
    backfill AND the revision stamp. That makes the window
    unobservable: a writer arriving inside it BLOCKS and then fails
    closed, and a writer ALREADY holding the reservation makes this very
    statement refuse before any schema is touched — refusing beats
    making safe. It must precede the first read because SQLite cannot
    upgrade an already-open deferred transaction to a write reservation.
    Release is automatic on commit, rollback and crash, so neither a
    refusal nor a fault can wedge the database against the restarted
    executor.

    PostgreSQL harm — the CROSSED-WALLET corruption its type system does
    NOT catch. The native ``uuid`` type does defeat the alias-split
    variant (an alias spelling is UNREPRESENTABLE, not merely normalized
    away), and transactional DDL does turn an UNRESOLVABLE late row into
    a clean atomic abort. But a RESOLVABLE crossed-wallet row survives
    both defences: a 0028 writer can commit an execution STORED under
    wallet B whose ``order_public_id`` references wallet A's VALID active
    order. :func:`_abort_on_broken_lineage` only takes ``ACCESS SHARE``
    (its ``SELECT``), which does NOT conflict with the writer's
    ``ROW EXCLUSIVE`` ``INSERT``; and the first DDL
    (:func:`op.add_column`) takes ``ACCESS EXCLUSIVE`` only LATER — so
    absent a fence the crossed row commits between the validation read
    and the DDL. Then :func:`_backfill_scope_and_counter` derives a valid
    ``(exchange, mode)`` by JOINING order A yet PRESERVES the stored
    wallet B and numbers ``scope_sequence`` inside wallet B's partition;
    every CHECK and the total-unique index pass, and the crossed fill is
    certified into wallet B's authoritative watermark — a FALSE
    authoritative verdict, the one failure this program exists to
    prevent. So PostgreSQL DOES need a fence, for the crossed-wallet
    reason (not the alias reason).

    PostgreSQL mechanism: ``LOCK TABLE executions,
    portfolio_spot_reconciliation_anchors IN ACCESS EXCLUSIVE MODE`` as
    the FIRST statement — the SAME locks the later DDL takes, only
    acquired up front. ``ACCESS EXCLUSIVE`` conflicts with the ``ROW
    EXCLUSIVE`` an ``INSERT`` takes, so no concurrent writer can commit
    between the validation read and the DDL; a writer already
    mid-INSERT holds ``ROW EXCLUSIVE`` and makes this ``LOCK`` wait for
    it (or time out under a bounded ``lock_timeout``) rather than racing
    it. The anchor table is locked for the SAME reason as executions:
    both directions read its emptiness (:func:`_assert_anchor_table_empty`)
    and then rebuild its ``source_watermark_kind`` CHECK, so without the
    lock a concurrent anchor writer could commit between that read and
    the CHECK swap and be rebuilt under a unit it cannot satisfy —
    fencing it here makes the emptiness assert authoritative in both the
    upgrade and the downgrade. PostgreSQL wraps the migration in a real
    transaction, so the locks span validation, backfill, DDL and the
    revision stamp and release automatically on commit, rollback or
    crash — no leaked lock can wedge the restarted executor. SQLite needs
    no separate anchor lock: ``BEGIN IMMEDIATE`` reserves the whole
    database, covering every table at once.
    """
    bind = _require_online_bind()
    if _is_sqlite():
        bind.execute(sa.text("BEGIN IMMEDIATE"))
        return
    bind.execute(
        sa.text(
            "LOCK TABLE executions, portfolio_spot_reconciliation_anchors IN ACCESS EXCLUSIVE MODE"
        )
    )


def _abort_with_samples(count: int, description: str, samples: list[str]) -> None:
    """Raise the fail-closed abort naming the violation with evidence."""
    raise RuntimeError(
        f"migration 0029 aborted: {count} execution row(s) with "
        f"{description}; sample public_ids: {samples}; remediate "
        "the lineage (or archive the rows out) and re-run"
    )


def _abort_on_broken_lineage() -> None:
    """ABORT on any execution whose active lineage is missing or crossed.

    ``update_order`` copies ``mode``/``instrument_public_id``/
    ``wallet_public_id`` verbatim to every SCD2 successor, so every
    legitimately written execution resolves through an active order
    version; a row that does not is corruption, and assigning it a scope
    would fabricate certification input. Fail-closed beats guessing —
    the operator remediates (or archives the row out) and re-runs. The
    crossed-wallet comparison canonicalizes UUID spellings on BOTH sides
    in Python because SQLite stores wallet identities verbatim: a raw
    text inequality would refuse alias spellings of one logical wallet.
    Runs BEFORE any DDL so a refusal leaves the schema at revision 0028.
    """
    bind = _require_online_bind()
    active = _active_literal()
    dangling_order_sql = (
        "SELECT e.public_id FROM executions e WHERE NOT EXISTS ("
        "SELECT 1 FROM orders o WHERE o.public_id = e.order_public_id "
        f"AND o.known_to = '{active}')"
    )
    count = bind.execute(
        sa.text(f"SELECT COUNT(*) FROM ({dangling_order_sql}) violations")
    ).scalar()
    if count:
        samples = [
            str(row[0])
            for row in bind.execute(sa.text(f"{dangling_order_sql} LIMIT {_LINEAGE_SAMPLE_LIMIT}"))
        ]
        _abort_with_samples(int(count), "dangling order lineage (no active order version)", samples)
    crossed = [
        str(row[0])
        for row in bind.execute(
            sa.text(
                "SELECT e.public_id, e.wallet_public_id, o.wallet_public_id "
                "FROM executions e JOIN orders o "
                f"ON o.public_id = e.order_public_id AND o.known_to = '{active}'"
            )
        )
        if _wallet_scope_key(str(row[1])) != _wallet_scope_key(str(row[2]))
    ]
    if crossed:
        _abort_with_samples(
            len(crossed),
            "crossed wallet lineage (active order owned by another wallet)",
            crossed[:_LINEAGE_SAMPLE_LIMIT],
        )
    dangling_instrument_sql = (
        "SELECT e.public_id FROM executions e JOIN orders o "
        f"ON o.public_id = e.order_public_id AND o.known_to = '{active}' "
        "WHERE NOT EXISTS (SELECT 1 FROM instruments i "
        f"WHERE i.public_id = o.instrument_public_id AND i.known_to = '{active}')"
    )
    count = bind.execute(
        sa.text(f"SELECT COUNT(*) FROM ({dangling_instrument_sql}) violations")
    ).scalar()
    if count:
        samples = [
            str(row[0])
            for row in bind.execute(
                sa.text(f"{dangling_instrument_sql} LIMIT {_LINEAGE_SAMPLE_LIMIT}")
            )
        ]
        _abort_with_samples(
            int(count), "dangling instrument lineage (no active instrument version)", samples
        )


def _abort_on_invalid_target_domain() -> None:
    """ABORT when a backfill-source instrument exchange breaks the scope CHECK.

    The legacy instruments CHECK (``ck_instrument_exchange_lower``)
    enforces lowercase only — a BLANK exchange satisfies it. The new
    executions CHECK additionally requires a non-empty value (an empty
    scope key would silently create a junk scope), so backfilling such a
    value would fail AFTER the DDL had already run. Detect and refuse
    BEFORE any DDL with the same abort style as the lineage checks.
    ``orders.mode`` needs no twin check: it is already CHECK-bound to
    the exact ``live``/``paper`` domain the executions CHECK mirrors.
    """
    bind = _require_online_bind()
    active = _active_literal()
    violation_sql = (
        "SELECT e.public_id FROM executions e JOIN orders o "
        f"ON o.public_id = e.order_public_id AND o.known_to = '{active}' "
        "JOIN instruments i ON i.public_id = o.instrument_public_id "
        f"AND i.known_to = '{active}' "
        "WHERE NOT (i.exchange = LOWER(i.exchange) AND LENGTH(TRIM(i.exchange)) > 0)"
    )
    count = bind.execute(sa.text(f"SELECT COUNT(*) FROM ({violation_sql}) violations")).scalar()
    if count:
        samples = [
            str(row[0])
            for row in bind.execute(sa.text(f"{violation_sql} LIMIT {_LINEAGE_SAMPLE_LIMIT}"))
        ]
        _abort_with_samples(
            int(count),
            "an active instrument exchange outside the executions scope "
            "domain (blank or non-lowercase)",
            samples,
        )


def _assert_anchor_table_empty(action: str) -> None:
    """ABORT the given action if any spot reconciliation anchor exists.

    Upgrade: no anchor writer has shipped (S4c-3), so the table must be
    empty in every deployment; a populated table would mean stored
    watermarks in the retired ``execution_id`` unit, which this migration
    deliberately does not data-migrate. Downgrade: a legitimate
    ``scope_sequence`` anchor created after the upgrade cannot be
    represented in the restored ``execution_id`` unit, so the downgrade
    refuses BEFORE any destructive DDL — SQLite offers no transactional
    DDL, and a late refusal would leave the executions table rebuilt
    while the revision still reads 0029.
    """
    bind = _require_online_bind()
    count = bind.execute(
        sa.text("SELECT COUNT(*) FROM portfolio_spot_reconciliation_anchors")
    ).scalar()
    if not count:
        return
    if action == "upgrade":
        raise RuntimeError(
            f"migration 0029 aborted: {count} spot reconciliation anchor row(s) "
            "exist but the source_watermark_kind unit is being retired; no "
            "anchor writer has shipped, so this is unexpected — investigate "
            "before re-running"
        )
    raise RuntimeError(
        f"migration 0029 downgrade aborted: {count} spot reconciliation anchor "
        "row(s) carry scope_sequence watermarks that the execution_id unit "
        "this downgrade restores cannot represent; remove or migrate the "
        "anchors, then re-run"
    )


def _normalize_wallet_aliases() -> None:
    """Rewrite alias UUID wallet spellings on executions to canonical form.

    The backfill partitions counters by the STORED wallet text and the
    post-migration writer persists the canonical spelling, so an alias
    spelling left behind would form a separate scope invisible to the
    canonical watermark capture. SQLite stores ``UUIDColumn`` values
    verbatim; PostgreSQL's native ``uuid`` type is already canonical, so
    this scan finds nothing there. No collision check is needed: the
    executions table carries no uniqueness over the wallet identity at
    revision 0028, and merging alias spellings of one logical wallet
    into one scope is exactly the intended semantics. Dialect-neutral
    DML — safely re-runnable, and it runs only after every validation
    has passed.
    """
    bind = _require_online_bind()
    stored = [
        str(row[0])
        for row in bind.execute(sa.text("SELECT DISTINCT wallet_public_id FROM executions"))
    ]
    for spelling in stored:
        canonical = _wallet_scope_key(spelling)
        if canonical != spelling:
            bind.execute(
                sa.text(
                    "UPDATE executions SET wallet_public_id = :canonical "
                    "WHERE wallet_public_id = :alias"
                ),
                {"canonical": canonical, "alias": spelling},
            )


def _backfill_scope_and_counter() -> None:
    """Backfill scope from ACTIVE lineage and number rows per scope by id."""
    active = _active_literal()
    if _is_sqlite():
        op.execute(
            "UPDATE executions SET "
            "exchange = (SELECT i.exchange FROM orders o JOIN instruments i "
            "ON i.public_id = o.instrument_public_id AND i.known_to = "
            f"'{active}' WHERE o.public_id = executions.order_public_id "
            f"AND o.known_to = '{active}'), "
            "mode = (SELECT o.mode FROM orders o "
            "WHERE o.public_id = executions.order_public_id "
            f"AND o.known_to = '{active}')"
        )
        op.execute(
            "UPDATE executions SET scope_sequence = ("
            "SELECT t.rn FROM (SELECT id, ROW_NUMBER() OVER ("
            "PARTITION BY wallet_public_id, exchange, mode ORDER BY id) AS rn "
            "FROM executions) AS t WHERE t.id = executions.id)"
        )
        return
    op.execute(
        "UPDATE executions AS e SET exchange = i.exchange, mode = o.mode "
        "FROM orders o JOIN instruments i "
        "ON i.public_id = o.instrument_public_id AND i.known_to = "
        f"'{active}' WHERE o.public_id = e.order_public_id "
        f"AND o.known_to = '{active}'"
    )
    op.execute(
        "UPDATE executions AS e SET scope_sequence = t.rn "
        "FROM (SELECT id, ROW_NUMBER() OVER ("
        "PARTITION BY wallet_public_id, exchange, mode ORDER BY id) AS rn "
        "FROM executions) AS t WHERE t.id = e.id"
    )


def upgrade() -> None:
    """Fence out concurrent writers, validate fail-closed, then backfill.

    The write fence is taken FIRST, before even the first validation
    READ: ``executions`` is live-written and quiescence is unenforced,
    so an unfenced validation is a TOCTOU read whose window a surviving
    old writer can corrupt (see
    :func:`_acquire_migration_write_fence`). Every validation (offline
    refusal, lineage, target scope domain, anchor emptiness) then runs
    BEFORE the first DDL statement so a refusal leaves revision 0028
    completely untouched; the alias-wallet normalization is re-runnable
    DML and runs only after every validation has passed — and, under the
    fence, no row can arrive to invalidate a passed check.
    """
    _acquire_migration_write_fence()
    _abort_on_broken_lineage()
    _abort_on_invalid_target_domain()
    _assert_anchor_table_empty("upgrade")
    _normalize_wallet_aliases()
    op.add_column("executions", sa.Column("exchange", sa.String(32), nullable=True))
    op.add_column("executions", sa.Column("mode", sa.String(8), nullable=True))
    op.add_column("executions", sa.Column("scope_sequence", _counter_col(), nullable=True))
    _backfill_scope_and_counter()
    if _is_sqlite():
        with op.batch_alter_table("executions", recreate="always") as batch:
            batch.alter_column("exchange", existing_type=sa.String(32), nullable=False)
            batch.alter_column("mode", existing_type=sa.String(8), nullable=False)
            batch.alter_column("scope_sequence", existing_type=_counter_col(), nullable=False)
            batch.create_check_constraint(
                "ck_executions_exchange_lower", _CK_EXECUTIONS_EXCHANGE_LOWER
            )
            batch.create_check_constraint("ck_executions_mode", _CK_EXECUTIONS_MODE)
            batch.create_check_constraint(
                "ck_executions_scope_sequence", _CK_EXECUTIONS_SCOPE_SEQUENCE
            )
    else:
        op.alter_column("executions", "exchange", existing_type=sa.String(32), nullable=False)
        op.alter_column("executions", "mode", existing_type=sa.String(8), nullable=False)
        op.alter_column(
            "executions", "scope_sequence", existing_type=_counter_col(), nullable=False
        )
        op.create_check_constraint(
            "ck_executions_exchange_lower", "executions", _CK_EXECUTIONS_EXCHANGE_LOWER
        )
        op.create_check_constraint("ck_executions_mode", "executions", _CK_EXECUTIONS_MODE)
        op.create_check_constraint(
            "ck_executions_scope_sequence", "executions", _CK_EXECUTIONS_SCOPE_SEQUENCE
        )
    op.create_index(
        "uq_executions_scope_sequence",
        "executions",
        ["wallet_public_id", "exchange", "mode", "scope_sequence"],
        unique=True,
    )
    if _is_sqlite():
        with op.batch_alter_table(
            "portfolio_spot_reconciliation_anchors", recreate="always"
        ) as batch:
            batch.drop_constraint("ck_portfolio_spot_anchor_watermark", type_="check")
            batch.create_check_constraint(
                "ck_portfolio_spot_anchor_watermark", _CK_ANCHOR_WATERMARK_NEW
            )
    else:
        op.drop_constraint(
            "ck_portfolio_spot_anchor_watermark",
            "portfolio_spot_reconciliation_anchors",
            type_="check",
        )
        op.create_check_constraint(
            "ck_portfolio_spot_anchor_watermark",
            "portfolio_spot_reconciliation_anchors",
            _CK_ANCHOR_WATERMARK_NEW,
        )


def downgrade() -> None:
    """Remove the scope plane and restore the execution-id anchor kind.

    Refuses BEFORE any destructive DDL while a ``scope_sequence`` anchor
    exists: SQLite keeps no transactional DDL, so a late refusal would
    leave the executions table already rebuilt (columns and index
    dropped) with the revision still stamped 0029. The write fence
    precedes that read for the same reason it precedes the upgrade's
    validations — an anchor committed by a live writer after the check
    passed would be rebuilt under the restored ``execution_id`` CHECK it
    cannot satisfy, failing mid-DDL.
    """
    _acquire_migration_write_fence()
    _assert_anchor_table_empty("downgrade")
    op.drop_index("uq_executions_scope_sequence", table_name="executions")
    column_names = ("scope_sequence", "mode", "exchange")
    if _is_sqlite():
        with op.batch_alter_table("executions", recreate="always") as batch:
            batch.drop_constraint("ck_executions_scope_sequence", type_="check")
            batch.drop_constraint("ck_executions_mode", type_="check")
            batch.drop_constraint("ck_executions_exchange_lower", type_="check")
            for column_name in column_names:
                batch.drop_column(column_name)
        with op.batch_alter_table(
            "portfolio_spot_reconciliation_anchors", recreate="always"
        ) as batch:
            batch.drop_constraint("ck_portfolio_spot_anchor_watermark", type_="check")
            batch.create_check_constraint(
                "ck_portfolio_spot_anchor_watermark", _CK_ANCHOR_WATERMARK_OLD
            )
        return
    op.drop_constraint("ck_executions_scope_sequence", "executions", type_="check")
    op.drop_constraint("ck_executions_mode", "executions", type_="check")
    op.drop_constraint("ck_executions_exchange_lower", "executions", type_="check")
    for column_name in column_names:
        op.drop_column("executions", column_name)
    op.drop_constraint(
        "ck_portfolio_spot_anchor_watermark",
        "portfolio_spot_reconciliation_anchors",
        type_="check",
    )
    op.create_check_constraint(
        "ck_portfolio_spot_anchor_watermark",
        "portfolio_spot_reconciliation_anchors",
        _CK_ANCHOR_WATERMARK_OLD,
    )
