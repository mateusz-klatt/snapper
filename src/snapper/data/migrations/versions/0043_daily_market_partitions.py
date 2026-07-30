"""Adopt daily PostgreSQL partitions for the three market-data tables.

PostgreSQL upgrades accept exactly two safe starting worlds. An already
partitioned table must match the complete anchor-specific topology and is
verified without modification. An ordinary table must be provably empty
under a transaction-held lock before it is renamed and adopted. Populated,
missing, partition-child, malformed, and partially converted relations are
refused.

The creation path deliberately spells out its own PostgreSQL DDL. It does
not import the runtime lifecycle implementation, so the independent manual
and migration branches can expose drift in the convergence proof.

SQLite retains ordinary tables. Its compatibility branch tightens
``trades.executed_at``, removes the legacy two-column uniqueness, and
installs the three-column trade arbiter needed by the shared writer contract.
"""

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast

import sqlalchemy as sa
from alembic import context
from alembic import op
from sqlalchemy.engine import Connection

revision: str = "0043"
down_revision: str | None = "0042"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ACTIVE_PREDICATE = "known_to = TIMESTAMPTZ '9999-12-31 23:59:59+00:00'"
_ACTIVE_PREDICATE_DEFINITION = "(known_to = '9999-12-31 23:59:59+00'::timestamp with time zone)"
_ANCHOR_PATTERN = re.compile(
    r"\d{4}-\d{2}-\d{2}T00:00:00\+00:00",
    flags=re.ASCII,
)
_SCHEMA = "public"
_FUTURE_LEAVES = 14


@dataclass(frozen=True)
class ColumnSpec:
    """Describe one exact PostgreSQL column catalog row."""

    name: str
    data_type: str
    nullable: bool
    default: str


@dataclass(frozen=True)
class MatchingIndex:
    """Describe an index inherited by every partition."""

    parent_name: str
    legacy_name: str
    columns: tuple[str, ...]
    unique: bool = False
    partial: bool = False
    introduced: bool = False


@dataclass(frozen=True)
class CatalogIndex:
    """Describe one expected named index in a catalog assertion."""

    name: str
    columns: tuple[str, ...]
    unique: bool
    primary: bool
    partial: bool
    parent: str


@dataclass(frozen=True)
class TableSpec:
    """Describe one independently adopted market-data topology."""

    name: str
    key: str
    columns: tuple[ColumnSpec, ...]
    checks: tuple[str, ...]
    matching_indexes: tuple[MatchingIndex, ...]
    active_public_id: bool = False
    legacy_u2: bool = False


_TEMPORAL_TAIL = (
    ColumnSpec("session_id", "uuid", False, ""),
    ColumnSpec("sequence_id", "integer", False, ""),
    ColumnSpec("timestamp", "timestamp with time zone", False, ""),
    ColumnSpec("known_to", "timestamp with time zone", False, ""),
)

_TABLES = (
    TableSpec(
        name="ticks",
        key="timestamp",
        columns=(
            ColumnSpec("id", "bigint", False, "nextval('ticks_id_seq'::regclass)"),
            ColumnSpec("public_id", "uuid", False, ""),
            ColumnSpec("instrument_public_id", "uuid", False, ""),
            ColumnSpec("bid", "double precision", True, ""),
            ColumnSpec("ask", "double precision", True, ""),
            ColumnSpec("last", "double precision", True, ""),
            ColumnSpec("volume", "double precision", False, ""),
            *_TEMPORAL_TAIL,
        ),
        checks=("ck_ticks_sequence_id",),
        matching_indexes=(
            MatchingIndex(
                "ticks_p_ix_instr_ts",
                "ix_tick_instrument_ts",
                ("instrument_public_id", "timestamp"),
            ),
        ),
    ),
    TableSpec(
        name="candles",
        key="open_at",
        columns=(
            ColumnSpec("id", "bigint", False, "nextval('candles_id_seq'::regclass)"),
            ColumnSpec("public_id", "uuid", False, ""),
            ColumnSpec("instrument_public_id", "uuid", False, ""),
            ColumnSpec("timeframe", "character varying(8)", False, ""),
            ColumnSpec("open_at", "timestamp with time zone", False, ""),
            ColumnSpec("open", "double precision", False, ""),
            ColumnSpec("high", "double precision", False, ""),
            ColumnSpec("low", "double precision", False, ""),
            ColumnSpec("close", "double precision", False, ""),
            ColumnSpec("volume", "double precision", False, ""),
            ColumnSpec("vwap", "double precision", True, ""),
            ColumnSpec("trades", "integer", True, ""),
            *_TEMPORAL_TAIL,
            ColumnSpec(
                "source",
                "character varying(16)",
                False,
                "'native'::character varying",
            ),
            ColumnSpec("complete", "boolean", False, "true"),
            ColumnSpec("price_basis", "character varying(16)", True, ""),
        ),
        checks=("ck_candles_sequence_id", "ck_candle_source"),
        matching_indexes=(
            MatchingIndex(
                "candles_p_uq_itf_open",
                "uq_candle_itf_open",
                ("instrument_public_id", "timeframe", "open_at"),
                unique=True,
                partial=True,
            ),
            MatchingIndex(
                "candles_p_ix_instr_open",
                "ix_candle_instrument_open",
                ("instrument_public_id", "open_at"),
            ),
        ),
        active_public_id=True,
    ),
    TableSpec(
        name="trades",
        key="executed_at",
        columns=(
            ColumnSpec("id", "bigint", False, "nextval('trades_id_seq'::regclass)"),
            ColumnSpec("public_id", "uuid", False, ""),
            ColumnSpec("instrument_public_id", "uuid", False, ""),
            ColumnSpec("trade_id", "character varying(64)", True, ""),
            ColumnSpec("price", "double precision", False, ""),
            ColumnSpec("size", "double precision", False, ""),
            ColumnSpec("side", "character varying(4)", False, ""),
            ColumnSpec("executed_at", "timestamp with time zone", False, ""),
            *_TEMPORAL_TAIL,
        ),
        checks=("ck_trades_sequence_id",),
        matching_indexes=(
            MatchingIndex(
                "trades_p_uq_instr_tid_exec",
                "uq_trade_instr_tid_exec",
                ("instrument_public_id", "trade_id", "executed_at"),
                unique=True,
                introduced=True,
            ),
            MatchingIndex(
                "trades_p_ix_instr_ts",
                "ix_trade_instrument_ts",
                ("instrument_public_id", "timestamp"),
            ),
            MatchingIndex(
                "trades_p_ix_ts",
                "ix_trades_timestamp",
                ("timestamp",),
            ),
            MatchingIndex(
                "trades_p_ix_exec",
                "ix_trades_executed_at",
                ("executed_at",),
            ),
        ),
        active_public_id=True,
        legacy_u2=True,
    ),
)


def _identifier(value: str) -> str:
    """Quote one fixed or internally generated PostgreSQL identifier."""
    return '"' + value.replace('"', '""') + '"'


def _qualified(value: str) -> str:
    """Return one relation qualified into the static public schema."""
    return f"{_identifier(_SCHEMA)}.{_identifier(value)}"


def _explicit_anchor() -> datetime | None:
    """Parse an explicitly supplied deterministic UTC partition boundary.

    Returns:
        An aware UTC midnight or ``None`` when the x-argument is absent.

    Raises:
        RuntimeError: If ``partition_anchor`` is repeated or is not in the
            canonical ``YYYY-MM-DDT00:00:00+00:00`` form.
    """
    values = [
        item.removeprefix("partition_anchor=")
        for item in context.get_x_argument()
        if item.startswith("partition_anchor=")
    ]
    malformed = [
        item
        for item in context.get_x_argument()
        if item == "partition_anchor" or item.startswith("partition_anchor:")
    ]
    if malformed or len(values) > 1:
        raise RuntimeError(
            "partition_anchor must be supplied at most once as "
            "-x partition_anchor=YYYY-MM-DDT00:00:00+00:00"
        )
    if not values:
        return None
    raw = values[0]
    if _ANCHOR_PATTERN.fullmatch(raw) is None:
        raise RuntimeError("partition_anchor must exactly match YYYY-MM-DDT00:00:00+00:00")
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise RuntimeError("partition_anchor is not a valid calendar date") from exc
    if parsed.tzinfo != UTC or parsed.isoformat(timespec="seconds") != raw:
        raise RuntimeError("partition_anchor must be UTC midnight with an explicit +00:00 offset")
    return parsed


def _infer_parent_anchor(bind: Connection, spec: TableSpec) -> datetime:
    """Read the legacy partition's exclusive upper bound from PostgreSQL."""
    legacy = f"{spec.name}_legacy"
    value = cast(
        datetime | None,
        bind.scalar(
            sa.text("""
                SELECT (
                    regexp_match(
                        pg_get_expr(child.relpartbound, child.oid, true),
                        $pattern$'([^']+)'$pattern$
                    )
                )[1]::timestamptz
                FROM pg_inherits AS inh
                JOIN pg_class AS parent ON parent.oid = inh.inhparent
                JOIN pg_namespace AS pn ON pn.oid = parent.relnamespace
                JOIN pg_class AS child ON child.oid = inh.inhrelid
                JOIN pg_namespace AS cn ON cn.oid = child.relnamespace
                WHERE pn.nspname = :schema
                  AND parent.relname = :parent
                  AND cn.nspname = :schema
                  AND child.relname = :legacy
                """).bindparams(
                schema=_SCHEMA,
                parent=spec.name,
                legacy=legacy,
            )
        ),
    )
    if value is None:
        raise RuntimeError(f"{spec.name} is partitioned but its legacy upper bound is unavailable")
    normalized = value.astimezone(UTC)
    if normalized != normalized.replace(hour=0, minute=0, second=0, microsecond=0):
        raise RuntimeError(f"{spec.name} legacy upper bound is not UTC midnight")
    return normalized


def _resolve_anchor(bind: Connection) -> datetime:
    """Choose an explicit, inferred, or fresh-install UTC anchor."""
    explicit = _explicit_anchor()
    if explicit is not None:
        return explicit
    inferred = [
        _infer_parent_anchor(bind, spec)
        for spec in _TABLES
        if _relation_kind(bind, spec.name) == "p:false"
    ]
    if not inferred:
        return datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    if any(value != inferred[0] for value in inferred[1:]):
        raise RuntimeError("existing market-data parents do not share one partition anchor")
    return inferred[0]


def _anchor_literal(value: datetime) -> str:
    """Render one validated UTC boundary as an explicit TIMESTAMPTZ literal."""
    return f"TIMESTAMPTZ '{value.isoformat(timespec='seconds')}'"


def _leaf_name(table: str, value: datetime) -> str:
    """Return the canonical daily child relation name."""
    return f"{table}_d{value.strftime('%Y%m%d')}"


def _daily_names(spec: TableSpec, anchor: datetime) -> tuple[str, ...]:
    """Return all fourteen canonical daily child relation names."""
    return tuple(
        _leaf_name(spec.name, anchor + timedelta(days=offset)) for offset in range(_FUTURE_LEAVES)
    )


def _relation_kind(bind: Connection, relation: str) -> str | None:
    """Return a compact relation-kind and partition-membership state."""
    return cast(
        str | None,
        bind.scalar(sa.text("""
                SELECT c.relkind::text || ':' || c.relispartition::text
                FROM pg_class AS c
                JOIN pg_namespace AS n ON n.oid = c.relnamespace
                WHERE n.nspname = :schema AND c.relname = :relation
                """).bindparams(schema=_SCHEMA, relation=relation)),
    )


def _require(bind: Connection, statement: sa.TextClause, message: str) -> None:
    """Raise a fail-closed migration error unless one catalog assertion holds."""
    if not cast(bool, bind.scalar(statement)):
        raise RuntimeError(message)


def _sql_text_array(values: Sequence[str]) -> str:
    """Render a safe text array from fixed or internally generated names."""
    members = ", ".join("'" + value.replace("'", "''") + "'" for value in values)
    return f"ARRAY[{members}]::text[]"


def _column_values(spec: TableSpec, ordinary: bool) -> str:
    """Render exact expected column rows for one catalog comparison."""
    rows: list[str] = []
    for position, column in enumerate(spec.columns, start=1):
        not_null = not column.nullable
        if ordinary and spec.name == "trades" and column.name == spec.key:
            not_null = False
        rows.append(
            "("
            f"{position}, "
            f"'{column.name}', "
            f"'{column.data_type}', "
            f"{str(not_null).upper()}, "
            f"'{column.default.replace("'", "''")}', "
            "'', ''"
            ")"
        )
    return ", ".join(rows)


def _verify_columns(
    bind: Connection,
    spec: TableSpec,
    relations: Sequence[str],
    ordinary: bool = False,
) -> None:
    """Verify exact type, order, nullability, default, identity, and generation."""
    expected = _column_values(spec, ordinary)
    for relation in relations:
        statement = sa.text(f"""
            WITH expected(
                position,
                name,
                data_type,
                not_null,
                default_expression,
                identity_state,
                generated_state
            ) AS (
                VALUES {expected}
            ),
            actual AS (
                SELECT
                    a.attnum::integer,
                    a.attname::text,
                    format_type(a.atttypid, a.atttypmod),
                    a.attnotnull,
                    COALESCE(pg_get_expr(d.adbin, d.adrelid), ''),
                    a.attidentity::text,
                    a.attgenerated::text
                FROM pg_attribute AS a
                LEFT JOIN pg_attrdef AS d
                  ON d.adrelid = a.attrelid
                 AND d.adnum = a.attnum
                WHERE a.attrelid = to_regclass(:qualified)
                  AND a.attnum > 0
                  AND NOT a.attisdropped
            )
            SELECT NOT EXISTS (
                (SELECT * FROM actual EXCEPT SELECT * FROM expected)
                UNION ALL
                (SELECT * FROM expected EXCEPT SELECT * FROM actual)
            )
            """).bindparams(qualified=f"{_SCHEMA}.{relation}")
        _require(
            bind,
            statement,
            f"{relation} columns do not match the exact 0043 catalog",
        )


def _check_definition(name: str) -> str:
    """Return one hardcoded ordinary CHECK definition from revision 0042."""
    if name == "ck_candle_source":
        return (
            "CHECK (source::text = ANY (ARRAY['native'::character varying, "
            "'calculated'::character varying, 'synthesized'::character "
            "varying]::text[]))"
        )
    return "CHECK (sequence_id > 0)"


def _not_null_constraint_rows(
    spec: TableSpec,
    ordinary: bool,
) -> tuple[tuple[str, str, str], ...]:
    """Return exact PostgreSQL 18 NOT NULL constraint catalog rows."""
    rows: list[tuple[str, str, str]] = []
    for column in spec.columns:
        if column.nullable:
            continue
        if ordinary and spec.name == "trades" and column.name == spec.key:
            continue
        identifier = f'"{column.name}"' if column.name == "timestamp" else column.name
        rows.append(
            (
                f"{spec.name}_{column.name}_not_null",
                "n",
                f"NOT NULL {identifier}",
            )
        )
    return tuple(rows)


def _constraint_rows(
    spec: TableSpec,
    relation: str,
    legacy: bool,
    anchor: datetime | None,
) -> tuple[tuple[str, str, str], ...]:
    """Return every exact expected constraint catalog row."""
    rows = [(name, "c", _check_definition(name)) for name in spec.checks]
    rows.extend(_not_null_constraint_rows(spec, ordinary=False))
    if relation != spec.name:
        pkey = f"{spec.name}_pkey" if legacy else f"{relation}_pkey"
        rows.append((pkey, "p", "PRIMARY KEY (id)"))
    if legacy:
        if anchor is None:
            raise RuntimeError("legacy constraint verification requires an anchor")
        anchor_text = anchor.strftime("%Y-%m-%d %H:%M:%S+00")
        key = '"timestamp"' if spec.key == "timestamp" else spec.key
        rows.append(
            (
                f"ck_{spec.name}_legacy_range",
                "c",
                (
                    f"CHECK ({key} IS NOT NULL AND {key} < "
                    f"'{anchor_text}'::timestamp with time zone)"
                ),
            )
        )
        if spec.legacy_u2:
            rows.append(
                (
                    "uq_trade_instrument_trade_id",
                    "u",
                    "UNIQUE (instrument_public_id, trade_id)",
                )
            )
    return tuple(rows)


def _verify_constraint_manifest(
    bind: Connection,
    spec: TableSpec,
    relation: str,
    legacy: bool,
    anchor: datetime | None,
) -> None:
    """Verify every constraint definition and state without filtering actuals."""
    constraints = _constraint_rows(spec, relation, legacy, anchor)
    expected_rows = ", ".join(
        "("
        f"'{name}', "
        f"'{constraint_type}', "
        f"'{definition.replace("'", "''")}', "
        "TRUE, FALSE, FALSE"
        ")"
        for name, constraint_type, definition in constraints
    )
    statement = sa.text(f"""
        WITH expected(
            name,
            constraint_type,
            definition,
            validated,
            is_deferrable,
            is_initially_deferred
        ) AS (
            VALUES {expected_rows}
        ),
        actual AS (
            SELECT
                c.conname::text,
                c.contype::text,
                pg_get_constraintdef(c.oid, true),
                c.convalidated,
                c.condeferrable,
                c.condeferred
            FROM pg_constraint AS c
            WHERE c.conrelid = to_regclass(:qualified)
        )
        SELECT NOT EXISTS (
            (SELECT * FROM actual EXCEPT SELECT * FROM expected)
            UNION ALL
            (SELECT * FROM expected EXCEPT SELECT * FROM actual)
        )
        """).bindparams(qualified=f"{_SCHEMA}.{relation}")
    _require(
        bind,
        statement,
        f"{relation} constraints do not match the exact 0043 catalog",
    )


def _verify_check_definitions(
    bind: Connection,
    spec: TableSpec,
    relations: Sequence[str],
    anchor: datetime,
) -> None:
    """Verify every CHECK against its hardcoded expected PostgreSQL form."""
    relation_array = _sql_text_array(relations)
    for check in spec.checks:
        definition = _check_definition(check)
        statement = sa.text(f"""
            SELECT
                count(*) = :relation_count
                AND bool_and(pg_get_constraintdef(c.oid, true) = :definition)
            FROM pg_constraint AS c
            JOIN pg_class AS r ON r.oid = c.conrelid
            JOIN pg_namespace AS n ON n.oid = r.relnamespace
            WHERE n.nspname = :schema
              AND r.relname = ANY ({relation_array})
              AND c.conname = :constraint
              AND c.contype = 'c'
              AND c.convalidated
            """).bindparams(
            relation_count=len(relations),
            schema=_SCHEMA,
            constraint=check,
            definition=definition,
        )
        _require(bind, statement, f"{check} does not match the hardcoded 0043 definition")
    legacy = f"{spec.name}_legacy"
    expected_range = next(
        definition
        for name, constraint_type, definition in _constraint_rows(
            spec,
            legacy,
            legacy=True,
            anchor=anchor,
        )
        if name == f"ck_{spec.name}_legacy_range" and constraint_type == "c"
    )
    statement = sa.text("""
        SELECT count(*) = 1
        FROM pg_constraint AS c
        WHERE c.conrelid = to_regclass(:qualified)
          AND c.conname = :constraint
          AND c.contype = 'c'
          AND c.convalidated
          AND NOT c.condeferrable
          AND NOT c.condeferred
          AND pg_get_constraintdef(c.oid, true) = :definition
        """).bindparams(
        qualified=f"{_SCHEMA}.{legacy}",
        constraint=f"ck_{spec.name}_legacy_range",
        definition=expected_range,
    )
    _require(bind, statement, f"{legacy} does not carry the exact validated anchor bound")


def _index_shape_rows(
    spec: TableSpec,
    relation: str,
    parent: bool,
    legacy: bool,
) -> list[str]:
    """Return structural expected-index rows for one relation."""
    rows: list[str] = []
    if not parent:
        pkey = f"{spec.name}_pkey" if legacy else f"{relation}_pkey"
        rows.append(_index_shape_row(CatalogIndex(pkey, ("id",), True, True, False, "")))
        if spec.legacy_u2 and legacy:
            rows.append(
                _index_shape_row(
                    CatalogIndex(
                        "uq_trade_instrument_trade_id",
                        ("instrument_public_id", "trade_id"),
                        True,
                        False,
                        False,
                        "",
                    )
                )
            )
        if spec.active_public_id:
            public_name = f"ix_{spec.name}_public_id" if legacy else f"{relation}_public_id"
            rows.append(
                _index_shape_row(
                    CatalogIndex(
                        public_name,
                        ("public_id",),
                        True,
                        False,
                        True,
                        "",
                    )
                )
            )
    for index in spec.matching_indexes:
        if parent:
            index_name = index.parent_name
            parent_name = ""
        elif legacy:
            index_name = index.legacy_name
            parent_name = index.parent_name
        else:
            continue
        rows.append(
            _index_shape_row(
                CatalogIndex(
                    index_name,
                    index.columns,
                    index.unique,
                    False,
                    index.partial,
                    parent_name,
                )
            )
        )
    return rows


def _index_shape_row(index: CatalogIndex) -> str:
    """Render one expected structural index row."""
    return (
        "("
        f"'{index.name}', "
        f"{str(index.unique).upper()}, "
        f"{str(index.primary).upper()}, "
        f"{_sql_text_array(index.columns)}, "
        f"{str(index.partial).upper()}, "
        f"'{index.parent}', "
        "TRUE, TRUE, TRUE, 'btree', FALSE, TRUE, FALSE, TRUE, TRUE, TRUE"
        ")"
    )


def _verify_named_indexes(
    bind: Connection,
    relation: str,
    expected_rows: Sequence[str],
) -> None:
    """Verify the exact named-index manifest for a parent or legacy table."""
    rows = ", ".join(expected_rows)
    statement = sa.text(f"""
        WITH expected(
            name,
            is_unique,
            is_primary,
            key_columns,
            has_predicate,
            parent_index,
            is_valid,
            is_ready,
            is_live,
            access_method,
            has_expressions,
            keys_only,
            nulls_not_distinct,
            default_key_semantics,
            default_reloptions,
            default_tablespace
        ) AS (
            VALUES {rows}
        ),
        actual AS (
            SELECT
                ic.relname::text,
                i.indisunique,
                i.indisprimary,
                ARRAY(
                    SELECT a.attname::text
                    FROM unnest(i.indkey)
                        WITH ORDINALITY AS k(attribute_number, position)
                    JOIN pg_attribute AS a
                      ON a.attrelid = i.indrelid
                     AND a.attnum = k.attribute_number
                    WHERE k.position <= i.indnkeyatts
                    ORDER BY k.position
                ),
                i.indpred IS NOT NULL,
                COALESCE(pic.relname, '')::text,
                i.indisvalid,
                i.indisready,
                i.indislive,
                am.amname::text,
                i.indexprs IS NOT NULL,
                i.indnkeyatts = i.indnatts,
                i.indnullsnotdistinct,
                NOT EXISTS (
                    SELECT 1
                    FROM unnest(
                        i.indkey::smallint[],
                        i.indclass::oid[],
                        i.indcollation::oid[],
                        i.indoption::smallint[]
                    ) WITH ORDINALITY AS key_part(
                        attribute_number,
                        opclass_oid,
                        collation_oid,
                        options,
                        position
                    )
                    JOIN pg_attribute AS key_attribute
                      ON key_attribute.attrelid = i.indrelid
                     AND key_attribute.attnum = key_part.attribute_number
                    JOIN pg_opclass AS opclass
                      ON opclass.oid = key_part.opclass_oid
                    WHERE key_part.position <= i.indnkeyatts
                      AND (
                          key_part.options <> 0
                          OR NOT opclass.opcdefault
                          OR key_part.collation_oid
                              <> key_attribute.attcollation
                        )
                ),
                ic.reloptions IS NULL,
                ic.reltablespace = 0
            FROM pg_index AS i
            JOIN pg_class AS ic ON ic.oid = i.indexrelid
            JOIN pg_am AS am ON am.oid = ic.relam
            LEFT JOIN pg_inherits AS inh ON inh.inhrelid = i.indexrelid
            LEFT JOIN pg_class AS pic ON pic.oid = inh.inhparent
            WHERE i.indrelid = to_regclass(:qualified)
        )
        SELECT NOT EXISTS (
            (SELECT * FROM actual EXCEPT SELECT * FROM expected)
            UNION ALL
            (SELECT * FROM expected EXCEPT SELECT * FROM actual)
        )
        """).bindparams(qualified=f"{_SCHEMA}.{relation}")
    _require(bind, statement, f"{relation} index manifest is not exact")


def _verify_generated_partition_indexes(
    bind: Connection,
    spec: TableSpec,
    relations: Sequence[str],
) -> None:
    """Verify every generated child index has one exact parentage edge."""
    expected_parent_names = tuple(index.parent_name for index in spec.matching_indexes)
    expected_columns = {index.parent_name: index.columns for index in spec.matching_indexes}
    expected_unique = {index.parent_name: index.unique for index in spec.matching_indexes}
    expected_partial = {index.parent_name: index.partial for index in spec.matching_indexes}
    for relation in relations:
        for parent_name in expected_parent_names:
            columns = expected_columns[parent_name]
            child_name = _generated_index_name(
                relation,
                next(index for index in spec.matching_indexes if index.parent_name == parent_name),
            )
            statement = sa.text("""
                SELECT count(*) = 1
                FROM pg_index AS i
                JOIN pg_class AS child_index ON child_index.oid = i.indexrelid
                JOIN pg_inherits AS inh ON inh.inhrelid = child_index.oid
                JOIN pg_class AS parent_index ON parent_index.oid = inh.inhparent
                JOIN pg_am AS am ON am.oid = child_index.relam
                WHERE i.indrelid = to_regclass(:qualified)
                  AND parent_index.relname = :parent_name
                  AND child_index.relname = :child_name
                  AND i.indisunique = :is_unique
                  AND NOT i.indisprimary
                  AND (i.indpred IS NOT NULL) = :has_predicate
                  AND i.indisvalid
                  AND i.indisready
                  AND i.indislive
                  AND i.indexprs IS NULL
                  AND i.indnkeyatts = i.indnatts
                  AND NOT i.indnullsnotdistinct
                  AND am.amname = 'btree'
                  AND child_index.reloptions IS NULL
                  AND child_index.reltablespace = 0
                  AND ARRAY(
                      SELECT a.attname::text
                      FROM unnest(i.indkey)
                          WITH ORDINALITY AS k(attribute_number, position)
                      JOIN pg_attribute AS a
                        ON a.attrelid = i.indrelid
                       AND a.attnum = k.attribute_number
                      WHERE k.position <= i.indnkeyatts
                      ORDER BY k.position
                  ) = :columns
                  AND NOT EXISTS (
                      SELECT 1
                      FROM unnest(
                          i.indkey::smallint[],
                          i.indclass::oid[],
                          i.indcollation::oid[],
                          i.indoption::smallint[]
                      ) WITH ORDINALITY AS key_part(
                          attribute_number,
                          opclass_oid,
                          collation_oid,
                          options,
                          position
                      )
                      JOIN pg_attribute AS key_attribute
                        ON key_attribute.attrelid = i.indrelid
                       AND key_attribute.attnum = key_part.attribute_number
                      JOIN pg_opclass AS opclass
                        ON opclass.oid = key_part.opclass_oid
                      WHERE key_part.position <= i.indnkeyatts
                        AND (
                            key_part.options <> 0
                            OR NOT opclass.opcdefault
                            OR key_part.collation_oid
                                <> key_attribute.attcollation
                        )
                  )
                """).bindparams(
                qualified=f"{_SCHEMA}.{relation}",
                parent_name=parent_name,
                child_name=child_name,
                is_unique=expected_unique[parent_name],
                has_predicate=expected_partial[parent_name],
                columns=list(columns),
            )
            _require(
                bind,
                statement,
                f"{relation} lacks the exact child of {parent_name}",
            )
        expected_count = len(spec.matching_indexes)
        statement = sa.text("""
            SELECT count(*) = :expected_count
            FROM pg_index AS i
            JOIN pg_inherits AS inh ON inh.inhrelid = i.indexrelid
            WHERE i.indrelid = to_regclass(:qualified)
            """).bindparams(
            qualified=f"{_SCHEMA}.{relation}",
            expected_count=expected_count,
        )
        _require(bind, statement, f"{relation} has unexpected index parentage")
        total_indexes = expected_count + 1 + int(spec.active_public_id)
        statement = sa.text("""
            SELECT count(*) = :expected_count
            FROM pg_index
            WHERE indrelid = to_regclass(:qualified)
            """).bindparams(
            qualified=f"{_SCHEMA}.{relation}",
            expected_count=total_indexes,
        )
        _require(bind, statement, f"{relation} has an unexpected index count")
        _verify_leaf_local_indexes(bind, spec, relation)


def _generated_index_name(relation: str, index: MatchingIndex) -> str:
    """Return PostgreSQL's deterministic cloned-index name for one child."""
    name = f"{relation}_{'_'.join(index.columns)}_idx"
    if len(name) > 63:
        raise RuntimeError(f"generated index contract exceeds NAMEDATALEN: {name}")
    return name


def _verify_leaf_local_indexes(
    bind: Connection,
    spec: TableSpec,
    relation: str,
) -> None:
    """Verify one leaf's local PK and optional active-public-id index."""
    pkey = f"{relation}_pkey"
    statement = sa.text("""
        SELECT count(*) = 1
        FROM pg_index AS i
        JOIN pg_class AS index_relation ON index_relation.oid = i.indexrelid
        JOIN pg_am AS am ON am.oid = index_relation.relam
        JOIN pg_constraint AS c
          ON c.conindid = i.indexrelid
         AND c.contype = 'p'
        WHERE i.indrelid = to_regclass(:qualified)
          AND index_relation.relname = :index_name
          AND i.indisunique
          AND i.indisprimary
          AND i.indisvalid
          AND i.indisready
          AND i.indislive
          AND i.indpred IS NULL
          AND i.indexprs IS NULL
          AND i.indnkeyatts = i.indnatts
          AND NOT i.indnullsnotdistinct
          AND am.amname = 'btree'
          AND index_relation.reloptions IS NULL
          AND index_relation.reltablespace = 0
          AND ARRAY(
              SELECT a.attname::text
              FROM unnest(i.indkey)
                  WITH ORDINALITY AS k(attribute_number, position)
              JOIN pg_attribute AS a
                ON a.attrelid = i.indrelid
               AND a.attnum = k.attribute_number
              WHERE k.position <= i.indnkeyatts
              ORDER BY k.position
          ) = ARRAY['id']::text[]
          AND NOT EXISTS (
              SELECT 1
              FROM unnest(
                  i.indkey::smallint[],
                  i.indclass::oid[],
                  i.indcollation::oid[],
                  i.indoption::smallint[]
              ) WITH ORDINALITY AS key_part(
                  attribute_number,
                  opclass_oid,
                  collation_oid,
                  options,
                  position
              )
              JOIN pg_attribute AS key_attribute
                ON key_attribute.attrelid = i.indrelid
               AND key_attribute.attnum = key_part.attribute_number
              JOIN pg_opclass AS opclass
                ON opclass.oid = key_part.opclass_oid
              WHERE key_part.position <= i.indnkeyatts
                AND (
                    key_part.options <> 0
                    OR NOT opclass.opcdefault
                    OR key_part.collation_oid
                        <> key_attribute.attcollation
                )
          )
        """).bindparams(
        qualified=f"{_SCHEMA}.{relation}",
        index_name=pkey,
    )
    _require(bind, statement, f"{relation} local primary key is not exact")
    if not spec.active_public_id:
        return
    public_id = f"{relation}_public_id"
    statement = sa.text("""
        SELECT count(*) = 1
        FROM pg_index AS i
        JOIN pg_class AS index_relation ON index_relation.oid = i.indexrelid
        JOIN pg_am AS am ON am.oid = index_relation.relam
        WHERE i.indrelid = to_regclass(:qualified)
          AND index_relation.relname = :index_name
          AND i.indisunique
          AND NOT i.indisprimary
          AND i.indisvalid
          AND i.indisready
          AND i.indislive
          AND i.indpred IS NOT NULL
          AND i.indexprs IS NULL
          AND i.indnkeyatts = i.indnatts
          AND NOT i.indnullsnotdistinct
          AND am.amname = 'btree'
          AND index_relation.reloptions IS NULL
          AND index_relation.reltablespace = 0
          AND NOT EXISTS (
              SELECT 1
              FROM pg_constraint AS c
              WHERE c.conindid = i.indexrelid
          )
          AND ARRAY(
              SELECT a.attname::text
              FROM unnest(i.indkey)
                  WITH ORDINALITY AS k(attribute_number, position)
              JOIN pg_attribute AS a
                ON a.attrelid = i.indrelid
               AND a.attnum = k.attribute_number
              WHERE k.position <= i.indnkeyatts
              ORDER BY k.position
          ) = ARRAY['public_id']::text[]
          AND pg_get_expr(i.indpred, i.indrelid) = :predicate
          AND NOT EXISTS (
              SELECT 1
              FROM unnest(
                  i.indkey::smallint[],
                  i.indclass::oid[],
                  i.indcollation::oid[],
                  i.indoption::smallint[]
              ) WITH ORDINALITY AS key_part(
                  attribute_number,
                  opclass_oid,
                  collation_oid,
                  options,
                  position
              )
              JOIN pg_attribute AS key_attribute
                ON key_attribute.attrelid = i.indrelid
               AND key_attribute.attnum = key_part.attribute_number
              JOIN pg_opclass AS opclass
                ON opclass.oid = key_part.opclass_oid
              WHERE key_part.position <= i.indnkeyatts
                AND (
                    key_part.options <> 0
                    OR NOT opclass.opcdefault
                    OR key_part.collation_oid
                        <> key_attribute.attcollation
                )
          )
        """).bindparams(
        qualified=f"{_SCHEMA}.{relation}",
        index_name=public_id,
        predicate=_ACTIVE_PREDICATE_DEFINITION,
    )
    _require(bind, statement, f"{relation} active-public-id partial is not exact")


def _verify_partial_predicates(
    bind: Connection,
    spec: TableSpec,
    relations: Sequence[str],
    parent: bool = False,
) -> None:
    """Verify every partial index is exactly the active-row predicate."""
    local_count = 0 if parent else int(spec.active_public_id)
    expected_per_relation = local_count + sum(int(index.partial) for index in spec.matching_indexes)
    if expected_per_relation == 0:
        return
    for relation in relations:
        statement = sa.text("""
            SELECT
                count(*) = :expected_count
                AND bool_and(
                    pg_get_expr(i.indpred, i.indrelid) = :predicate
                )
            FROM pg_index AS i
            WHERE i.indrelid = to_regclass(:qualified)
              AND i.indpred IS NOT NULL
            """).bindparams(
            expected_count=expected_per_relation,
            qualified=f"{_SCHEMA}.{relation}",
            predicate=_ACTIVE_PREDICATE_DEFINITION,
        )
        _require(bind, statement, f"{relation} has an unexpected partial predicate")


def _verify_standalone_unique_indexes(
    bind: Connection,
    spec: TableSpec,
    legacy: str,
) -> None:
    """Verify parent unique indexes and the trade U3 child are standalone."""
    names = [index.parent_name for index in spec.matching_indexes if index.unique]
    names.extend(
        index.legacy_name for index in spec.matching_indexes if index.unique and index.introduced
    )
    if not names:
        return
    statement = sa.text(f"""
        SELECT NOT EXISTS (
            SELECT 1
            FROM pg_constraint AS c
            JOIN pg_class AS i ON i.oid = c.conindid
            JOIN pg_namespace AS n ON n.oid = i.relnamespace
            WHERE n.nspname = :schema
              AND i.relname = ANY ({_sql_text_array(names)})
        )
        """).bindparams(schema=_SCHEMA)
    _require(
        bind,
        statement,
        f"{spec.name} parent uniqueness must use standalone indexes",
    )
    if spec.name == "trades":
        statement = sa.text("""
            SELECT i.indrelid = to_regclass(:legacy)
            FROM pg_index AS i
            WHERE i.indexrelid = to_regclass(:index_name)
            """).bindparams(
            legacy=f"{_SCHEMA}.{legacy}",
            index_name=f"{_SCHEMA}.uq_trade_instr_tid_exec",
        )
        _require(bind, statement, "trade U3 is not the legacy standalone index")


def _verify_sequence(bind: Connection, spec: TableSpec, owner: str) -> None:
    """Verify sequence parameters and exact ownership target."""
    sequence = f"{spec.name}_id_seq"
    statement = sa.text("""
        SELECT count(*) = 1
        FROM pg_class AS s
        JOIN pg_namespace AS sn ON sn.oid = s.relnamespace
        JOIN pg_sequence AS q ON q.seqrelid = s.oid
        JOIN pg_depend AS d
          ON d.classid = 'pg_class'::regclass
         AND d.objid = s.oid
         AND d.objsubid = 0
         AND d.refclassid = 'pg_class'::regclass
         AND d.deptype = 'a'
        JOIN pg_class AS t ON t.oid = d.refobjid
        JOIN pg_namespace AS tn ON tn.oid = t.relnamespace
        JOIN pg_attribute AS a
          ON a.attrelid = t.oid
         AND a.attnum = d.refobjsubid
        WHERE sn.nspname = :schema
          AND s.relname = :sequence
          AND s.relkind = 'S'
          AND tn.nspname = :schema
          AND t.relname = :owner
          AND a.attname = 'id'
          AND q.seqtypid = 'bigint'::regtype
          AND q.seqstart = 1
          AND q.seqincrement = 1
          AND q.seqmax = 9223372036854775807
          AND q.seqmin = 1
          AND q.seqcache = 1
          AND NOT q.seqcycle
        """).bindparams(
        schema=_SCHEMA,
        sequence=sequence,
        owner=owner,
    )
    _require(bind, statement, f"{sequence} parameters or ownership are not exact")


def _verify_partition_tree(
    bind: Connection,
    spec: TableSpec,
    anchor: datetime,
) -> tuple[str, ...]:
    """Verify exact relation names, inheritance edges, bounds, and range key."""
    legacy = f"{spec.name}_legacy"
    daily = _daily_names(spec, anchor)
    default = f"{spec.name}_default"
    children = (legacy, *daily, default)
    anchor_text = anchor.strftime("%Y-%m-%d %H:%M:%S+00")
    expected_bounds = {
        legacy: f"FOR VALUES FROM (MINVALUE) TO ('{anchor_text}')",
        default: "DEFAULT",
    }
    for offset, child in enumerate(daily):
        lower = anchor + timedelta(days=offset)
        upper = lower + timedelta(days=1)
        expected_bounds[child] = (
            f"FOR VALUES FROM ('{lower.strftime('%Y-%m-%d %H:%M:%S+00')}') "
            f"TO ('{upper.strftime('%Y-%m-%d %H:%M:%S+00')}')"
        )
    expected_rows = ", ".join(
        "('" + child + "', '" + expected_bounds[child].replace("'", "''") + "')"
        for child in children
    )
    statement = sa.text(f"""
        WITH expected(child_name, bound) AS (
            VALUES {expected_rows}
        ),
        actual AS (
            SELECT
                child.relname::text,
                pg_get_expr(child.relpartbound, child.oid, true)
            FROM pg_inherits AS inh
            JOIN pg_class AS parent ON parent.oid = inh.inhparent
            JOIN pg_namespace AS pn ON pn.oid = parent.relnamespace
            JOIN pg_class AS child ON child.oid = inh.inhrelid
            JOIN pg_namespace AS cn ON cn.oid = child.relnamespace
            WHERE pn.nspname = :schema
              AND parent.relname = :parent
              AND cn.nspname = :schema
        )
        SELECT NOT EXISTS (
            (SELECT * FROM actual EXCEPT SELECT * FROM expected)
            UNION ALL
            (SELECT * FROM expected EXCEPT SELECT * FROM actual)
        )
        """).bindparams(schema=_SCHEMA, parent=spec.name)
    _require(bind, statement, f"{spec.name} partition bounds or table edges differ")
    statement = sa.text("""
        SELECT
            p.partstrat = 'r'
            AND p.partnatts = 1
            AND p.partexprs IS NULL
            AND p.partattrs[0] = a.attnum
        FROM pg_partitioned_table AS p
        JOIN pg_class AS t ON t.oid = p.partrelid
        JOIN pg_namespace AS n ON n.oid = t.relnamespace
        JOIN pg_attribute AS a
          ON a.attrelid = t.oid
         AND a.attname = :key
        WHERE n.nspname = :schema
          AND t.relname = :parent
          AND t.relkind = 'p'
          AND NOT t.relispartition
        """).bindparams(schema=_SCHEMA, parent=spec.name, key=spec.key)
    _require(bind, statement, f"{spec.name} partition strategy or key differs")
    return children


def _verify_partitioned(
    bind: Connection,
    spec: TableSpec,
    anchor: datetime,
) -> None:
    """Verify one already adopted table as an exact no-op candidate."""
    bind.execute(sa.text(f"LOCK TABLE ONLY {_qualified(spec.name)} IN ACCESS SHARE MODE"))
    children = _verify_partition_tree(bind, spec, anchor)
    relations = (spec.name, *children)
    _verify_columns(bind, spec, relations)
    legacy = f"{spec.name}_legacy"
    _verify_constraint_manifest(bind, spec, spec.name, legacy=False, anchor=anchor)
    for relation in children:
        _verify_constraint_manifest(
            bind,
            spec,
            relation,
            legacy=relation == legacy,
            anchor=anchor,
        )
    _verify_check_definitions(bind, spec, relations, anchor)
    _verify_named_indexes(
        bind,
        spec.name,
        _index_shape_rows(spec, spec.name, parent=True, legacy=False),
    )
    _verify_named_indexes(
        bind,
        legacy,
        _index_shape_rows(spec, legacy, parent=False, legacy=True),
    )
    generated_children = tuple(relation for relation in children if relation != legacy)
    _verify_generated_partition_indexes(bind, spec, generated_children)
    _verify_partial_predicates(bind, spec, (spec.name,), parent=True)
    _verify_partial_predicates(bind, spec, children)
    _verify_standalone_unique_indexes(bind, spec, legacy)
    _verify_sequence(bind, spec, spec.name)


def _ordinary_index_rows(spec: TableSpec, has_trade_u3: bool) -> list[str]:
    """Return exact named index rows allowed before PostgreSQL adoption."""
    rows = [
        _index_shape_row(
            CatalogIndex(
                f"{spec.name}_pkey",
                ("id",),
                True,
                True,
                False,
                "",
            )
        )
    ]
    if spec.legacy_u2:
        rows.append(
            _index_shape_row(
                CatalogIndex(
                    "uq_trade_instrument_trade_id",
                    ("instrument_public_id", "trade_id"),
                    True,
                    False,
                    False,
                    "",
                )
            )
        )
    if spec.active_public_id:
        rows.append(
            _index_shape_row(
                CatalogIndex(
                    f"ix_{spec.name}_public_id",
                    ("public_id",),
                    True,
                    False,
                    True,
                    "",
                )
            )
        )
    for index in spec.matching_indexes:
        if not index.introduced or has_trade_u3:
            rows.append(
                _index_shape_row(
                    CatalogIndex(
                        index.legacy_name,
                        index.columns,
                        index.unique,
                        False,
                        index.partial,
                        "",
                    )
                )
            )
    return rows


def _verify_ordinary(bind: Connection, spec: TableSpec) -> None:
    """Verify the unpartitioned 0042 schema before any conversion DDL."""
    has_trade_u3 = (
        spec.name == "trades" and _relation_kind(bind, "uq_trade_instr_tid_exec") == "i:false"
    )
    _verify_columns(bind, spec, (spec.name,), ordinary=spec.name == "trades")
    constraints = [(name, "c", _check_definition(name)) for name in spec.checks]
    constraints.extend(_not_null_constraint_rows(spec, ordinary=True))
    constraints.append((f"{spec.name}_pkey", "p", "PRIMARY KEY (id)"))
    if spec.legacy_u2:
        constraints.append(
            (
                "uq_trade_instrument_trade_id",
                "u",
                "UNIQUE (instrument_public_id, trade_id)",
            )
        )
    expected_rows = ", ".join(
        "("
        f"'{name}', "
        f"'{constraint_type}', "
        f"'{definition.replace("'", "''")}', "
        "TRUE, FALSE, FALSE"
        ")"
        for name, constraint_type, definition in constraints
    )
    statement = sa.text(f"""
        WITH expected(
            name,
            constraint_type,
            definition,
            validated,
            is_deferrable,
            is_initially_deferred
        ) AS (
            VALUES {expected_rows}
        ),
        actual AS (
            SELECT
                conname::text,
                contype::text,
                pg_get_constraintdef(oid, true),
                convalidated,
                condeferrable,
                condeferred
            FROM pg_constraint
            WHERE conrelid = to_regclass(:qualified)
        )
        SELECT NOT EXISTS (
            (SELECT * FROM actual EXCEPT SELECT * FROM expected)
            UNION ALL
            (SELECT * FROM expected EXCEPT SELECT * FROM actual)
        )
        """).bindparams(qualified=f"{_SCHEMA}.{spec.name}")
    _require(bind, statement, f"{spec.name} ordinary constraints are not exact")
    _verify_named_indexes(
        bind,
        spec.name,
        _ordinary_index_rows(spec, has_trade_u3),
    )
    _verify_partial_predicates(bind, spec, (spec.name,))
    _verify_sequence(bind, spec, spec.name)


def _assert_names_available(
    bind: Connection,
    spec: TableSpec,
    anchor: datetime,
) -> None:
    """Refuse an ordinary-table conversion with any partial topology debris."""
    names = [
        f"{spec.name}_legacy",
        f"{spec.name}_default",
        *_daily_names(spec, anchor),
        *(index.parent_name for index in spec.matching_indexes),
    ]
    statement = sa.text(f"""
        SELECT NOT EXISTS (
            SELECT 1
            FROM pg_class AS c
            JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname = :schema
              AND c.relname = ANY ({_sql_text_array(names)})
        )
        """).bindparams(schema=_SCHEMA)
    _require(
        bind,
        statement,
        f"{spec.name} has partial 0043 topology objects; refusing conversion",
    )


def _preflight(
    bind: Connection,
    spec: TableSpec,
    anchor: datetime,
) -> str:
    """Classify and lock one table into an allowed migration state."""
    state = _relation_kind(bind, spec.name)
    if state == "p:false":
        _verify_partitioned(bind, spec, anchor)
        return "partitioned"
    if state != "r:false":
        raise RuntimeError(
            f"{spec.name} must be an ordinary table or the exact 0043 parent; "
            f"observed {state or 'missing'}"
        )
    populated = cast(
        bool,
        bind.scalar(sa.text(f"SELECT EXISTS (SELECT 1 FROM {_qualified(spec.name)} LIMIT 1)")),
    )
    if populated:
        raise RuntimeError(
            f"{spec.name} is populated; 0043 refuses automatic adoption and "
            "requires the public manual path"
        )
    bind.execute(sa.text(f"LOCK TABLE {_qualified(spec.name)} IN SHARE MODE"))
    populated_after_lock = cast(
        bool,
        bind.scalar(sa.text(f"SELECT EXISTS (SELECT 1 FROM {_qualified(spec.name)} LIMIT 1)")),
    )
    if populated_after_lock:
        raise RuntimeError(
            f"{spec.name} became populated before the migration lock; refusing adoption"
        )
    _verify_ordinary(bind, spec)
    _assert_names_available(bind, spec, anchor)
    return "ordinary"


def _create_parent_checks(spec: TableSpec) -> None:
    """Create the exact ordinary CHECK constraints on a minimal parent."""
    op.execute(
        f"ALTER TABLE {_qualified(spec.name)} "
        f"ADD CONSTRAINT {_identifier(f'ck_{spec.name}_sequence_id')} "
        "CHECK (sequence_id > 0)"
    )
    if spec.name == "candles":
        op.execute(
            f"ALTER TABLE {_qualified(spec.name)} "
            "ADD CONSTRAINT ck_candle_source "
            "CHECK (source IN ('native', 'calculated', 'synthesized'))"
        )


def _create_parent_indexes(spec: TableSpec) -> None:
    """Create standalone parent indexes before any partition is attached."""
    for index in spec.matching_indexes:
        unique = "UNIQUE " if index.unique else ""
        predicate = f" WHERE {_ACTIVE_PREDICATE}" if index.partial else ""
        columns = ", ".join(_identifier(column) for column in index.columns)
        op.execute(
            f"CREATE {unique}INDEX {_qualified(index.parent_name)} "
            f"ON {_qualified(spec.name)} ({columns}){predicate}"
        )


def _create_local_leaf_objects(spec: TableSpec, relation: str) -> None:
    """Create the leaf-local PK and active-public-id partial."""
    op.execute(
        f"ALTER TABLE {_qualified(relation)} "
        f"ADD CONSTRAINT {_identifier(f'{relation}_pkey')} PRIMARY KEY (id)"
    )
    if spec.active_public_id:
        op.execute(
            f"CREATE UNIQUE INDEX {_qualified(f'{relation}_public_id')} "
            f"ON {_qualified(relation)} (public_id) WHERE {_ACTIVE_PREDICATE}"
        )


def _create_partitions(spec: TableSpec, anchor: datetime) -> None:
    """Create fourteen ordered daily leaves followed by the DEFAULT leaf."""
    for offset in range(_FUTURE_LEAVES):
        lower = anchor + timedelta(days=offset)
        upper = lower + timedelta(days=1)
        relation = _leaf_name(spec.name, lower)
        op.execute(
            f"CREATE TABLE {_qualified(relation)} "
            f"PARTITION OF {_qualified(spec.name)} "
            f"FOR VALUES FROM ({_anchor_literal(lower)}) "
            f"TO ({_anchor_literal(upper)})"
        )
        _create_local_leaf_objects(spec, relation)
    default = f"{spec.name}_default"
    op.execute(f"CREATE TABLE {_qualified(default)} PARTITION OF {_qualified(spec.name)} DEFAULT")
    _create_local_leaf_objects(spec, default)


def _prepare_legacy(spec: TableSpec, anchor: datetime) -> None:
    """Tighten the key, add the anchor proof, and ensure the trade U3."""
    key = _identifier(spec.key)
    if spec.name == "trades":
        op.execute(f"ALTER TABLE {_qualified(spec.name)} ALTER COLUMN {key} SET NOT NULL")
        if _relation_kind(op.get_bind(), "uq_trade_instr_tid_exec") is None:
            op.execute(
                "CREATE UNIQUE INDEX public.uq_trade_instr_tid_exec "
                "ON public.trades "
                "(instrument_public_id, trade_id, executed_at)"
            )
    constraint = _identifier(f"ck_{spec.name}_legacy_range")
    op.execute(
        f"ALTER TABLE {_qualified(spec.name)} "
        f"ADD CONSTRAINT {constraint} "
        f"CHECK ({key} IS NOT NULL AND {key} < {_anchor_literal(anchor)})"
    )


def _adopt(bind: Connection, spec: TableSpec, anchor: datetime) -> None:
    """Convert one locked empty ordinary table through the manual adoption shape."""
    _prepare_legacy(spec, anchor)
    legacy = f"{spec.name}_legacy"
    op.execute(f"ALTER TABLE {_qualified(spec.name)} RENAME TO {_identifier(legacy)}")
    op.execute(
        f"CREATE TABLE {_qualified(spec.name)} "
        f"(LIKE {_qualified(legacy)} INCLUDING DEFAULTS INCLUDING GENERATED "
        "INCLUDING STORAGE INCLUDING COMPRESSION) "
        f"PARTITION BY RANGE ({_identifier(spec.key)})"
    )
    _create_parent_checks(spec)
    _create_parent_indexes(spec)
    _create_partitions(spec, anchor)
    op.execute(
        f"ALTER TABLE {_qualified(spec.name)} "
        f"ATTACH PARTITION {_qualified(legacy)} "
        f"FOR VALUES FROM (MINVALUE) TO ({_anchor_literal(anchor)})"
    )
    op.execute(
        f"ALTER SEQUENCE {_qualified(f'{spec.name}_id_seq')} "
        f"OWNED BY {_qualified(spec.name)}.id"
    )
    _verify_partitioned(bind, spec, anchor)


def _upgrade_sqlite(bind: Connection) -> None:
    """Tighten the SQLite writer contract while retaining ordinary tables."""
    has_null = cast(
        bool,
        bind.scalar(
            sa.text("SELECT EXISTS (SELECT 1 FROM trades WHERE executed_at IS NULL LIMIT 1)")
        ),
    )
    if has_null:
        raise RuntimeError("trades.executed_at contains NULL; 0043 refuses SQLite tightening")
    with op.batch_alter_table("trades", recreate="always") as batch:
        batch.drop_constraint("uq_trade_instrument_trade_id", type_="unique")
        batch.alter_column(
            "executed_at",
            existing_type=sa.DateTime(timezone=True),
            nullable=False,
        )
    op.create_index(
        "uq_trade_instr_tid_exec",
        "trades",
        ["instrument_public_id", "trade_id", "executed_at"],
        unique=True,
    )


def upgrade() -> None:
    """Adopt empty PostgreSQL tables or verify the exact manual topology."""
    bind = op.get_bind()
    if bind.dialect.name == "sqlite":
        _upgrade_sqlite(bind)
        return
    if bind.dialect.name != "postgresql":
        raise RuntimeError("0043 supports only PostgreSQL and SQLite")
    bind.execute(sa.text("SET LOCAL TIME ZONE 'UTC'"))
    bind.execute(sa.text("SET LOCAL lock_timeout = '5s'"))
    bind.execute(sa.text("SET LOCAL statement_timeout = '30s'"))
    anchor = _resolve_anchor(bind)
    states = {spec.name: _preflight(bind, spec, anchor) for spec in _TABLES}
    for spec in _TABLES:
        if states[spec.name] == "ordinary":
            _adopt(bind, spec, anchor)


def downgrade() -> None:
    """Reverse only the SQLite compatibility slice.

    Raises:
        RuntimeError: PostgreSQL partition collapse would require row movement
            and is intentionally unavailable as an automatic migration.
    """
    bind = op.get_bind()
    if bind.dialect.name == "sqlite":
        op.drop_index("uq_trade_instr_tid_exec", table_name="trades")
        with op.batch_alter_table("trades", recreate="always") as batch:
            batch.alter_column(
                "executed_at",
                existing_type=sa.DateTime(timezone=True),
                nullable=True,
            )
            batch.create_unique_constraint(
                "uq_trade_instrument_trade_id",
                ["instrument_public_id", "trade_id"],
            )
        return
    raise RuntimeError(
        "0043 PostgreSQL downgrade is intentionally fail-closed; "
        "partition collapse requires a separate manual data-movement plan"
    )
