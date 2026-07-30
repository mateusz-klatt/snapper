"""Independent PostgreSQL convergence proof for daily market partitions.

The opt-in integration tests construct one low-priority PostgreSQL 18.4
cluster with private Unix-socket access and exactly two databases. The manual
database runs through 0042, proves four fail-closed migration states, invokes
the public runtime adoption API, and lets 0043 verify without changing it. The
fresh database runs 0001 and then independently executes Alembic through 0043.

The catalog oracle compares every logical topology field requested by the
partitioning contract and excludes only unstable physical identifiers and the
unrelated Alembic version row. Seven transactional adversarial mutations prove
the live PostgreSQL comparison rejects every load-bearing catalog dimension.
A lightweight synthetic version of the same seven oracle checks remains in the
default suite even when the host cannot safely start a scratch cluster.
"""

import ast
from collections.abc import Generator
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Final
from typing import Literal

import pytest

from tests.helpers.market_partition_convergence import CatalogFingerprint
from tests.helpers.market_partition_convergence import CatalogMutation
from tests.helpers.market_partition_convergence import PostgresBranches
from tests.helpers.market_partition_convergence import ScratchClusterUnavailableError
from tests.helpers.market_partition_convergence import UnsafeHostLoadError
from tests.helpers.market_partition_convergence import alembic_revision
from tests.helpers.market_partition_convergence import apply_catalog_mutation
from tests.helpers.market_partition_convergence import assert_catalogs_identical
from tests.helpers.market_partition_convergence import catalog_fingerprint
from tests.helpers.market_partition_convergence import postgres_branches

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_MIGRATION_PATH = (
    _PROJECT_ROOT / "src/snapper/data/migrations/versions/0043_daily_market_partitions.py"
)
_RUNTIME_PATH = _PROJECT_ROOT / "src/snapper/data/daily_partitions.py"
_HELPER_PATH = _PROJECT_ROOT / "tests/helpers/market_partition_convergence.py"

_REFERENCE = CatalogFingerprint(
    relations=(("trades", "p", False, "r", "RANGE (executed_at)"),),
    table_inheritance=(("trades", "trades_d20260730", 1),),
    partition_bounds=(
        (
            "trades_d20260730",
            "FOR VALUES FROM ('2026-07-30 00:00:00+00') TO ('2026-07-31 00:00:00+00')",
        ),
    ),
    indexes=(
        (
            "trades",
            "trades_p_uq_instr_tid_exec",
            (
                "CREATE UNIQUE INDEX trades_p_uq_instr_tid_exec ON ONLY public.trades "
                "USING btree (instrument_public_id, trade_id, executed_at)"
            ),
            False,
            True,
            True,
            None,
        ),
        (
            "trades_d20260730",
            "trades_d20260730_public_id",
            (
                "CREATE UNIQUE INDEX trades_d20260730_public_id ON public.trades_d20260730 "
                "USING btree (public_id) WHERE "
                "(known_to = 'infinity'::timestamp with time zone)"
            ),
            False,
            True,
            True,
            "(known_to = 'infinity'::timestamp with time zone)",
        ),
    ),
    index_inheritance=(
        (
            "trades_p_uq_instr_tid_exec",
            "trades_d20260730_instrument_public_id_trade_id_executed_at_idx",
            1,
        ),
    ),
    constraints=(
        (
            "trades_d20260730",
            "trades_d20260730_pkey",
            "p",
            "PRIMARY KEY (id)",
            True,
            False,
            False,
        ),
    ),
    columns=(
        (
            "trades",
            8,
            "executed_at",
            "timestamp with time zone",
            False,
            None,
            "",
            "",
        ),
    ),
    sequences=(
        (
            "trades_id_seq",
            "bigint",
            1,
            1,
            9223372036854775807,
            1,
            False,
            1,
            "trades",
            "id",
            "a",
        ),
    ),
)

_SYNTHETIC_MUTATIONS: Final[tuple[tuple[CatalogFingerprint, str, str], ...]] = (
    (
        replace(
            _REFERENCE,
            partition_bounds=(
                (
                    "trades_d20260730",
                    "FOR VALUES FROM ('2026-07-30 00:00:00+00') TO ('2026-07-30 12:00:00+00')",
                ),
            ),
        ),
        "partition_bounds",
        "partition-bound",
    ),
    (
        replace(
            _REFERENCE,
            indexes=(
                _REFERENCE.indexes[0],
                (
                    *_REFERENCE.indexes[1][0:6],
                    (
                        "(known_to = 'infinity'::timestamp with time zone) "
                        "AND (public_id IS NOT NULL)"
                    ),
                ),
            ),
        ),
        "indexes",
        "local-active-public-id-partial",
    ),
    (
        replace(
            _REFERENCE,
            indexes=(
                _REFERENCE.indexes[0],
                (
                    _REFERENCE.indexes[1][0],
                    "trades_d20260730_public_id_mutated",
                    _REFERENCE.indexes[1][2],
                    *_REFERENCE.indexes[1][3:],
                ),
            ),
        ),
        "indexes",
        "index-name",
    ),
    (
        replace(
            _REFERENCE,
            index_inheritance=(
                (
                    "trades_p_uq_instr_tid_exec",
                    "trades_d20260731_instrument_public_id_trade_id_executed_at_idx",
                    1,
                ),
            ),
        ),
        "index_inheritance",
        "index-parentage",
    ),
    (
        replace(
            _REFERENCE,
            columns=(
                (
                    *_REFERENCE.columns[0][0:4],
                    True,
                    *_REFERENCE.columns[0][5:],
                ),
            ),
        ),
        "columns",
        "partition-key-nullability",
    ),
    (
        replace(
            _REFERENCE,
            indexes=(
                (
                    *_REFERENCE.indexes[0][0:2],
                    (
                        "CREATE UNIQUE INDEX trades_p_uq_instr_tid_exec ON ONLY public.trades "
                        "USING btree (instrument_public_id, executed_at, trade_id)"
                    ),
                    *_REFERENCE.indexes[0][3:],
                ),
                _REFERENCE.indexes[1],
            ),
        ),
        "indexes",
        "trade-u3-definition",
    ),
    (
        replace(
            _REFERENCE,
            sequences=(
                (
                    *_REFERENCE.sequences[0][0:8],
                    "trades_legacy",
                    *_REFERENCE.sequences[0][9:],
                ),
            ),
        ),
        "sequences",
        "sequence-ownership",
    ),
)

_MUTATION_SECTIONS: Final[dict[CatalogMutation, str]] = {
    CatalogMutation.PARTITION_BOUND: "partition_bounds",
    CatalogMutation.LOCAL_ACTIVE_PUBLIC_ID_PARTIAL: "indexes",
    CatalogMutation.INDEX_NAME: "relations",
    CatalogMutation.INDEX_PARENTAGE: "index_inheritance",
    CatalogMutation.PARTITION_KEY_NULLABILITY: "columns",
    CatalogMutation.TRADE_U3_DEFINITION: "indexes",
    CatalogMutation.SEQUENCE_OWNERSHIP: "sequences",
}

_PARENT_INDEXES: Final[set[str]] = {
    "ticks_p_ix_instr_ts",
    "candles_p_uq_itf_open",
    "candles_p_ix_instr_open",
    "trades_p_uq_instr_tid_exec",
    "trades_p_ix_instr_ts",
    "trades_p_ix_ts",
    "trades_p_ix_exec",
}
_ACTIVE_PREDICATE = "(known_to = '9999-12-31 23:59:59+00'::timestamp with time zone)"
_UNEXPECTED_PARTITION_BOUND = (
    "FOR VALUES FROM ('2026-09-01 00:00:00+00') TO ('2026-09-02 00:00:00+00')"
)
_ROOT_PARTITION_KEYS: Final[dict[str, str]] = {
    "ticks": 'RANGE ("timestamp")',
    "candles": "RANGE (open_at)",
    "trades": "RANGE (executed_at)",
}
_ROOT_KEY_COLUMNS: Final[dict[str, str]] = {
    "ticks": "timestamp",
    "candles": "open_at",
    "trades": "executed_at",
}
_ANCHOR = datetime(2026, 7, 30, tzinfo=UTC)
_TRADES_U3_DEFINITION = (
    "CREATE UNIQUE INDEX trades_p_uq_instr_tid_exec ON ONLY public.trades "
    "USING btree (instrument_public_id, trade_id, executed_at)"
)
_CANDLE_UNIQUE_DEFINITION = (
    "CREATE UNIQUE INDEX candles_p_uq_itf_open ON ONLY public.candles "
    "USING btree (instrument_public_id, timeframe, open_at) "
    f"WHERE {_ACTIVE_PREDICATE}"
)
_EXPECTED_PARENT_INDEX_ROWS: Final[
    dict[tuple[str, str], tuple[str, bool, bool, bool, str | None]]
] = {
    (
        "ticks",
        "ticks_p_ix_instr_ts",
    ): (
        (
            "CREATE INDEX ticks_p_ix_instr_ts ON ONLY public.ticks "
            'USING btree (instrument_public_id, "timestamp")'
        ),
        False,
        False,
        True,
        None,
    ),
    (
        "candles",
        "candles_p_uq_itf_open",
    ): (
        _CANDLE_UNIQUE_DEFINITION,
        False,
        True,
        True,
        _ACTIVE_PREDICATE,
    ),
    (
        "candles",
        "candles_p_ix_instr_open",
    ): (
        (
            "CREATE INDEX candles_p_ix_instr_open ON ONLY public.candles "
            "USING btree (instrument_public_id, open_at)"
        ),
        False,
        False,
        True,
        None,
    ),
    (
        "trades",
        "trades_p_uq_instr_tid_exec",
    ): (
        _TRADES_U3_DEFINITION,
        False,
        True,
        True,
        None,
    ),
    (
        "trades",
        "trades_p_ix_instr_ts",
    ): (
        (
            "CREATE INDEX trades_p_ix_instr_ts ON ONLY public.trades "
            'USING btree (instrument_public_id, "timestamp")'
        ),
        False,
        False,
        True,
        None,
    ),
    (
        "trades",
        "trades_p_ix_ts",
    ): (
        'CREATE INDEX trades_p_ix_ts ON ONLY public.trades USING btree ("timestamp")',
        False,
        False,
        True,
        None,
    ),
    (
        "trades",
        "trades_p_ix_exec",
    ): (
        "CREATE INDEX trades_p_ix_exec ON ONLY public.trades USING btree (executed_at)",
        False,
        False,
        True,
        None,
    ),
}


def _imported_modules(path: Path) -> set[str]:
    """Collect static import module names from one independent implementation.

    Args:
        path: Python implementation file to parse.

    Returns:
        Imported module names.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported.add(node.module)
    return imported


@pytest.fixture(scope="session")
def converged_postgresql_branches() -> Generator[PostgresBranches]:
    """Build two independent branches or skip before unsafe heavy work.

    Yields:
        Manual and fresh scratch databases from one private cluster.
    """
    try:
        with postgres_branches() as branches:
            yield branches
    except (ScratchClusterUnavailableError, UnsafeHostLoadError) as exc:
        pytest.skip(str(exc))


def test_runtime_and_migration_implementations_have_no_shared_import_path() -> None:
    """Pin the independence that makes the convergence comparison meaningful.

    Given: The public runtime adoption module and migration 0043 source files.
    When: Their static imports are parsed without importing either implementation.
    Then: Migration 0043 does not call through runtime adoption or the test
        helper, and runtime adoption does not call through migration 0043.
    """
    migration_imports = _imported_modules(_MIGRATION_PATH)
    runtime_imports = _imported_modules(_RUNTIME_PATH)

    assert "snapper.data.daily_partitions" not in migration_imports
    assert "tests.helpers.market_partition_convergence" not in migration_imports
    assert "snapper.data.migrations.versions.0043_daily_market_partitions" not in runtime_imports
    assert "tests.helpers.market_partition_convergence" not in runtime_imports


def test_catalog_sql_walks_complete_table_and_index_inheritance_components() -> None:
    """Pin the scope rules that keep malformed inheritance edges observable.

    Given: Catalog SQL rooted at each market table and its indexes.
    When: The source-level graph traversal invariants are inspected.
    Then: Table traversal follows both edge directions without a relation-kind
        filter, and index traversal follows both directions from every market
        index before relations, definitions, and edges are fingerprinted.
    """
    source = _HELPER_PATH.read_text(encoding="utf-8")

    assert "child.relkind IN ('r', 'p')" not in source
    assert "topology.oid = inheritance.inhparent" in source
    assert "topology.oid = inheritance.inhrelid" in source
    assert "selected_indexes(index_oid) AS" in source
    assert "selected_indexes.index_oid = inheritance.inhparent" in source
    assert "selected_indexes.index_oid = inheritance.inhrelid" in source
    assert "_RELATIONS_SQL = _INDEX_GRAPH_CTE" in source
    assert "_INDEXES_SQL = _INDEX_GRAPH_CTE" in source
    assert "_INDEX_INHERITANCE_SQL = _INDEX_GRAPH_CTE" in source


@pytest.mark.parametrize(
    ("mutated", "section"),
    [
        pytest.param(mutated, section, id=mutation_id)
        for mutated, section, mutation_id in _SYNTHETIC_MUTATIONS
    ],
)
def test_catalog_comparator_rejects_each_required_synthetic_mutation(
    mutated: CatalogFingerprint,
    section: str,
) -> None:
    """Reject all seven load-bearing changes without starting PostgreSQL.

    Args:
        mutated: Reference fingerprint changed in one required dimension.
        section: Comparator section expected to refuse the change.

    Given: One reference catalog fingerprint and a copy perturbed in exactly
        one required dimension.
    When: The convergence comparator evaluates the pair.
    Then: It fails in the expected catalog section, proving the default test
        suite continuously protects the comparison oracle itself.
    """
    with pytest.raises(AssertionError, match=section):
        assert_catalogs_identical(_REFERENCE, mutated)
    with pytest.raises(AssertionError, match=section):
        assert_catalogs_identical(mutated, _REFERENCE)


def _expected_partition_names() -> dict[str, tuple[str, ...]]:
    """Return the exact legacy, daily, and DEFAULT names for every root."""
    day_suffixes = (
        "20260730",
        "20260731",
        "20260801",
        "20260802",
        "20260803",
        "20260804",
        "20260805",
        "20260806",
        "20260807",
        "20260808",
        "20260809",
        "20260810",
        "20260811",
        "20260812",
    )
    return {
        table: (
            f"{table}_legacy",
            *(f"{table}_d{suffix}" for suffix in day_suffixes),
            f"{table}_default",
        )
        for table in ("ticks", "candles", "trades")
    }


def _assert_root_contract(fingerprint: CatalogFingerprint) -> None:
    """Require exact parent keys, standalone indexes, and index inheritance."""
    root_relations = {
        name: (kind, is_partition, strategy, key)
        for name, kind, is_partition, strategy, key in fingerprint.relations
        if name in {"ticks", "candles", "trades"}
    }
    assert root_relations == {
        table: ("p", False, "r", key) for table, key in _ROOT_PARTITION_KEYS.items()
    }

    indexes = {
        (table, name): (definition, primary, unique, valid, predicate)
        for table, name, definition, primary, unique, valid, predicate in fingerprint.indexes
        if table in {"ticks", "candles", "trades"}
    }
    assert indexes == _EXPECTED_PARENT_INDEX_ROWS
    assert not any(
        constraint_type in {"p", "u"}
        for table, _, constraint_type, _, _, _, _ in fingerprint.constraints
        if table in {"ticks", "candles", "trades"}
    )
    assert len(fingerprint.index_inheritance) == 112
    index_tables = {name: table for table, name, _, _, _, _, _ in fingerprint.indexes}
    partitions = _expected_partition_names()
    for parent in _PARENT_INDEXES:
        children = tuple(
            child
            for edge_parent, child, sequence in fingerprint.index_inheritance
            if edge_parent == parent and sequence == 1
        )
        assert len(children) == 16
        root = parent.split("_p_", maxsplit=1)[0]
        assert {index_tables[child] for child in children} == set(partitions[root])


def _assert_partition_tree(fingerprint: CatalogFingerprint) -> None:
    """Require the exact 48 named children and table inheritance edges."""
    expected_by_root = _expected_partition_names()
    expected_names = {child for children in expected_by_root.values() for child in children}
    expected_bounds: dict[str, str] = {}
    for root in ("ticks", "candles", "trades"):
        anchor_text = _ANCHOR.strftime("%Y-%m-%d %H:%M:%S+00")
        expected_bounds[f"{root}_legacy"] = f"FOR VALUES FROM (MINVALUE) TO ('{anchor_text}')"
        for offset in range(14):
            lower = _ANCHOR + timedelta(days=offset)
            upper = lower + timedelta(days=1)
            expected_bounds[f"{root}_d{lower:%Y%m%d}"] = (
                f"FOR VALUES FROM ('{lower:%Y-%m-%d %H:%M:%S+00}') "
                f"TO ('{upper:%Y-%m-%d %H:%M:%S+00}')"
            )
        expected_bounds[f"{root}_default"] = "DEFAULT"
    assert dict(fingerprint.partition_bounds) == expected_bounds
    assert set(expected_bounds) == expected_names
    assert set(fingerprint.table_inheritance) == {
        (parent, child, 1) for parent, children in expected_by_root.items() for child in children
    }


def _assert_leaf_local_contract(fingerprint: CatalogFingerprint) -> None:
    """Require every child PK and each candles/trades active partial index."""
    indexes = {
        (table, name): (definition, primary, unique, valid, predicate)
        for table, name, definition, primary, unique, valid, predicate in fingerprint.indexes
    }
    constraints = {
        (table, name): (constraint_type, definition, validated, deferrable, deferred)
        for (
            table,
            name,
            constraint_type,
            definition,
            validated,
            deferrable,
            deferred,
        ) in fingerprint.constraints
    }
    for root, children in _expected_partition_names().items():
        for child in children:
            primary_name = f"{root}_pkey" if child.endswith("_legacy") else f"{child}_pkey"
            primary = indexes[(child, primary_name)]
            assert primary == (
                f"CREATE UNIQUE INDEX {primary_name} ON public.{child} USING btree (id)",
                True,
                True,
                True,
                None,
            )
            constraint = constraints[(child, primary_name)]
            assert constraint == ("p", "PRIMARY KEY (id)", True, False, False)
            if root == "ticks":
                assert not any(
                    table == child and name.endswith("_public_id") for table, name in indexes
                )
                continue
            partial_name = (
                f"ix_{root}_public_id" if child.endswith("_legacy") else f"{child}_public_id"
            )
            partial = indexes[(child, partial_name)]
            partial_definition = (
                f"CREATE UNIQUE INDEX {partial_name} ON public.{child} USING btree "
                f"(public_id) WHERE {_ACTIVE_PREDICATE}"
            )
            assert partial == (
                partial_definition,
                False,
                True,
                True,
                _ACTIVE_PREDICATE,
            )


def _assert_partition_keys_not_null(fingerprint: CatalogFingerprint) -> None:
    """Require NOT NULL on every root and child partition-key column."""
    columns = {
        (table, name): nullable for table, _, name, _, nullable, _, _, _ in fingerprint.columns
    }
    topology_tables = {
        "ticks",
        "candles",
        "trades",
        *(name for name, _ in fingerprint.partition_bounds),
    }
    for table in topology_tables:
        root = table.split("_", maxsplit=1)[0]
        assert columns[(table, _ROOT_KEY_COLUMNS[root])] is False


def _assert_expected_topology(fingerprint: CatalogFingerprint) -> None:
    """Require every load-bearing fact in the specified daily topology."""
    _assert_root_contract(fingerprint)
    _assert_partition_tree(fingerprint)
    _assert_leaf_local_contract(fingerprint)
    _assert_partition_keys_not_null(fingerprint)
    assert fingerprint.sequences == tuple(
        (
            f"{table}_id_seq",
            "bigint",
            1,
            1,
            9223372036854775807,
            1,
            False,
            1,
            table,
            "id",
            "a",
        )
        for table in ("candles", "ticks", "trades")
    )


@pytest.mark.integration
@pytest.mark.timeout(300)
def test_0043_refuses_populated_and_malformed_manual_states(
    converged_postgresql_branches: PostgresBranches,
) -> None:
    """Prove four fail-closed paths leave revision 0042 and preserve topology.

    Args:
        converged_postgresql_branches: Independent scratch branch fixture.

    Given: The manual database first has a populated ordinary ``ticks`` table,
        then an adopted topology with an unexpected nested direct child, and
        finally adopted topologies with legacy-index storage drift and one
        required parent index renamed.
    When: Migration 0043 is attempted against each invalid state.
    Then: All four attempts refuse at revision 0042; ordinary roots remain
        untouched, the nested child is preserved through refusal and removed
        explicitly, and both index drifts remain until explicitly restored.
    """
    evidence = converged_postgresql_branches.refusals
    assert evidence.populated_revision == "0042"
    assert evidence.populated_root_kinds == (
        ("candles", "r", False),
        ("ticks", "r", False),
        ("trades", "r", False),
    )
    assert evidence.populated_legacy_relations == ()
    assert evidence.unexpected_partition_revision == "0042"
    assert evidence.unexpected_partition_error == "trades partition bounds or table edges differ"
    assert evidence.unexpected_partition == (
        "trades_unexpected_partitioned",
        "p",
        True,
        "trades",
        _UNEXPECTED_PARTITION_BOUND,
    )
    assert evidence.unexpected_partition_removed
    assert evidence.storage_drift_revision == "0042"
    assert evidence.storage_drift_error == "trades_legacy index manifest is not exact"
    assert evidence.storage_drift_reloptions == "fillfactor=70"
    assert evidence.storage_drift_restored
    assert evidence.malformed_revision == "0042"
    assert evidence.malformed_index_name == "trades_p_ix_ts_malformed"


@pytest.mark.integration
@pytest.mark.timeout(300)
def test_0043_manual_and_fresh_catalogs_converge_exactly(
    converged_postgresql_branches: PostgresBranches,
) -> None:
    """Prove public runtime adoption and independent Alembic SQL converge.

    Args:
        converged_postgresql_branches: Independent scratch branch fixture.

    Given: One database adopted through the public runtime path at revision
        0042 and one database built independently by Alembic from 0001.
    When: The manual database verifies/no-ops through 0043 and every requested
        logical catalog field is normalized.
    Then: Both revisions are 0043, both contain the exact expected topology,
        and their complete fingerprints are equal.
    """
    with converged_postgresql_branches.manual.connect() as manual_connection:
        manual_revision = alembic_revision(manual_connection)
        manual = catalog_fingerprint(manual_connection)
    with converged_postgresql_branches.fresh.connect() as fresh_connection:
        fresh_revision = alembic_revision(fresh_connection)
        fresh = catalog_fingerprint(fresh_connection)

    assert manual_revision == "0043"
    assert fresh_revision == "0043"
    _assert_expected_topology(manual)
    _assert_expected_topology(fresh)
    assert_catalogs_identical(manual, fresh)


@pytest.mark.integration
@pytest.mark.timeout(300)
@pytest.mark.parametrize("mutation", list(CatalogMutation), ids=lambda value: value.value)
@pytest.mark.parametrize("target", ("manual", "fresh"), ids=("manual-branch", "fresh-branch"))
def test_postgresql_comparator_rejects_each_transactional_catalog_mutation(
    converged_postgresql_branches: PostgresBranches,
    mutation: CatalogMutation,
    target: Literal["manual", "fresh"],
) -> None:
    """Prove the live catalog comparison fails for every required perturbation.

    Args:
        converged_postgresql_branches: Independent scratch branch fixture.
        mutation: Required catalog dimension to perturb.
        target: Branch to perturb while the other remains untouched.

    Given: Equal converged databases built through independent implementations.
    When: A transaction in each branch in turn changes a bound, local
        active-public-id predicate, index name, index edge, key nullability,
        U3 key, or sequence owner according to the parameter.
    Then: The comparator fails in the expected section for either orientation,
        the target transaction rolls back, and exact equality is restored
        before the next branch perturbation.
    """
    with converged_postgresql_branches.manual.connect() as manual_connection:
        manual = catalog_fingerprint(manual_connection)
    with converged_postgresql_branches.fresh.connect() as fresh_connection:
        fresh = catalog_fingerprint(fresh_connection)
    assert_catalogs_identical(manual, fresh)

    engine = (
        converged_postgresql_branches.manual
        if target == "manual"
        else converged_postgresql_branches.fresh
    )
    with engine.connect() as mutated_connection:
        transaction = mutated_connection.begin()
        try:
            apply_catalog_mutation(mutated_connection, mutation)
            mutated = catalog_fingerprint(mutated_connection)
            with pytest.raises(AssertionError, match=_MUTATION_SECTIONS[mutation]):
                if target == "manual":
                    assert_catalogs_identical(mutated, fresh)
                else:
                    assert_catalogs_identical(manual, mutated)
        finally:
            transaction.rollback()
    with engine.connect() as restored_connection:
        restored = catalog_fingerprint(restored_connection)
    if target == "manual":
        assert_catalogs_identical(restored, fresh)
    else:
        assert_catalogs_identical(manual, restored)
