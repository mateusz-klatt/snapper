"""Physical 0064 scenarios; placeholder parents never certify authenticated transport.

This candidate exercises actual SQL constraints only. Its empty canonical
parent/payload strings are physical fixtures, not valid ownership payloads.
Closed producer, actual commit, privacy and typed reader scenarios remain a
separate obligation after the transport owner finalizes its receive ticket.
"""

from collections.abc import Iterator
from importlib import import_module
from pathlib import Path
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.exc import IntegrityError

from tests.data.test_migration_0063_native_account_authorities import _retain_graph
from tests.data.test_migration_0063_native_account_authorities import _values as _authority_values

_MIGRATION = import_module("snapper.data.migrations.versions.0064_private_source_ownership")
_PARENTS = tuple(
    import_module("snapper.data.migrations.versions." + name)
    for name in (
        "0058_contract_accounting_evidence",
        "0061_native_account_read_lifecycle",
        "0062_native_account_identity_proofs",
    )
)
_TABLE = "private_source_ownership_receipts"
_BASE_UUID = "00000000-0000-7000-8000-000000000001"
_NEW_UUID = "00000000-0000-7000-8000-000000000011"
_NEW_RAW = "00000000-0000-7000-8000-000000000017"
_ABSENT = "00000000-0000-7000-8000-000000000099"
_T0 = "2026-10-23 00:00:00.000000"
_T1 = "2026-10-23 00:00:01.000000"
_T2 = "2026-10-23 00:00:02.000000"
_T3 = "2026-10-23 00:00:03.000000"
type _Value = str | int | None


def _values() -> dict[str, _Value]:
    """Keep every safe indexed field explicit without inventing an authenticated payload."""
    return {
        "public_id": _BASE_UUID,
        "session_id": _BASE_UUID,
        "sequence_id": 1,
        "timestamp": _T2,
        "known_to": "9999-12-31 23:59:59.000000",
        "wallet_public_id": _BASE_UUID,
        "exchange": "kraken_futures",
        "mode": "live",
        "environment": "production",
        "margin_account": None,
        "account_family_digest": "d" * 64,
        "client_selection_digest": "e" * 64,
        "credential_public_id": "00000000-0000-7000-8000-000000000002",
        "credential_timestamp": _T0,
        "public_api_origin": "https://futures.kraken.com",
        "raw_receipt_public_id": "00000000-0000-7000-8000-000000000007",
        "raw_content_digest": "a" * 64,
        "target_json_pointer": "/fills/0",
        "frame_basis_digest": "f" * 64,
        "key_proof_public_id": "00000000-0000-7000-8000-000000000004",
        "key_proof_content_digest": "8" * 64,
        "key_proof_recorded_at": _T0,
        "key_owner_root_public_id": "00000000-0000-7000-8000-000000000004",
        "key_owner_root_content_digest": "8" * 64,
        "client_capture_public_id": "00000000-0000-7000-8000-000000000021",
        "connection_epoch_public_id": "00000000-0000-7000-8000-000000000022",
        "subscription_dispatch_public_id": "00000000-0000-7000-8000-000000000023",
        "frame_receive_public_id": "00000000-0000-7000-8000-000000000024",
        "subscription_feed": "fills",
        "raw_feed": "fills_snapshot",
        "auth_encoded_at": _T0,
        "ack_observed_at": _T0,
        "frame_observed_at": _T1,
        "raw_observed_at": _T1,
        "raw_recorded_at": _T1,
        "family": "kraken_futures_private_ws",
        "schema_version": 1,
        "source_policy": "kraken_futures_closed_private_transport_v1",
        "binding_kind": "actual_receive",
        "origin_sidecar_public_id": None,
        "origin_sidecar_content_digest": None,
        "content_digest": "b" * 64,
        "canonical_payload": "{}",
    }


def _direct(connection: sa.Connection, table: str, values: dict[str, _Value]) -> None:
    """Insert only the two statically allowed physical fixture relations."""
    assert table in {_TABLE, "accounting_raw_receipts"}
    columns = ", ".join(values)
    parameters = ", ".join(f":{name}" for name in values)
    connection.execute(sa.text(f"INSERT INTO {table} ({columns}) VALUES ({parameters})"), values)


def _parents(connection: sa.Connection, values: dict[str, _Value]) -> None:
    """Retain real parent rows while preserving each raw receipt's actual fixture clocks."""
    exists = connection.scalar(
        sa.text("SELECT COUNT(*) FROM accounting_raw_receipts WHERE public_id=:id"),
        {"id": values["raw_receipt_public_id"]},
    )
    if not exists:
        _direct(
            connection,
            "accounting_raw_receipts",
            {
                "public_id": values["raw_receipt_public_id"],
                "claimed_public_id": values["raw_receipt_public_id"],
                "session_id": _BASE_UUID,
                "sequence_id": 1,
                "timestamp": values["raw_recorded_at"],
                "known_to": "9999-12-31 23:59:59.000000",
                "wallet_public_id": values["wallet_public_id"],
                "exchange": "kraken_futures",
                "mode": "live",
                "environment": values["environment"],
                "margin_account": None,
                "scope_digest": "c" * 64,
                "route_authority_public_id": None,
                "kind": "execution",
                "observed_at": values["raw_observed_at"],
                "content_digest": values["raw_content_digest"],
                "canonical_payload": "{}",
            },
        )
    authority = {
        **_authority_values(),
        "wallet_public_id": values["wallet_public_id"],
        "environment": values["environment"],
        "raw_receipt_public_id": values["raw_receipt_public_id"],
        "owner_proof_public_id": values["key_proof_public_id"],
    }
    _retain_graph(connection, authority)


def _insert(connection: sa.Connection, values: dict[str, _Value]) -> None:
    """Preserve actual FK parents before exercising the exact prospective sidecar declaration."""
    _parents(connection, values)
    _direct(connection, _TABLE, values)


def _invoke(connection: sa.Connection, direction: str) -> None:
    """Run the exact public candidate migration under caller-controlled completion."""
    with patch.object(_MIGRATION, "op", Operations(MigrationContext.configure(connection))):
        getattr(_MIGRATION, direction)()


@pytest.fixture
def database(tmp_path: Path) -> Iterator[sa.Engine]:
    """Construct actual parent migrations and the candidate on a unique private SQLite file."""
    engine = sa.create_engine(f"sqlite:///{tmp_path / '0064.db'}")
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql("PRAGMA foreign_keys=ON")
            connection.exec_driver_sql("PRAGMA recursive_triggers=ON")
            for migration in _PARENTS:
                with patch.object(
                    migration, "op", Operations(MigrationContext.configure(connection))
                ):
                    migration.upgrade()
            _invoke(connection, "upgrade")
        yield engine
    finally:
        engine.dispose()


def test_original_receipts_may_share_receive_identity_and_selector(database: sa.Engine) -> None:
    """Transport identity does not prohibit separately held raw observations of the same frame child.

    Given: One retained actual-receive sidecar and another legitimate raw UUID,
    When: Both refer to the same original receive UUID and selected child,
    Then: Both physical sidecars remain without asserting two economic postings.
    """
    with database.begin() as connection:
        _insert(connection, _values())
        _insert(
            connection, {**_values(), "public_id": _NEW_UUID, "raw_receipt_public_id": _NEW_RAW}
        )
        assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {_TABLE}")) == 2


def test_direct_recovery_keeps_original_clocks_and_later_retention(database: sa.Engine) -> None:
    """A direct projection preserves the original receive identity and honest later row clock.

    Given: An actual original sidecar and a separately retained recovered child,
    When: The projection references the original sidecar UUID/digest and copies its source clocks,
    Then: Both rows remain with distinct raw UUIDs and a later recovery timestamp.
    """
    with database.begin() as connection:
        _insert(connection, _values())
        _insert(
            connection,
            {
                **_values(),
                "public_id": _NEW_UUID,
                "raw_receipt_public_id": _NEW_RAW,
                "binding_kind": "recovery_projection",
                "origin_sidecar_public_id": _BASE_UUID,
                "origin_sidecar_content_digest": "b" * 64,
                "target_json_pointer": "/fills/1",
                "raw_recorded_at": _T3,
                "timestamp": _T3,
            },
        )
        assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {_TABLE}")) == 2


@pytest.mark.parametrize(
    "field",
    (
        "raw_receipt_public_id",
        "key_proof_public_id",
        "key_owner_root_public_id",
        "origin_sidecar_public_id",
    ),
)
def test_missing_actual_source_or_origin_is_physically_refused(
    database: sa.Engine, field: str
) -> None:
    """An absent actual parent cannot be substituted by a plausible public UUID.

    Given: All ordinary real parent rows exist,
    When: One required source pointer names a missing row,
    Then: Immediate foreign-key enforcement rejects the sidecar.
    """
    with database.begin() as connection:
        values = _values()
        _parents(connection, values)
        if field == "origin_sidecar_public_id":
            values.update(
                binding_kind="recovery_projection", origin_sidecar_content_digest="b" * 64
            )
        values[field] = _ABSENT
        with pytest.raises(IntegrityError, match="FOREIGN KEY"):
            _direct(connection, _TABLE, values)


@pytest.mark.parametrize(
    "changes",
    (
        {"key_proof_recorded_at": _T1},
        {"auth_encoded_at": _T1},
        {"ack_observed_at": _T2},
        {"frame_observed_at": _T2},
        {"raw_observed_at": _T2},
        {"raw_recorded_at": _T3},
        {"credential_timestamp": _T1},
    ),
)
def test_original_clock_order_cannot_be_reconstructed_from_later_knowledge(
    database: sa.Engine, changes: dict[str, _Value]
) -> None:
    """Later key proofs and backdated retention cannot satisfy the original transport chronology.

    Given: One otherwise valid physical source graph,
    When: One indexed clock breaks the original proof/auth/ACK/frame/raw/recording order,
    Then: The physical chronology check refuses insertion.
    """
    with database.begin() as connection:
        _parents(connection, _values())
        with pytest.raises(IntegrityError):
            _direct(connection, _TABLE, {**_values(), **changes})


@pytest.mark.parametrize(
    "changes",
    (
        {"schema_version": 2},
        {"family": "other"},
        {"source_policy": "owned_decode_only"},
        {"binding_kind": "unknown"},
        {"margin_account": "flex"},
        {"exchange": "kraken"},
        {"mode": "paper"},
        {"environment": "paper"},
        {"public_api_origin": "https://demo-futures.kraken.com"},
        {"subscription_feed": "account_log"},
        {"raw_feed": "balances"},
        {"target_json_pointer": "/fills/01"},
        {"target_json_pointer": "/fills/-1"},
        {"target_json_pointer": "/fills/0/qty"},
        {"target_json_pointer": "/fills/"},
        {"target_json_pointer": "/fills/١"},
        {"target_json_pointer": "/logs/0"},
        {"account_family_digest": "short"},
        {"frame_basis_digest": "short"},
    ),
)
def test_unproven_family_feed_or_child_identity_is_physically_refused(
    database: sa.Engine, changes: dict[str, _Value]
) -> None:
    """A decoder-only label, wrong route or ambiguous selector cannot become a physical ownership row.

    Given: Valid actual SQL parents without a trusted producer claim,
    When: The candidate indexed family, feed, origin, digest or child pointer is invalid,
    Then: The physical constraint refuses the row without changing raw retention.
    """
    with database.begin() as connection:
        _parents(connection, _values())
        with pytest.raises(IntegrityError):
            _direct(connection, _TABLE, {**_values(), **changes})
        assert connection.scalar(sa.text("SELECT COUNT(*) FROM accounting_raw_receipts")) == 1


@pytest.mark.parametrize(
    "changes",
    (
        {"origin_sidecar_public_id": _ABSENT},
        {"origin_sidecar_content_digest": "b" * 64},
        {"binding_kind": "recovery_projection"},
        {
            "binding_kind": "recovery_projection",
            "origin_sidecar_public_id": _BASE_UUID,
            "origin_sidecar_content_digest": "b" * 64,
        },
    ),
)
def test_origin_tuple_is_complete_kind_correct_and_never_self_referencing(
    database: sa.Engine, changes: dict[str, _Value]
) -> None:
    """Direct origins cannot be partial, attached to original receives or point to the new row itself.

    Given: A valid prospective sidecar and actual raw/key parents,
    When: Its binding discriminator and origin tuple disagree or the origin is itself,
    Then: A physical check refuses that source history.
    """
    with database.begin() as connection:
        _parents(connection, _values())
        with pytest.raises(IntegrityError):
            _direct(connection, _TABLE, {**_values(), **changes})


@pytest.mark.parametrize("operation", ("update", "delete", "replace"))
def test_committed_transport_history_cannot_be_rewritten(
    database: sa.Engine, operation: str
) -> None:
    """Actual append-only guards preserve an original transport event and its clocks.

    Given: One retained original receive sidecar,
    When: Direct SQL changes, deletes or replaces its public identity,
    Then: The physical guard preserves the original row.
    """
    with database.begin() as connection:
        _insert(connection, _values())
        with pytest.raises(IntegrityError, match="append-only"):
            if operation == "replace":
                values = _values()
                connection.execute(
                    sa.text(
                        f"INSERT OR REPLACE INTO {_TABLE} ({', '.join(values)}) VALUES ({', '.join(':' + name for name in values)})"
                    ),
                    values,
                )
            else:
                statement = (
                    f"UPDATE {_TABLE} SET sequence_id=2"
                    if operation == "update"
                    else f"DELETE FROM {_TABLE}"
                )
                connection.execute(sa.text(statement))
        assert connection.scalar(sa.text(f"SELECT COUNT(*) FROM {_TABLE}")) == 1


def test_retained_original_or_recovery_refuses_lossy_downgrade(database: sa.Engine) -> None:
    """Retained transport facts prevent an empty-verdict downgrade from discarding them.

    Given: One actual retained original receive and its physical mutation guards,
    When: The exact prospective downgrade evaluates its caller-owned fence,
    Then: It refuses and preserves the table, source row and update/delete guards.
    """
    with database.begin() as connection:
        _insert(connection, _values())
        with pytest.raises(RuntimeError, match="prevents a lossless downgrade"):
            _invoke(connection, "downgrade")
        with pytest.raises(IntegrityError, match="append-only"):
            connection.execute(sa.text(f"DELETE FROM {_TABLE}"))
