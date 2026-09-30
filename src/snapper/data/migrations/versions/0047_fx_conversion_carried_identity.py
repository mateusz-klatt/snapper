"""Give a carried election an identity: cover it with the resolved indexes.

Revision 0045 admitted the ``carried`` completeness state into the CHECK
constraints but left the two partial unique indexes matching only
``('complete', 'partial')``. A carried election could therefore never collide
with its own re-derivation, so nothing stopped a snapshotter tick from
inserting a fresh active row for an unchanged manifest, and the latest-visible
read that shares this predicate never returned one either.

The gap was invisible while carry-forward was unreachable. It becomes an
unbounded row leak and a permanently unpinned requirement the moment it fires,
so the index must learn the state before the election path does.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0047"
down_revision: str | None = "0046"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "fx_conversion_elections"
_SHARED_INDEX = "uq_fx_elections_shared_identity"
_INSTRUMENT_INDEX = "uq_fx_elections_instrument_identity"

_IDENTITY_COLUMNS = (
    "requirement_manifest_digest",
    "election_policy_version",
    "calculation_version",
    "scope_kind",
)
_PAIR_COLUMNS = (
    "source_currency",
    "target_currency",
    "unordered_pair",
    "resolved_knowledge_at",
)
_SHARED_COLUMNS = (*_IDENTITY_COLUMNS, *_PAIR_COLUMNS)
_INSTRUMENT_COLUMNS = (
    *_IDENTITY_COLUMNS,
    "consumer_instrument_public_id",
    *_PAIR_COLUMNS,
)

_RESOLVED_BEFORE = "completeness_state IN ('complete', 'partial')"
_RESOLVED_AFTER = "completeness_state IN ('complete', 'carried', 'partial')"

_KNOWN_TO_MAX_SQLITE = "'9999-12-31 23:59:59.000000'"
_KNOWN_TO_MAX_POSTGRESQL = "'9999-12-31T23:59:59+00:00'"


def _known_to_max() -> str:
    """Return this dialect's active-row sentinel as an SQL literal."""
    if op.get_bind().dialect.name == "sqlite":
        return _KNOWN_TO_MAX_SQLITE
    return _KNOWN_TO_MAX_POSTGRESQL


def _predicate(scope_kind: str, resolved: str) -> str:
    """Compose one partial-index predicate.

    Args:
        scope_kind: Election scope this index is restricted to.
        resolved: Completeness-state membership clause to apply.

    Returns:
        The complete partial-index ``WHERE`` predicate for this dialect.
    """
    return f"known_to = {_known_to_max()} AND scope_kind = '{scope_kind}' AND {resolved}"


def _create(name: str, columns: Sequence[str], scope_kind: str, resolved: str) -> None:
    """Create one partial unique index for the running dialect.

    Only the running dialect's ``*_where`` keyword takes effect, and
    :func:`_predicate` already resolved the sentinel for it, so passing the same
    text to both is correct rather than merely convenient.

    Args:
        name: Index name to create.
        columns: Ordered identity columns the index covers.
        scope_kind: Election scope this index is restricted to.
        resolved: Completeness-state membership clause to apply.
    """
    predicate = _predicate(scope_kind, resolved)
    op.create_index(
        name,
        _TABLE,
        list(columns),
        unique=True,
        sqlite_where=sa.text(predicate),
        postgresql_where=sa.text(predicate),
    )


def _rebuild(resolved: str) -> None:
    """Recreate both resolved-identity indexes under one membership clause.

    Args:
        resolved: Completeness-state membership clause to apply.
    """
    op.drop_index(_SHARED_INDEX, table_name=_TABLE)
    op.drop_index(_INSTRUMENT_INDEX, table_name=_TABLE)
    _create(_SHARED_INDEX, _SHARED_COLUMNS, "shared_pair", resolved)
    _create(_INSTRUMENT_INDEX, _INSTRUMENT_COLUMNS, "instrument_owned", resolved)


def upgrade() -> None:
    """Extend both resolved-identity indexes to cover carried elections."""
    _rebuild(_RESOLVED_AFTER)


def downgrade() -> None:
    """Restore the two-state indexes, refusing if carried elections exist.

    Raises:
        RuntimeError: If any active carried election relies on the index being
            narrowed, because dropping it from the uniqueness key would let
            duplicates accumulate unnoticed under the older predicate.
    """
    carried: int = (
        op.get_bind()
        .execute(
            sa.text(
                f"SELECT count(*) FROM {_TABLE} WHERE completeness_state = 'carried' "
                f"AND known_to = {_known_to_max()}"
            )
        )
        .scalar_one()
    )
    if carried:
        raise RuntimeError(
            f"refused: {carried} active carried election(s) rely on this index; "
            "narrowing it would drop their uniqueness guarantee"
        )
    _rebuild(_RESOLVED_BEFORE)
