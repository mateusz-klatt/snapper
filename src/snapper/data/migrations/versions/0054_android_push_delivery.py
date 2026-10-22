"""Persist immutable Android configuration activation and bound FCM outbox hints."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0054"
down_revision: str | None = "0053"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Store configuration tombstones and immutable provider-specific delivery identity."""
    uuid_type = sa.String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql")
    op.create_table(
        "android_push_configurations",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("public_id", uuid_type, nullable=False),
        sa.Column("session_id", uuid_type, nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("slot", sa.String(16), nullable=False, server_default="android"),
        sa.Column("configuration_id", uuid_type, nullable=True),
        sa.Column("public_options_json", sa.Text(), nullable=False),
        sa.Column("state", sa.String(16), nullable=False),
        sa.CheckConstraint(
            "(state = 'disabled' AND configuration_id IS NULL) OR (state IN ('active', 'retired') AND configuration_id IS NOT NULL)",
            name="ck_android_push_configuration_identity",
        ),
        sa.CheckConstraint(
            "state IN ('active', 'retired', 'disabled')", name="ck_android_push_configuration_state"
        ),
    )
    for name, column, active in (
        ("uq_android_push_configuration_current", "configuration_id", ""),
        ("uq_android_push_configuration_active", "slot", " AND state IN ('active', 'disabled')"),
    ):
        op.create_index(
            name,
            "android_push_configurations",
            [column],
            unique=True,
            sqlite_where=sa.text("known_to = '9999-12-31 23:59:59.000000'" + active),
            postgresql_where=sa.text("known_to = '9999-12-31 23:59:59+00'" + active),
        )
    op.add_column(
        "alert_deliveries",
        sa.Column("provider", sa.String(10), nullable=False, server_default="apns"),
    )
    for name in ("configuration_id", "binding_id"):
        op.add_column("alert_deliveries", sa.Column(name, uuid_type, nullable=True))
    op.add_column(
        "alert_deliveries", sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column("alert_deliveries", sa.Column("provider_receipt", sa.String(512), nullable=True))
    with op.batch_alter_table("alert_deliveries") as batch:
        batch.create_check_constraint(
            "ck_alert_delivery_provider_identity",
            "provider = 'apns' OR (provider = 'fcm' AND configuration_id IS NOT NULL AND binding_id IS NOT NULL AND expires_at IS NOT NULL)",
        )


def downgrade() -> None:
    """Refuse erasing configuration retirement or accepted provider-delivery history."""
    connection = op.get_bind()
    configurations = connection.scalar(sa.text("SELECT COUNT(*) FROM android_push_configurations"))
    deliveries = connection.scalar(
        sa.text("SELECT COUNT(*) FROM alert_deliveries WHERE provider = 'fcm'")
    )
    if configurations or deliveries:
        raise RuntimeError("Android push history prevents a lossless downgrade")
    with op.batch_alter_table("alert_deliveries") as batch:
        batch.drop_constraint("ck_alert_delivery_provider_identity", type_="check")
    for name in ("provider_receipt", "expires_at", "binding_id", "configuration_id", "provider"):
        op.drop_column("alert_deliveries", name)
    op.drop_table("android_push_configurations")
