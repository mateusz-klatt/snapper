"""Add isolated Android FID identities while preserving APNs registration rows."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0053"
down_revision: str | None = "0052"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "notification_devices"
_ID_COLUMNS = ("configuration_id", "binding_id", "installation_id")
_PROVIDER_CHECK = (
    "(provider = 'apns' AND device_token IS NOT NULL AND env IS NOT NULL) OR "
    "(provider = 'fcm' AND device_token IS NULL AND env IS NULL AND "
    "configuration_id IS NOT NULL AND binding_id IS NOT NULL AND "
    "installation_id IS NOT NULL AND firebase_installation_id IS NOT NULL AND package_name IS NOT NULL)"
)


def upgrade() -> None:
    """Extend the temporal inventory and fence concurrent installation/FID claims."""
    uuid_type = sa.String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql")
    with op.batch_alter_table(_TABLE) as batch:
        batch.add_column(
            sa.Column("provider", sa.String(10), nullable=False, server_default="apns")
        )
        for name in _ID_COLUMNS:
            batch.add_column(sa.Column(name, uuid_type, nullable=True))
        batch.add_column(sa.Column("firebase_installation_id", sa.String(256), nullable=True))
        batch.add_column(sa.Column("package_name", sa.String(255), nullable=True))
        batch.alter_column("device_token", existing_type=sa.String(200), nullable=True)
        batch.alter_column("env", existing_type=sa.String(10), nullable=True)
        batch.alter_column("app_version", existing_type=sa.String(20), type_=sa.String(32))
        batch.create_check_constraint("ck_notification_devices_provider_identity", _PROVIDER_CHECK)
    _indexes()
    op.create_index(
        "uq_android_binding_current",
        _TABLE,
        ["binding_id"],
        unique=True,
        sqlite_where=sa.text("provider = 'fcm' AND known_to = '9999-12-31 23:59:59.000000'"),
        postgresql_where=sa.text("provider = 'fcm' AND known_to = '9999-12-31 23:59:59+00'"),
    )


def _indexes() -> None:
    """Keep retired installation heads unique; FID uniqueness applies only when active."""
    for name, column, active in (
        ("uq_android_installation_current", "installation_id", ""),
        ("uq_android_fid_active", "firebase_installation_id", " AND token_status = 'active'"),
    ):
        op.create_index(
            name,
            _TABLE,
            ["configuration_id", column],
            unique=True,
            sqlite_where=sa.text(
                "provider = 'fcm' AND known_to = '9999-12-31 23:59:59.000000'" + active
            ),
            postgresql_where=sa.text(
                "provider = 'fcm' AND known_to = '9999-12-31 23:59:59+00'" + active
            ),
        )


def downgrade() -> None:
    """Refuse lossy downgrade once Android registration history has been written."""
    count = op.get_bind().scalar(
        sa.text("SELECT COUNT(*) FROM notification_devices WHERE provider = 'fcm'")
    )
    if count:
        raise RuntimeError("Android registration history prevents a lossless downgrade")
    op.drop_index("uq_android_binding_current", table_name=_TABLE)
    op.drop_index("uq_android_installation_current", table_name=_TABLE)
    op.drop_index("uq_android_fid_active", table_name=_TABLE)
    with op.batch_alter_table(_TABLE) as batch:
        batch.drop_constraint("ck_notification_devices_provider_identity", type_="check")
        for name in (*_ID_COLUMNS, "firebase_installation_id", "package_name", "provider"):
            batch.drop_column(name)
        batch.alter_column("device_token", existing_type=sa.String(200), nullable=False)
        batch.alter_column("env", existing_type=sa.String(10), nullable=False)
        batch.alter_column("app_version", existing_type=sa.String(32), type_=sa.String(20))
