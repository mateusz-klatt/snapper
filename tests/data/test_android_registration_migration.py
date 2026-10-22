"""Forward and lossless reverse migration witnesses for legacy APNs rows."""

from importlib import import_module

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text


@pytest.mark.parametrize("android_history", [False, True])
def test_upgrade_preserves_apns_and_downgrade_refuses_android_history(
    monkeypatch: pytest.MonkeyPatch,
    android_history: bool,
) -> None:
    """A provider migration neither invents APNs data nor deletes Android history."""
    migration = import_module("snapper.data.migrations.versions.0053_android_registration_bindings")
    engine = create_engine("sqlite://")
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "CREATE TABLE notification_devices (id INTEGER PRIMARY KEY, "
                    "device_token VARCHAR(200) NOT NULL, env VARCHAR(10) NOT NULL, "
                    "app_version VARCHAR(20), known_to TEXT NOT NULL, token_status TEXT NOT NULL)"
                )
            )
            connection.execute(
                text(
                    "INSERT INTO notification_devices VALUES "
                    "(1, 'legacy-apns-token', 'sandbox', '1.0', '9999-12-31 23:59:59.000000', 'active')"
                )
            )
            monkeypatch.setattr(migration, "op", Operations(MigrationContext.configure(connection)))
            migration.upgrade()
            row = connection.execute(
                text("SELECT provider, device_token, env FROM notification_devices")
            ).one()
            assert tuple(row) == ("apns", "legacy-apns-token", "sandbox")
            if android_history:
                connection.execute(
                    text(
                        "INSERT INTO notification_devices (id, provider, configuration_id, binding_id, "
                        "installation_id, firebase_installation_id, package_name, known_to, token_status) "
                        "VALUES (2, 'fcm', 'config', 'binding', 'install', 'FID', 'package', "
                        "'9999-12-31 23:59:59.000000', 'active')"
                    )
                )
                with pytest.raises(RuntimeError, match="lossless downgrade"):
                    migration.downgrade()
                assert connection.scalar(text("SELECT COUNT(*) FROM notification_devices")) == 2
            else:
                migration.downgrade()
                assert "provider" not in {
                    column["name"]
                    for column in inspect(connection).get_columns("notification_devices")
                }
    finally:
        engine.dispose()
