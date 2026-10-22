"""Real SQLite migration witnesses for provider-bound delivery and config tombstones."""

from importlib import import_module

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text


@pytest.mark.parametrize("history", ["none", "configuration", "delivery"])
def test_delivery_migration_preserves_apns_and_refuses_lossy_downgrade(
    monkeypatch: pytest.MonkeyPatch,
    history: str,
) -> None:
    """Existing APNs rows retain defaults while any Android history prevents dropping its identity."""
    migration = import_module("snapper.data.migrations.versions.0054_android_push_delivery")
    engine = create_engine("sqlite://")
    try:
        with engine.begin() as connection:
            connection.execute(
                text("CREATE TABLE alert_deliveries (id INTEGER PRIMARY KEY, apns_id TEXT)")
            )
            connection.execute(text("INSERT INTO alert_deliveries VALUES (1, 'legacy-receipt')"))
            monkeypatch.setattr(migration, "op", Operations(MigrationContext.configure(connection)))
            migration.upgrade()
            assert connection.execute(
                text("SELECT provider, apns_id, provider_receipt FROM alert_deliveries")
            ).one() == ("apns", "legacy-receipt", None)
            if history == "configuration":
                connection.execute(
                    text(
                        "INSERT INTO android_push_configurations "
                        "(public_id, session_id, sequence_id, timestamp, known_to, configuration_id, public_options_json, state) "
                        "VALUES ('id', 'sid', 1, '2026-10-16', '9999-12-31 23:59:59.000000', 'config', '{}', 'retired')"
                    )
                )
            elif history == "delivery":
                connection.execute(
                    text(
                        "INSERT INTO alert_deliveries "
                        "(id, provider, configuration_id, binding_id, expires_at) "
                        "VALUES (2, 'fcm', 'config', 'binding', '2026-10-17')"
                    )
                )
            if history != "none":
                with pytest.raises(RuntimeError, match="lossless downgrade"):
                    migration.downgrade()
            else:
                migration.downgrade()
                assert "android_push_configurations" not in inspect(connection).get_table_names()
                assert connection.execute(text("SELECT * FROM alert_deliveries")).one() == (
                    1,
                    "legacy-receipt",
                )
    finally:
        engine.dispose()
