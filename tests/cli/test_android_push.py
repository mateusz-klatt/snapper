"""CLI composition tests with synthetic transports and no network sockets."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

import snapper.application.notify.android_runtime as runtime_module
import snapper.cli.android_push as state_module
import snapper.cli.app as app_module
from snapper.application.notify.android_config import AndroidPushRuntime
from snapper.application.notify.fcm_client import FcmClient
from snapper.data.android_devices import AndroidConfigurationRevision


def test_operator_state_command_is_read_only_and_credential_free(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The operator gets only the opaque CAS revision and current configuration identity.

    Given: A configured operator-state repository boundary.
    When: The read-only CLI command is invoked.
    Then: Its JSON contains only the opaque revision and active configuration ID.
    """
    store = MagicMock()
    store.revision = AsyncMock(
        return_value=AndroidConfigurationRevision("revision", "configuration")
    )
    monkeypatch.setattr(state_module, "AndroidConfigurationStore", MagicMock(return_value=store))
    monkeypatch.setattr(state_module, "get_repository", MagicMock())
    monkeypatch.setattr(
        state_module, "get_bootstrap_settings", lambda: SimpleNamespace(db_url="synthetic")
    )
    result = CliRunner().invoke(app_module.app, ["android-push-state"])
    assert result.exit_code == 0
    assert json.loads(result.stdout) == {
        "revision": "revision",
        "active_configuration_id": "configuration",
    }
    store.revision.assert_awaited_once()


def test_fcm_only_notify_does_not_load_apns_and_closes_owned_sender(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An FCM-only process starts without APNs settings and owns sender cleanup.

    Given: An FCM-only runtime with synthetic transport collaborators.
    When: The notify command starts and stops.
    Then: APNs settings are never loaded and the owned sender closes once.
    """
    sender = MagicMock(spec=FcmClient)
    sender.aclose = AsyncMock()
    runtime = AndroidPushRuntime(sender_configured=True, sender=sender, apns_enabled=False)
    monkeypatch.setattr(runtime_module, "build_android_runtime", AsyncMock(return_value=runtime))
    bootstrap = SimpleNamespace(
        db_url="synthetic", zmq_broker_xsub="tcp://127.0.0.1:1", zmq_broker_xpub="tcp://127.0.0.1:2"
    )
    monkeypatch.setattr(app_module, "get_bootstrap_settings", lambda: bootstrap)
    monkeypatch.setattr(app_module, "get_repository", MagicMock())
    monkeypatch.setattr(app_module, "get_settings_service", AsyncMock(return_value=MagicMock()))
    apns = MagicMock(side_effect=AssertionError("APNs must not load"))
    monkeypatch.setattr(app_module, "load_apns_config", apns)
    sidecar = MagicMock(start=AsyncMock(), stop=AsyncMock())
    constructor = MagicMock(return_value=sidecar)
    monkeypatch.setattr(app_module, "NotifySidecar", constructor)
    monkeypatch.setattr(app_module, "ValidatedSubscriber", MagicMock())
    monkeypatch.setattr(app_module, "apply_hwm", MagicMock())
    context = MagicMock()
    monkeypatch.setattr(app_module.zmq.asyncio, "Context", lambda: context)
    result = CliRunner().invoke(app_module.app, ["notify"])
    assert result.exit_code == 0
    assert constructor.call_args.kwargs["apns"] is None
    sidecar.configure_android.assert_called_once_with(sender)
    sidecar.start.assert_awaited_once()
    sidecar.stop.assert_awaited_once()
    sender.aclose.assert_awaited_once()
    apns.assert_not_called()
    context.term.assert_called_once()
