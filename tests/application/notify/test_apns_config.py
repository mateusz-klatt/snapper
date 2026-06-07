"""Tests for ``snapper.application.notify.apns_config.load_apns_config``.

Covers the happy path + every failure mode: missing settings,
unknown environment value, malformed base64, decoded payload missing
the PKCS#8 PEM marker. The loader fails loud at sidecar startup
rather than producing cryptic APNs errors at first-send time, so
every rejection path is worth verifying.
"""

import base64
from unittest.mock import MagicMock

import pytest

from snapper.application.notify.apns_config import load_apns_config


def _valid_pem() -> str:
    """A minimal-but-structurally-valid PKCS#8 PEM string.

    The loader checks the ``BEGIN PRIVATE KEY`` marker, not the key
    content, so a short fake body is sufficient to exercise the
    happy path without embedding a real private key in the test tree.
    """
    return (
        "-----BEGIN PRIVATE KEY-----\n"
        "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg+fake+bytes\n"
        "-----END PRIVATE KEY-----\n"
    )


def _ready_settings(**overrides: object) -> MagicMock:
    """Return a mock ``SettingsService`` pre-seeded with every APNs key."""
    defaults: dict[str, object] = {
        "apns_team_id": "ABCDE12345",
        "apns_key_id": "2V2KBT3VMQ",
        "apns_bundle_id": "ie.klatt.snapper",
        "apns_topic": "ie.klatt.snapper",
        "apns_environment": "sandbox_and_production",
        "apns_private_key_p8_base64": base64.b64encode(_valid_pem().encode("ascii")).decode(
            "ascii"
        ),
    }
    defaults.update(overrides)
    service = MagicMock()
    service.get_setting = MagicMock(side_effect=lambda key, default=None: defaults.get(key))
    return service


class TestLoadApnsConfig:
    """Behaviour of the APNs config loader."""

    def test_happy_path_hydrates_all_fields(self) -> None:
        """Every field round-trips; PEM is decoded from base64."""
        config = load_apns_config(_ready_settings())

        assert config.team_id == "ABCDE12345"
        assert config.key_id == "2V2KBT3VMQ"
        assert config.bundle_id == "ie.klatt.snapper"
        assert config.topic == "ie.klatt.snapper"
        assert config.environment == "sandbox_and_production"
        assert "BEGIN PRIVATE KEY" in config.private_key_pem
        assert "END PRIVATE KEY" in config.private_key_pem

    def test_missing_keys_listed_in_error(self) -> None:
        """Every absent setting appears in the error message."""
        service = MagicMock()
        service.get_setting = MagicMock(return_value=None)

        with pytest.raises(ValueError) as exc:
            load_apns_config(service)

        msg = str(exc.value)
        for key in (
            "apns_team_id",
            "apns_key_id",
            "apns_bundle_id",
            "apns_topic",
            "apns_environment",
            "apns_private_key_p8_base64",
        ):
            assert key in msg

    def test_unknown_environment_rejected(self) -> None:
        """Only sandbox / production / sandbox_and_production are allowed."""
        with pytest.raises(ValueError) as exc:
            load_apns_config(_ready_settings(apns_environment="staging"))

        assert "apns_environment" in str(exc.value)

    def test_base64_decode_failure_reported_clearly(self) -> None:
        """Non-base64 payload raises with a reason string, not a raw traceback."""
        with pytest.raises(ValueError) as exc:
            load_apns_config(_ready_settings(apns_private_key_p8_base64="!!!-not-base64-!!!"))

        assert "base64" in str(exc.value).lower()

    def test_decoded_payload_must_contain_pem_marker(self) -> None:
        """A base64-valid payload that isn't a PEM is rejected."""
        payload = base64.b64encode(b"no-pem-marker-here").decode("ascii")

        with pytest.raises(ValueError) as exc:
            load_apns_config(_ready_settings(apns_private_key_p8_base64=payload))

        assert "BEGIN" in str(exc.value)
