"""Unit tests for TunnelDescriptor + load_declared_tunnels.

The descriptor model is a pure Pydantic value type — tested by
validation cases. The loader is async + driven by a mocked
``SettingsService`` so the tests are hermetic (no DB, no encryption
setup needed).
"""

import dataclasses
import json
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from snapper.infrastructure.network.egress_tunnel_models import _MAX_TUNNEL_DECLARED
from snapper.infrastructure.network.egress_tunnel_models import LoadedTunnel
from snapper.infrastructure.network.egress_tunnel_models import LoadResult
from snapper.infrastructure.network.egress_tunnel_models import TunnelDescriptor
from snapper.infrastructure.network.egress_tunnel_models import TunnelLoadFailure
from snapper.infrastructure.network.egress_tunnel_models import get_tunnel_state_label
from snapper.infrastructure.network.egress_tunnel_models import load_declared_tunnels


def _valid_descriptor_dict() -> dict[str, Any]:
    """Helper — minimal descriptor that passes validation."""
    return {
        "interface": "wg-uk-1",
        "address": "10.64.12.34",
        "prefix_length": 32,
        "peer_pubkey": "A" * 44,
        "peer_endpoint": "vpn.example.com:51820",
        "socks5_listen_port": 1081,
        "priority": 10,
    }


def _make_settings_service(
    cache: dict[str, Any],
    secret_lookups: dict[str, Any] | None = None,
) -> MagicMock:
    """Helper — SettingsService mock with given cache + get_setting map."""
    service = MagicMock()
    service.get_all_settings = AsyncMock(return_value=cache)
    secrets = secret_lookups or {}

    def fake_get_setting(key: str, default: Any = None) -> Any:
        return secrets.get(key, default)

    service.get_setting = MagicMock(side_effect=fake_get_setting)
    return service


class TestTunnelDescriptor:
    """Tests for the TunnelDescriptor Pydantic model."""

    def test_minimal_descriptor_validates(self) -> None:
        """Spec — minimal valid descriptor constructs cleanly.

        Given the required-field set,
        When TunnelDescriptor is instantiated,
        Then no exception fires and defaults apply for allowed_ips/dns.
        """
        d = TunnelDescriptor(id="wg-de-1", **_valid_descriptor_dict())
        assert d.id == "wg-de-1"
        assert d.allowed_ips == ("0.0.0.0/0", "::/0")
        assert d.dns == ()

    def test_id_rejects_underscore(self) -> None:
        """Spec — id MUST NOT contain underscore (loader regex relies on this).

        Given id="bad_id",
        When TunnelDescriptor is instantiated,
        Then ValidationError fires (pattern violation).
        """
        descriptor_fields = _valid_descriptor_dict()
        with pytest.raises(ValidationError):
            TunnelDescriptor(id="bad_id", **descriptor_fields)

    def test_id_rejects_empty(self) -> None:
        """Spec — id min_length=1.

        Given id="",
        When the model is constructed,
        Then ValidationError fires.
        """
        descriptor_fields = _valid_descriptor_dict()
        with pytest.raises(ValidationError):
            TunnelDescriptor(id="", **descriptor_fields)

    def test_interface_must_start_with_wg_prefix(self) -> None:
        """Spec — interface name MUST start with ``wg-``.

        Given interface="eth0",
        When the model is constructed,
        Then ValidationError fires.
        """
        bad = _valid_descriptor_dict()
        bad["interface"] = "eth0"
        with pytest.raises(ValidationError):
            TunnelDescriptor(id="wg-de-1", **bad)

    def test_interface_max_length_15(self) -> None:
        """Spec — Linux IFNAMSIZ cap is 15 chars including the wg- prefix.

        Given interface="wg-very-long-name-x",
        When the model is constructed,
        Then ValidationError fires.
        """
        bad = _valid_descriptor_dict()
        bad["interface"] = "wg-very-long-name-x"
        with pytest.raises(ValidationError):
            TunnelDescriptor(id="wg-de-1", **bad)

    def test_rejects_invalid_ip_address(self) -> None:
        """Spec — address must parse as IPv4 or IPv6.

        Given address="not.an.ip",
        When the model is constructed,
        Then ValidationError fires with a clear message.
        """
        bad = _valid_descriptor_dict()
        bad["address"] = "not.an.ip"
        with pytest.raises(ValidationError, match="not a valid"):
            TunnelDescriptor(id="wg-de-1", **bad)

    def test_accepts_ipv6_address(self) -> None:
        """Spec — IPv6 address with prefix_length=128 constructs cleanly.

        Given address="fd00::1" + prefix_length=128,
        When the model is constructed,
        Then no exception fires.
        """
        d = _valid_descriptor_dict()
        d["address"] = "fd00::1"
        d["prefix_length"] = 128
        assert TunnelDescriptor(id="wg-de-1", **d).address == "fd00::1"

    def test_rejects_ipv4_with_prefix_above_32(self) -> None:
        """Spec — IPv4 address must have prefix_length <= 32.

        Given an IPv4 address with prefix_length=64,
        When the model is constructed,
        Then ValidationError fires.
        """
        bad = _valid_descriptor_dict()
        bad["prefix_length"] = 64
        with pytest.raises(ValidationError, match=">"):
            TunnelDescriptor(id="wg-de-1", **bad)

    def test_pubkey_must_be_44_chars(self) -> None:
        """Spec — peer_pubkey is exactly 44 base64 chars.

        Given peer_pubkey of 43 chars,
        When the model is constructed,
        Then ValidationError fires.
        """
        bad = _valid_descriptor_dict()
        bad["peer_pubkey"] = "A" * 43
        with pytest.raises(ValidationError):
            TunnelDescriptor(id="wg-de-1", **bad)

    def test_socks5_listen_port_rejects_privileged(self) -> None:
        """Spec — SOCKS5 listen port must be in [1024, 65535].

        Given socks5_listen_port=80 (privileged),
        When the model is constructed,
        Then ValidationError fires.
        """
        bad = _valid_descriptor_dict()
        bad["socks5_listen_port"] = 80
        with pytest.raises(ValidationError):
            TunnelDescriptor(id="wg-de-1", **bad)

    def test_socks5_listen_port_rejects_above_max(self) -> None:
        """Spec — SOCKS5 listen port must be ≤ 65535.

        Given socks5_listen_port=70000,
        When the model is constructed,
        Then ValidationError fires.
        """
        bad = _valid_descriptor_dict()
        bad["socks5_listen_port"] = 70000
        with pytest.raises(ValidationError):
            TunnelDescriptor(id="wg-de-1", **bad)

    def test_priority_rejects_negative(self) -> None:
        """Spec — priority must be ≥ 0.

        Given priority=-1,
        When the model is constructed,
        Then ValidationError fires.
        """
        bad = _valid_descriptor_dict()
        bad["priority"] = -1
        with pytest.raises(ValidationError):
            TunnelDescriptor(id="wg-de-1", **bad)

    def test_unknown_field_rejected(self) -> None:
        """Spec — extra fields are rejected (extra="forbid").

        Given a dict with an unknown ``wibble`` field,
        When TunnelDescriptor.model_validate runs,
        Then ValidationError fires.
        """
        payload_with_unknown_field = {"id": "wg-de-1", "wibble": 1, **_valid_descriptor_dict()}
        with pytest.raises(ValidationError):
            TunnelDescriptor.model_validate(payload_with_unknown_field)

    def test_allowed_ips_accepts_json_list(self) -> None:
        """Spec — JSON array → tuple coercion via the before-validator.

        Given allowed_ips=["10.0.0.0/8"] (the shape json.loads produces),
        When TunnelDescriptor.model_validate runs,
        Then the model is accepted AND allowed_ips is a tuple.

        Without the BeforeValidator, ``ConfigDict(strict=True)`` would
        reject the list and operator configs with explicit allowed_ips
        would fail to load.
        """
        d = _valid_descriptor_dict()
        d["allowed_ips"] = ["10.0.0.0/8", "2001:db8::/32"]
        descriptor = TunnelDescriptor(id="wg-de-1", **d)
        assert descriptor.allowed_ips == ("10.0.0.0/8", "2001:db8::/32")
        assert isinstance(descriptor.allowed_ips, tuple)

    def test_dns_accepts_json_list(self) -> None:
        """Spec — JSON array → tuple coercion also applies to dns.

        Given dns=["1.1.1.1", "1.0.0.1"],
        When TunnelDescriptor.model_validate runs,
        Then the model is accepted AND dns is a tuple.
        """
        d = _valid_descriptor_dict()
        d["dns"] = ["1.1.1.1", "1.0.0.1"]
        descriptor = TunnelDescriptor(id="wg-de-1", **d)
        assert descriptor.dns == ("1.1.1.1", "1.0.0.1")
        assert isinstance(descriptor.dns, tuple)

    def test_allowed_ips_rejects_non_string_element(self) -> None:
        """Spec — non-string list element falls through to Pydantic's rejector.

        Given allowed_ips=[1, 2] (ints not strings),
        When TunnelDescriptor.model_validate runs,
        Then ValidationError fires (the coercer leaves non-string
        sequences alone so the standard tuple-of-string validation
        kicks in).
        """
        d = _valid_descriptor_dict()
        d["allowed_ips"] = [1, 2]
        with pytest.raises(ValidationError):
            TunnelDescriptor(id="wg-de-1", **d)

    def test_allowed_ips_rejects_non_sequence(self) -> None:
        """Spec — non-sequence input falls through to Pydantic's rejector.

        Given allowed_ips="not-a-list",
        When TunnelDescriptor.model_validate runs,
        Then ValidationError fires.
        """
        d = _valid_descriptor_dict()
        d["allowed_ips"] = "not-a-list"
        with pytest.raises(ValidationError):
            TunnelDescriptor(id="wg-de-1", **d)

    def test_frozen_after_construction(self) -> None:
        """Spec — model is frozen.

        Given a constructed descriptor,
        When a field is reassigned,
        Then ValidationError fires.
        """
        d = TunnelDescriptor(id="wg-de-1", **_valid_descriptor_dict())
        with pytest.raises(ValidationError):
            d.id = "other"


@pytest.mark.asyncio
class TestLoadDeclaredTunnels:
    """End-to-end tests for load_declared_tunnels."""

    async def test_empty_cache_returns_empty_lists(self) -> None:
        """Spec — no tunnel settings → empty tunnels + failures lists.

        Given a SettingsService cache with no egress_tunnel_* keys,
        When load_declared_tunnels runs,
        Then both tunnels and failures lists are empty.
        """
        service = _make_settings_service(cache={})
        result = await load_declared_tunnels(service)
        assert result.tunnels == []
        assert result.failures == []

    async def test_loads_single_tunnel_happy_path(self) -> None:
        """Spec — one descriptor + matching keys → one LoadedTunnel.

        Given a cache with egress_tunnel_wg-de-1 (dict) AND
            egress_tunnel_wg-de-1_private_key (string),
        When load_declared_tunnels runs,
        Then result has one LoadedTunnel with the parsed descriptor +
            decrypted private key.
        """
        descriptor = _valid_descriptor_dict()
        cache = {"egress_tunnel_wg-de-1": descriptor}
        secrets = {"egress_tunnel_wg-de-1_private_key": "PRIVKEY"}
        service = _make_settings_service(cache=cache, secret_lookups=secrets)
        result = await load_declared_tunnels(service)
        assert len(result.tunnels) == 1
        assert result.tunnels[0].descriptor.id == "wg-de-1"
        assert result.tunnels[0].private_key == "PRIVKEY"
        assert result.tunnels[0].preshared_key is None
        assert result.failures == []

    async def test_loads_with_preshared_key(self) -> None:
        """Spec — optional preshared key populated when present.

        Given a cache with descriptor + private + preshared,
        When load_declared_tunnels runs,
        Then LoadedTunnel.preshared_key carries the value.
        """
        cache = {"egress_tunnel_wg-de-1": _valid_descriptor_dict()}
        secrets = {
            "egress_tunnel_wg-de-1_private_key": "PRIVKEY",
            "egress_tunnel_wg-de-1_preshared_key": "PSK",
        }
        service = _make_settings_service(cache=cache, secret_lookups=secrets)
        result = await load_declared_tunnels(service)
        assert result.tunnels[0].preshared_key == "PSK"

    async def test_preshared_key_empty_string_becomes_none(self) -> None:
        """Spec — empty preshared_key string is treated as no PSK.

        Given preshared_key="" (operator left the field blank),
        When load_declared_tunnels runs,
        Then LoadedTunnel.preshared_key is None (not "").
        """
        cache = {"egress_tunnel_wg-de-1": _valid_descriptor_dict()}
        secrets = {
            "egress_tunnel_wg-de-1_private_key": "PRIVKEY",
            "egress_tunnel_wg-de-1_preshared_key": "",
        }
        service = _make_settings_service(cache=cache, secret_lookups=secrets)
        result = await load_declared_tunnels(service)
        assert result.tunnels[0].preshared_key is None

    async def test_missing_private_key_records_failure(self) -> None:
        """Spec — descriptor present but no private_key → failure.

        Given the descriptor in cache but no _private_key setting,
        When load_declared_tunnels runs,
        Then result.tunnels is empty and failures has one
            entry with reason about the missing key.
        """
        cache = {"egress_tunnel_wg-de-1": _valid_descriptor_dict()}
        service = _make_settings_service(cache=cache, secret_lookups={})
        result = await load_declared_tunnels(service)
        assert result.tunnels == []
        assert len(result.failures) == 1
        assert "private_key" in result.failures[0].reason

    async def test_non_string_private_key_records_failure(self) -> None:
        """Spec — defensive: private_key not a string → failure.

        Given private_key returned as an int (cache corruption),
        When load_declared_tunnels runs,
        Then a failure is recorded.
        """
        cache = {"egress_tunnel_wg-de-1": _valid_descriptor_dict()}
        secrets = {"egress_tunnel_wg-de-1_private_key": 12345}
        service = _make_settings_service(cache=cache, secret_lookups=secrets)
        result = await load_declared_tunnels(service)
        assert result.tunnels == []
        assert len(result.failures) == 1

    async def test_non_string_preshared_records_failure(self) -> None:
        """Spec — defensive: preshared_key not a string → failure.

        Given preshared_key returned as an int (cache corruption),
        When load_declared_tunnels runs,
        Then a failure is recorded.
        """
        cache = {"egress_tunnel_wg-de-1": _valid_descriptor_dict()}
        secrets = {
            "egress_tunnel_wg-de-1_private_key": "PRIVKEY",
            "egress_tunnel_wg-de-1_preshared_key": 42,
        }
        service = _make_settings_service(cache=cache, secret_lookups=secrets)
        result = await load_declared_tunnels(service)
        assert result.tunnels == []
        assert len(result.failures) == 1
        assert "preshared_key" in result.failures[0].reason

    async def test_malformed_descriptor_records_failure(self) -> None:
        """Spec — descriptor missing required fields → failure.

        Given a descriptor missing peer_pubkey,
        When load_declared_tunnels runs,
        Then a failure is recorded and tunnels stays empty.
        """
        descriptor = _valid_descriptor_dict()
        del descriptor["peer_pubkey"]
        cache = {"egress_tunnel_wg-de-1": descriptor}
        secrets = {"egress_tunnel_wg-de-1_private_key": "PRIVKEY"}
        service = _make_settings_service(cache=cache, secret_lookups=secrets)
        result = await load_declared_tunnels(service)
        assert result.tunnels == []
        assert len(result.failures) == 1
        assert "validation" in result.failures[0].reason

    async def test_string_descriptor_payload_is_json_decoded(self) -> None:
        """Spec — descriptor stored as a JSON string also loads cleanly.

        Given the descriptor stored as a JSON string,
        When load_declared_tunnels runs,
        Then it is decoded + validated successfully.
        """
        descriptor_json = json.dumps(_valid_descriptor_dict())
        cache = {"egress_tunnel_wg-de-1": descriptor_json}
        secrets = {"egress_tunnel_wg-de-1_private_key": "PRIVKEY"}
        service = _make_settings_service(cache=cache, secret_lookups=secrets)
        result = await load_declared_tunnels(service)
        assert len(result.tunnels) == 1

    async def test_malformed_json_string_records_failure(self) -> None:
        """Spec — invalid JSON string → failure.

        Given a descriptor stored as a syntactically-invalid string,
        When load_declared_tunnels runs,
        Then a failure is recorded with the JSON error.
        """
        cache = {"egress_tunnel_wg-de-1": "{not json"}
        service = _make_settings_service(cache=cache, secret_lookups={})
        result = await load_declared_tunnels(service)
        assert result.tunnels == []
        assert len(result.failures) == 1
        assert "JSON" in result.failures[0].reason

    async def test_json_string_decoding_to_non_dict_records_failure(self) -> None:
        """Spec — JSON string that decodes to non-dict → failure.

        Given a JSON array instead of a dict,
        When load_declared_tunnels runs,
        Then a failure is recorded.
        """
        cache = {"egress_tunnel_wg-de-1": "[1, 2, 3]"}
        service = _make_settings_service(cache=cache, secret_lookups={})
        result = await load_declared_tunnels(service)
        assert result.tunnels == []
        assert len(result.failures) == 1
        assert "decode to a dict" in result.failures[0].reason

    async def test_unexpected_descriptor_type_records_failure(self) -> None:
        """Spec — non-dict, non-string descriptor value → failure.

        Given a descriptor stored as an integer (cache corruption),
        When load_declared_tunnels runs,
        Then a failure is recorded.
        """
        cache = {"egress_tunnel_wg-de-1": 42}
        service = _make_settings_service(cache=cache, secret_lookups={})
        result = await load_declared_tunnels(service)
        assert result.tunnels == []
        assert len(result.failures) == 1
        assert "unexpected type" in result.failures[0].reason

    async def test_string_descriptor_validation_failure_recorded(self) -> None:
        """Spec — JSON string with invalid descriptor fields → failure.

        Given a descriptor stored as a JSON string with an invalid field
            (interface without wg- prefix),
        When load_declared_tunnels runs,
        Then the Pydantic validation failure is captured.
        """
        bad = _valid_descriptor_dict()
        bad["interface"] = "eth0"
        cache = {"egress_tunnel_wg-de-1": json.dumps(bad)}
        service = _make_settings_service(cache=cache, secret_lookups={})
        result = await load_declared_tunnels(service)
        assert result.tunnels == []
        assert len(result.failures) == 1
        assert "validation" in result.failures[0].reason

    async def test_ignores_non_tunnel_keys(self) -> None:
        """Spec — other settings in the cache are ignored.

        Given a cache with egress_pool + egress_tunnel_wg-de-1 +
            unrelated settings,
        When load_declared_tunnels runs,
        Then only the matching tunnel is loaded.
        """
        descriptor = _valid_descriptor_dict()
        cache = {
            "egress_pool": {"enabled": False, "routes": []},
            "egress_tunnel_wg-de-1": descriptor,
            "polygon_api_key": "secret",
        }
        secrets = {"egress_tunnel_wg-de-1_private_key": "PRIVKEY"}
        service = _make_settings_service(cache=cache, secret_lookups=secrets)
        result = await load_declared_tunnels(service)
        assert len(result.tunnels) == 1
        assert result.tunnels[0].descriptor.id == "wg-de-1"

    async def test_does_not_match_private_key_setting_as_tunnel(self) -> None:
        """Spec — egress_tunnel_<id>_private_key MUST NOT be parsed as a tunnel.

        Given the cache contains BOTH egress_tunnel_wg-de-1 AND
            egress_tunnel_wg-de-1_private_key as keys,
        When load_declared_tunnels runs,
        Then only the descriptor key is matched as a tunnel.
            (Pinned by the regex constraint that the id segment may
            not contain underscores.)
        """
        descriptor = _valid_descriptor_dict()
        cache = {
            "egress_tunnel_wg-de-1": descriptor,
            "egress_tunnel_wg-de-1_private_key": "should-not-match",
        }
        secrets = {"egress_tunnel_wg-de-1_private_key": "PRIVKEY"}
        service = _make_settings_service(cache=cache, secret_lookups=secrets)
        result = await load_declared_tunnels(service)
        assert len(result.tunnels) == 1
        assert result.tunnels[0].descriptor.id == "wg-de-1"

    async def test_tunnels_returned_in_sorted_id_order(self) -> None:
        """Spec — tunnels are returned sorted by id for deterministic index.

        Given three tunnels with ids that would sort to wg-uk-1,
            wg-uk-2, wg-uk-3 (lexical),
        When load_declared_tunnels runs,
        Then the returned tunnels list is in that order.
        """
        cache: dict[str, Any] = {}
        secrets: dict[str, Any] = {}
        for tid in ("wg-uk-3", "wg-uk-1", "wg-uk-2"):
            d = _valid_descriptor_dict()
            d["interface"] = f"wg-{tid[-1]}-x"
            d["socks5_listen_port"] = 1080 + int(tid[-1])
            cache[f"egress_tunnel_{tid}"] = d
            secrets[f"egress_tunnel_{tid}_private_key"] = "K"
        service = _make_settings_service(cache=cache, secret_lookups=secrets)
        result = await load_declared_tunnels(service)
        assert [t.descriptor.id for t in result.tunnels] == [
            "wg-uk-1",
            "wg-uk-2",
            "wg-uk-3",
        ]

    async def test_over_limit_tunnels_recorded_as_failures(self) -> None:
        """Spec — tunnels beyond _MAX_TUNNEL_DECLARED → failures, not LoadedTunnels.

        Given more than _MAX_TUNNEL_DECLARED tunnel descriptors,
        When load_declared_tunnels runs,
        Then the extras are recorded as TunnelLoadFailure entries
            (the orchestrator can surface them on /tunnels rather
            than silently dropping them).
        """
        cache: dict[str, Any] = {}
        secrets: dict[str, Any] = {}
        for i in range(_MAX_TUNNEL_DECLARED + 2):
            tid = f"t{i:04d}"
            d = _valid_descriptor_dict()
            d["interface"] = f"wg-z{i % 9}"
            d["socks5_listen_port"] = 30000 + i
            cache[f"egress_tunnel_{tid}"] = d
            secrets[f"egress_tunnel_{tid}_private_key"] = "K"
        service = _make_settings_service(cache=cache, secret_lookups=secrets)
        result = await load_declared_tunnels(service)
        assert len(result.tunnels) == _MAX_TUNNEL_DECLARED
        assert len(result.failures) == 2
        for failure in result.failures:
            assert "exceeds maximum" in failure.reason


class TestGetTunnelStateLabel:
    """Trivial helper test for the /tunnels endpoint contract."""

    def test_up_when_no_failure(self) -> None:
        """Spec — None → "up"."""
        assert get_tunnel_state_label(None) == "up"

    def test_failed_when_failure_present(self) -> None:
        """Spec — TunnelLoadFailure → "failed"."""
        f = TunnelLoadFailure(tunnel_id="x", reason="any")
        assert get_tunnel_state_label(f) == "failed"


class TestLoadResultDataclass:
    """Verify the dataclasses are immutable so callers cannot mutate."""

    def test_loaded_tunnel_is_frozen(self) -> None:
        """Spec — LoadedTunnel is a frozen dataclass.

        Given a LoadedTunnel instance,
        When a field is reassigned,
        Then dataclasses.FrozenInstanceError fires.
        """
        d = TunnelDescriptor(id="wg-de-1", **_valid_descriptor_dict())
        loaded = LoadedTunnel(descriptor=d, private_key="K", preshared_key=None)
        with pytest.raises(dataclasses.FrozenInstanceError):
            loaded.private_key = "Z"

    def test_load_result_holds_tunnels_and_failures(self) -> None:
        """Spec — LoadResult exposes both lists.

        Given a LoadResult constructed manually,
        When attributes are accessed,
        Then both lists are present.
        """
        r = LoadResult(tunnels=[], failures=[])
        assert r.tunnels == []
        assert r.failures == []
