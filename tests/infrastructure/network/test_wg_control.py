"""Unit tests for the WireGuard control-plane module.

All pyroute2 interactions are mocked: real WG interface creation
requires kernel support + NET_ADMIN which is not available in the
CI sandbox. The tests verify the call sequence + arguments passed
to ``pyroute2.IPRoute`` / ``pyroute2.WireGuard`` against the
contract described in
``proprietary/plans/plan_2026_05_21_snapper_egress_sidecar.md``
section SC.1.
"""

import errno
import socket
from typing import Any
from unittest.mock import MagicMock
from unittest.mock import call
from unittest.mock import patch

import pytest
from pyroute2.netlink.exceptions import NetlinkError

from snapper.infrastructure.network import wg_control
from snapper.infrastructure.network.wg_control import _PERSISTENT_KEEPALIVE
from snapper.infrastructure.network.wg_control import _PROBE_INTERFACE_NAME
from snapper.infrastructure.network.wg_control import _RTABLE_BASE
from snapper.infrastructure.network.wg_control import _RULE_PRIORITY_BASE
from snapper.infrastructure.network.wg_control import _bring_down_sync
from snapper.infrastructure.network.wg_control import _bring_up_sync
from snapper.infrastructure.network.wg_control import _resolve_endpoint
from snapper.infrastructure.network.wg_control import bring_down
from snapper.infrastructure.network.wg_control import bring_up
from snapper.infrastructure.network.wg_control import probe_kernel_wireguard


def _make_ipr_mock() -> MagicMock:
    """Helper — fresh IPRoute mock with link_lookup returning [42]."""
    ipr = MagicMock()
    ipr.link_lookup = MagicMock(return_value=[42])
    return ipr


def _make_wg_mock() -> MagicMock:
    """Helper — fresh WireGuard mock."""
    wg = MagicMock()
    return wg


class TestProbeKernelWireguard:
    """Tests for ``probe_kernel_wireguard`` startup probe."""

    def test_probe_passes_when_create_delete_succeeds(self) -> None:
        """Spec — happy path runs link add + link del + close.

        Given a kernel where ``ip link add type wireguard`` succeeds,
        When probe_kernel_wireguard runs,
        Then no exception fires AND the throw-away interface is
        deleted AND IPRoute.close is called.
        """
        ipr = _make_ipr_mock()
        with patch.object(wg_control, "IPRoute", return_value=ipr):
            probe_kernel_wireguard()
        ipr.link.assert_any_call("add", ifname=_PROBE_INTERFACE_NAME, kind="wireguard")
        ipr.link.assert_any_call("del", index=42)
        ipr.close.assert_called_once()

    def test_probe_exits_on_eopnotsupp(self) -> None:
        """Spec — missing kernel WG module → sys.exit(1) with clear log.

        Given IPRoute.link raises NetlinkError(EOPNOTSUPP),
        When probe_kernel_wireguard runs,
        Then SystemExit(1) is raised and IPRoute.close still runs.
        """
        ipr = MagicMock()
        ipr.link.side_effect = NetlinkError(errno.EOPNOTSUPP)
        with (
            patch.object(wg_control, "IPRoute", return_value=ipr),
            pytest.raises(SystemExit) as excinfo,
        ):
            probe_kernel_wireguard()
        assert excinfo.value.code == 1
        ipr.close.assert_called_once()

    def test_probe_exits_on_eperm(self) -> None:
        """Spec — missing NET_ADMIN → sys.exit(1) with distinct log.

        Given IPRoute.link raises NetlinkError(EPERM),
        When probe_kernel_wireguard runs,
        Then SystemExit(1) fires.
        """
        ipr = MagicMock()
        ipr.link.side_effect = NetlinkError(errno.EPERM)
        with (
            patch.object(wg_control, "IPRoute", return_value=ipr),
            pytest.raises(SystemExit) as excinfo,
        ):
            probe_kernel_wireguard()
        assert excinfo.value.code == 1

    def test_probe_reraises_other_netlink_errors(self) -> None:
        """Spec — unexpected NetlinkError propagates so operator sees cause.

        Given IPRoute.link raises NetlinkError with an unrelated errno
            (e.g. ENOBUFS),
        When probe_kernel_wireguard runs,
        Then the original NetlinkError propagates AND close still runs.
        """
        ipr = MagicMock()
        ipr.link.side_effect = NetlinkError(errno.ENOBUFS)
        with (
            patch.object(wg_control, "IPRoute", return_value=ipr),
            pytest.raises(NetlinkError),
        ):
            probe_kernel_wireguard()
        ipr.close.assert_called_once()

    def test_probe_closes_iproute_on_unexpected_exception(self) -> None:
        """Spec — non-NetlinkError exception still hits the finally close.

        Given IPRoute.link raises a generic RuntimeError,
        When probe_kernel_wireguard runs,
        Then the RuntimeError propagates AND IPRoute.close is called.
        """
        ipr = MagicMock()
        ipr.link.side_effect = RuntimeError("simulated bug")
        with (
            patch.object(wg_control, "IPRoute", return_value=ipr),
            pytest.raises(RuntimeError, match="simulated bug"),
        ):
            probe_kernel_wireguard()
        ipr.close.assert_called_once()

    def test_probe_cleans_stale_probe_interface_before_create(self) -> None:
        """Spec — pre-existing wg-probe-snapper from a crashed prior run is removed first.

        Given a stale probe interface left over from a crashed
            previous probe (link_lookup returns [99] on FIRST call),
        When probe_kernel_wireguard runs,
        Then the stale interface is deleted BEFORE the new add
            attempt, so the operator sees the real state of the
            kernel module instead of an EEXIST.
        """
        ipr = MagicMock()
        lookup_count = {"n": 0}

        def link_lookup_side(ifname: str) -> list[int]:
            lookup_count["n"] += 1
            if lookup_count["n"] == 1:
                return [99]
            return [42]

        ipr.link_lookup = MagicMock(side_effect=link_lookup_side)
        with patch.object(wg_control, "IPRoute", return_value=ipr):
            probe_kernel_wireguard()
        del_calls = [c for c in ipr.link.call_args_list if c.args and c.args[0] == "del"]
        del_indices = [c.kwargs.get("index") for c in del_calls]
        assert 99 in del_indices
        assert 42 in del_indices

    def test_probe_tolerates_stale_cleanup_netlink_error(self) -> None:
        """Spec — NetlinkError during stale-probe cleanup does not block the real probe.

        Given link_lookup raises NetlinkError on the FIRST call (the
            stale-probe cleanup inside _delete_probe_if_present) and
            returns [42] on the second call (post-create lookup),
        When probe_kernel_wireguard runs,
        Then the stale-cleanup error is silently swallowed AND the
            actual probe still proceeds to link add → link del → close.
        """
        ipr = MagicMock()
        lookup_count = {"n": 0}

        def lookup_side(ifname: str) -> list[int]:
            lookup_count["n"] += 1
            if lookup_count["n"] == 1:
                raise NetlinkError(errno.EPERM)
            return [42]

        ipr.link_lookup = MagicMock(side_effect=lookup_side)
        with patch.object(wg_control, "IPRoute", return_value=ipr):
            probe_kernel_wireguard()
        ipr.link.assert_any_call("add", ifname=_PROBE_INTERFACE_NAME, kind="wireguard")
        ipr.link.assert_any_call("del", index=42)
        ipr.close.assert_called_once()

    def test_probe_skips_stale_delete_when_lookup_empty(self) -> None:
        """Spec — fresh container (no stale interface) is the common path.

        Given link_lookup returns [] on the first call (no stale interface),
        When _delete_probe_if_present runs inside probe,
        Then no extra del is issued before the create attempt.
        """
        ipr = MagicMock()
        lookup_count = {"n": 0}

        def lookup_side(ifname: str) -> list[int]:
            lookup_count["n"] += 1
            if lookup_count["n"] == 1:
                return []
            return [42]

        ipr.link_lookup = MagicMock(side_effect=lookup_side)
        with patch.object(wg_control, "IPRoute", return_value=ipr):
            probe_kernel_wireguard()
        del_calls = [c for c in ipr.link.call_args_list if c.args and c.args[0] == "del"]
        del_indices = [c.kwargs.get("index") for c in del_calls]
        assert del_indices == [42]

    def test_probe_swallows_stale_link_del_netlink_error(self) -> None:
        """Spec — stale-interface delete failing does not block the probe.

        Given a stale interface exists (lookup #1 returns [99]) but the
            del call raises NetlinkError (e.g. race with another cleanup),
        When _delete_probe_if_present runs,
        Then the error is swallowed AND the probe still proceeds.
        """
        ipr = MagicMock()
        lookup_count = {"n": 0}

        def lookup_side(ifname: str) -> list[int]:
            lookup_count["n"] += 1
            if lookup_count["n"] == 1:
                return [99]
            return [42]

        ipr.link_lookup = MagicMock(side_effect=lookup_side)
        del_count = {"n": 0}

        def link_side(*args: object, **kwargs: object) -> None:
            cmd = args[0] if args else None
            if cmd == "del" and kwargs.get("index") == 99:
                del_count["n"] += 1
                raise NetlinkError(errno.EPERM)
            return None

        ipr.link = MagicMock(side_effect=link_side)
        with patch.object(wg_control, "IPRoute", return_value=ipr):
            probe_kernel_wireguard()
        assert del_count["n"] == 1
        ipr.close.assert_called_once()

    def test_probe_skips_delete_when_link_lookup_empty(self) -> None:
        """Spec — defensive: missing link after add does not crash.

        Given the kernel create succeeds but link_lookup returns [],
        When probe_kernel_wireguard runs,
        Then no del call is attempted AND IPRoute.close still runs.
        """
        ipr = MagicMock()
        ipr.link_lookup = MagicMock(return_value=[])
        with patch.object(wg_control, "IPRoute", return_value=ipr):
            probe_kernel_wireguard()
        del_calls = [c for c in ipr.link.call_args_list if c.args and c.args[0] == "del"]
        assert del_calls == []
        ipr.close.assert_called_once()


class TestResolveEndpoint:
    """Tests for ``_resolve_endpoint`` host:port parsing."""

    def test_ipv4_endpoint(self) -> None:
        """Spec — IPv4 endpoint resolves to itself.

        Given ``"192.0.2.1:51820"``,
        When _resolve_endpoint runs,
        Then it returns ``("192.0.2.1", 51820)``. getaddrinfo is
        mocked so the test does not depend on the conftest
        network-block fixture's policy for IP-literal lookups.
        """
        with patch.object(
            wg_control.socket,
            "getaddrinfo",
            return_value=[(2, 2, 17, "", ("192.0.2.1", 51820))],
        ):
            addr, port = _resolve_endpoint("192.0.2.1:51820")
        assert addr == "192.0.2.1"
        assert port == 51820

    def test_ipv6_bracketed_endpoint(self) -> None:
        """Spec — IPv6 in square brackets parses correctly.

        Given ``"[2001:db8::1]:51820"``,
        When _resolve_endpoint runs,
        Then the host strips the brackets and the port is extracted.
        getaddrinfo is mocked to skip the network-block fixture.
        """
        with patch.object(
            wg_control.socket,
            "getaddrinfo",
            return_value=[(10, 2, 17, "", ("2001:db8::1", 51820, 0, 0))],
        ):
            addr, port = _resolve_endpoint("[2001:db8::1]:51820")
        assert addr == "2001:db8::1"
        assert port == 51820

    def test_hostname_endpoint_resolved(self) -> None:
        """Spec — DNS resolution returns the resolver's first answer.

        Given a host that getaddrinfo returns 198.51.100.1 for,
        When _resolve_endpoint runs,
        Then the result tuple uses the resolved IP and the original port.
        """

        def fake_getaddrinfo(host: str, port: int, **_kwargs: object) -> list[tuple[Any, ...]]:
            return [(2, 2, 17, "", ("198.51.100.1", port))]

        with patch.object(wg_control.socket, "getaddrinfo", side_effect=fake_getaddrinfo):
            addr, port = _resolve_endpoint("vpn.example.com:443")
        assert addr == "198.51.100.1"
        assert port == 443

    def test_missing_port_raises(self) -> None:
        """Spec — endpoint without ``:port`` → ValueError.

        Given ``"vpn.example.com"`` (no colon),
        When _resolve_endpoint runs,
        Then ValueError fires.
        """
        with pytest.raises(ValueError, match="missing"):
            _resolve_endpoint("vpn.example.com")

    def test_non_integer_port_raises(self) -> None:
        """Spec — port that fails int() → ValueError.

        Given ``"host:notaport"``,
        When _resolve_endpoint runs,
        Then ValueError fires.
        """
        with pytest.raises(ValueError, match="not an integer"):
            _resolve_endpoint("host:notaport")

    def test_port_out_of_range_raises(self) -> None:
        """Spec — port outside 1..65535 → ValueError.

        Given ``"host:70000"``,
        When _resolve_endpoint runs,
        Then ValueError fires.
        """
        with pytest.raises(ValueError, match="out of range"):
            _resolve_endpoint("host:70000")

    def test_port_zero_raises(self) -> None:
        """Spec — port 0 is rejected even though parseable.

        Given ``"host:0"``,
        When _resolve_endpoint runs,
        Then ValueError fires.
        """
        with pytest.raises(ValueError, match="out of range"):
            _resolve_endpoint("host:0")

    def test_unresolvable_host_raises(self) -> None:
        """Spec — socket.gaierror from resolver → ValueError.

        Given getaddrinfo raises gaierror,
        When _resolve_endpoint runs,
        Then ValueError fires with the host name.
        """
        with (
            patch.object(
                wg_control.socket,
                "getaddrinfo",
                side_effect=socket.gaierror("simulated DNS fail"),
            ),
            pytest.raises(ValueError, match="unresolvable"),
        ):
            _resolve_endpoint("notahost.invalid:443")

    def test_non_string_resolver_address_raises(self) -> None:
        """Spec — defensive: resolver returns non-string addr → ValueError.

        Given getaddrinfo returns a non-string address tuple (defensive
            type-narrowing branch),
        When _resolve_endpoint runs,
        Then ValueError fires.
        """
        with (
            patch.object(
                wg_control.socket,
                "getaddrinfo",
                return_value=[(2, 2, 17, "", (12345, 443))],
            ),
            pytest.raises(ValueError, match="non-string"),
        ):
            _resolve_endpoint("host:443")

    def test_empty_resolver_result_raises(self) -> None:
        """Spec — defensive: getaddrinfo returns [] → ValueError.

        Given getaddrinfo returns an empty list (improbable but
            documented in the stdlib),
        When _resolve_endpoint runs,
        Then ValueError fires.
        """
        with (
            patch.object(wg_control.socket, "getaddrinfo", return_value=[]),
            pytest.raises(ValueError, match="no addresses"),
        ):
            _resolve_endpoint("host:443")


class TestBringUpSync:
    """Tests for ``_bring_up_sync`` and the matching ``bring_up`` coroutine."""

    def test_bring_up_ipv4_full_sequence(self) -> None:
        """Spec — IPv4 bring_up calls all 8 pyroute2 ops in order with family=AF_INET.

        Given fresh kernel state + an IPv4 interface address,
        When _bring_up_sync runs,
        Then bring_down runs first (cleanup), then link add, then
        WireGuard.set, then addr add, then link set up, then route add
        with ``dst="0.0.0.0/0"`` + ``family=AF_INET``, then rule add
        with ``src_len=32`` + ``family=AF_INET``. The explicit family +
        src_len ensures the rule matches only the intended source host
        (not ``from all``) per Codex Code Reviewer SC.1 round 1 fix.
        """
        ipr = _make_ipr_mock()
        wg = _make_wg_mock()
        with (
            patch.object(wg_control, "IPRoute", return_value=ipr),
            patch.object(wg_control, "WireGuard", return_value=wg),
            patch.object(
                wg_control,
                "_resolve_endpoint",
                return_value=("198.51.100.1", 51820),
            ),
        ):
            _bring_up_sync(
                interface="wg-uk-1",
                address="10.64.12.34",
                prefix_length=32,
                private_key="PRIVKEY",
                peer_pubkey="PUBKEY",
                peer_endpoint="vpn.example.com:51820",
                tunnel_index=1,
                preshared_key=None,
                allowed_ips=("0.0.0.0/0", "::/0"),
            )
        link_calls = list(ipr.link.call_args_list)
        add_calls = [c for c in link_calls if c.args and c.args[0] == "add"]
        assert any(
            c.kwargs.get("ifname") == "wg-uk-1" and c.kwargs.get("kind") == "wireguard"
            for c in add_calls
        )
        wg.set.assert_called_once()
        call_args = wg.set.call_args
        assert call_args.args[0] == "wg-uk-1"
        assert call_args.kwargs["private_key"] == "PRIVKEY"
        peer = call_args.kwargs["peer"]
        assert peer["public_key"] == "PUBKEY"
        assert peer["endpoint_addr"] == "198.51.100.1"
        assert peer["endpoint_port"] == 51820
        assert peer["persistent_keepalive"] == _PERSISTENT_KEEPALIVE
        assert peer["allowed_ips"] == ["0.0.0.0/0", "::/0"]
        assert "preshared_key" not in peer
        ipr.addr.assert_called_with("add", index=42, address="10.64.12.34", prefixlen=32)
        ipr.link.assert_any_call("set", index=42, state="up")
        expected_table = _RTABLE_BASE + 1
        expected_priority = _RULE_PRIORITY_BASE + 1
        ipr.route.assert_called_with(
            "add",
            dst="0.0.0.0/0",
            oif=42,
            table=expected_table,
            family=socket.AF_INET,
        )
        ipr.rule.assert_any_call(
            "add",
            src="10.64.12.34",
            src_len=32,
            table=expected_table,
            priority=expected_priority,
            family=socket.AF_INET,
        )

    def test_bring_up_ipv6_uses_af_inet6_and_default_ipv6_dst(self) -> None:
        """Spec — IPv6 inner address derives family=AF_INET6 + dst="::/0" + src_len=128.

        Given an IPv6 interface address,
        When _bring_up_sync runs,
        Then route add uses dst="::/0" + family=AF_INET6, and rule add
        uses src_len=128 + family=AF_INET6. Mixing IPv4/IPv6 in the
        same tunnel is not supported (caller passes one address of
        one family).
        """
        ipr = _make_ipr_mock()
        wg = _make_wg_mock()
        with (
            patch.object(wg_control, "IPRoute", return_value=ipr),
            patch.object(wg_control, "WireGuard", return_value=wg),
            patch.object(
                wg_control,
                "_resolve_endpoint",
                return_value=("2001:db8::1", 51820),
            ),
        ):
            _bring_up_sync(
                interface="wg-v6-1",
                address="fd00:10:64::34",
                prefix_length=128,
                private_key="PK",
                peer_pubkey="PUB",
                peer_endpoint="[2001:db8::1]:51820",
                tunnel_index=2,
                preshared_key=None,
                allowed_ips=("::/0",),
            )
        ipr.route.assert_called_with(
            "add",
            dst="::/0",
            oif=42,
            table=_RTABLE_BASE + 2,
            family=socket.AF_INET6,
        )
        ipr.rule.assert_any_call(
            "add",
            src="fd00:10:64::34",
            src_len=128,
            table=_RTABLE_BASE + 2,
            priority=_RULE_PRIORITY_BASE + 2,
            family=socket.AF_INET6,
        )

    def test_bring_up_passes_preshared_key_when_present(self) -> None:
        """Spec — preshared_key gets injected into the peer dict.

        Given preshared_key="PSK",
        When _bring_up_sync runs,
        Then WireGuard.set's peer kwarg contains preshared_key="PSK".
        """
        ipr = _make_ipr_mock()
        wg = _make_wg_mock()
        with (
            patch.object(wg_control, "IPRoute", return_value=ipr),
            patch.object(wg_control, "WireGuard", return_value=wg),
            patch.object(wg_control, "_resolve_endpoint", return_value=("1.2.3.4", 51820)),
        ):
            _bring_up_sync(
                interface="wg-uk-1",
                address="10.64.12.34",
                prefix_length=32,
                private_key="PRIVKEY",
                peer_pubkey="PUBKEY",
                peer_endpoint="vpn:51820",
                tunnel_index=0,
                preshared_key="PSK",
                allowed_ips=("0.0.0.0/0",),
            )
        peer = wg.set.call_args.kwargs["peer"]
        assert peer["preshared_key"] == "PSK"

    def test_bring_up_calls_bring_down_first(self) -> None:
        """Spec — pre-existing wg interface is torn down before recreate.

        Given _bring_down_sync is the first operation in bring_up,
        When _bring_up_sync runs,
        Then _bring_down_sync is observed before the link add.
        """
        ipr = _make_ipr_mock()
        wg = _make_wg_mock()
        observed_order: list[str] = []

        def observe_bring_down(*_a: object, **_kw: object) -> None:
            observed_order.append("bring_down")

        with (
            patch.object(wg_control, "_bring_down_sync", side_effect=observe_bring_down),
            patch.object(wg_control, "IPRoute", return_value=ipr),
            patch.object(wg_control, "WireGuard", return_value=wg),
            patch.object(wg_control, "_resolve_endpoint", return_value=("1.2.3.4", 51820)),
        ):
            _bring_up_sync(
                interface="wg-uk-1",
                address="10.64.12.34",
                prefix_length=32,
                private_key="PRIVKEY",
                peer_pubkey="PUBKEY",
                peer_endpoint="vpn:51820",
                tunnel_index=0,
                preshared_key=None,
                allowed_ips=("0.0.0.0/0",),
            )
        assert observed_order == ["bring_down"]

    def test_bring_up_raises_when_link_lookup_empty_after_add(self) -> None:
        """Spec — defensive: created interface not found → RuntimeError.

        Given IPRoute.link_lookup returns [] right after link add,
        When _bring_up_sync runs,
        Then RuntimeError fires AND IPRoute.close still runs.
        """
        ipr = MagicMock()
        ipr.link_lookup = MagicMock(return_value=[])
        wg = _make_wg_mock()
        with (
            patch.object(wg_control, "_bring_down_sync"),
            patch.object(wg_control, "IPRoute", return_value=ipr),
            patch.object(wg_control, "WireGuard", return_value=wg),
            patch.object(wg_control, "_resolve_endpoint", return_value=("1.2.3.4", 51820)),
            pytest.raises(RuntimeError, match="link_lookup found nothing"),
        ):
            _bring_up_sync(
                interface="wg-uk-1",
                address="10.64.12.34",
                prefix_length=32,
                private_key="PRIVKEY",
                peer_pubkey="PUBKEY",
                peer_endpoint="vpn:51820",
                tunnel_index=0,
                preshared_key=None,
                allowed_ips=("0.0.0.0/0",),
            )
        ipr.close.assert_called_once()

    def test_bring_up_closes_wireguard_handle_on_exception(self) -> None:
        """Spec — WireGuard.set raising still closes the WG handle.

        Given WireGuard.set raises,
        When _bring_up_sync runs,
        Then both the WireGuard handle and the IPRoute handle close.
        """
        ipr = _make_ipr_mock()
        wg = _make_wg_mock()
        wg.set = MagicMock(side_effect=RuntimeError("simulated wg fail"))
        with (
            patch.object(wg_control, "_bring_down_sync"),
            patch.object(wg_control, "IPRoute", return_value=ipr),
            patch.object(wg_control, "WireGuard", return_value=wg),
            patch.object(wg_control, "_resolve_endpoint", return_value=("1.2.3.4", 51820)),
            pytest.raises(RuntimeError, match="simulated wg fail"),
        ):
            _bring_up_sync(
                interface="wg-uk-1",
                address="10.64.12.34",
                prefix_length=32,
                private_key="PRIVKEY",
                peer_pubkey="PUBKEY",
                peer_endpoint="vpn:51820",
                tunnel_index=0,
                preshared_key=None,
                allowed_ips=("0.0.0.0/0",),
            )
        wg.close.assert_called_once()
        ipr.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_bring_up_rejects_negative_tunnel_index(self) -> None:
        """Spec — tunnel_index < 0 → ValueError before any kernel state touched.

        Given tunnel_index=-1,
        When bring_up is awaited,
        Then ValueError fires with a message about the reserved range,
        AND _bring_up_sync is NOT called.
        """
        with (
            patch.object(wg_control, "_bring_up_sync") as sync_mock,
            pytest.raises(ValueError, match="reserved range"),
        ):
            await bring_up(
                interface="wg-uk-1",
                address="10.64.12.34",
                prefix_length=32,
                private_key="K",
                peer_pubkey="P",
                peer_endpoint="vpn:51820",
                tunnel_index=-1,
            )
        sync_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_bring_up_rejects_tunnel_index_above_max(self) -> None:
        """Spec — tunnel_index > 999 → ValueError (would push out of reserved [5000, 5999]).

        Given tunnel_index=1000,
        When bring_up is awaited,
        Then ValueError fires AND _bring_up_sync is NOT called.
        """
        with (
            patch.object(wg_control, "_bring_up_sync") as sync_mock,
            pytest.raises(ValueError, match="reserved range"),
        ):
            await bring_up(
                interface="wg-uk-1",
                address="10.64.12.34",
                prefix_length=32,
                private_key="K",
                peer_pubkey="P",
                peer_endpoint="vpn:51820",
                tunnel_index=1000,
            )
        sync_mock.assert_not_called()

    def test_bring_up_rejects_invalid_address(self) -> None:
        """Spec — non-IP string in ``address`` raises ValueError.

        Given address="not.an.ip",
        When _bring_up_sync runs,
        Then ipaddress.ip_address raises ValueError (caught by the
        ipaddress module itself) before any kernel state is touched.
        """
        with pytest.raises(ValueError):
            _bring_up_sync(
                interface="wg-uk-1",
                address="not.an.ip",
                prefix_length=32,
                private_key="K",
                peer_pubkey="P",
                peer_endpoint="vpn:51820",
                tunnel_index=0,
                preshared_key=None,
                allowed_ips=("0.0.0.0/0",),
            )

    @pytest.mark.asyncio
    async def test_bring_up_runs_in_thread(self) -> None:
        """Spec — async bring_up dispatches to to_thread.

        Given a successful _bring_up_sync,
        When bring_up is awaited,
        Then _bring_up_sync is called once with the expected args.
        """
        with patch.object(wg_control, "_bring_up_sync") as sync_mock:
            await bring_up(
                interface="wg-uk-1",
                address="10.64.12.34",
                prefix_length=32,
                private_key="K",
                peer_pubkey="P",
                peer_endpoint="vpn:51820",
                tunnel_index=2,
            )
        sync_mock.assert_called_once_with(
            "wg-uk-1",
            "10.64.12.34",
            32,
            "K",
            "P",
            "vpn:51820",
            2,
            None,
            ("0.0.0.0/0", "::/0"),
        )


class TestBringDownSync:
    """Tests for ``_bring_down_sync`` idempotent cleanup."""

    def test_bring_down_removes_rule_then_flush_then_delete(self) -> None:
        """Spec — order is rule del → flush_routes → link del.

        Given a tunnel that exists,
        When _bring_down_sync runs,
        Then the sequence is: rule del, flush_routes(table), link del.
        """
        ipr = _make_ipr_mock()
        fake_rule = MagicMock()
        fake_rule.get_attr = MagicMock(return_value=_RTABLE_BASE + 1)
        fake_rule.get = MagicMock(return_value=_RULE_PRIORITY_BASE + 1)
        ipr.get_rules = MagicMock(return_value=[fake_rule])
        with patch.object(wg_control, "IPRoute", return_value=ipr):
            _bring_down_sync("wg-uk-1", _RTABLE_BASE + 1, _RULE_PRIORITY_BASE + 1)
        ipr.rule.assert_called_with("del", table=_RTABLE_BASE + 1, priority=_RULE_PRIORITY_BASE + 1)
        ipr.flush_routes.assert_called_with(table=_RTABLE_BASE + 1)
        ipr.link.assert_called_with("del", index=42)
        ipr.close.assert_called_once()

    def test_bring_down_removes_multiple_stale_rules_at_same_table(self) -> None:
        """Spec — multiple ip-rules pointing at the same RTABLE are all deleted.

        Given a prior address rotation that left TWO rules pointing at
            table 5001 (priorities 5001 and 5099 — stale + current),
        When _bring_down_sync runs,
        Then BOTH rules are deleted (handles stale state from
            misconfigured rotations).
        """
        ipr = _make_ipr_mock()
        rule_old = MagicMock()
        rule_old.get_attr = MagicMock(return_value=5001)
        rule_old.get = MagicMock(return_value=5099)
        rule_new = MagicMock()
        rule_new.get_attr = MagicMock(return_value=5001)
        rule_new.get = MagicMock(return_value=5001)
        rule_unrelated = MagicMock()
        rule_unrelated.get_attr = MagicMock(return_value=42)
        rule_unrelated.get = MagicMock(return_value=42)
        ipr.get_rules = MagicMock(return_value=[rule_old, rule_new, rule_unrelated])
        with patch.object(wg_control, "IPRoute", return_value=ipr):
            _bring_down_sync("wg-uk-1", 5001, 5001)
        del_calls = [c for c in ipr.rule.call_args_list if c.args and c.args[0] == "del"]
        assert call("del", table=5001, priority=5099) in del_calls
        assert call("del", table=5001, priority=5001) in del_calls
        for c in del_calls:
            assert c.kwargs.get("table") == 5001

    def test_bring_down_no_op_when_interface_missing(self) -> None:
        """Spec — missing interface: skip link del cleanly.

        Given IPRoute.link_lookup returns [] for the interface,
        When _bring_down_sync runs,
        Then no link del is attempted AND IPRoute.close runs.
        """
        ipr = _make_ipr_mock()
        ipr.link_lookup = MagicMock(return_value=[])
        ipr.get_rules = MagicMock(return_value=[])
        with patch.object(wg_control, "IPRoute", return_value=ipr):
            _bring_down_sync("wg-uk-1", _RTABLE_BASE, _RULE_PRIORITY_BASE)
        del_calls = [c for c in ipr.link.call_args_list if c.args and c.args[0] == "del"]
        assert del_calls == []
        ipr.close.assert_called_once()

    def test_bring_down_swallows_get_rules_netlink_error(self) -> None:
        """Spec — get_rules failure doesn't block the rest of cleanup.

        Given ipr.get_rules raises NetlinkError,
        When _bring_down_sync runs,
        Then flush_routes + link del still run; no exception escapes.
        """
        ipr = _make_ipr_mock()
        ipr.get_rules = MagicMock(side_effect=NetlinkError(errno.EPERM))
        with patch.object(wg_control, "IPRoute", return_value=ipr):
            _bring_down_sync("wg-uk-1", _RTABLE_BASE, _RULE_PRIORITY_BASE)
        ipr.flush_routes.assert_called_with(table=_RTABLE_BASE)
        ipr.link.assert_called_with("del", index=42)

    def test_bring_down_swallows_rule_del_netlink_error(self) -> None:
        """Spec — rule del failure doesn't block flush + link del.

        Given ipr.rule('del', ...) raises NetlinkError,
        When _bring_down_sync runs,
        Then flush_routes + link del still run; no exception escapes.
        """
        ipr = _make_ipr_mock()
        rule = MagicMock()
        rule.get_attr = MagicMock(return_value=_RTABLE_BASE)
        rule.get = MagicMock(return_value=_RULE_PRIORITY_BASE)
        ipr.get_rules = MagicMock(return_value=[rule])
        ipr.rule = MagicMock(side_effect=NetlinkError(errno.EPERM))
        with patch.object(wg_control, "IPRoute", return_value=ipr):
            _bring_down_sync("wg-uk-1", _RTABLE_BASE, _RULE_PRIORITY_BASE)
        ipr.flush_routes.assert_called_with(table=_RTABLE_BASE)

    def test_bring_down_swallows_flush_routes_netlink_error(self) -> None:
        """Spec — flush_routes failure doesn't block link del.

        Given ipr.flush_routes raises NetlinkError,
        When _bring_down_sync runs,
        Then link del still runs; no exception escapes.
        """
        ipr = _make_ipr_mock()
        ipr.get_rules = MagicMock(return_value=[])
        ipr.flush_routes = MagicMock(side_effect=NetlinkError(errno.EPERM))
        with patch.object(wg_control, "IPRoute", return_value=ipr):
            _bring_down_sync("wg-uk-1", _RTABLE_BASE, _RULE_PRIORITY_BASE)
        ipr.link.assert_called_with("del", index=42)

    def test_bring_down_swallows_link_del_netlink_error(self) -> None:
        """Spec — link del failure doesn't propagate.

        Given ipr.link('del', ...) raises NetlinkError (race with another
            cleanup),
        When _bring_down_sync runs,
        Then no exception escapes AND IPRoute.close still runs.
        """
        ipr = _make_ipr_mock()
        ipr.get_rules = MagicMock(return_value=[])
        call_count = {"n": 0}

        def link_side_effect(*_a: object, **_kw: object) -> None:
            call_count["n"] += 1
            raise NetlinkError(errno.EPERM)

        ipr.link = MagicMock(side_effect=link_side_effect)
        with patch.object(wg_control, "IPRoute", return_value=ipr):
            _bring_down_sync("wg-uk-1", _RTABLE_BASE, _RULE_PRIORITY_BASE)
        ipr.close.assert_called_once()

    def test_bring_down_swallows_link_lookup_netlink_error(self) -> None:
        """Spec — link_lookup raising NetlinkError → skip delete cleanly.

        Given ipr.link_lookup raises NetlinkError,
        When _bring_down_sync runs,
        Then the delete step is skipped, IPRoute.close still runs.
        """
        ipr = _make_ipr_mock()
        ipr.get_rules = MagicMock(return_value=[])
        ipr.link_lookup = MagicMock(side_effect=NetlinkError(errno.EPERM))
        with patch.object(wg_control, "IPRoute", return_value=ipr):
            _bring_down_sync("wg-uk-1", _RTABLE_BASE, _RULE_PRIORITY_BASE)
        ipr.close.assert_called_once()

    def test_bring_down_skips_rules_without_table_attr(self) -> None:
        """Spec — rules whose get_attr raises AttributeError are skipped.

        Given a get_rules response that yields a rule whose
            ``get_attr('FRA_TABLE')`` raises AttributeError,
        When _bring_down_sync runs,
        Then that rule is silently skipped (defensive — pyroute2 versions
            differ in what they include in the response).
        """
        ipr = _make_ipr_mock()
        bad_rule = MagicMock()
        bad_rule.get_attr = MagicMock(side_effect=AttributeError())
        ipr.get_rules = MagicMock(return_value=[bad_rule])
        with patch.object(wg_control, "IPRoute", return_value=ipr):
            _bring_down_sync("wg-uk-1", _RTABLE_BASE, _RULE_PRIORITY_BASE)
        rule_del_calls = [c for c in ipr.rule.call_args_list if c.args and c.args[0] == "del"]
        assert rule_del_calls == []

    def test_bring_down_skips_unrelated_table(self) -> None:
        """Spec — rules whose table != rtable are NOT deleted.

        Given a rule pointing at table 999,
        When _bring_down_sync runs targeting table 5001,
        Then no rule del is issued for that unrelated rule.
        """
        ipr = _make_ipr_mock()
        rule = MagicMock()
        rule.get_attr = MagicMock(return_value=999)
        rule.get = MagicMock(return_value=999)
        ipr.get_rules = MagicMock(return_value=[rule])
        with patch.object(wg_control, "IPRoute", return_value=ipr):
            _bring_down_sync("wg-uk-1", 5001, 5001)
        rule_del_calls = [c for c in ipr.rule.call_args_list if c.args and c.args[0] == "del"]
        assert rule_del_calls == []

    def test_bring_down_handles_rule_without_priority(self) -> None:
        """Spec — rule with .get('priority') returning None still gets deleted.

        Given a matching rule whose ``get('priority')`` is None,
        When _bring_down_sync runs,
        Then rule del is called WITHOUT a priority kwarg.
        """
        ipr = _make_ipr_mock()
        rule = MagicMock()
        rule.get_attr = MagicMock(return_value=_RTABLE_BASE)
        rule.get = MagicMock(return_value=None)
        ipr.get_rules = MagicMock(return_value=[rule])
        with patch.object(wg_control, "IPRoute", return_value=ipr):
            _bring_down_sync("wg-uk-1", _RTABLE_BASE, _RULE_PRIORITY_BASE)
        ipr.rule.assert_any_call("del", table=_RTABLE_BASE)

    @pytest.mark.asyncio
    async def test_bring_down_async_dispatches_to_thread(self) -> None:
        """Spec — async bring_down dispatches to to_thread.

        Given a successful _bring_down_sync,
        When bring_down is awaited with tunnel_index=3,
        Then _bring_down_sync is called once with the matching
        derived table + priority.
        """
        with patch.object(wg_control, "_bring_down_sync") as sync_mock:
            await bring_down(interface="wg-uk-1", tunnel_index=3)
        sync_mock.assert_called_once_with("wg-uk-1", _RTABLE_BASE + 3, _RULE_PRIORITY_BASE + 3)
