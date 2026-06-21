"""Unit tests for egress route configuration models."""

import dataclasses

import pytest
from pydantic import ValidationError

from snapper.infrastructure.network.egress_models import EgressPoolConfig
from snapper.infrastructure.network.egress_models import RouteConfig
from snapper.infrastructure.network.egress_models import RouteSelection
from snapper.infrastructure.network.egress_models import RouteSnapshot
from snapper.infrastructure.network.egress_models import RouteState


class TestRouteConfig:
    """Tests for the RouteConfig Pydantic model."""

    def test_direct_route_accepts_no_proxy_url(self) -> None:
        """Spec — direct kind with no proxy_url constructs cleanly.

        Given a RouteConfig request with kind="direct" and proxy_url=None,
        When the model is constructed,
        Then construction succeeds and proxy_url remains None.
        """
        route = RouteConfig(id="default", kind="direct")
        assert route.proxy_url is None
        assert route.kind == "direct"

    def test_direct_route_rejects_proxy_url(self) -> None:
        """Spec — direct kind with proxy_url raises ValidationError.

        Given a RouteConfig request with kind="direct" and a non-empty proxy_url,
        When the model is constructed,
        Then Pydantic ValidationError is raised.
        """
        with pytest.raises(ValidationError):
            RouteConfig(id="d", kind="direct", proxy_url="socks5h://x:1081")

    def test_socks5_route_requires_proxy_url(self) -> None:
        """Spec — socks5 kind with no proxy_url raises ValidationError.

        Given a RouteConfig with kind="socks5" and proxy_url=None,
        When constructed,
        Then ValidationError is raised.
        """
        with pytest.raises(ValidationError):
            RouteConfig(id="s", kind="socks5")

    def test_socks5_route_rejects_empty_proxy_url(self) -> None:
        """Spec — empty proxy_url string is rejected.

        Given proxy_url="" for a socks5 route,
        When the model is constructed,
        Then ValidationError fires.
        """
        with pytest.raises(ValidationError):
            RouteConfig(id="s", kind="socks5", proxy_url="")

    def test_socks5_route_requires_socks5h_scheme(self) -> None:
        """Spec — only socks5h:// scheme is accepted (DNS-leak protection).

        Given proxy_url="socks5://x:1081" (note: no trailing h),
        When the model is constructed,
        Then ValidationError fires explaining the DNS-leak rationale.
        """
        with pytest.raises(ValidationError, match="socks5h://"):
            RouteConfig(id="s", kind="socks5", proxy_url="socks5://x:1081")

    def test_socks5_route_accepts_socks5h_scheme(self) -> None:
        """Spec — socks5h:// scheme constructs cleanly.

        Given proxy_url="socks5h://snapper-egress:1081",
        When constructed,
        Then the model is accepted.
        """
        route = RouteConfig(
            id="wg-uk-1",
            kind="socks5",
            proxy_url="socks5h://snapper-egress:1081",
        )
        assert route.proxy_url == "socks5h://snapper-egress:1081"

    def test_id_rejected_when_empty(self) -> None:
        """Spec — RouteConfig.id requires min_length=1.

        Given id="" for any kind,
        When constructed,
        Then ValidationError fires.
        """
        with pytest.raises(ValidationError):
            RouteConfig(id="", kind="direct")

    def test_id_rejected_when_too_long(self) -> None:
        """Spec — RouteConfig.id has max_length=64.

        Given id of 65 characters,
        When constructed,
        Then ValidationError fires.
        """
        with pytest.raises(ValidationError):
            RouteConfig(id="x" * 65, kind="direct")

    def test_websocket_proxy_kwarg_direct_returns_proxy_none(self) -> None:
        """Spec — direct routes return {"proxy": None}.

        Given a direct route,
        When websocket_proxy_kwarg() is called,
        Then the result is {"proxy": None}. This explicitly overrides
        the websockets-16 default of ``proxy=True`` so HTTPS_PROXY env
        vars cannot silently activate on a direct route.
        """
        route = RouteConfig(id="d", kind="direct")
        assert route.websocket_proxy_kwarg() == {"proxy": None}

    def test_websocket_proxy_kwarg_socks5_returns_proxy_url(self) -> None:
        """Spec — socks5 routes return {"proxy": "socks5h://..."}.

        Given a socks5 route with proxy_url="socks5h://x:1081",
        When websocket_proxy_kwarg() is called,
        Then the result is {"proxy": "socks5h://x:1081"}.
        """
        route = RouteConfig(id="s", kind="socks5", proxy_url="socks5h://x:1081")
        assert route.websocket_proxy_kwarg() == {"proxy": "socks5h://x:1081"}

    def test_route_config_is_frozen(self) -> None:
        """Spec — RouteConfig is immutable after construction.

        Given a constructed RouteConfig,
        When a field is reassigned,
        Then ValidationError fires (frozen=True).
        """
        route = RouteConfig(id="d", kind="direct")
        with pytest.raises(ValidationError):
            route.id = "e"

    def test_route_config_rejects_unknown_fields(self) -> None:
        """Spec — extra="forbid" rejects unknown keys.

        Given a dict containing an unknown field,
        When validated,
        Then ValidationError fires.
        """
        with pytest.raises(ValidationError):
            RouteConfig.model_validate({"id": "d", "kind": "direct", "wibble": 1})

    def test_default_allowed_exchanges_is_empty(self) -> None:
        """Spec — RouteConfig.allowed_exchanges defaults to ().

        Empty tuple is the back-compatible default: pool selection
        treats it as "any exchange". Existing prod seed entries
        without the field continue to work without migration.
        """
        route = RouteConfig(id="d", kind="direct")
        assert route.allowed_exchanges == ()

    def test_allowed_exchanges_non_sequence_input_falls_through(self) -> None:
        """Spec — non-list/tuple input falls through the BeforeValidator.

        The StringSequence helper only coerces ``list[str]`` /
        ``tuple[str, ...]`` shapes; any other value (e.g. ``int``) is
        returned unchanged so Pydantic's own strict validator emits the
        standard "input must be a tuple" error. This guards against
        the helper silently accepting malformed JSON like ``42``.
        """
        with pytest.raises(ValidationError):
            RouteConfig.model_validate({"id": "d", "kind": "direct", "allowed_exchanges": 42})

    def test_allowed_exchanges_coerces_list_to_tuple(self) -> None:
        """Spec — JSON ``list`` is coerced to ``tuple`` via BeforeValidator.

        Operators write ``egress_pool`` as JSON; ``json.loads``
        produces a Python ``list``. The StringSequence helper turns
        that into the strict ``tuple[str, ...]`` field type so
        Pydantic's strict=True mode accepts the input.
        """
        route = RouteConfig(
            id="s",
            kind="socks5",
            proxy_url="socks5h://x:1",
            allowed_exchanges=["walutomat"],
        )
        assert route.allowed_exchanges == ("walutomat",)

    def test_optional_operator_metadata_fields_parse(self) -> None:
        """Spec — RouteConfig accepts optional operator metadata fields.

        Given a route dict with region, exit_ip, and provider,
        When RouteConfig validates it,
        Then the fields are retained and existing configs remain optional.
        """
        route = RouteConfig.model_validate(
            {
                "id": "wg-pl-1",
                "kind": "socks5",
                "proxy_url": "socks5h://x:1084",
                "region": "pl-waw",
                "exit_ip": "203.0.113.10",
                "provider": "mullvad",
            }
        )
        default_route = RouteConfig(id="default", kind="direct")
        assert route.region == "pl-waw"
        assert route.exit_ip == "203.0.113.10"
        assert route.provider == "mullvad"
        assert default_route.region is None
        assert default_route.exit_ip is None
        assert default_route.provider is None


class TestEgressPoolConfig:
    """Tests for the top-level EgressPoolConfig model."""

    def test_default_construction_is_disabled(self) -> None:
        """Spec — EgressPoolConfig() yields enabled=False.

        Given an EgressPoolConfig with no arguments,
        When constructed,
        Then enabled is False, routes is empty, on_all_quarantined is "wait".
        """
        config = EgressPoolConfig()
        assert config.enabled is False
        assert config.routes == []
        assert config.on_all_quarantined == "wait"
        assert config.private_fallback_route_id is None

    def test_enabled_requires_direct_route(self) -> None:
        """Spec — enabled=True with no direct route is rejected.

        Given enabled=True and only socks5 routes,
        When constructed,
        Then ValidationError fires explaining the missing fallback.
        """
        with pytest.raises(ValidationError, match="direct route"):
            EgressPoolConfig(
                enabled=True,
                routes=[RouteConfig(id="s", kind="socks5", proxy_url="socks5h://x:1081")],
            )

    def test_enabled_requires_enabled_direct_route(self) -> None:
        """Spec — direct route with enabled=False does not count.

        Given enabled=True and one direct route with enabled=False,
        When constructed,
        Then ValidationError fires.
        """
        with pytest.raises(ValidationError, match="direct route"):
            EgressPoolConfig(
                enabled=True,
                routes=[RouteConfig(id="d", kind="direct", enabled=False)],
            )

    def test_disabled_pool_allows_no_routes(self) -> None:
        """Spec — disabled pool accepts any routes including none.

        Given enabled=False and routes=[],
        When constructed,
        Then no error.
        """
        config = EgressPoolConfig(enabled=False, routes=[])
        assert config.enabled is False

    def test_disabled_pool_allows_pure_socks5(self) -> None:
        """Spec — disabled pool may have only socks5 routes.

        The direct-route requirement only applies when enabled=True.
        """
        config = EgressPoolConfig(
            enabled=False,
            routes=[RouteConfig(id="s", kind="socks5", proxy_url="socks5h://x:1081")],
        )
        assert config.enabled is False

    def test_on_all_quarantined_accepts_wait(self) -> None:
        """Spec — on_all_quarantined accepts the literal "wait".

        Given on_all_quarantined="wait",
        When the model is built,
        Then the value is stored verbatim.
        """
        config = EgressPoolConfig(
            enabled=True,
            on_all_quarantined="wait",
            routes=[RouteConfig(id="d", kind="direct")],
        )
        assert config.on_all_quarantined == "wait"

    def test_on_all_quarantined_accepts_raise(self) -> None:
        """Spec — on_all_quarantined accepts the literal "raise".

        Given on_all_quarantined="raise",
        When the model is built,
        Then the value is stored verbatim.
        """
        config = EgressPoolConfig(
            enabled=True,
            on_all_quarantined="raise",
            routes=[RouteConfig(id="d", kind="direct")],
        )
        assert config.on_all_quarantined == "raise"

    def test_on_all_quarantined_rejects_unknown(self) -> None:
        """Spec — on_all_quarantined rejects values outside the Literal set.

        Given an unrecognised string,
        When validated,
        Then ValidationError fires.
        """
        with pytest.raises(ValidationError):
            EgressPoolConfig(
                enabled=True,
                on_all_quarantined="explode",
                routes=[RouteConfig(id="d", kind="direct")],
            )

    def test_rejects_unknown_exchange_name_in_allowed_exchanges(self) -> None:
        """Spec — typo'd exchange name in allowed_exchanges raises at construction.

        ``allowed_exchanges`` values must be ``ExchangeEnum`` *values*
        (e.g. ``"walutomat"``, not ``"WALUTOMAT"``). A typo like
        ``"krakeen"`` would silently make the route unreachable for
        any reserve() call, then fall back to direct. The validator
        surfaces this at config-load time instead.
        """
        with pytest.raises(ValidationError, match="unknown exchange"):
            EgressPoolConfig(
                enabled=True,
                routes=[
                    RouteConfig(id="d", kind="direct", priority=100),
                    RouteConfig(
                        id="bad",
                        kind="socks5",
                        proxy_url="socks5h://x:1",
                        allowed_exchanges=("krakeen",),
                    ),
                ],
            )

    def test_model_validate_accepts_dict_input(self) -> None:
        """Spec — model_validate accepts a plain dict.

        Given a dict matching the schema,
        When EgressPoolConfig.model_validate is called,
        Then the model is constructed successfully.
        """
        config = EgressPoolConfig.model_validate(
            {
                "enabled": True,
                "on_all_quarantined": "wait",
                "routes": [{"id": "d", "kind": "direct"}],
            }
        )
        assert len(config.routes) == 1

    def test_private_fallback_route_id_accepts_declared_route(self) -> None:
        """Spec — private fallback id may name an existing route.

        Given a config whose ``private_fallback_route_id`` points at
            a declared route,
        When EgressPoolConfig is constructed,
        Then the fallback id is stored.
        """
        config = EgressPoolConfig(
            enabled=True,
            private_fallback_route_id="pl",
            routes=[
                RouteConfig(id="default", kind="direct"),
                RouteConfig(
                    id="pl",
                    kind="socks5",
                    proxy_url="socks5h://x:1084",
                    allowed_exchanges=("walutomat",),
                ),
            ],
        )
        assert config.private_fallback_route_id == "pl"

    def test_private_fallback_route_id_rejects_unknown_route(self) -> None:
        """Spec — private fallback id must name an existing route.

        Given a config whose ``private_fallback_route_id`` points at
            no declared route,
        When EgressPoolConfig is constructed,
        Then ValidationError is raised.
        """
        with pytest.raises(ValidationError, match="private_fallback_route_id"):
            EgressPoolConfig(
                enabled=True,
                private_fallback_route_id="missing",
                routes=[RouteConfig(id="default", kind="direct")],
            )


class TestRouteStateDefaults:
    """Tests for RouteState default values."""

    def test_route_state_defaults_to_healthy_zero_in_use(self) -> None:
        """Spec — RouteState constructed from a config starts healthy.

        Given a fresh RouteState wrapping a direct RouteConfig,
        When constructed,
        Then enabled mirrors config, quarantine_until is None,
            in_use_count is 0, and all timestamps are None.
        """
        config = RouteConfig(id="d", kind="direct")
        state = RouteState(config=config)
        assert state.config is config
        assert state.enabled is True
        assert state.quarantine_until is None
        assert state.in_use_count == 0
        assert state.last_pick_at is None
        assert state.last_handshake_429_at is None
        assert state.last_close_1015_at is None


class TestRouteSelection:
    """Tests for RouteSelection structural defaults."""

    def test_selection_defaults_is_fallback_false(self) -> None:
        """Spec — RouteSelection.is_fallback defaults to False.

        Given a RouteSelection with only state supplied,
        When constructed,
        Then is_fallback is False.
        """
        config = RouteConfig(id="d", kind="direct")
        state = RouteState(config=config)
        selection = RouteSelection(state=state)
        assert selection.is_fallback is False


class TestRouteSnapshot:
    """Tests for the RouteSnapshot value object."""

    def test_snapshot_is_frozen(self) -> None:
        """Spec — RouteSnapshot is a frozen dataclass.

        Given a constructed RouteSnapshot,
        When a field is reassigned,
        Then dataclasses.FrozenInstanceError is raised.
        """
        snap = RouteSnapshot(
            id="d",
            kind="direct",
            proxy_url=None,
            priority=0,
            enabled=True,
            quarantine_until=None,
            in_use_count=0,
            last_handshake_429_at=None,
            last_close_1015_at=None,
        )
        with pytest.raises(dataclasses.FrozenInstanceError):
            snap.id = "e"
