"""Tests for the centralized plan-params order-type reader (#156)."""

from snapper.application.plans.params import core_order_type_from_plan_params


class TestCoreOrderTypeFromPlanParams:
    """CORE-first reading with legacy wire-vocabulary fallback."""

    def test_order_type_param_is_preferred(self) -> None:
        """The CORE order_type param wins over any legacy param.

        Given: params carrying both order_type and a contradictory
            legacy venue_order_type,
        When: the helper reads them,
        Then: the CORE param is returned untouched.
        """
        params = {"order_type": "stop", "venue_order_type": "limit"}
        assert core_order_type_from_plan_params(params) == "stop"

    def test_legacy_wire_value_normalizes_to_core(self) -> None:
        """A pre-#156 plan with only venue_order_type stays cancellable.

        Given: params holding the legacy wire vocabulary value,
        When: the helper reads them,
        Then: the value is normalized to CORE through the reverse map.
        """
        assert core_order_type_from_plan_params({"venue_order_type": "stop-loss"}) == "stop"
        assert (
            core_order_type_from_plan_params({"venue_order_type": "stop-loss-limit"})
            == "stop_limit"
        )

    def test_legacy_market_and_limit_pass_through(self) -> None:
        """Wire values that coincide with CORE map to themselves.

        Given: legacy params with market/limit venue_order_type,
        When: the helper reads them,
        Then: the identical CORE value comes back.
        """
        assert core_order_type_from_plan_params({"venue_order_type": "market"}) == "market"
        assert core_order_type_from_plan_params({"venue_order_type": "limit"}) == "limit"

    def test_unmapped_legacy_value_passes_through_unchanged(self) -> None:
        """An unknown wire value is surfaced, never silently guessed.

        Given: legacy params with a wire value outside the reverse map,
        When: the helper reads them,
        Then: the raw value passes through for downstream rejection.
        """
        assert (
            core_order_type_from_plan_params({"venue_order_type": "trailing-stop"})
            == "trailing-stop"
        )

    def test_absent_params_default_to_market(self) -> None:
        """No order-type params at all keeps the pre-existing default.

        Given: params with neither order_type nor venue_order_type
            (or non-string junk in both),
        When: the helper reads them,
        Then: the historical 'market' default is returned.
        """
        assert core_order_type_from_plan_params({}) == "market"
        assert core_order_type_from_plan_params({"order_type": 7, "venue_order_type": ""}) == (
            "market"
        )
