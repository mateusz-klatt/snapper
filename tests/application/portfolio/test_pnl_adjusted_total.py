"""Tests for the bounded adjusted-total flow publication fallback (5A.18)."""

import math
import random
import sys

import pytest

from snapper.application.portfolio.exact_sum import exact_units_of
from snapper.application.portfolio.exact_sum import faithful_roundings
from snapper.application.portfolio.exact_sum import project_units
from snapper.application.portfolio.pnl_adjusted_total import C_MAX
from snapper.application.portfolio.pnl_adjusted_total import K_ABSORBER
from snapper.application.portfolio.pnl_adjusted_total import K_TOTAL
from snapper.application.portfolio.pnl_adjusted_total import AdjustedFlowProjection
from snapper.application.portfolio.pnl_adjusted_total import adjust_flow_family

_FIXTURE_TOTAL = (
    -16187040773089496689807813778615044681873555847504026964709744282801324220636663165141420382999691947667737329626062877669405702741424182330992699763554396228386556886607244170576859028906793708002794870342794738304763884267161140975065380223652479163254244609630524029572230521452066859450989515298031448105643567097053184
)
_FIXTURE_INST = (
    5361476197134840320096735317440546918520996399417344764382578331621090648590857901786894243669468614905325299253947376625787145163081905307946713932148370702392918455230133447041578288870780239940661915775715845443631879060247800307003540034719339353640155446539419963913051893705241878671894422578636794903279211867275264,
    -112164005775907702666686220246966565620777309509136256407691393787668115464736441634535938865555355429056828434059161354702954508170558866159192566833205993835219791543018946787285655173245396494637753481040334352474621014061538369487798964455533222381259743213793472643126434325689830651866824453359369815080251510717677568,
    90615488805683365656781671150910974020382757262214884678599071173245700595508920567607624238886194866483765805179151100407761660266052778520253153137503226904440316201181569169667217855467822546694296694921823768726225250734129428205730044197161403864365343157623528649641151910532521913743940515482701572071328731753349120,
)
_FIXTURE_ATTR = (
    -102487506302311749445472305063199727752442905566769588598069958233747763487429829947722515539591971378672815641467717170021876982787320767818859678549992612942370688053644763016454268763561576421322763633113261963003452712972741387650513242708956567096885611408519695897324066021364506479756910441762607296829607598792638464,
    86300465529222252755664491284584683070569349719265561633360213950946439266793166782581095156592279431005078311841654292352471280045896585487866978786438216713984131167037518845877409734654782713319968762770467224698688828705580246675447862485304087933631366798889171867751835499912439620305920926464575848723964031695585280,
)


def _fixture_inputs() -> (
    tuple[int, tuple[tuple[str, int], ...], tuple[tuple[tuple[str, str | None], int], ...]]
):
    """Return the exact-unit PLN fixture inputs in canonical key order."""
    instruments = tuple((f"inst-{index}", units) for index, units in enumerate(_FIXTURE_INST))
    attributions = (
        (("manual", None), _FIXTURE_ATTR[0]),
        (("plan", "dca"), _FIXTURE_ATTR[1]),
    )
    return _FIXTURE_TOTAL, instruments, attributions


def _assert_dimension_invariants[KeyT](
    buckets: tuple[tuple[KeyT, int], ...],
    published: object,
    absorber_key: object,
    is_absorber_dim: bool,
    u_units: int,
) -> None:
    """Assert exact non-absorber faithfulness and ULP-bounded absorber movement."""
    assert isinstance(published, dict)
    for key, units in buckets:
        value = published[key]
        if is_absorber_dim and key == absorber_key:
            assert abs(exact_units_of(value) - units) <= K_ABSORBER * u_units
        else:
            assert value in faithful_roundings(units)


def _assert_projection_invariants(
    projection: AdjustedFlowProjection[str, tuple[str, str | None]],
    total_units: int,
    instruments: tuple[tuple[str, int], ...],
    attributions: tuple[tuple[tuple[str, str | None], int], ...],
) -> None:
    """Assert exact float-sum conservation, fixed non-absorbers, and ULP bounds."""
    u_units = exact_units_of(math.ulp(project_units(total_units)))
    assert sum(projection.instruments[key] for key, _ in instruments) == projection.total
    assert sum(projection.attributions[key] for key, _ in attributions) == projection.total
    assert abs(exact_units_of(projection.total) - total_units) <= K_TOTAL * u_units
    assert projection.total_deviation_ulps <= float(K_TOTAL)
    assert projection.absorber_deviation_ulps <= float(K_ABSORBER)
    _assert_dimension_invariants(
        instruments,
        dict(projection.instruments),
        projection.absorber_key,
        projection.absorber_dimension == "instrument",
        u_units,
    )
    _assert_dimension_invariants(
        attributions,
        dict(projection.attributions),
        projection.absorber_key,
        projection.absorber_dimension == "attribution",
        u_units,
    )


def test_fixture_publishes_adjusted_total_with_expected_deviations() -> None:
    """Publish the PLN fixture exactly as specified by section 4.1.

    Given: The verbatim exact-unit PLN realized family whose faithful float sets
        are disjoint.
    When: ``adjust_flow_family`` evaluates the family.
    Then: It publishes ``-0.07997460753815056`` with ``inst-0`` at
        ``0.026489211999998978``, expected ULP deviations, and exact float sums
        in both dimensions.
    """
    total_units, instruments, attributions = _fixture_inputs()
    projection = adjust_flow_family(total_units, instruments, attributions)
    assert projection is not None
    assert projection.total == -0.07997460753815056
    u_units = exact_units_of(math.ulp(project_units(total_units)))
    assert u_units == exact_units_of(2.0**-56)
    expected_tot_dev = abs(exact_units_of(-0.07997460753815056) - total_units) / u_units
    expected_abs_dev = abs(exact_units_of(0.026489211999998978) - _FIXTURE_INST[0]) / u_units
    assert projection.total_deviation_ulps == expected_tot_dev
    assert projection.total_deviation_ulps == pytest.approx(1.85, abs=0.01)
    assert projection.absorber_dimension == "instrument"
    assert projection.absorber_key == "inst-0"
    assert projection.instruments["inst-0"] == 0.026489211999998978
    assert projection.absorber_deviation_ulps == expected_abs_dev
    assert projection.absorber_deviation_ulps == pytest.approx(1.77, abs=0.01)
    _assert_projection_invariants(projection, total_units, instruments, attributions)


def test_refuses_when_total_deviation_exceeds_eight_ulps() -> None:
    """Refuse when every coarser candidate deviates by more than eight ULPs.

    Given: A coarser attribution dimension whose sums are 16 ULPs from exact.
    When: ``adjust_flow_family`` evaluates candidates within ``K_TOTAL = 8``.
    Then: It fails closed and returns ``None``.
    """
    u_units = exact_units_of(math.ulp(1.0))
    a0 = exact_units_of(32.0) + 16 * u_units
    a1 = exact_units_of(-31.0)
    i0 = exact_units_of(0.25) + 16 * u_units
    i1 = exact_units_of(0.75)
    assert (
        adjust_flow_family(
            a0 + a1,
            (("i0", i0), ("i1", i1)),
            (("a0", a0), ("a1", a1)),
        )
        is None
    )


def test_refuses_when_absorber_deviation_exceeds_four_ulps() -> None:
    """Refuse when the only candidate within 8u forces an absorber above 4u.

    Given: One total candidate at 6u and an absorber whose valid sum exceeds 4u.
    When: ``adjust_flow_family`` searches non-absorber combinations.
    Then: It rejects the over-shifted absorber and returns ``None``.
    """
    u = math.ulp(1.0)
    u_units = exact_units_of(u)
    a0 = exact_units_of(16.0) + 6 * u_units
    a1 = exact_units_of(-15.0)
    i0 = exact_units_of(0.25) + exact_units_of(0.125 * u)
    i1 = exact_units_of(0.75) + 6 * u_units - exact_units_of(0.125 * u)
    assert (
        adjust_flow_family(
            a0 + a1,
            (("i0", i0), ("i1", i1)),
            (("a0", a0), ("a1", a1)),
        )
        is None
    )


def test_refuses_when_coarser_or_finer_exceeds_combination_cap() -> None:
    """Fail closed above ``2**C_MAX`` combinations in either dimension.

    Given: Nine inexact buckets in the coarser dimension, or nine inexact
        non-absorber buckets in the finer dimension.
    When: ``adjust_flow_family`` checks the combination cap ``C_MAX = 8``.
    Then: Both cases return ``None`` without enumerating.
    """
    u_units = exact_units_of(math.ulp(1.0))
    nine_inexact = tuple((f"k-{idx}", exact_units_of(2.0) + 1) for idx in range(C_MAX + 1))
    ten_inexact = tuple((f"k-{idx}", exact_units_of(0.25) + 1) for idx in range(C_MAX + 2))
    total_coarser = sum(units for _, units in nine_inexact)
    assert (
        adjust_flow_family(
            total_coarser,
            (("i0", exact_units_of(0.25) + 1), ("i1", total_coarser - exact_units_of(0.25) - 1)),
            nine_inexact,
        )
        is None
    )
    total_finer = sum(units for _, units in ten_inexact)
    assert (
        adjust_flow_family(
            total_finer,
            ten_inexact,
            (
                ("a0", total_finer + exact_units_of(8.0) - u_units),
                ("a1", -exact_units_of(8.0) + u_units),
            ),
        )
        is None
    )


def test_refuses_when_finer_dimension_has_only_exact_or_zero_buckets() -> None:
    """Refuse when the finer dimension contains no nonzero inexact absorber.

    Given: A coarser instrument dimension and a finer attribution dimension
        containing only exact and zero buckets.
    When: ``adjust_flow_family`` selects an absorber.
    Then: No absorber is eligible and it returns ``None``.
    """
    u_units = exact_units_of(math.ulp(1.0))
    i0 = exact_units_of(16.0) + u_units
    i1 = exact_units_of(-15.0) - u_units
    assert (
        adjust_flow_family(
            exact_units_of(1.0),
            (("i0", i0), ("i1", i1)),
            (("a0", exact_units_of(1.0)), ("a1", 0)),
        )
        is None
    )


@pytest.mark.parametrize(
    ("total_units", "instruments", "attributions"),
    [
        (2 * exact_units_of(sys.float_info.max), (("i0", 0),), (("a0", 0),)),
        (0, (("i0", 0), ("i0", 0)), (("a0", 0),)),
        (1, (("i0", 0),), (("a0", 1),)),
        (
            0,
            (
                ("i0", 2 * exact_units_of(sys.float_info.max)),
                ("i1", -2 * exact_units_of(sys.float_info.max)),
            ),
            (("a0", 0),),
        ),
        (0, (("i0", 0),), (("a0", 0), ("a0", 0))),
        (0, (("i0", 1), ("i1", -1)), (("a0", 0),)),
        (0, (("i0", 0),), (("a0", 1), ("a1", -1))),
    ],
)
def test_refuses_invalid_or_all_zero_dimensions(
    total_units: int,
    instruments: tuple[tuple[str, int], ...],
    attributions: tuple[tuple[str, int], ...],
) -> None:
    """Reject non-finite, duplicate, non-conserving, or all-zero dimensions.

    Given: Structural violations or a wholly zero dimension.
    When: ``adjust_flow_family`` validates its inputs.
    Then: It fails closed and returns ``None``.
    """
    assert adjust_flow_family(total_units, instruments, attributions) is None


def test_coarser_tie_breaks_to_attribution_and_finer_tie_breaks_by_key_order() -> None:
    """Break coarser ULP ties toward attribution and absorber ties by key order.

    Given: Equal minimum nonzero ULP across both dimensions, and two finest-ULP
        nonzero inexact instrument buckets competing for the absorber role.
    When: ``adjust_flow_family`` runs.
    Then: Attribution is chosen as coarser, and the first instrument key becomes
        the absorber.
    """
    u = math.ulp(1.0)
    total_units = exact_units_of(1.0) + exact_units_of(0.5 * u)
    i0 = exact_units_of(0.25) + 1
    i1 = exact_units_of(0.25) + exact_units_of(0.5 * u) - 1
    i2 = exact_units_of(0.5)
    a0 = exact_units_of(0.25) + 1
    a1 = total_units - a0
    projection = adjust_flow_family(
        total_units,
        (("i0", i0), ("i1", i1), ("i2", i2)),
        (("a0", a0), ("a1", a1)),
    )
    assert projection is not None
    assert projection.absorber_dimension == "instrument"
    assert projection.absorber_key == "i0"
    assert (
        projection.instruments["i0"] + projection.instruments["i1"] + projection.instruments["i2"]
        == projection.total
    )


def test_instrument_coarser_selects_attribution_absorber() -> None:
    """Choose attribution as the absorber dimension when instruments are coarser.

    Given: Instruments with larger minimum nonzero ULP than attributions.
    When: ``adjust_flow_family`` solves the constellation.
    Then: ``absorber_dimension`` is ``"attribution"`` and attribution sums match.
    """
    u = math.ulp(1.0)
    u_units = exact_units_of(u)
    i0 = exact_units_of(8.0) + 3 * u_units + 1
    i1 = exact_units_of(-7.0)
    total_units = i0 + i1
    a0 = exact_units_of(0.25) + 3 * u_units + 1
    a1 = exact_units_of(16.0) + 8 * u_units
    a2 = exact_units_of(-15.25 - 8.0 * u)
    projection = adjust_flow_family(
        total_units,
        (("i0", i0), ("i1", i1)),
        ((("manual", None), a0), (("plan", "dca"), a1), (("system", "s"), a2)),
    )
    assert projection is not None
    assert projection.absorber_dimension == "attribution"
    assert projection.absorber_key == ("manual", None)
    assert sum(projection.instruments.values()) == projection.total
    assert sum(projection.attributions.values()) == projection.total


def test_instrument_coarser_returns_none_when_attribution_refuses() -> None:
    """Return ``None`` when attribution is finer and fails the absorber check.

    Given: Coarser instruments and finer attributions containing only exact values.
    When: ``adjust_flow_family`` executes the attribution-absorber path.
    Then: It fails closed and returns ``None``.
    """
    u_units = exact_units_of(math.ulp(1.0))
    assert (
        adjust_flow_family(
            exact_units_of(1.0),
            (("i0", exact_units_of(16.0) + u_units), ("i1", exact_units_of(-15.0) - u_units)),
            (("a0", exact_units_of(1.0)),),
        )
        is None
    )


def test_skips_first_candidate_when_absorber_exceeds_bound_and_handles_candidate_ties() -> None:
    """Continue to later coarser candidates when the first violates ``K_ABSORBER``.

    Given: Two coarser candidates within 8u where the closer candidate forces an
        absorber deviation above 4u while the second candidate fits within 4u, and
        a second constellation where two candidates tie on ULP distance.
    When: ``adjust_flow_family`` evaluates candidates in ``(|dev|, cand)`` order.
    Then: It skips the first candidate and selects the valid second candidate, and
        breaks candidate distance ties in favor of the smaller float total.
    """
    u = math.ulp(1.0)
    u_units = exact_units_of(u)
    a0 = exact_units_of(8.0) + 3 * u_units + 1
    a1 = exact_units_of(-7.0)
    tot = a0 + a1
    i0 = exact_units_of(0.25) + 3 * u_units + 1
    i1 = exact_units_of(16.0) + 8 * u_units
    i2 = exact_units_of(-15.25 - 8.0 * u)
    skipped = adjust_flow_family(
        tot,
        (("i0", i0), ("i1", i1), ("i2", i2)),
        (("a0", a0), ("a1", a1)),
    )
    assert skipped is not None
    assert skipped.total == 1.0 + 8.0 * u

    tie_a0 = exact_units_of(8.0) + 4 * u_units
    tie_a1 = exact_units_of(-7.0)
    tie_i0 = exact_units_of(0.25) + 4 * u_units - 1
    tie_i1 = exact_units_of(0.75) + 1
    tied = adjust_flow_family(
        tie_a0 + tie_a1,
        (("i0", tie_i0), ("i1", tie_i1)),
        (("a0", tie_a0), ("a1", tie_a1)),
    )
    assert tied is not None
    assert tied.total == 1.0


def test_handles_overflowing_sums_duplicate_totals_and_max_float_nextafter() -> None:
    """Exercise overflow filtering, duplicate coarser totals, and max-float steps.

    Given: Constellations with overflowing non-absorber/coarser sums, duplicate
        coarser sums across combinations, and a ``base`` at ``sys.float_info.max``.
    When: ``adjust_flow_family`` filters candidates and scans ``nextafter`` steps.
    Then: Non-finite intermediate sums are safely skipped, duplicate coarser totals
        preserve nearest-first values, and ``nextafter`` stops at infinity.
    """
    m = sys.float_info.max
    mu = exact_units_of(m)
    m_ulp = exact_units_of(math.ulp(m))
    assert (
        adjust_flow_family(
            mu,
            (("i0", mu), ("i1", mu), ("i2", -mu)),
            (("a0", mu), ("a1", mu), ("a2", -mu)),
        )
        is None
    )

    u = math.ulp(1.0)
    u_units = exact_units_of(u)
    dup = adjust_flow_family(
        exact_units_of(1.0) + 2 * u_units,
        (("i0", exact_units_of(0.25) + 1), ("i1", exact_units_of(0.75) + 2 * u_units - 1)),
        (("a0", exact_units_of(0.5) + u_units + 1), ("a1", exact_units_of(0.5) + u_units - 1)),
    )
    assert dup is not None
    assert dup.total == 1.0 + 2.0 * u

    a0_m = exact_units_of(0.6 * m)
    a1_m = -exact_units_of(0.6 * m) + m_ulp // 4
    a2_m = mu - m_ulp // 2
    tot_m = a0_m + a1_m + a2_m
    near_max = adjust_flow_family(
        tot_m,
        (("i0", tot_m),),
        (("a0", a0_m), ("a1", a1_m), ("a2", a2_m)),
    )
    assert near_max is not None
    assert near_max.total == m
    assert near_max.instruments["i0"] == m

    assert (
        adjust_flow_family(
            exact_units_of(1.0) + 1,
            (
                ("i0", exact_units_of(1.0) + 1),
                ("i1", a0_m),
                ("i2", a0_m),
                ("i3", -a0_m),
                ("i4", -a0_m),
            ),
            (("a0", exact_units_of(1024.0) + 1), ("a1", exact_units_of(-1023.0))),
        )
        is None
    )


def test_adjust_flow_family_is_deterministic() -> None:
    """Produce bit-identical projections across repeated invocations.

    Given: The PLN fixture exact units.
    When: ``adjust_flow_family`` is invoked repeatedly.
    Then: Both returned :class:`AdjustedFlowProjection` instances are identical.
    """
    total_units, instruments, attributions = _fixture_inputs()
    first = adjust_flow_family(total_units, instruments, attributions)
    second = adjust_flow_family(total_units, instruments, attributions)
    assert first is not None
    assert first == second


def _random_buckets(
    rng: random.Random,
    count: int,
    scale_exp: int,
    target_total: int | None,
) -> list[int]:
    """Generate ``count`` mixed-sign exact-unit buckets conserving an optional total."""
    buckets: list[int] = []
    for idx in range(count - 1):
        if idx % 4 == 0:
            buckets.append(0)
        elif target_total is None and idx % 4 == 1:
            buckets.append(exact_units_of(rng.choice((-1.0, 1.0)) * (2.0 ** (scale_exp - 4))))
        else:
            base = rng.choice((-1.0, 1.0)) * rng.uniform(0.1, 8.0) * (2.0**scale_exp)
            buckets.append(exact_units_of(base) + rng.randint(-15, 15))
    final_sum = (
        exact_units_of(rng.uniform(-0.2, 0.2)) + rng.randint(-7, 7)
        if target_total is None
        else target_total
    )
    buckets.append(final_sum - sum(buckets))
    return buckets


def test_seeded_random_mixed_sign_constellations_preserve_sums_and_bounds() -> None:
    """Verify sum identity, bounds, and determinism across random converted buckets.

    Given: 400 seeded random constellations with 2 to 12 mixed-sign buckets
        including exact doubles and exact zeros under cancellation.
    When: ``adjust_flow_family`` evaluates each constellation.
    Then: Every returned projection conserves ``sum(buckets) == total`` in both
        dimensions, stays within ``K_TOTAL`` and ``K_ABSORBER`` ULPs, keeps
        non-absorbers faithful, and repeats identically.
    """
    rng = random.Random(20261122)
    solved_count = 0
    for trial in range(400):
        scale_exp = rng.randint(-12, 16)
        inst_raw = _random_buckets(rng, rng.randint(2, 12), scale_exp, None)
        total_units = sum(inst_raw)
        attr_raw = _random_buckets(rng, rng.randint(2, 12), scale_exp, total_units)
        instruments = tuple((f"i-{trial}-{idx}", units) for idx, units in enumerate(inst_raw))
        attributions = tuple(
            (("manual", f"s-{idx}") if idx else ("manual", None), units)
            for idx, units in enumerate(attr_raw)
        )
        projection = adjust_flow_family(total_units, instruments, attributions)
        if projection is None:
            continue
        solved_count += 1
        _assert_projection_invariants(projection, total_units, instruments, attributions)
        assert adjust_flow_family(total_units, instruments, attributions) == projection
    assert solved_count > 0
