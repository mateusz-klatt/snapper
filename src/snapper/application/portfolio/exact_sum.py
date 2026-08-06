"""Exact accumulation of IEEE-754 doubles, with non-finite inputs latched.

Every finite double is an exact integer multiple of ``2**-1074``, the smallest
positive subnormal, because ``float.as_integer_ratio()`` always yields a
power-of-two denominator with exponent at most 1074. So a sum of finite doubles
is exactly one Python ``int`` of scaled units, and needs neither
:class:`fractions.Fraction` nor a retained expansion.

Why not :func:`math.fsum`. It raises where the code it would replace degrades
gracefully, and in TWO ways, only one of which is about infinity:

    math.fsum([1.7e308, 1.7e308])   OverflowError: intermediate overflow in fsum
    math.fsum([inf, -inf])          ValueError: -inf + inf in fsum

The first takes FINITE inputs, so no upstream non-finite screen would catch it.
A plain ``+=`` loop and the builtin ``sum`` both answer ``inf`` there, which the
P&L point valuation already recognises and turns into an honest withhold with
reason ``cumulative_non_finite``. Raising instead would convert a withheld
minute into a 500 on the hottest path in the engine — strictly worse than the
defect being fixed. This accumulator therefore never raises: it latches what it
saw and reports it as the corresponding non-finite double.

The latch is deliberately coarse. Once any non-finite term arrives the exact
total is meaningless, so the accumulator stops tracking units and only
remembers enough to answer with the same double the naive loop would have
produced: NaN if a NaN was ever added or if infinities of both signs met, and
otherwise the infinity that was seen.
"""

import math
from dataclasses import dataclass
from dataclasses import field

_SCALE_EXPONENT: int = 1074
"""Binary exponent of the accumulator's unit, the smallest positive subnormal.

``2**-1074`` divides every finite double exactly, which is what makes an integer
count of these units an EXACT representation of any finite sum rather than a
rounded one.
"""

_SCALE_DENOMINATOR: int = 1 << _SCALE_EXPONENT
"""The scale as an exact integer, so projection is one correctly-rounded divide."""

_MIN_SUBNORMAL: float = 5e-324
"""``2**-1074`` as a literal, pinned so the scale cannot silently drift.

Asserted against the exponent by the module's own tests rather than computed
here, so a wrong scale fails loudly at test time instead of quietly halving
every accumulated value.
"""


@dataclass(slots=True)
class ExactSum:
    """A running sum of doubles that is exact while every term is finite.

    Attributes:
        units: Exact total as a count of ``2**-1074`` units, valid only while
            ``saw_nan`` is false and ``infinities`` is empty.
        infinities: The distinct signs of any infinite terms seen, as ``1`` and
            ``-1``. Two opposite signs make the result NaN, matching what the
            naive ``+=`` loop produces.
        saw_nan: Whether any term was NaN, which poisons the result forever.
    """

    units: int = 0
    infinities: set[int] = field(default_factory=set)
    saw_nan: bool = False

    def add(self, value: float) -> None:
        """Accumulate one term, latching it if it is not finite.

        Args:
            value: The term to add. Any double is accepted, including NaN and
                both infinities; none of them raises.
        """
        if math.isnan(value):
            self.saw_nan = True
            return
        if math.isinf(value):
            self.infinities.add(1 if value > 0.0 else -1)
            return
        self.units += _exact_units(value)

    def is_finite(self) -> bool:
        """Report whether the exact total is meaningful.

        Returns:
            Whether every term so far was finite, and therefore whether
            :meth:`exact_units` describes a number that exists.
        """
        return not self.saw_nan and not self.infinities

    def exact_units(self) -> int:
        """Return the exact total in ``2**-1074`` units.

        Only meaningful while :meth:`is_finite` holds; callers that have not
        checked it are asking for a number that does not exist.

        Returns:
            The exact count of scale units accumulated so far.
        """
        return self.units

    def to_float(self) -> float:
        """Project the running total onto a double.

        A finite total is correctly rounded once, here, rather than drifting
        across every ``+=``. A latched total answers exactly what the naive loop
        would have: NaN when a NaN was seen or when both infinities met, and
        otherwise the single infinity seen.

        The projection divides by the scale rather than converting the unit
        count first: one whole unit is ``2**1074`` units, so ``float(units)``
        overflows for any value at all. Integer true division is correctly
        rounded in CPython and accepts operands of any width, so the exact total
        is rounded exactly once. A total beyond the double range overflows that
        division, and the answer there is the same infinity the naive loop
        reaches by overflowing mid-stream.

        Returns:
            The projected total, which may be non-finite.
        """
        if self.saw_nan or len(self.infinities) > 1:
            return math.nan
        if self.infinities:
            return math.inf if 1 in self.infinities else -math.inf
        try:
            return self.units / _SCALE_DENOMINATOR
        except OverflowError:
            return math.inf if self.units > 0 else -math.inf


def _exact_units(value: float) -> int:
    """Convert one FINITE double to its exact count of ``2**-1074`` units.

    Args:
        value: A finite double. Callers must screen non-finite input; both
            ``inf`` and ``nan`` make ``as_integer_ratio`` raise, which is
            precisely the failure this module exists to prevent from reaching
            the engine.

    Returns:
        The exact scaled integer.
    """
    numerator, denominator = value.as_integer_ratio()
    return numerator << (_SCALE_EXPONENT - denominator.bit_length() + 1)
