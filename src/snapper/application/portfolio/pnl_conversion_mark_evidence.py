"""Mark event conversions that depended on evidence outside the event requirement walk."""

import math
from dataclasses import replace
from datetime import datetime

from snapper.application.portfolio.fx_rates import currency_pair_key
from snapper.application.portfolio.pnl_fx_replay_state import _FxInstrumentRequirements
from snapper.application.portfolio.pnl_timeline import TimelineAccrual
from snapper.application.portfolio.pnl_timeline import TimelineExecution
from snapper.core.numeric import is_exact_zero
from snapper.core.numeric import is_positive_finite
from snapper.data.repository_types import PnlTimelineAccrualRow
from snapper.data.repository_types import PnlTimelineOpeningExecutionRow


def _event_conversion_uses_mark_evidence(
    requirements: _FxInstrumentRequirements,
    instrument_public_id: str,
    currency: str,
    valuation_ccy: str,
    rate_minute: datetime,
) -> bool:
    """Return whether one foreign conversion was not requested by the event walk.

    Args:
        requirements: Event-produced minutes grouped by instrument and pair.
        instrument_public_id: Instrument whose input is being converted.
        currency: Source currency of the conversion.
        valuation_ccy: Target series currency.
        rate_minute: Exact conversion minute.

    Returns:
        ``True`` when the conversion's pair and minute are absent.
    """
    pair = currency_pair_key(currency, valuation_ccy)
    return rate_minute not in requirements.get(instrument_public_id, {}).get(pair, set())


def _execution_with_conversion_mark_evidence(
    converted: TimelineExecution,
    row: PnlTimelineOpeningExecutionRow,
    price_currency: str | None,
    requirements: _FxInstrumentRequirements,
    valuation_ccy: str,
) -> TimelineExecution:
    """Stamp fee or price conversions not requested by the event walk.

    Args:
        converted: Engine execution produced by the unchanged converter.
        row: Source repository execution row.
        price_currency: Proven execution-price denomination, when available.
        requirements: Event-produced minutes grouped by instrument and pair.
        valuation_ccy: Target series currency.

    Returns:
        The execution with its exact mark-evidence dependency flag.
    """
    rate_minute = row["timestamp"].replace(second=0, microsecond=0)
    fee_uses_mark = (
        math.isfinite(row["fee"])
        and not is_exact_zero(row["fee"])
        and row["fee_asset"] != valuation_ccy
        and _event_conversion_uses_mark_evidence(
            requirements,
            row["instrument_public_id"],
            row["fee_asset"],
            valuation_ccy,
            rate_minute,
        )
    )
    price_uses_mark = (
        price_currency is not None
        and is_positive_finite(row["price"])
        and price_currency != valuation_ccy
        and _event_conversion_uses_mark_evidence(
            requirements,
            row["instrument_public_id"],
            price_currency,
            valuation_ccy,
            rate_minute,
        )
    )
    return replace(
        converted,
        conversion_uses_mark_evidence=fee_uses_mark or price_uses_mark,
    )


def _accrual_with_conversion_mark_evidence(
    converted: TimelineAccrual,
    row: PnlTimelineAccrualRow,
    requirements: _FxInstrumentRequirements,
    valuation_ccy: str,
) -> TimelineAccrual:
    """Stamp an accrual conversion not requested by the event walk.

    Args:
        converted: Engine accrual produced by the unchanged converter.
        row: Source repository accrual row.
        requirements: Event-produced minutes grouped by instrument and pair.
        valuation_ccy: Target series currency.

    Returns:
        The accrual with its exact mark-evidence dependency flag.
    """
    uses_mark = (
        math.isfinite(row["amount"])
        and not is_exact_zero(row["amount"])
        and row["amount_asset"] != valuation_ccy
        and _event_conversion_uses_mark_evidence(
            requirements,
            row["instrument_public_id"],
            row["amount_asset"],
            valuation_ccy,
            row["accrued_at"].replace(second=0, microsecond=0),
        )
    )
    return replace(converted, conversion_uses_mark_evidence=uses_mark)
