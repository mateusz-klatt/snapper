"""Order-entry capability guard shared by all submit routes.

Single point of enforcement for the ``can_trade`` predicate on every
REST submit handler (``order_routes``, ``execution_plan_routes``
``trailing_stop_routes``). Routes MUST call
``require_tradable(repo, identifier, exchange, as_of)`` before
inserting a trade command so that market-data-only instruments
(Kraken FCM / TradFi index futures being the driver, but
applicable to any ``SymbolExchangeCapability(can_trade=False)`` row)
fail submission with a structured 422 response rather than reaching
the coordinator / trade-service path.
The helper is deliberately thin: it owns only the identifier-to-native
translation and the error-body shape. The authoritative ``can_trade``
check itself lives in
``snapper.infrastructure.symbols.functions.is_tradeable``, which reads
``SymbolExchangeCapability.can_trade`` via the DB mapper cache
(default-deny on missing capability rows). Routes that already resolve
an instrument to a UUID before the guard runs simply pass the UUID in
routes that still carry a native symbol in the ``instrument_public_id``
field pass that string in verbatim and the guard short-circuits
the DB lookup.
Do NOT use ``Repository.get_instrument_order_capabilities`` here — that
method inspects ``InstrumentOrderCapability`` rows (order-feature
metadata like limit types and TIF), which are a different capability
model from ``SymbolExchangeCapability.can_trade``.
"""

from datetime import datetime
from uuid import UUID

from fastapi import HTTPException
from fastapi import status
from loguru import logger

from snapper.data.repository import Repository
from snapper.infrastructure.symbols.functions import is_tradeable

_ERROR_CODE_MARKET_DATA_ONLY = "instrument_market_data_only"
_ERROR_CODE_UNKNOWN_INSTRUMENT = "unknown_instrument"


def _is_uuid_shape(identifier: str) -> bool:
    """Detect whether ``identifier`` is formatted as a UUID.

    UUID detection uses ``uuid.UUID`` strict parsing so that native
    symbols that happen to contain hex characters + dashes do not
    accidentally trigger the DB lookup path; the contract stays
    defensive even though such symbols are not currently expected.
    """
    try:
        UUID(identifier)
    except (ValueError, TypeError, AttributeError):
        return False
    return True


async def resolve_native_symbol(
    repo: Repository,
    identifier: str,
    as_of: datetime,
) -> str | None:
    """Translate a REST-route ``identifier`` to a native symbol string.

    Args:
        repo: Active repository handle.
        identifier: Value of the route's ``instrument_public_id`` field.
            Today (per the pre-existing frontend contract at
            ``NewOrderModal.tsx``) this can be either a UUID string
            (backend-resolved instrument_public_id) OR a native symbol
            string passed through verbatim. The guard must tolerate
            both.
        as_of: Temporal snapshot for the Instrument+Symbol lookup.

    Returns:
        The native symbol string when resolution succeeds, or ``None``
        when the identifier is UUID-shaped but no active instrument or
        symbol row exists for it at ``as_of``. Non-UUID identifiers are
        returned verbatim (they are already native per the existing
        frontend contract).
    """
    if not _is_uuid_shape(identifier):
        return identifier
    return await repo.get_symbol_for_instrument(identifier, as_of=as_of)


async def require_tradable(
    repo: Repository,
    identifier: str,
    exchange: str,
    as_of: datetime,
) -> None:
    """Raise ``HTTPException(422)`` when the target instrument is not tradable.

    Args:
        repo: Active repository handle (used for UUID→native translation).
        identifier: Value of the route's ``instrument_public_id`` field
            — either a backend UUID or a native symbol string.
        exchange: Exchange identifier from the submit body.
        as_of: Temporal snapshot for capability evaluation.

    Raises:
        HTTPException: HTTP 422 with a structured ``detail`` dict when
            the instrument is market-data-only, unknown, or its
            ``SymbolExchangeCapability.can_trade`` row is False.
            ``detail`` shape is ``{error_code, symbol, exchange, reason}``
            so downstream UIs can branch on ``error_code``.
    """
    native_symbol = await resolve_native_symbol(repo, identifier, as_of=as_of)
    if native_symbol is None:
        logger.warning(
            f"capability_guard: unknown instrument '{identifier}' "
            f"on exchange '{exchange}' at {as_of.isoformat()}"
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "error_code": _ERROR_CODE_UNKNOWN_INSTRUMENT,
                "symbol": identifier,
                "exchange": exchange,
                "reason": "no active Instrument/Symbol row found for this identifier",
            },
        )
    if not is_tradeable(native_symbol, exchange):
        logger.info(
            f"capability_guard: rejected market-data-only submit for {native_symbol}/{exchange}"
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "error_code": _ERROR_CODE_MARKET_DATA_ONLY,
                "symbol": native_symbol,
                "exchange": exchange,
                "reason": (
                    "SymbolExchangeCapability.can_trade is False for this "
                    "(symbol, exchange); use an execution-capable instrument"
                ),
            },
        )
