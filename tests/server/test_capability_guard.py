"""Tests for the order-entry capability guard (``_capability_guard.py``)."""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import patch
from uuid import uuid4

import pytest
from fastapi import HTTPException

from snapper.server._capability_guard import require_tradable
from snapper.server._capability_guard import resolve_native_symbol


@pytest.mark.capability_guard
class TestResolveNativeSymbol:
    """Tests for ``resolve_native_symbol`` UUID/native-symbol dispatch."""

    @pytest.mark.asyncio
    async def test_non_uuid_identifier_is_returned_verbatim(self) -> None:
        """A native symbol string bypasses the DB lookup.

        Given: an identifier like ``"MNQM6-CME"`` that fails ``uuid.UUID`` parse,
        When: ``resolve_native_symbol`` is called,
        Then: the string is returned unchanged and the repo is never touched.
        """
        repo = AsyncMock()
        assert await resolve_native_symbol(repo, "MNQM6-CME", as_of=_NOW) == "MNQM6-CME"
        repo.get_symbol_for_instrument.assert_not_called()

    @pytest.mark.asyncio
    async def test_uuid_identifier_queries_repo(self) -> None:
        """A UUID-shaped identifier is translated through the repository.

        Given: an identifier that parses as UUID and a repo that resolves it,
        When: ``resolve_native_symbol`` is called,
        Then: ``repo.get_symbol_for_instrument`` is awaited with the UUID
            and the native symbol it returns is propagated.
        """
        instrument_uuid = str(uuid4())
        repo = AsyncMock()
        repo.get_symbol_for_instrument = AsyncMock(return_value="MNQM6-CME")
        result = await resolve_native_symbol(repo, instrument_uuid, as_of=_NOW)
        assert result == "MNQM6-CME"
        repo.get_symbol_for_instrument.assert_awaited_once_with(instrument_uuid, as_of=_NOW)

    @pytest.mark.asyncio
    async def test_uuid_identifier_unknown_returns_none(self) -> None:
        """Unknown UUIDs propagate as None without raising.

        Given: a UUID-shaped identifier with no active Instrument row,
        When: ``resolve_native_symbol`` is called,
        Then: None is returned so ``require_tradable`` can raise 422.
        """
        repo = AsyncMock()
        repo.get_symbol_for_instrument = AsyncMock(return_value=None)
        assert await resolve_native_symbol(repo, str(uuid4()), as_of=_NOW) is None


@pytest.mark.capability_guard
class TestRequireTradable:
    """Tests for the ``require_tradable`` HTTP-422 contract."""

    @pytest.mark.asyncio
    async def test_tradable_symbol_passes(self) -> None:
        """Tradable (symbol, exchange) pair returns without raising.

        Given: ``is_tradeable`` returns True,
        When: ``require_tradable`` is called,
        Then: no exception is raised.
        """
        repo = AsyncMock()
        with patch(
            "snapper.server._capability_guard.is_tradeable",
            return_value=True,
        ):
            await require_tradable(repo, "BTC-USD", "kraken", as_of=_NOW)

    @pytest.mark.asyncio
    async def test_market_data_only_raises_422_with_structured_detail(self) -> None:
        """Non-tradable (market-data-only) symbol raises structured 422.

        Given: ``is_tradeable`` returns False,
        When: ``require_tradable`` is called,
        Then: ``HTTPException(status_code=422)`` is raised with a detail
            dict containing ``error_code=instrument_market_data_only`` +
            the symbol/exchange/reason for the frontend to branch on.
        """
        repo = AsyncMock()
        with (
            patch(
                "snapper.server._capability_guard.is_tradeable",
                return_value=False,
            ),
            pytest.raises(HTTPException) as exc_info,
        ):
            await require_tradable(repo, "MNQM6-CME", "kraken_equities", as_of=_NOW)
        assert exc_info.value.status_code == 422
        detail = exc_info.value.detail
        assert isinstance(detail, dict)
        assert detail["error_code"] == "instrument_market_data_only"
        assert detail["symbol"] == "MNQM6-CME"
        assert detail["exchange"] == "kraken_equities"
        assert "can_trade" in detail["reason"]

    @pytest.mark.asyncio
    async def test_unknown_uuid_raises_422_with_structured_detail(self) -> None:
        """Unknown UUID-shaped identifier raises ``unknown_instrument`` 422.

        Given: a UUID identifier that the repo cannot resolve,
        When: ``require_tradable`` is called,
        Then: ``HTTPException(status_code=422)`` is raised with
            ``error_code=unknown_instrument`` so the caller receives a
            distinct signal from the market-data-only path.
        """
        instrument_uuid = str(uuid4())
        repo = AsyncMock()
        repo.get_symbol_for_instrument = AsyncMock(return_value=None)
        with pytest.raises(HTTPException) as exc_info:
            await require_tradable(repo, instrument_uuid, "kraken", as_of=_NOW)
        assert exc_info.value.status_code == 422
        detail = exc_info.value.detail
        assert isinstance(detail, dict)
        assert detail["error_code"] == "unknown_instrument"
        assert detail["symbol"] == instrument_uuid

    @pytest.mark.asyncio
    async def test_tradable_uuid_translation_then_pass(self) -> None:
        """UUID identifier is translated to native then checked.

        Given: a UUID identifier that translates to ``BTC-USD`` and
            ``is_tradeable`` returns True for that native symbol,
        When: ``require_tradable`` is called,
        Then: no exception is raised; the repo translation is awaited once.
        """
        instrument_uuid = str(uuid4())
        repo = AsyncMock()
        repo.get_symbol_for_instrument = AsyncMock(return_value="BTC-USD")
        with patch(
            "snapper.server._capability_guard.is_tradeable",
            return_value=True,
        ) as mock_is_tradeable:
            await require_tradable(repo, instrument_uuid, "kraken", as_of=_NOW)
        repo.get_symbol_for_instrument.assert_awaited_once_with(instrument_uuid, as_of=_NOW)
        mock_is_tradeable.assert_called_once_with("BTC-USD", "kraken")


_NOW = datetime(2026, 4, 21, 12, 0, 0, tzinfo=UTC)
