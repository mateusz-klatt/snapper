"""Unit tests for PolygonGroupedDailyBackfillService."""

from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.updaters.historical.grouped import PolygonGroupedDailyBackfillService
from snapper.config.app import AppSettings


class _PathStub:
    """Test stub for Path object with exists and label."""

    def __init__(self, exists: bool, label: str) -> None:
        self._exists = exists
        self._label = label

    def exists(self) -> bool:
        return self._exists

    def __str__(self) -> str:
        return self._label


class _LoaderStub:
    """Test stub for grouped data loader."""

    def __init__(self) -> None:
        self.fetch_grouped_daily = AsyncMock()
        self._existing: set[tuple[date, str, str]] = set()
        self.recorded_paths: list[_PathStub] = []

    def add_existing(self, target_day: date, market_type: str, locale: str) -> None:
        self._existing.add((target_day, market_type, locale))

    def get_grouped_csv_path(self, target_day: date, market_type: str, locale: str) -> _PathStub:
        exists = (target_day, market_type, locale) in self._existing
        path = _PathStub(exists, f"/{target_day.isoformat()}.csv")
        self.recorded_paths.append(path)
        return path


class _FakeDatetime:
    """Test fake datetime for deterministic tests."""

    def __init__(self, current: datetime) -> None:
        self._current = current

    def now(self, tz: Any = None) -> datetime:
        return self._current


@pytest.fixture
def service_fixture(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[PolygonGroupedDailyBackfillService, _LoaderStub]:
    """Provide configured service and loader stub for testing."""
    settings = SimpleNamespace(
        polygon_api_key="api-key",
        db_url="sqlite:///:memory:",
        zmq_broker_xpub="tcp://127.0.0.1:7501",
        master_password="pwd",
    )
    monkeypatch.setattr(
        "snapper.application.updaters.historical.grouped.get_settings",
        lambda: settings,
    )
    monkeypatch.setattr(
        "snapper.application.updaters.historical.grouped.get_settings_service",
        AsyncMock(return_value=SimpleNamespace()),
    )
    monkeypatch.setattr(
        "snapper.application.updaters.historical.grouped.get_settings_with_service",
        lambda _svc: settings,
    )
    loader = _LoaderStub()
    monkeypatch.setattr(
        "snapper.application.updaters.historical.grouped.PolygonExchangeClient",
        MagicMock,
    )
    monkeypatch.setattr(
        "snapper.application.updaters.historical.grouped.PolygonHistoricalLoader",
        lambda *_args, **_kwargs: loader,
    )
    service_instance = PolygonGroupedDailyBackfillService(days=2, save_csv=True)
    service_instance._loader = cast(Any, loader)
    return service_instance, loader


def test_get_default_parameters_reflects_expected_defaults() -> None:
    """Verify get_default_parameters returns expected default values.

    Given: Dummy settings object,
    When: get_default_parameters called,
    Then: Dictionary with correct default values returned.
    """
    dummy_settings = cast(AppSettings, SimpleNamespace())
    defaults = PolygonGroupedDailyBackfillService.get_default_parameters(dummy_settings)
    assert defaults == {
        "market_type": "crypto",
        "days": 3,
        "locale": "global",
        "save_csv": True,
        "adjusted": True,
    }


@pytest.mark.asyncio
async def test_start_invokes_fetch_for_previous_days(
    monkeypatch: pytest.MonkeyPatch,
    service_fixture: tuple[PolygonGroupedDailyBackfillService, _LoaderStub],
) -> None:
    """Verify start method fetches data for previous days.

    Given: Service configured with days=2 and fixed datetime,
    When: start method called,
    Then: _fetch_day called for each day in range.
    """
    service_instance, _loader = service_fixture
    fixed_now = datetime(2024, 1, 10, 12, tzinfo=UTC)
    monkeypatch.setattr(
        "snapper.application.updaters.historical.grouped.datetime",
        _FakeDatetime(fixed_now),
    )
    fetch_day = AsyncMock()
    monkeypatch.setattr(service_instance, "_fetch_day", fetch_day)
    await service_instance.start()
    expected_calls = [
        (fixed_now.date() - timedelta(days=offset),)
        for offset in range(1, service_instance._days + 2)
    ]
    actual_calls = [call.args for call in fetch_day.call_args_list]
    assert actual_calls == expected_calls


@pytest.mark.asyncio
async def test_start_requires_polygon_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify start raises when Polygon API key missing.

    Given: Settings with empty polygon_api_key,
    When: start method called,
    Then: ValueError raised with appropriate message.
    """
    settings = SimpleNamespace(
        polygon_api_key="",
        db_url="sqlite:///:memory:",
        zmq_broker_xpub="tcp://127.0.0.1:7501",
        master_password="pwd",
    )
    monkeypatch.setattr(
        "snapper.application.updaters.historical.grouped.get_settings",
        lambda: settings,
    )
    monkeypatch.setattr(
        "snapper.application.updaters.historical.grouped.get_settings_service",
        AsyncMock(return_value=SimpleNamespace()),
    )
    monkeypatch.setattr(
        "snapper.application.updaters.historical.grouped.get_settings_with_service",
        lambda _svc: settings,
    )
    service_instance = PolygonGroupedDailyBackfillService(days=1)
    with pytest.raises(ValueError, match="Polygon API key not configured"):
        await service_instance.start()


@pytest.mark.asyncio
async def test_fetch_day_skips_when_csv_exists(
    service_fixture: tuple[PolygonGroupedDailyBackfillService, _LoaderStub],
) -> None:
    """Verify _fetch_day skips when CSV already exists.

    Given: Loader configured with existing CSV for target day,
    When: _fetch_day called for that day,
    Then: fetch_grouped_daily not called.
    """
    service_instance, loader = service_fixture
    target_day = datetime.now(UTC).date() - timedelta(days=1)
    loader.add_existing(target_day, "crypto", "global")
    await service_instance._fetch_day(target_day)
    loader.fetch_grouped_daily.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_day_requests_missing_csv(
    service_fixture: tuple[PolygonGroupedDailyBackfillService, _LoaderStub],
) -> None:
    """Verify _fetch_day fetches when CSV missing.

    Given: Loader configured without existing CSV,
    When: _fetch_day called for target day,
    Then: fetch_grouped_daily called with correct parameters.
    """
    service_instance, loader = service_fixture
    target_day = datetime.now(UTC).date() - timedelta(days=3)
    await service_instance._fetch_day(target_day)
    loader.fetch_grouped_daily.assert_awaited_once_with(
        target_day,
        market_type="crypto",
        locale="global",
        adjusted=True,
        save_csv=True,
    )
