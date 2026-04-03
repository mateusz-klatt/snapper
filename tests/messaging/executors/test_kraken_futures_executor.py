"""Unit tests for KrakenFuturesOrderExecutor."""

from types import SimpleNamespace
from typing import Any
from typing import cast

import pytest

from snapper.config.app import AppSettings
from snapper.messaging.executors.kraken_futures import KrakenFuturesOrderExecutor


@pytest.fixture
def mocked_settings(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """Provide mocked AppSettings with Kraken Futures API credentials."""
    settings = SimpleNamespace(
        kraken_futures_api_key="futures-api-key",
        kraken_futures_api_secret="futures-api-secret",
        db_url="sqlite:///:memory:",
        zmq_broker_xpub="tcp://127.0.0.1:5555",
        zmq_broker_xsub="tcp://127.0.0.1:5556",
        master_password="master",
    )
    monkeypatch.setattr(
        "snapper.messaging.executors.base.get_settings",
        lambda: settings,
    )
    return settings


def test_create_exchange_client_uses_executor_settings(
    monkeypatch: pytest.MonkeyPatch,
    mocked_settings: SimpleNamespace,
) -> None:
    """Verify exchange client receives correct API credentials.

    Given a KrakenFuturesOrderExecutor with mocked settings,
    When _create_exchange_client is called,
    Then KrakenFuturesExchangeClient receives api_key, api_secret, and repository.
    """
    created_kwargs: dict[str, Any] = {}

    class _StubClient:
        def __init__(self, **kwargs: Any) -> None:
            created_kwargs.update(kwargs)

    monkeypatch.setattr(
        "snapper.messaging.executors.kraken_futures.KrakenFuturesExchangeClient",
        _StubClient,
    )
    monkeypatch.setattr(
        "snapper.messaging.executors.kraken_futures.get_repository",
        lambda _: None,
    )
    executor = KrakenFuturesOrderExecutor()
    create_client = cast(Any, executor)._create_exchange_client
    client = create_client()
    assert isinstance(client, _StubClient)
    assert created_kwargs == {
        "api_key": mocked_settings.kraken_futures_api_key,
        "api_secret": mocked_settings.kraken_futures_api_secret,
        "repository": None,
    }


def test_get_exchange_name_returns_literal(
    mocked_settings: SimpleNamespace,
) -> None:
    """Verify exchange name returns 'kraken_futures'.

    Given a KrakenFuturesOrderExecutor instance,
    When _get_exchange_name is called,
    Then it returns the literal string 'kraken_futures'.
    """
    executor = KrakenFuturesOrderExecutor()
    get_exchange_name = cast(Any, executor)._get_exchange_name
    assert get_exchange_name() == "kraken_futures"


def test_get_default_parameters_returns_empty_dict(
    mocked_settings: SimpleNamespace,
) -> None:
    """Verify default kwargs is empty (credentials from settings).

    Given the KrakenFuturesOrderExecutor class,
    When get_default_parameters is called with any settings,
    Then it returns an empty dict (executor uses settings directly).
    """
    defaults = KrakenFuturesOrderExecutor.get_default_parameters(
        cast(AppSettings, SimpleNamespace())
    )
    assert defaults == {}
