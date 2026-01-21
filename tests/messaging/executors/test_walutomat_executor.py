"""Unit tests for WalutomatOrderExecutor."""

from types import SimpleNamespace
from typing import Any
from typing import cast

import pytest

from snapper.config.app import AppSettings
from snapper.messaging.executors.walutomat import WalutomatOrderExecutor


@pytest.fixture
def mocked_settings(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """Provide mocked AppSettings with Walutomat API credentials."""
    settings = SimpleNamespace(
        walutomat_api_key="api-key",
        walutomat_private_key="-----BEGIN KEY-----...",
        db_url="sqlite://",
        zmq_broker_xpub="tcp://127.0.0.1:5555",
        zmq_broker_xsub="tcp://127.0.0.1:5556",
        master_password="master",
        encryption_salt="salt",
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

    Given a WalutomatOrderExecutor with mocked settings,
    When _create_exchange_client is called,
    Then WalutomatExchangeClient receives api_key, private_key_data, and repository.
    """
    created_kwargs: dict[str, Any] = {}

    class _StubClient:
        def __init__(self, **kwargs: Any) -> None:
            created_kwargs.update(kwargs)

    monkeypatch.setattr(
        "snapper.messaging.executors.walutomat.WalutomatExchangeClient",
        _StubClient,
    )
    monkeypatch.setattr(
        "snapper.messaging.executors.walutomat.get_repository",
        lambda _: None,
    )
    executor = WalutomatOrderExecutor()
    create_client = cast(Any, executor)._create_exchange_client
    client = create_client()
    assert isinstance(client, _StubClient)
    assert created_kwargs == {
        "api_key": mocked_settings.walutomat_api_key,
        "private_key_data": mocked_settings.walutomat_private_key,
        "repository": None,
    }


def test_get_exchange_name_returns_literal(
    mocked_settings: SimpleNamespace,
) -> None:
    """Verify exchange name returns 'walutomat'.

    Given a WalutomatOrderExecutor instance,
    When _get_exchange_name is called,
    Then it returns the literal string 'walutomat'.
    """
    executor = WalutomatOrderExecutor()
    get_exchange_name = cast(Any, executor)._get_exchange_name
    assert get_exchange_name() == "walutomat"


def test_get_default_kwargs_returns_empty_dict(
    mocked_settings: SimpleNamespace,
) -> None:
    """Verify default kwargs is empty (credentials from settings).

    Given the WalutomatOrderExecutor class,
    When get_default_kwargs is called with any settings,
    Then it returns an empty dict (executor uses settings directly).
    """
    defaults = WalutomatOrderExecutor.get_default_kwargs(cast(AppSettings, SimpleNamespace()))
    assert defaults == {}
