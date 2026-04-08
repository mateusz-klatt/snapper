"""Unit tests for KrakenOrderExecutor."""

from types import SimpleNamespace
from typing import Any
from typing import cast

import pytest

from snapper.config.app import AppSettings
from snapper.messaging.executors.kraken import KrakenOrderExecutor


@pytest.fixture
def mocked_settings(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """Provide mocked AppSettings with Kraken API credentials."""
    settings = SimpleNamespace(
        kraken_api_key="api-key",
        kraken_api_secret="api-secret",
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


@pytest.fixture
def wallet_credential_envelope() -> SimpleNamespace:
    """Phase 0c: per-wallet credential envelope used by tests below."""
    return SimpleNamespace(
        api_key_value="wallet-kraken-public-id",
        api_secret_value="wallet-kraken-signing-blob",
    )


def test_create_exchange_client_uses_executor_settings(
    monkeypatch: pytest.MonkeyPatch,
    mocked_settings: SimpleNamespace,
) -> None:
    """Verify exchange client receives correct API credentials.

    Given a KrakenOrderExecutor with mocked settings,
    When _create_exchange_client is called,
    Then KrakenExchangeClient receives api_key, api_secret, and repository.
    """
    created_kwargs: dict[str, Any] = {}

    class _StubClient:
        def __init__(self, **kwargs: Any) -> None:
            created_kwargs.update(kwargs)

    monkeypatch.setattr(
        "snapper.messaging.executors.kraken.KrakenExchangeClient",
        _StubClient,
    )
    monkeypatch.setattr(
        "snapper.messaging.executors.kraken.get_repository",
        lambda _: None,
    )
    executor = KrakenOrderExecutor()
    create_client = cast(Any, executor)._create_exchange_client
    client = create_client()
    assert isinstance(client, _StubClient)
    assert created_kwargs == {
        "api_key": mocked_settings.kraken_api_key,
        "api_secret": mocked_settings.kraken_api_secret,
        "repository": None,
    }


def test_get_exchange_name_returns_literal(
    mocked_settings: SimpleNamespace,
) -> None:
    """Verify exchange name returns 'kraken'.

    Given a KrakenOrderExecutor instance,
    When _get_exchange_name is called,
    Then it returns the literal string 'kraken'.
    """
    executor = KrakenOrderExecutor()
    get_exchange_name = cast(Any, executor)._get_exchange_name
    assert get_exchange_name() == "kraken"


def test_get_default_parameters_advertises_wallet_public_id(
    mocked_settings: SimpleNamespace,
) -> None:
    """Phase 0c: default kwargs advertises wallet_public_id.

    Given the KrakenOrderExecutor class,
    When ``get_default_parameters`` is called,
    Then it returns ``{"wallet_public_id": ""}`` so the process
        launcher knows the parameter exists for per-wallet spawning.
    """
    defaults = KrakenOrderExecutor.get_default_parameters(cast(AppSettings, SimpleNamespace()))
    assert defaults == {"wallet_public_id": ""}


def test_create_exchange_client_uses_credential_dict_when_wallet_set(
    monkeypatch: pytest.MonkeyPatch,
    mocked_settings: SimpleNamespace,
    wallet_credential_envelope: SimpleNamespace,
) -> None:
    """Phase 0c: per-wallet credentials override AppSettings fallback.

    Given a KrakenOrderExecutor with a non-empty wallet_public_id and
        a populated ``self._credentials`` dict,
    When ``_create_exchange_client`` is called,
    Then KrakenExchangeClient receives the per-wallet api_key/secret
        from the credential dict, NOT the legacy AppSettings values.
    """
    created_kwargs: dict[str, Any] = {}

    class _StubClient:
        def __init__(self, **kwargs: Any) -> None:
            created_kwargs.update(kwargs)

    monkeypatch.setattr(
        "snapper.messaging.executors.kraken.KrakenExchangeClient",
        _StubClient,
    )
    monkeypatch.setattr(
        "snapper.messaging.executors.kraken.get_repository",
        lambda _: None,
    )
    executor = KrakenOrderExecutor(wallet_public_id="019d5a8b3c7d4e5f")
    cast(Any, executor)._credentials = {
        "api_key": wallet_credential_envelope.api_key_value,
        "api_secret": wallet_credential_envelope.api_secret_value,
    }
    client = cast(Any, executor)._create_exchange_client()
    assert isinstance(client, _StubClient)
    assert created_kwargs == {
        "api_key": wallet_credential_envelope.api_key_value,
        "api_secret": wallet_credential_envelope.api_secret_value,
        "repository": None,
    }
