"""Unit tests for ZondaOrderExecutor."""

from types import SimpleNamespace
from typing import Any
from typing import cast

import pytest

from snapper.config.app import AppSettings
from snapper.messaging.executors.zonda import ZondaOrderExecutor


@pytest.fixture
def mocked_settings(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """Provide mocked AppSettings (post-0c: no per-exchange credential properties)."""
    settings = SimpleNamespace(
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
        api_key_value="wallet-zonda-public-id",
        api_secret_value="wallet-zonda-signing-blob",
    )


def test_create_exchange_client_uses_injected_credentials(
    monkeypatch: pytest.MonkeyPatch,
    mocked_settings: SimpleNamespace,
    wallet_credential_envelope: SimpleNamespace,
) -> None:
    """Verify exchange client receives credentials from ``self._credentials``.

    Given: A ``ZondaOrderExecutor`` with an injected ``_credentials``
        envelope containing ``api_key`` / ``api_secret``,
    When: ``_create_exchange_client`` is called,
    Then: ``ZondaExchangeClient`` is constructed with the injected
        credentials — post-0c cleanup removed the legacy
        ``AppSettings.zonda_api_key`` fallback.
    """
    created_kwargs: dict[str, Any] = {}

    class _StubClient:
        def __init__(self, **kwargs: Any) -> None:
            created_kwargs.update(kwargs)

    monkeypatch.setattr(
        "snapper.messaging.executors.zonda.ZondaExchangeClient",
        _StubClient,
    )
    monkeypatch.setattr(
        "snapper.messaging.executors.zonda.get_repository",
        lambda _: None,
    )
    executor = ZondaOrderExecutor()
    executor._credentials = {
        "api_key": wallet_credential_envelope.api_key_value,
        "api_secret": wallet_credential_envelope.api_secret_value,
    }
    create_client = cast(Any, executor)._create_exchange_client
    client = create_client()
    assert isinstance(client, _StubClient)
    assert created_kwargs == {
        "api_key": wallet_credential_envelope.api_key_value,
        "api_secret": wallet_credential_envelope.api_secret_value,
        "repository": None,
    }


def test_create_exchange_client_raises_without_credentials(
    monkeypatch: pytest.MonkeyPatch,
    mocked_settings: SimpleNamespace,
) -> None:
    """Post-0c: no credentials means fail-fast RuntimeError.

    Given: A ``ZondaOrderExecutor`` instantiated without a
        wallet_public_id and ``self._credentials`` still ``None``,
    When: ``_create_exchange_client`` is called,
    Then: A ``RuntimeError`` is raised with an actionable message.
    """
    monkeypatch.setattr(
        "snapper.messaging.executors.zonda.get_repository",
        lambda _: None,
    )
    executor = ZondaOrderExecutor()
    create_client = cast(Any, executor)._create_exchange_client
    with pytest.raises(RuntimeError, match="credentials not resolved"):
        create_client()


def test_get_exchange_name_returns_literal(
    mocked_settings: SimpleNamespace,
) -> None:
    """Verify exchange name returns 'zonda'.

    Given a ZondaOrderExecutor instance,
    When _get_exchange_name is called,
    Then it returns the literal string 'zonda'.
    """
    executor = ZondaOrderExecutor()
    get_exchange_name = cast(Any, executor)._get_exchange_name
    assert get_exchange_name() == "zonda"


def test_get_default_parameters_advertises_wallet_public_id(
    mocked_settings: SimpleNamespace,
) -> None:
    """Phase 0c: default kwargs advertises wallet_public_id.

    Given the ZondaOrderExecutor class,
    When ``get_default_parameters`` is called,
    Then it returns ``{"wallet_public_id": ""}``.
    """
    defaults = ZondaOrderExecutor.get_default_parameters(cast(AppSettings, SimpleNamespace()))
    assert defaults == {"wallet_public_id": ""}


def test_create_exchange_client_uses_credential_dict_when_wallet_set(
    monkeypatch: pytest.MonkeyPatch,
    mocked_settings: SimpleNamespace,
    wallet_credential_envelope: SimpleNamespace,
) -> None:
    """Phase 0c: per-wallet credentials override AppSettings fallback.

    Given a ZondaOrderExecutor with a non-empty wallet_public_id and
        a populated ``self._credentials`` dict,
    When ``_create_exchange_client`` is called,
    Then ZondaExchangeClient receives the per-wallet api_key/secret
        from the credential dict.
    """
    created_kwargs: dict[str, Any] = {}

    class _StubClient:
        def __init__(self, **kwargs: Any) -> None:
            created_kwargs.update(kwargs)

    monkeypatch.setattr(
        "snapper.messaging.executors.zonda.ZondaExchangeClient",
        _StubClient,
    )
    monkeypatch.setattr(
        "snapper.messaging.executors.zonda.get_repository",
        lambda _: None,
    )
    executor = ZondaOrderExecutor(wallet_public_id="019d5a8b3c7d4e5f")
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
