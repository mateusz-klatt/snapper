"""Tests for WebSocket token service and errors."""

import threading
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from unittest.mock import Mock
from unittest.mock import patch

import jwt
import pytest

from snapper.api.auth.errors.ws_token import WsTokenAlreadyUsedError
from snapper.api.auth.errors.ws_token import WsTokenError
from snapper.api.auth.services.ws_token_service import WsTokenService
from snapper.api.auth.services.ws_token_service import compute_sid_hash
from snapper.api.auth.services.ws_token_store import WsTokenStore
from snapper.application.services.settings import SettingsService


@pytest.fixture(autouse=True)
def clear_singleton() -> Any:
    """Provide clean WsTokenService singleton state for each test."""
    WsTokenService.clear_instance()
    yield
    WsTokenService.clear_instance()


def test_compute_sid_hash() -> None:
    """Verify compute_sid_hash returns consistent hash.

    Given: A session ID string,
    When: compute_sid_hash is called multiple times,
    Then: Same hash is returned; different IDs produce different hashes.
    """
    sid = "test-session-id"
    hash1 = compute_sid_hash(sid)
    hash2 = compute_sid_hash(sid)
    assert hash1 == hash2
    assert len(hash1) == 64
    assert hash1 != compute_sid_hash("different-session")


def test_ws_token_service_singleton() -> None:
    """Verify WsTokenService follows singleton pattern.

    Given: Multiple WsTokenService instantiation attempts,
    When: Creating instances,
    Then: All references point to the same instance.
    """
    service1 = WsTokenService()
    service2 = WsTokenService()
    assert service1 is service2


def test_settings_property_uses_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify settings property uses get_settings fallback.

    Given: A WsTokenService without explicit settings,
    When: Accessing settings property,
    Then: Settings are retrieved via get_settings.
    """
    service = WsTokenService()
    sentinel_settings = object()
    monkeypatch.setattr(
        "snapper.api.auth.services.ws_token_service.get_settings",
        lambda: sentinel_settings,
    )
    assert service.settings is sentinel_settings


def test_generate_ws_token() -> None:
    """Verify generate creates valid WS token with payload.

    Given: A WsTokenService with settings configured,
    When: generate is called with user and session ID,
    Then: Token result contains token, expiration, and payload.
    """
    service = WsTokenService()
    mock_settings_service = Mock(spec=SettingsService)
    service.set_settings_service(mock_settings_service)
    result = service.generate(user_id="test_user", session_id="test_session")
    assert result.token
    assert isinstance(result.expires_at, datetime)
    assert result.expires_at > datetime.now(UTC)
    assert result.payload.sub == "test_user"
    assert result.payload.purpose == "ws_connect"
    assert result.payload.jti


def test_generate_ws_token_backdates_iat() -> None:
    """Verify generated tokens tolerate immediate verification clock skew.

    Given: A WsTokenService with a fixed current time,
    When: generate is called,
    Then: The iat claim is one second before the issuance boundary.
    """
    service = WsTokenService()
    mock_settings_service = Mock(spec=SettingsService)
    service.set_settings_service(mock_settings_service)
    issued_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    with patch.object(service, "_now", return_value=issued_at):
        result = service.generate(user_id="test_user", session_id="test_session")
    assert result.payload.iat == int(issued_at.timestamp()) - 1


def test_verify_valid_token() -> None:
    """Verify valid token is accepted.

    Given: A generated WS token,
    When: verify is called with correct subject and session hash,
    Then: Payload is returned with correct claims.
    """
    service = WsTokenService()
    mock_settings_service = Mock(spec=SettingsService)
    service.set_settings_service(mock_settings_service)
    user_id = "test_user"
    session_id = "test_session"
    result = service.generate(user_id=user_id, session_id=session_id)
    sid_hash = compute_sid_hash(session_id)
    payload = service.verify(
        result.token,
        expected_sub=user_id,
        expected_sid_hash=sid_hash,
    )
    assert payload.sub == user_id
    assert payload.sid_hash == sid_hash
    assert payload.purpose == "ws_connect"


def test_verify_token_wrong_subject() -> None:
    """Verify subject mismatch raises WsTokenError.

    Given: A generated WS token,
    When: verify is called with different subject,
    Then: WsTokenError with ws_token_subject_mismatch is raised.
    """
    service = WsTokenService()
    mock_settings_service = Mock(spec=SettingsService)
    service.set_settings_service(mock_settings_service)
    result = service.generate(user_id="user1", session_id="session1")
    sid_hash = compute_sid_hash("session1")
    with pytest.raises(WsTokenError, match="ws_token_subject_mismatch"):
        service.verify(
            result.token,
            expected_sub="different_user",
            expected_sid_hash=sid_hash,
        )


def test_verify_token_wrong_session() -> None:
    """Verify session hash mismatch raises WsTokenError.

    Given: A generated WS token,
    When: verify is called with different session hash,
    Then: WsTokenError with ws_token_session_mismatch is raised.
    """
    service = WsTokenService()
    mock_settings_service = Mock(spec=SettingsService)
    service.set_settings_service(mock_settings_service)
    result = service.generate(user_id="user1", session_id="session1")
    wrong_sid_hash = compute_sid_hash("different_session")
    with pytest.raises(WsTokenError, match="ws_token_session_mismatch"):
        service.verify(
            result.token,
            expected_sub="user1",
            expected_sid_hash=wrong_sid_hash,
        )


def test_verify_token_wrong_purpose() -> None:
    """Verify wrong purpose field raises WsTokenError.

    Given: A token with tampered purpose field,
    When: verify is called,
    Then: WsTokenError with invalid_ws_token_purpose is raised.
    """
    service = WsTokenService()
    mock_settings_service = Mock(spec=SettingsService)
    service.set_settings_service(mock_settings_service)
    result = service.generate(user_id="user1", session_id="session1")
    sid_hash = compute_sid_hash("session1")
    tampered_payload = result.payload.model_copy(update={"purpose": "unexpected"})
    tampered_token = jwt.encode(
        tampered_payload.model_dump(
            exclude={"public_id", "timestamp", "session_id", "sequence_id", "type"},
        ),
        service.settings.auth_secret_key,
        algorithm=service.settings.auth_algorithm,
    )
    with pytest.raises(WsTokenError, match="invalid_ws_token_purpose"):
        service.verify(
            tampered_token,
            expected_sub="user1",
            expected_sid_hash=sid_hash,
        )


def test_verify_invalid_token() -> None:
    """Verify invalid token string raises WsTokenError.

    Given: An invalid token string,
    When: verify is called,
    Then: WsTokenError with invalid_ws_token is raised.
    """
    service = WsTokenService()
    mock_settings_service = Mock(spec=SettingsService)
    service.set_settings_service(mock_settings_service)
    with pytest.raises(WsTokenError, match="invalid_ws_token"):
        service.verify(
            "invalid.token.here",
            expected_sub="user1",
            expected_sid_hash="hash",
        )


def test_token_replay_protection() -> None:
    """Verify used token raises WsTokenAlreadyUsedError.

    Given: A token that has been verified and marked as used,
    When: verify is called again with the same token,
    Then: WsTokenAlreadyUsedError is raised.
    """
    service = WsTokenService()
    mock_settings_service = Mock(spec=SettingsService)
    service.set_settings_service(mock_settings_service)
    user_id = "test_user"
    session_id = "test_session"
    result = service.generate(user_id=user_id, session_id=session_id)
    sid_hash = compute_sid_hash(session_id)
    payload = service.verify(
        result.token,
        expected_sub=user_id,
        expected_sid_hash=sid_hash,
    )
    service.mark_used(payload)
    with pytest.raises(WsTokenAlreadyUsedError, match="ws_token_already_used"):
        service.verify(
            result.token,
            expected_sub=user_id,
            expected_sid_hash=sid_hash,
        )


def test_expired_token() -> None:
    """Verify expired token raises WsTokenError.

    Given: A token generated with past timestamp,
    When: verify is called,
    Then: WsTokenError with invalid_ws_token is raised.
    """
    service = WsTokenService()
    mock_settings_service = Mock(spec=SettingsService)
    service.set_settings_service(mock_settings_service)
    sid_hash = compute_sid_hash("session1")
    past_time = datetime.now(UTC) - timedelta(hours=1)
    with patch.object(service, "_now", return_value=past_time):
        expired_result = service.generate(user_id="user1", session_id="session1")
    with pytest.raises(WsTokenError, match="invalid_ws_token"):
        service.verify(
            expired_result.token,
            expected_sub="user1",
            expected_sid_hash=sid_hash,
        )


def test_expired_token_after_decode() -> None:
    """Verify token with expired exp claim raises WsTokenError.

    Given: A token with expired exp field in payload,
    When: verify is called,
    Then: WsTokenError with ws_token_expired is raised.
    """
    service = WsTokenService()
    mock_settings_service = Mock(spec=SettingsService)
    service.set_settings_service(mock_settings_service)
    user_id = "user1"
    session_id = "session1"
    result = service.generate(user_id=user_id, session_id=session_id)
    sid_hash = compute_sid_hash(session_id)
    expired_payload = result.payload.model_copy(
        update={"exp": int(service._now().timestamp()) - 10}
    )
    with (
        patch(
            "snapper.api.auth.services.ws_token_service.jwt.decode",
            return_value=expired_payload.model_dump(),
        ),
        pytest.raises(WsTokenError, match="ws_token_expired"),
    ):
        service.verify(
            result.token,
            expected_sub=user_id,
            expected_sid_hash=sid_hash,
        )


def test_get_instance() -> None:
    """Verify get_instance returns singleton.

    Given: Multiple calls to get_instance,
    When: Comparing returned instances,
    Then: All references point to the same instance.
    """
    service = WsTokenService.get_instance()
    assert isinstance(service, WsTokenService)
    assert service is WsTokenService.get_instance()


def test_clear_instance() -> None:
    """Verify clear_instance allows new instance creation.

    Given: An existing singleton instance,
    When: clear_instance is called and new instance created,
    Then: New instance is different from previous one.
    """
    service1 = WsTokenService()
    WsTokenService.clear_instance()
    service2 = WsTokenService()
    assert service1 is not service2


def test_ws_token_store_marks_and_detects_usage() -> None:
    """Verify WsTokenStore tracks token usage.

    Given: A token marked as used,
    When: is_used is called,
    Then: Returns True for used token, False for different token.
    """
    store = WsTokenStore()
    store.mark_used("token-1", exp=200)
    assert store.is_used("token-1", now_ts=100) is True
    assert store.is_used("different", now_ts=100) is False


def test_ws_token_store_cleanup_removes_expired_tokens() -> None:
    """Verify WsTokenStore cleans up expired tokens.

    Given: Tokens with different expiration times,
    When: Checking is_used after some have expired,
    Then: Expired tokens are not detected, active ones are.
    """
    store = WsTokenStore()
    store.mark_used("expired-token", exp=50)
    store.mark_used("active-token", exp=150)
    assert store.is_used("expired-token", now_ts=100) is False
    assert store.is_used("active-token", now_ts=100) is True


def test_ws_token_store_ignores_stale_heap_entries_for_reused_jti() -> None:
    """Verify stale expiry heap entries do not remove refreshed JTI state.

    Given: A JTI marked used twice with a later expiration,
    When: The first expiration is cleaned up,
    Then: The token remains marked used until its latest expiration.
    """
    store = WsTokenStore()
    store.mark_used("token-1", exp=50)
    store.mark_used("token-1", exp=200)
    assert store.is_used("token-1", now_ts=100) is True
    assert store.is_used("token-1", now_ts=200) is False


def test_ws_token_store_cleanup_is_bounded_per_lookup() -> None:
    """Verify lookup cleanup does not scan all expired tokens.

    Given: More expired tokens than the store cleanup batch,
    When: Checking an active token,
    Then: The active token is detected and some expired entries remain queued.
    """
    store = WsTokenStore()
    for index in range(100):
        store.mark_used(f"expired-{index}", exp=index)
    store.mark_used("active-token", exp=500)

    assert store.is_used("active-token", now_ts=200) is True
    remaining_expired = [jti for jti, exp in store._used_tokens.items() if exp <= 200]
    assert remaining_expired


def test_ws_token_store_queried_expired_token_is_removed_after_bounded_cleanup() -> None:
    """Verify the queried JTI is checked directly after bounded cleanup.

    Given: Expired tokens exceed the cleanup batch and the queried token is expired,
    When: The queried token is checked before global cleanup drains all entries,
    Then: The queried token is treated as unused and removed immediately.
    """
    store = WsTokenStore()
    for index in range(100):
        store.mark_used(f"expired-{index}", exp=index)
    store.mark_used("queried-token", exp=150)

    assert store.is_used("queried-token", now_ts=200) is False
    assert "queried-token" not in store._used_tokens


def test_ws_token_store_consume_first_succeeds_second_fails() -> None:
    """Verify WsTokenStore.consume succeeds once and rejects replay without mutating state.

    Given: An empty WsTokenStore,
    When: consume is called twice for the same unexpired JTI,
    Then: The first call returns True and the second returns False without altering state.
    """
    store = WsTokenStore()
    assert store.consume("token-1", exp=200, now_ts=100) is True
    assert store.is_used("token-1", now_ts=100) is True
    assert store.consume("token-1", exp=300, now_ts=100) is False
    assert store._used_tokens["token-1"] == 200
    assert len(store._expiry_heap) == 1


def test_ws_token_store_consume_allows_reconsumption_after_expiry() -> None:
    """Verify WsTokenStore.consume treats expired entries as unused.

    Given: A JTI previously consumed with an expiration at or before now_ts,
    When: consume is called again once expired,
    Then: The expired entry is treated as unused and consume returns True.
    """
    store = WsTokenStore()
    assert store.consume("token-1", exp=100, now_ts=50) is True
    assert store.consume("token-1", exp=300, now_ts=100) is True
    assert store.is_used("token-1", now_ts=150) is True
    assert store.is_used("token-1", now_ts=300) is False


def test_ws_token_store_consume_cleanup_is_bounded() -> None:
    """Verify WsTokenStore.consume performs bounded cleanup and handles expired queued JTI.

    Given: More expired entries than a single cleanup batch plus an expired target JTI,
    When: consume is called for the expired target JTI,
    Then: Cleanup stays bounded, the target JTI is re-consumed, and expired leftovers remain.
    """
    store = WsTokenStore()
    for index in range(100):
        store.mark_used(f"expired-{index}", exp=index)
    store.mark_used("queried-token", exp=150)

    assert store.consume("queried-token", exp=500, now_ts=200) is True
    assert store._used_tokens["queried-token"] == 500
    remaining_expired = [jti for jti, exp in store._used_tokens.items() if exp <= 200]
    assert remaining_expired


def test_ws_token_store_consume_is_atomic_across_threads() -> None:
    """Verify concurrent WsTokenStore.consume calls allow only one winner.

    Given: Multiple threads synchronized at a barrier targeting the same JTI,
    When: All threads invoke consume simultaneously,
    Then: Exactly one thread receives True and all others receive False.
    """
    store = WsTokenStore()
    thread_count = 16
    barrier = threading.Barrier(thread_count)
    outcomes: list[bool] = []
    errors: list[BaseException] = []
    results_lock = threading.Lock()

    def worker() -> None:
        try:
            barrier.wait(timeout=5)
            accepted = store.consume("race-jti", exp=500, now_ts=100)
            with results_lock:
                outcomes.append(accepted)
        except BaseException as exc:
            with results_lock:
                errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(thread_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
        assert not thread.is_alive()

    assert errors == []
    assert len(outcomes) == thread_count
    assert outcomes.count(True) == 1
    assert outcomes.count(False) == thread_count - 1
    assert len(store._expiry_heap) == 1


def test_verify_and_consume_valid_token_and_rejects_replay() -> None:
    """Verify verify_and_consume validates claims and consumes atomically.

    Given: A newly generated valid WS token,
    When: verify_and_consume is called twice with matching subject and session hash,
    Then: The first call returns the payload and the second raises WsTokenAlreadyUsedError.
    """
    service = WsTokenService()
    mock_settings_service = Mock(spec=SettingsService)
    service.set_settings_service(mock_settings_service)
    user_id = "test_user"
    session_id = "test_session"
    result = service.generate(user_id=user_id, session_id=session_id)
    sid_hash = compute_sid_hash(session_id)

    payload = service.verify_and_consume(
        result.token,
        expected_sub=user_id,
        expected_sid_hash=sid_hash,
    )
    assert payload.sub == user_id
    assert payload.sid_hash == sid_hash
    assert payload.purpose == "ws_connect"
    assert payload.jti == result.payload.jti

    with pytest.raises(WsTokenAlreadyUsedError, match="ws_token_already_used"):
        service.verify_and_consume(
            result.token,
            expected_sub=user_id,
            expected_sid_hash=sid_hash,
        )
    with pytest.raises(WsTokenAlreadyUsedError, match="ws_token_already_used"):
        service.verify(
            result.token,
            expected_sub=user_id,
            expected_sid_hash=sid_hash,
        )


def test_verify_remains_non_consuming() -> None:
    """Verify verify keeps its non-consuming contract alongside verify_and_consume.

    Given: A newly generated valid WS token,
    When: verify is called twice before verify_and_consume,
    Then: Both verify calls succeed, verify_and_consume succeeds once, and replay then fails.
    """
    service = WsTokenService()
    mock_settings_service = Mock(spec=SettingsService)
    service.set_settings_service(mock_settings_service)
    user_id = "test_user"
    session_id = "test_session"
    result = service.generate(user_id=user_id, session_id=session_id)
    sid_hash = compute_sid_hash(session_id)

    first = service.verify(result.token, expected_sub=user_id, expected_sid_hash=sid_hash)
    second = service.verify(result.token, expected_sub=user_id, expected_sid_hash=sid_hash)
    assert first.jti == second.jti == result.payload.jti

    consumed = service.verify_and_consume(
        result.token,
        expected_sub=user_id,
        expected_sid_hash=sid_hash,
    )
    assert consumed.jti == result.payload.jti
    with pytest.raises(WsTokenAlreadyUsedError, match="ws_token_already_used"):
        service.verify_and_consume(
            result.token,
            expected_sub=user_id,
            expected_sid_hash=sid_hash,
        )


def test_verify_and_consume_raises_validation_errors_without_consuming() -> None:
    """Verify verify_and_consume raises all validation errors without marking used.

    Given: Tokens with invalid signature, expired timestamp, wrong purpose, wrong sub, or wrong sid,
    When: verify_and_consume is called for each error case,
    Then: Each matching WsTokenError is raised and valid tokens remain unconsumed.
    """
    service = WsTokenService()
    mock_settings_service = Mock(spec=SettingsService)
    service.set_settings_service(mock_settings_service)
    sid_hash = compute_sid_hash("session1")

    with pytest.raises(WsTokenError, match="invalid_ws_token"):
        service.verify_and_consume(
            "invalid.token.here",
            expected_sub="user1",
            expected_sid_hash=sid_hash,
        )

    past_time = datetime.now(UTC) - timedelta(hours=1)
    with patch.object(service, "_now", return_value=past_time):
        expired_result = service.generate(user_id="user1", session_id="session1")
    with pytest.raises(WsTokenError, match="invalid_ws_token"):
        service.verify_and_consume(
            expired_result.token,
            expected_sub="user1",
            expected_sid_hash=sid_hash,
        )

    valid_result = service.generate(user_id="user1", session_id="session1")
    expired_payload = valid_result.payload.model_copy(
        update={"exp": int(service._now().timestamp()) - 10}
    )
    with (
        patch(
            "snapper.api.auth.services.ws_token_service.jwt.decode",
            return_value=expired_payload.model_dump(),
        ),
        pytest.raises(WsTokenError, match="ws_token_expired"),
    ):
        service.verify_and_consume(
            valid_result.token,
            expected_sub="user1",
            expected_sid_hash=sid_hash,
        )

    tampered_payload = valid_result.payload.model_copy(update={"purpose": "unexpected"})
    tampered_token = jwt.encode(
        tampered_payload.model_dump(
            exclude={"public_id", "timestamp", "session_id", "sequence_id", "type"},
        ),
        service.settings.auth_secret_key,
        algorithm=service.settings.auth_algorithm,
    )
    with pytest.raises(WsTokenError, match="invalid_ws_token_purpose"):
        service.verify_and_consume(
            tampered_token,
            expected_sub="user1",
            expected_sid_hash=sid_hash,
        )

    with pytest.raises(WsTokenError, match="ws_token_subject_mismatch"):
        service.verify_and_consume(
            valid_result.token,
            expected_sub="different_user",
            expected_sid_hash=sid_hash,
        )

    with pytest.raises(WsTokenError, match="ws_token_session_mismatch"):
        service.verify_and_consume(
            valid_result.token,
            expected_sub="user1",
            expected_sid_hash=compute_sid_hash("different_session"),
        )

    accepted = service.verify_and_consume(
        valid_result.token,
        expected_sub="user1",
        expected_sid_hash=sid_hash,
    )
    assert accepted.jti == valid_result.payload.jti


def test_verify_and_consume_is_atomic_across_threads() -> None:
    """Verify concurrent WsTokenService.verify_and_consume calls allow one winner.

    Given: A valid generated WS token and N threads waiting on a barrier,
    When: All threads call verify_and_consume concurrently,
    Then: Exactly one thread receives the payload and N-1 raise WsTokenAlreadyUsedError.
    """
    service = WsTokenService()
    mock_settings_service = Mock(spec=SettingsService)
    service.set_settings_service(mock_settings_service)
    user_id = "race_user"
    session_id = "race_session"
    result = service.generate(user_id=user_id, session_id=session_id)
    sid_hash = compute_sid_hash(session_id)

    thread_count = 12
    barrier = threading.Barrier(thread_count)
    success_jtis: list[str] = []
    replay_errors: list[WsTokenAlreadyUsedError] = []
    unexpected_errors: list[BaseException] = []
    lock = threading.Lock()

    def worker() -> None:
        try:
            barrier.wait(timeout=5)
            payload = service.verify_and_consume(
                result.token,
                expected_sub=user_id,
                expected_sid_hash=sid_hash,
            )
            with lock:
                success_jtis.append(payload.jti)
        except WsTokenAlreadyUsedError as exc:
            with lock:
                replay_errors.append(exc)
        except BaseException as exc:
            with lock:
                unexpected_errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(thread_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)
        assert not thread.is_alive()

    assert unexpected_errors == []
    assert success_jtis == [result.payload.jti]
    assert len(replay_errors) == thread_count - 1
