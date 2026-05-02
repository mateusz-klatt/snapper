"""Tests for UAT signal sending script."""

import json
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from scripts.uat_send_signal import build_signal_payload
from scripts.uat_send_signal import build_topic
from scripts.uat_send_signal import main


def test_build_signal_payload_has_required_fields() -> None:
    """Verify payload contains all SignalData required fields.

    Given: Valid parameters,
    When: build_signal_payload is called,
    Then: All required SignalData fields are present.
    """
    payload = build_signal_payload(
        exchange="kraken",
        instrument="BTC-USD",
        side="buy",
        strength=0.5,
        price=50000.0,
    )
    assert payload["type"] == "signal"
    assert payload["instrument"] == "BTC-USD"
    assert payload["exchange"] == "kraken"
    assert payload["side"] == "buy"
    assert payload["strength"] == pytest.approx(0.5)
    assert payload["price"] == pytest.approx(50000.0)
    assert payload["strategy_name"] == "uat_test"
    assert payload["reason"] == "UAT test signal"
    assert "public_id" in payload
    assert "timestamp" in payload
    assert "session_id" in payload
    assert "fired_at" in payload
    assert payload["sequence_id"] == 1


def test_build_signal_payload_sell_side() -> None:
    """Verify payload accepts sell side.

    Given: side='sell',
    When: build_signal_payload is called,
    Then: Payload side is 'sell'.
    """
    payload = build_signal_payload(
        exchange="paper",
        instrument="ETH-USD",
        side="sell",
        strength=0.8,
        price=3000.0,
    )
    assert payload["side"] == "sell"
    assert payload["exchange"] == "paper"


def test_build_topic_format_live() -> None:
    """Verify live exchange topic uses 'live' suffix.

    Given: Live exchange name,
    When: build_topic is called,
    Then: Fourth segment is 'live'.
    """
    assert build_topic("kraken", "BTC-USD") == "signals.kraken.BTC-USD.live"
    assert build_topic("walutomat", "ETH-USD") == "signals.walutomat.ETH-USD.live"


def test_build_topic_format_paper() -> None:
    """Verify paper exchange topic uses strategy_name suffix.

    Given: paper exchange,
    When: build_topic is called,
    Then: Fourth segment is strategy_name, not 'live'.
    """
    assert build_topic("paper", "BTC-USD") == "signals.paper.BTC-USD.uat_test"
    assert build_topic("paper", "ETH-USD", "my_strat") == "signals.paper.ETH-USD.my_strat"


def test_build_signal_payload_unique_ids() -> None:
    """Verify each call generates unique public_id and session_id.

    Given: Two calls with same parameters,
    When: build_signal_payload is called twice,
    Then: public_id and session_id differ.
    """
    p1 = build_signal_payload("kraken", "BTC-USD", "buy", 0.1, 50000.0)
    p2 = build_signal_payload("kraken", "BTC-USD", "buy", 0.1, 50000.0)
    assert p1["public_id"] != p2["public_id"]
    assert p1["session_id"] != p2["session_id"]


@patch("scripts.uat_send_signal.time.sleep")
@patch("scripts.uat_send_signal.zmq.Context")
def test_main_sends_signal_and_returns_zero(
    mock_zmq_context: MagicMock,
    mock_sleep: MagicMock,
) -> None:
    """Verify main() sends ZMQ message and returns 0.

    Given: Mocked ZMQ context,
    When: main() is called with default args,
    Then: Message is sent and exit code is 0.
    """
    mock_socket = MagicMock()
    mock_zmq_context.return_value.socket.return_value = mock_socket
    with patch("sys.argv", ["uat_send_signal.py", "--price", "50000"]):
        result = main()
    assert result == 0
    mock_socket.connect.assert_called_once()
    mock_socket.send_multipart.assert_called_once()
    mock_socket.close.assert_called_once()
    sent_args = mock_socket.send_multipart.call_args[0][0]
    assert sent_args[0] == b"signals.kraken.BTC-USD.live"


@patch("scripts.uat_send_signal.time.sleep")
@patch("scripts.uat_send_signal.zmq.Context")
def test_main_default_price_fallback(
    mock_zmq_context: MagicMock,
    mock_sleep: MagicMock,
) -> None:
    """Verify main() uses 95000.0 when no price specified.

    Given: Mocked ZMQ with no --price argument,
    When: main() is called,
    Then: Payload price is 95000.0.
    """
    mock_socket = MagicMock()
    mock_zmq_context.return_value.socket.return_value = mock_socket
    with patch("sys.argv", ["uat_send_signal.py"]):
        main()
    sent_bytes = mock_socket.send_multipart.call_args[0][0][1]

    payload = json.loads(sent_bytes)
    assert payload["price"] == pytest.approx(95000.0)
