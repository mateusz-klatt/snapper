"""Send a single test signal to the trading engine via ZMQ.

Usage:
    python scripts/uat_send_signal.py
    python scripts/uat_send_signal.py --exchange paper
    python scripts/uat_send_signal.py --exchange kraken --side buy --instrument BTC-USD

Requires: snapper server running (make run-server or make dev-backend).
"""

import argparse
import json
import time
from datetime import UTC
from datetime import datetime
from typing import Any
from uuid import uuid7

import zmq


def build_signal_payload(
    exchange: str,
    instrument: str,
    side: str,
    strength: float,
    price: float,
) -> dict[str, Any]:
    """Build a SignalData-compatible JSON payload.

    Args:
        exchange: Target exchange name.
        instrument: Trading instrument symbol.
        side: Signal side ('buy' or 'sell').
        strength: Signal strength (0.0 to 1.0).
        price: Reference price (positive value required).

    Returns:
        Dict matching SignalData schema, ready for JSON serialization.
    """
    now = datetime.now(UTC).isoformat()
    return {
        "type": "signal",
        "public_id": str(uuid7()),
        "timestamp": now,
        "session_id": f"uat-{uuid7()}",
        "sequence_id": 1,
        "instrument": instrument,
        "exchange": exchange,
        "side": side,
        "strength": strength,
        "reason": "UAT test signal",
        "price": price,
        "strategy_name": "uat_test",
        "fired_at": now,
    }


def build_topic(exchange: str, instrument: str, strategy_name: str = "uat_test") -> str:
    """Build ZMQ topic string for a signal.

    For paper exchange the fourth segment must be the strategy_name
    (required by SignalData validation). For live exchanges the
    convention is 'live'.

    Args:
        exchange: Target exchange name.
        instrument: Trading instrument symbol.
        strategy_name: Strategy name (used as suffix for paper).

    Returns:
        ZMQ topic string like 'signals.kraken.BTC-USD.live'.
    """
    suffix = strategy_name if exchange == "paper" else "live"
    return f"signals.{exchange}.{instrument}.{suffix}"


def main() -> int:
    """Send a single test signal to the trading engine via ZMQ.

    Returns:
        Exit code (0 on success).
    """
    parser = argparse.ArgumentParser(description="Send a single test signal via ZMQ")
    parser.add_argument("--exchange", default="kraken", help="Target exchange (default: kraken)")
    parser.add_argument(
        "--instrument", default="BTC-USD", help="Trading instrument (default: BTC-USD)"
    )
    parser.add_argument(
        "--side", default="buy", choices=["buy", "sell"], help="Signal side (default: buy)"
    )
    parser.add_argument(
        "--strength", type=float, default=0.001, help="Signal strength (default: 0.001 = tiny)"
    )
    parser.add_argument(
        "--price", type=float, default=0.0, help="Reference price (0 = use 95000.0)"
    )
    args = parser.parse_args()

    effective_price = args.price if args.price > 0 else 95000.0
    payload = build_signal_payload(
        exchange=args.exchange,
        instrument=args.instrument,
        side=args.side,
        strength=args.strength,
        price=effective_price,
    )
    topic = build_topic(args.exchange, args.instrument)

    xsub = "tcp://127.0.0.1:7500"
    context = zmq.Context()
    publisher = context.socket(zmq.PUB)
    publisher.connect(xsub)
    time.sleep(0.5)

    message = json.dumps(payload).encode("utf-8")
    publisher.send_multipart([topic.encode("utf-8"), message])

    print(f"Sent signal on topic: {topic}")
    print(f"  exchange:   {args.exchange}")
    print(f"  instrument: {args.instrument}")
    print(f"  side:       {args.side}")
    print(f"  strength:   {args.strength}")
    print(f"  price:      {effective_price}")
    print(f"  public_id:  {payload['public_id']}")

    publisher.close()
    context.term()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
