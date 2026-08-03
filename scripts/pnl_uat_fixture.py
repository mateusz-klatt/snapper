"""Seed one deterministic, paper-only P&L browser-UAT fixture.

The command is intentionally one-shot and accepts its database URL only through
``DB_URL``. It refuses every non-PostgreSQL target, every non-loopback host, and
every database whose name is outside the dedicated ``snapper_pnl_uat_``
namespace. The target must already be migrated to the repository Alembic head
and must contain no active non-paper credential.

Fixture market and lineage rows commit in one transaction. Six durable P&L
activation anchors are then created only through the production public service;
this second step intentionally uses independent transactions because it tests
the real writer. If it fails, the operator must discard the disposable one-shot
database. Stable public identities, wallet labels, symbol names, and the fixture
session identity form a collision fence, and the resulting manifest contains no
credential material.
"""

import argparse
import asyncio
import hashlib
import ipaddress
import json
import os
import re
import sys
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Final
from uuid import UUID
from uuid import uuid5

import bcrypt
from alembic.config import Config
from alembic.script import ScriptDirectory
from cryptography.fernet import InvalidToken
from pydantic import BaseModel
from pydantic import ConfigDict
from sqlalchemy import func
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from snapper.application.engine.service import compute_shard_key
from snapper.application.portfolio.pnl_timeline_service import ensure_wallet_pnl_anchor
from snapper.core.json_types import JsonObject
from snapper.core.types import ExchangeEnum
from snapper.core.types import ExecutionModeEnum
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import AiReview
from snapper.data.models import AiReviewEvent
from snapper.data.models import Base
from snapper.data.models import Candle
from snapper.data.models import Execution
from snapper.data.models import Instrument
from snapper.data.models import Operator
from snapper.data.models import Order
from snapper.data.models import Setting
from snapper.data.models import Signal
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.models import SymbolExchangeCapability
from snapper.data.models import TradeCommand
from snapper.data.models import User
from snapper.data.models import UserOperatorMembership
from snapper.data.models import VenueEvent
from snapper.data.models import Wallet
from snapper.data.models import WalletCredential
from snapper.data.repository import SQLAlchemyRepository
from snapper.infrastructure.security.encryption import get_encryption_service
from snapper.infrastructure.security.path_validation import UnsafePathError
from snapper.infrastructure.security.path_validation import resolve_operator_file

_ROOT: Final[Path] = Path(__file__).resolve().parents[1]
_ALEMBIC_INI: Final[Path] = _ROOT / "alembic.ini"
_DATABASE_PREFIX: Final[str] = "snapper_pnl_uat_"
_DATABASE_NAME_PATTERN: Final[re.Pattern[str]] = re.compile(r"^snapper_pnl_uat_[a-z0-9][a-z0-9_]*$")
_SESSION_ID: Final[str] = "00000000-0000-7000-8000-00000000f001"
_CANDLE_NAMESPACE: Final[UUID] = UUID("00000000-0000-5000-8000-00000000f002")
_VENUE_EVENT_NAMESPACE: Final[UUID] = UUID("00000000-0000-5000-8000-00000000f003")
_HAPPY_WALLET_ID: Final[str] = "00000000-0000-7000-8000-00000000a101"
_INCOMPLETE_WALLET_ID: Final[str] = "00000000-0000-7000-8000-00000000a102"
_EUR_PLN_SOURCE_INSTRUMENT_ID: Final[str] = "00000000-0000-7000-8000-00000000a301"
_BTC_USD_SOURCE_INSTRUMENT_ID: Final[str] = "00000000-0000-7000-8000-00000000a302"
_ETH_USD_SOURCE_INSTRUMENT_ID: Final[str] = "00000000-0000-7000-8000-00000000a303"
_USD_PLN_SOURCE_INSTRUMENT_ID: Final[str] = "00000000-0000-7000-8000-00000000a304"
_EUR_USD_SOURCE_INSTRUMENT_ID: Final[str] = "00000000-0000-7000-8000-00000000a305"
_EUR_PLN_PAPER_INSTRUMENT_ID: Final[str] = "00000000-0000-7000-8000-00000000a311"
_BTC_USD_PAPER_INSTRUMENT_ID: Final[str] = "00000000-0000-7000-8000-00000000a312"
_ETH_USD_PAPER_INSTRUMENT_ID: Final[str] = "00000000-0000-7000-8000-00000000a313"
_EUR_PLN_ORDER_ID: Final[str] = "00000000-0000-7000-8000-00000000a401"
_BTC_USD_ORDER_ID: Final[str] = "00000000-0000-7000-8000-00000000a402"
_ETH_USD_ORDER_ID: Final[str] = "00000000-0000-7000-8000-00000000a403"
_EUR_PLN_EXECUTION_ID: Final[str] = "00000000-0000-7000-8000-00000000a501"
_BTC_USD_EXECUTION_ID: Final[str] = "00000000-0000-7000-8000-00000000a502"
_ETH_USD_EXECUTION_ID: Final[str] = "00000000-0000-7000-8000-00000000a503"
_MANUAL_COMMAND_ID: Final[str] = "00000000-0000-7000-8000-00000000a601"
_SYSTEM_COMMAND_ID: Final[str] = "00000000-0000-7000-8000-00000000a602"
_EXECUTED_SIGNAL_ID: Final[str] = "00000000-0000-7000-8000-00000000a701"
_NO_FILL_SIGNAL_ID: Final[str] = "00000000-0000-7000-8000-00000000a702"
_AI_REVIEW_ID: Final[str] = "00000000-0000-7000-8000-00000000a801"
_AI_EVENT_ID: Final[str] = "00000000-0000-7000-8000-00000000a802"
_AI_STRATEGY_ID: Final[str] = "00000000-0000-7000-8000-00000000a803"
_AI_DELEGATE_ID: Final[str] = "00000000-0000-7000-8000-00000000a804"
_AI_OPERATOR_ID: Final[str] = "00000000-0000-7000-8000-00000000a805"
_MANUAL_CORRELATION_ID: Final[str] = "00000000-0000-7000-8000-00000000a901"
_SYSTEM_CORRELATION_ID: Final[str] = "00000000-0000-7000-8000-00000000a902"
_HAPPY_WALLET_LABEL: Final[str] = "P&L Complete"
_INCOMPLETE_WALLET_LABEL: Final[str] = "P&L Incomplete"
_MANUAL_CLIENT_ORDER_ID: Final[str] = "pnl-uat-manual-eur-pln"
_SYSTEM_CLIENT_ORDER_ID: Final[str] = "pnl-uat-system-btc-usd"
_INCOMPLETE_CLIENT_ORDER_ID: Final[str] = "pnl-uat-incomplete-eth-usd"
_WINDOW: Final[timedelta] = timedelta(hours=24)
_EUR_FILL_OFFSET: Final[timedelta] = timedelta(hours=12)
_INCOMPLETE_FILL_OFFSET: Final[timedelta] = timedelta(hours=2)
_INCOMPLETE_POINT_COUNT: Final[int] = 121
_DEV_PASSWORD: Final[bytes] = b"change-me-after-first-login"
_OSS_BASELINE_REFUSAL: Final[str] = (
    "database is not a fresh migrated and bundled-OSS-seeded P&L UAT baseline"
)
_OSS_BASELINE_TABLE_COUNTS: Final[dict[str, int]] = {
    "operators": 1,
    "settings": 13,
    "symbol_aliases": 36,
    "symbol_exchange_capabilities": 20,
    "symbols": 11,
    "user_operator_memberships": 3,
    "users": 3,
    "wallet_credentials": 1,
    "wallets": 1,
}
_OSS_BASELINE_USERS: Final[dict[str, tuple[str, str, int, int]]] = {
    "admin": ("admin@snapper.local", "admin", 1, 1),
    "operator": ("operator@snapper.local", "operator", 2, 2),
    "viewer": ("viewer@snapper.local", "viewer", 3, 3),
}
_OSS_BASELINE_ENCRYPTED_SETTINGS: Final[frozenset[str]] = frozenset(
    {"gemini_api_key", "kimi_api_key"}
)
_OSS_BASELINE_SETTINGS: Final[dict[str, tuple[str, str, str, int, int]]] = {
    "ui_origin": (
        "http://localhost:3000,http://localhost:8000",
        "server",
        "Allowed UI origins for WebSocket CORS (comma-separated)",
        1,
        1,
    ),
    "market_persist_ticks": (
        '{"mode": "auto"}',
        "market_persist",
        (
            "Tick persistence mode (auto = wallet-scope-derived, explicit = configured "
            "allowlist). MarketPersistPolicy"
        ),
        2,
        2,
    ),
    "market_persist_trades": (
        '{"mode": "auto"}',
        "market_persist",
        (
            "Trade persistence mode (auto = wallet-scope-derived, explicit = configured "
            "allowlist). MarketPersistPolicy"
        ),
        3,
        3,
    ),
    "market_persist_candles": (
        '{"mode": "auto"}',
        "market_persist",
        (
            "Candle persistence mode (auto = wallet-scope-derived, explicit = configured "
            "allowlist). MarketPersistPolicy"
        ),
        4,
        4,
    ),
    "market_persist_extra": (
        '{"ticks": {}, "trades": {}, "candles": {}}',
        "market_persist",
        (
            "Per-data-type per-exchange overlay-INCLUDE allowlist applied on top of mode. "
            "MarketPersistPolicy"
        ),
        5,
        5,
    ),
    "market_persist_exclude": (
        '{"ticks": {}, "trades": {}, "candles": {}}',
        "market_persist",
        (
            "Per-data-type per-exchange overlay-EXCLUDE blocklist subtracted from the "
            "resolved set. MarketPersistPolicy"
        ),
        6,
        6,
    ),
    "market_stats_pairs": (
        "[]",
        "market_stats",
        (
            "Cross-exchange pairs for Pearson + cointegration computation (cap 50, "
            "configured at implementation time)"
        ),
        7,
        7,
    ),
    "live_trading_mode": (
        "halted",
        "risk",
        (
            "Live-trading interlock: halted|reduce_only|enabled. Seeded halted; only enabled "
            "admits non-paper submits. Read fresh from DB on every non-paper submit; paper "
            "is exempt. Flipping to enabled is an explicit operator money-risk action."
        ),
        8,
        8,
    ),
    "risk_max_leverage": (
        "1.0",
        "risk",
        (
            "Maximum prospective gross exposure as a multiple of equity, enforced by the "
            "portfolio risk gate (Phase 6). Placeholder until the gate ships."
        ),
        9,
        9,
    ),
    "risk_max_drawdown": (
        "0.15",
        "risk",
        (
            "Maximum flow-adjusted drawdown fraction from the epoch peak before the "
            "portfolio risk gate blocks exposure increases (Phase 6). Placeholder until "
            "the gate ships."
        ),
        10,
        10,
    ),
    "risk_r_per_trade": (
        "0.005",
        "risk",
        (
            "Fraction of equity risked per trade for risk-based position sizing. Placeholder "
            "until the sizing/gate path ships."
        ),
        11,
        11,
    ),
    "gemini_api_key": (
        "",
        "api",
        (
            "Google Gemini API key (OpenAI-compat, "
            "generativelanguage.googleapis.com/v1beta/openai). Placeholder: real value "
            "lives in the environment seed, never this skeleton."
        ),
        12,
        12,
    ),
    "kimi_api_key": (
        "",
        "api",
        (
            "Moonshot Kimi K3 API key (OpenAI-compat, api.moonshot.ai/v1). Placeholder: "
            "real value lives in the environment seed, never this skeleton."
        ),
        13,
        13,
    ),
}
_OSS_BASELINE_SYMBOLS: Final[set[tuple[str, str, str | None, str]]] = {
    ("BTC-USD", "BTC", "USD", "crypto"),
    ("BTC-EUR", "BTC", "EUR", "crypto"),
    ("ETH-USD", "ETH", "USD", "crypto"),
    ("ETH-EUR", "ETH", "EUR", "crypto"),
    ("ETH-BTC", "ETH", "BTC", "crypto"),
    ("EUR-USD", "EUR", "USD", "forex"),
    ("USD-PLN", "USD", "PLN", "forex"),
    ("EUR-PLN", "EUR", "PLN", "forex"),
    ("GBP-PLN", "GBP", "PLN", "forex"),
    ("GBP-USD", "GBP", "USD", "forex"),
    ("EUR-GBP", "EUR", "GBP", "forex"),
}
_OSS_BASELINE_ALIASES: Final[tuple[tuple[str, str, str, str], ...]] = (
    ("BTC-USD", "kraken", "ws", "BTC/USD"),
    ("BTC-USD", "kraken", "rest", "XXBTZUSD"),
    ("BTC-USD", "kraken", "ccxt", "BTC/USD"),
    ("BTC-USD", "polygon", "rest", "X:BTCUSD"),
    ("BTC-EUR", "kraken", "ws", "BTC/EUR"),
    ("BTC-EUR", "kraken", "rest", "XXBTZEUR"),
    ("BTC-EUR", "kraken", "ccxt", "BTC/EUR"),
    ("BTC-EUR", "polygon", "rest", "X:BTCEUR"),
    ("ETH-USD", "kraken", "ws", "ETH/USD"),
    ("ETH-USD", "kraken", "rest", "XETHZUSD"),
    ("ETH-USD", "kraken", "ccxt", "ETH/USD"),
    ("ETH-USD", "polygon", "rest", "X:ETHUSD"),
    ("ETH-EUR", "kraken", "ws", "ETH/EUR"),
    ("ETH-EUR", "kraken", "rest", "XETHZEUR"),
    ("ETH-EUR", "kraken", "ccxt", "ETH/EUR"),
    ("ETH-BTC", "kraken", "ws", "ETH/BTC"),
    ("ETH-BTC", "kraken", "rest", "XETHXXBT"),
    ("ETH-BTC", "kraken", "ccxt", "ETH/BTC"),
    ("ETH-BTC", "polygon", "rest", "X:ETHBTC"),
    ("EUR-USD", "kraken", "ws", "EUR/USD"),
    ("EUR-USD", "kraken", "rest", "ZEURZUSD"),
    ("EUR-USD", "kraken", "ccxt", "EUR/USD"),
    ("EUR-USD", "polygon", "rest", "C:EURUSD"),
    ("EUR-USD", "walutomat", "ws", "EUR_USD"),
    ("EUR-USD", "walutomat", "rest", "EURUSD"),
    ("USD-PLN", "polygon", "rest", "C:USDPLN"),
    ("USD-PLN", "walutomat", "ws", "USD_PLN"),
    ("USD-PLN", "walutomat", "rest", "USDPLN"),
    ("EUR-PLN", "polygon", "rest", "C:EURPLN"),
    ("EUR-PLN", "walutomat", "ws", "EUR_PLN"),
    ("EUR-PLN", "walutomat", "rest", "EURPLN"),
    ("GBP-PLN", "polygon", "rest", "C:GBPPLN"),
    ("GBP-PLN", "walutomat", "ws", "GBP_PLN"),
    ("GBP-PLN", "walutomat", "rest", "GBPPLN"),
    ("GBP-USD", "polygon", "rest", "C:GBPUSD"),
    ("EUR-GBP", "polygon", "rest", "C:EURGBP"),
)
_OSS_BASELINE_CAPABILITIES: Final[tuple[tuple[str, str, bool, bool, str, None], ...]] = (
    ("BTC-USD", "kraken", True, True, "seed", None),
    ("BTC-USD", "polygon", True, False, "seed", None),
    ("BTC-EUR", "kraken", True, True, "seed", None),
    ("BTC-EUR", "polygon", True, False, "seed", None),
    ("ETH-USD", "kraken", True, True, "seed", None),
    ("ETH-USD", "polygon", True, False, "seed", None),
    ("ETH-EUR", "kraken", True, True, "seed", None),
    ("ETH-BTC", "kraken", True, True, "seed", None),
    ("ETH-BTC", "polygon", True, False, "seed", None),
    ("EUR-USD", "kraken", True, True, "seed", None),
    ("EUR-USD", "polygon", True, False, "seed", None),
    ("EUR-USD", "walutomat", True, True, "seed", None),
    ("USD-PLN", "polygon", True, False, "seed", None),
    ("USD-PLN", "walutomat", True, True, "seed", None),
    ("EUR-PLN", "polygon", True, False, "seed", None),
    ("EUR-PLN", "walutomat", True, True, "seed", None),
    ("GBP-PLN", "polygon", True, False, "seed", None),
    ("GBP-PLN", "walutomat", True, True, "seed", None),
    ("GBP-USD", "polygon", True, False, "seed", None),
    ("EUR-GBP", "polygon", True, False, "seed", None),
)
_POSTGRES_LOCK_STATEMENT: Final[str] = (
    "LOCK TABLE "
    + ", ".join(
        f'"{table.name}"' for table in sorted(Base.metadata.tables.values(), key=lambda t: t.name)
    )
    + " IN SHARE ROW EXCLUSIVE MODE"
)


class PnlUatFixtureError(RuntimeError):
    """Represent a safe, credential-free fixture refusal."""


class ExpectedPointEconomics(BaseModel):
    """Expected latest complete point and EUR-PLN contribution values."""

    model_config = ConfigDict(frozen=True)

    realized_pnl: float
    fee_pnl: float
    accrual_pnl: float
    unrealized_pnl: float
    net_pnl: float


class ExpectedCurrencyEconomics(BaseModel):
    """Expected complete economics in every supported valuation currency."""

    model_config = ConfigDict(frozen=True)

    usd: ExpectedPointEconomics
    pln: ExpectedPointEconomics
    eur: ExpectedPointEconomics


class ExpectedIncompleteEconomics(BaseModel):
    """Expected missing-mark result for the incomplete wallet."""

    model_config = ConfigDict(frozen=True)

    incomplete_point_count: int
    reason: str
    trigger_instrument_public_id: str
    realized_pnl: float
    fee_pnl: float
    accrual_pnl: float
    unrealized_pnl: None
    net_pnl: None


class FixtureIds(BaseModel):
    """Stable public identities consumed by the browser UAT."""

    model_config = ConfigDict(frozen=True)

    happy_wallet_public_id: str
    incomplete_wallet_public_id: str
    eur_pln_instrument_public_id: str
    btc_usd_instrument_public_id: str
    eth_usd_instrument_public_id: str
    eur_pln_order_public_id: str
    btc_usd_order_public_id: str
    eth_usd_order_public_id: str
    eur_pln_execution_public_id: str
    btc_usd_execution_public_id: str
    eth_usd_execution_public_id: str
    executed_signal_public_id: str
    no_fill_signal_public_id: str
    ai_review_public_id: str
    ai_event_public_id: str


class FixtureTimes(BaseModel):
    """Stable relative instants defining the requested P&L window."""

    model_config = ConfigDict(frozen=True)

    anchor: datetime
    window_from: datetime
    eur_pln_fill_at: datetime
    btc_usd_fill_at: datetime
    eth_usd_fill_at: datetime


class FixtureExpected(BaseModel):
    """All deterministic values the real-stack UAT may assert."""

    model_config = ConfigDict(frozen=True)

    complete: ExpectedCurrencyEconomics
    incomplete: ExpectedIncompleteEconomics
    marker_kinds: tuple[str, ...]
    marker_outcomes: tuple[str, ...]


class PnlUatManifest(BaseModel):
    """Credential-free handoff between the backend fixture and Playwright."""

    model_config = ConfigDict(frozen=True)

    fixture: str
    version: int
    mode: str
    happy_wallet_label: str
    incomplete_wallet_label: str
    ids: FixtureIds
    times: FixtureTimes
    expected: FixtureExpected


def validate_target_database(db_url: str | URL) -> URL:
    """Validate that a URL names only a disposable local PostgreSQL database.

    Args:
        db_url: Candidate raw or parsed SQLAlchemy database URL.

    Returns:
        Parsed SQLAlchemy URL after every safety fence passes.

    Raises:
        PnlUatFixtureError: If the dialect, host, or database namespace is unsafe.
    """
    try:
        candidate = (
            db_url.render_as_string(hide_password=False) if isinstance(db_url, URL) else db_url
        )
        url = make_url(candidate)
        host = url.host
        database = url.database
    except (ArgumentError, ValueError) as exc:
        raise PnlUatFixtureError("DB_URL is not a valid SQLAlchemy URL") from exc
    if url.drivername != "postgresql+asyncpg":
        raise PnlUatFixtureError("P&L UAT fixture requires a PostgreSQL asyncpg target")
    if url.query:
        raise PnlUatFixtureError("P&L UAT fixture DB_URL must not contain query parameters")
    if host is None or not _is_loopback_host(host):
        raise PnlUatFixtureError(
            "P&L UAT fixture requires a literal 127.0.0.0/8 or ::1 database host"
        )
    if database is None or len(database) > 63 or _DATABASE_NAME_PATTERN.fullmatch(database) is None:
        raise PnlUatFixtureError(
            f"database name must use the dedicated {_DATABASE_PREFIX}<run_id> namespace"
        )
    return url


def _is_loopback_host(host: str) -> bool:
    """Return whether a host is exactly an IPv4 or IPv6 loopback literal."""
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return False
    if isinstance(address, ipaddress.IPv4Address):
        return address in ipaddress.IPv4Network("127.0.0.0/8")
    return address == ipaddress.IPv6Address("::1")


def parse_anchor(raw: str | None, now: datetime | None = None) -> datetime:
    """Parse a non-future UTC minute or derive the current UTC minute.

    Args:
        raw: Optional ISO-8601 instant.
        now: Optional clock value used by tests.

    Returns:
        A timezone-aware UTC instant aligned to a whole minute.

    Raises:
        PnlUatFixtureError: If the instant is invalid, naive, non-minute, or future.
    """
    clock = datetime.now(UTC) if now is None else now.astimezone(UTC)
    current_minute = clock.replace(second=0, microsecond=0)
    if raw is None:
        return current_minute
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PnlUatFixtureError("anchor must be a valid ISO-8601 datetime") from exc
    if parsed.tzinfo is None:
        raise PnlUatFixtureError("anchor must include an explicit timezone")
    anchor = parsed.astimezone(UTC)
    if anchor.second != 0 or anchor.microsecond != 0:
        raise PnlUatFixtureError("anchor must be aligned to a whole minute")
    if anchor > current_minute:
        raise PnlUatFixtureError("anchor must not be in the future")
    return anchor


def build_manifest(anchor: datetime) -> PnlUatManifest:
    """Build the deterministic, credential-free fixture manifest.

    Args:
        anchor: Inclusive P&L window endpoint.

    Returns:
        Manifest containing stable identities, times, and exact economics.
    """
    zero = 0.0
    return PnlUatManifest(
        fixture="snapper_pnl_uat",
        version=2,
        mode="paper",
        happy_wallet_label=_HAPPY_WALLET_LABEL,
        incomplete_wallet_label=_INCOMPLETE_WALLET_LABEL,
        ids=FixtureIds(
            happy_wallet_public_id=_HAPPY_WALLET_ID,
            incomplete_wallet_public_id=_INCOMPLETE_WALLET_ID,
            eur_pln_instrument_public_id=_EUR_PLN_PAPER_INSTRUMENT_ID,
            btc_usd_instrument_public_id=_BTC_USD_PAPER_INSTRUMENT_ID,
            eth_usd_instrument_public_id=_ETH_USD_PAPER_INSTRUMENT_ID,
            eur_pln_order_public_id=_EUR_PLN_ORDER_ID,
            btc_usd_order_public_id=_BTC_USD_ORDER_ID,
            eth_usd_order_public_id=_ETH_USD_ORDER_ID,
            eur_pln_execution_public_id=_EUR_PLN_EXECUTION_ID,
            btc_usd_execution_public_id=_BTC_USD_EXECUTION_ID,
            eth_usd_execution_public_id=_ETH_USD_EXECUTION_ID,
            executed_signal_public_id=_EXECUTED_SIGNAL_ID,
            no_fill_signal_public_id=_NO_FILL_SIGNAL_ID,
            ai_review_public_id=_AI_REVIEW_ID,
            ai_event_public_id=_AI_EVENT_ID,
        ),
        times=FixtureTimes(
            anchor=anchor,
            window_from=anchor - _WINDOW,
            eur_pln_fill_at=anchor - _EUR_FILL_OFFSET,
            btc_usd_fill_at=anchor,
            eth_usd_fill_at=anchor - _INCOMPLETE_FILL_OFFSET,
        ),
        expected=FixtureExpected(
            complete=ExpectedCurrencyEconomics(
                usd=ExpectedPointEconomics(
                    realized_pnl=zero,
                    fee_pnl=-0.04,
                    accrual_pnl=zero,
                    unrealized_pnl=5.0,
                    net_pnl=4.96,
                ),
                pln=ExpectedPointEconomics(
                    realized_pnl=zero,
                    fee_pnl=-0.16,
                    accrual_pnl=zero,
                    unrealized_pnl=20.0,
                    net_pnl=19.84,
                ),
                eur=ExpectedPointEconomics(
                    realized_pnl=zero,
                    fee_pnl=-0.04,
                    accrual_pnl=zero,
                    unrealized_pnl=zero,
                    net_pnl=-0.04,
                ),
            ),
            incomplete=ExpectedIncompleteEconomics(
                incomplete_point_count=_INCOMPLETE_POINT_COUNT,
                reason="mark_unavailable",
                trigger_instrument_public_id=_ETH_USD_PAPER_INSTRUMENT_ID,
                realized_pnl=zero,
                fee_pnl=zero,
                accrual_pnl=zero,
                unrealized_pnl=None,
                net_pnl=None,
            ),
            marker_kinds=("fill", "signal", "ai_decision"),
            marker_outcomes=("executed", "no_fill", "rejected"),
        ),
    )


def _alembic_head() -> str:
    """Return the sole Alembic head declared by this checkout."""
    config = Config(str(_ALEMBIC_INI))
    head = ScriptDirectory.from_config(config).get_current_head()
    if head is None:
        raise PnlUatFixtureError("repository has no Alembic head")
    return head


async def _require_schema_head(session: AsyncSession) -> None:
    """Require the target schema version to equal the checkout Alembic head."""
    current = (
        await session.execute(text("SELECT version_num FROM alembic_version"))
    ).scalar_one_or_none()
    expected = _alembic_head()
    if not isinstance(current, str) or current != expected:
        raise PnlUatFixtureError(
            f"database schema is not at head; expected {expected}, found {current or 'none'}"
        )


async def _lock_fixture_tables_for_stable_cut(
    session: AsyncSession,
    dialect_name: str,
) -> None:
    """Serialize mapped-table writers across PostgreSQL checks and commit."""
    if dialect_name == "postgresql":
        await session.execute(text(_POSTGRES_LOCK_STATEMENT))


async def _require_paper_only_credentials(session: AsyncSession) -> None:
    """Reject a target containing any active credential that could reach a venue."""
    count = (
        await session.execute(
            select(func.count())
            .select_from(WalletCredential)
            .where(
                WalletCredential.known_to == KNOWN_TO_MAX,
                or_(
                    WalletCredential.exchange != "paper",
                    WalletCredential.credential_type != "paper",
                ),
            )
        )
    ).scalar_one()
    if int(count) != 0:
        raise PnlUatFixtureError("active non-paper credentials are forbidden in P&L UAT")


async def _require_pristine_namespace(session: AsyncSession) -> None:
    """Reject any prior fixture identity, label, symbol, or session collision."""
    wallet_collision = (
        await session.execute(
            select(func.count())
            .select_from(Wallet)
            .where(
                or_(
                    Wallet.public_id.in_((_HAPPY_WALLET_ID, _INCOMPLETE_WALLET_ID)),
                    Wallet.label.in_((_HAPPY_WALLET_LABEL, _INCOMPLETE_WALLET_LABEL)),
                    Wallet.session_id == _SESSION_ID,
                )
            )
        )
    ).scalar_one()
    temporal_collisions = (
        await session.execute(
            select(
                select(func.count())
                .select_from(Instrument)
                .where(Instrument.session_id == _SESSION_ID)
                .scalar_subquery()
                + select(func.count())
                .select_from(Order)
                .where(Order.session_id == _SESSION_ID)
                .scalar_subquery()
                + select(func.count())
                .select_from(Execution)
                .where(Execution.session_id == _SESSION_ID)
                .scalar_subquery()
                + select(func.count())
                .select_from(Signal)
                .where(Signal.session_id == _SESSION_ID)
                .scalar_subquery()
                + select(func.count())
                .select_from(TradeCommand)
                .where(TradeCommand.session_id == _SESSION_ID)
                .scalar_subquery()
                + select(func.count())
                .select_from(Candle)
                .where(Candle.session_id == _SESSION_ID)
                .scalar_subquery()
                + select(func.count())
                .select_from(AiReview)
                .where(AiReview.session_id == _SESSION_ID)
                .scalar_subquery()
            )
        )
    ).scalar_one()
    ai_event_collision = (
        await session.execute(
            select(func.count())
            .select_from(AiReviewEvent)
            .where(AiReviewEvent.public_id == _AI_EVENT_ID)
        )
    ).scalar_one()
    if any(
        int(value) != 0
        for value in (
            wallet_collision,
            temporal_collisions,
            ai_event_collision,
        )
    ):
        raise PnlUatFixtureError(
            "fixture namespace is already populated; discard the one-shot database"
        )


async def _require_canonical_symbol_seed_baseline(session: AsyncSession) -> None:
    """Require the exact migration-seeded symbol, alias, and capability graph."""
    symbol_rows = (
        await session.execute(
            select(
                Symbol.id,
                Symbol.public_id,
                Symbol.native_symbol,
                Symbol.base,
                Symbol.quote,
                Symbol.asset_type,
                Symbol.created_at,
                Symbol.session_id,
                Symbol.sequence_id,
                Symbol.timestamp,
                Symbol.known_to,
            ).order_by(Symbol.id)
        )
    ).all()
    symbols = {
        (native_symbol, base, quote, asset_type)
        for (
            _,
            _,
            native_symbol,
            base,
            quote,
            asset_type,
            _,
            _,
            _,
            _,
            _,
        ) in symbol_rows
    }
    symbol_seed_sessions = {session_id for *_, session_id, _, _, _ in symbol_rows}
    symbol_seed_timestamps = {timestamp for *_, timestamp, _ in symbol_rows}
    if (
        symbols != _OSS_BASELINE_SYMBOLS
        or tuple(row.id for row in symbol_rows) != tuple(range(1, len(_OSS_BASELINE_SYMBOLS) + 1))
        or tuple(row.sequence_id for row in symbol_rows)
        != tuple(range(1, len(_OSS_BASELINE_SYMBOLS) + 1))
        or any(
            row.known_to != KNOWN_TO_MAX or row.created_at != row.timestamp for row in symbol_rows
        )
        or len(symbol_seed_sessions) != 1
        or len(symbol_seed_timestamps) != 1
    ):
        raise PnlUatFixtureError(_OSS_BASELINE_REFUSAL)
    symbol_seed_session = next(iter(symbol_seed_sessions))
    symbol_seed_timestamp = next(iter(symbol_seed_timestamps))
    alias_rows = (
        await session.execute(
            select(
                Symbol.native_symbol,
                SymbolAlias.exchange,
                SymbolAlias.channel,
                SymbolAlias.exchange_symbol,
                SymbolAlias.id,
                SymbolAlias.sequence_id,
                SymbolAlias.known_to,
                SymbolAlias.session_id,
                SymbolAlias.created_at,
                SymbolAlias.timestamp,
            )
            .join(Symbol, Symbol.public_id == SymbolAlias.symbol_public_id)
            .order_by(SymbolAlias.id)
        )
    ).all()
    expected_aliases = tuple(
        (
            *alias,
            row_id,
            len(_OSS_BASELINE_SYMBOLS) + row_id,
            KNOWN_TO_MAX,
            symbol_seed_session,
            symbol_seed_timestamp,
            symbol_seed_timestamp,
        )
        for row_id, alias in enumerate(_OSS_BASELINE_ALIASES, start=1)
    )
    if tuple(alias_rows) != expected_aliases:
        raise PnlUatFixtureError(_OSS_BASELINE_REFUSAL)
    capability_rows = (
        await session.execute(
            select(
                Symbol.native_symbol,
                SymbolExchangeCapability.exchange,
                SymbolExchangeCapability.can_market_data,
                SymbolExchangeCapability.can_trade,
                SymbolExchangeCapability.source,
                SymbolExchangeCapability.reason,
                SymbolExchangeCapability.id,
                SymbolExchangeCapability.sequence_id,
                SymbolExchangeCapability.known_to,
                SymbolExchangeCapability.session_id,
                SymbolExchangeCapability.created_at,
                SymbolExchangeCapability.timestamp,
            )
            .join(Symbol, Symbol.public_id == SymbolExchangeCapability.symbol_public_id)
            .order_by(SymbolExchangeCapability.id)
        )
    ).all()
    capability_sequence_offset = len(_OSS_BASELINE_SYMBOLS) + len(_OSS_BASELINE_ALIASES)
    expected_capabilities = tuple(
        (
            *capability,
            row_id,
            capability_sequence_offset + row_id,
            KNOWN_TO_MAX,
            symbol_seed_session,
            symbol_seed_timestamp,
            symbol_seed_timestamp,
        )
        for row_id, capability in enumerate(_OSS_BASELINE_CAPABILITIES, start=1)
    )
    if tuple(capability_rows) != expected_capabilities:
        raise PnlUatFixtureError(_OSS_BASELINE_REFUSAL)


async def _require_fresh_oss_seed_baseline(session: AsyncSession) -> None:
    """Require the exact migrated and bundled-OSS-seeded database baseline.

    Every mapped table that the OSS seed does not populate must be empty. The
    nine populated tables must retain their exact row counts, while the users,
    settings, symbols, wallet, and credential are checked semantically. This
    prevents a merely prefix-named but previously used database from being
    mistaken for a disposable UAT target.

    Args:
        session: Transactional session used for baseline reads.

    Raises:
        PnlUatFixtureError: If any row differs from the expected fresh baseline.
    """
    for table in Base.metadata.sorted_tables:
        expected_count = _OSS_BASELINE_TABLE_COUNTS.get(table.name, 0)
        count = (await session.execute(select(func.count()).select_from(table))).scalar_one()
        if int(count) != expected_count:
            raise PnlUatFixtureError(_OSS_BASELINE_REFUSAL)
    user_rows = (await session.execute(select(User).order_by(User.id))).scalars().all()
    users = {
        user.username: (
            user.email,
            user.role,
            user.is_active,
            user.default_language,
            user.created_by_user_public_id,
            user.id,
            user.sequence_id,
            user.created_at == user.timestamp,
            user.known_to,
        )
        for user in user_rows
    }
    expected_users = {
        username: (
            email,
            role,
            True,
            None,
            None,
            row_id,
            sequence_id,
            True,
            KNOWN_TO_MAX,
        )
        for username, (email, role, row_id, sequence_id) in _OSS_BASELINE_USERS.items()
    }
    user_sessions = {user.session_id for user in user_rows}
    try:
        passwords_match = all(
            bcrypt.checkpw(_DEV_PASSWORD, user.password_hash.encode()) for user in user_rows
        )
    except ValueError:
        passwords_match = False
    if (users, len(user_sessions), passwords_match) != (expected_users, 1, True):
        raise PnlUatFixtureError(_OSS_BASELINE_REFUSAL)
    seed_session_id = next(iter(user_sessions))
    setting_rows = (await session.execute(select(Setting).order_by(Setting.id))).scalars().all()
    try:
        settings = {
            setting.key: (
                (
                    get_encryption_service().decrypt(setting.value)
                    if setting.is_encrypted
                    else setting.value
                ),
                setting.category,
                setting.description,
                setting.is_encrypted,
                setting.updated_by,
                setting.id,
                setting.sequence_id,
                setting.known_to,
                setting.session_id == seed_session_id,
            )
            for setting in setting_rows
        }
    except (InvalidToken, TypeError, ValueError):
        raise PnlUatFixtureError(_OSS_BASELINE_REFUSAL) from None
    expected_settings = {
        key: (
            value,
            category,
            description,
            key in _OSS_BASELINE_ENCRYPTED_SETTINGS,
            None,
            row_id,
            sequence_id,
            KNOWN_TO_MAX,
            True,
        )
        for key, (
            value,
            category,
            description,
            row_id,
            sequence_id,
        ) in _OSS_BASELINE_SETTINGS.items()
    }
    if settings != expected_settings:
        raise PnlUatFixtureError(_OSS_BASELINE_REFUSAL)
    await _require_canonical_symbol_seed_baseline(session)
    operator = (await session.execute(select(Operator))).scalar_one()
    wallet = (await session.execute(select(Wallet))).scalar_one()
    credential = (await session.execute(select(WalletCredential))).scalar_one()
    memberships = (
        (await session.execute(select(UserOperatorMembership).order_by(UserOperatorMembership.id)))
        .scalars()
        .all()
    )
    user_public_ids = {user.public_id for user in user_rows}
    try:
        credential_envelope: object = json.loads(
            get_encryption_service().decrypt(credential.encrypted_payload)
        )
    except (InvalidToken, TypeError, ValueError):
        raise PnlUatFixtureError(_OSS_BASELINE_REFUSAL) from None
    tenant_state = (
        operator.id,
        operator.label,
        operator.description,
        operator.sequence_id,
        operator.known_to,
        operator.session_id == seed_session_id,
        wallet.id,
        wallet.label,
        wallet.description,
        wallet.is_paper,
        wallet.sequence_id,
        wallet.known_to,
        wallet.session_id == seed_session_id,
        credential.id,
        credential.wallet_public_id == wallet.public_id,
        credential.exchange,
        credential.credential_type,
        credential.label,
        credential.sequence_id,
        credential.known_to,
        credential.session_id == seed_session_id,
        credential.timestamp == wallet.timestamp,
    )
    expected_tenant_state = (
        1,
        "default",
        "Default seed operator for single-user deployment",
        1,
        KNOWN_TO_MAX,
        True,
        1,
        "paper",
        "Paper-mode wallet seeded for open-source dev deployment (10k USD bootstrap)",
        True,
        1,
        KNOWN_TO_MAX,
        True,
        1,
        True,
        "paper",
        "paper",
        "paper bootstrap",
        1,
        KNOWN_TO_MAX,
        True,
        True,
    )
    public_id_to_username = {user.public_id: user.username for user in user_rows}
    membership_state = tuple(
        (
            public_id_to_username.get(membership.user_public_id),
            membership.operator_public_id == operator.public_id,
            membership.is_primary,
            membership.known_to,
            membership.session_id == seed_session_id,
            membership.id,
            membership.sequence_id,
        )
        for membership in memberships
    )
    expected_membership_state = tuple(
        (username, True, True, KNOWN_TO_MAX, True, index, index)
        for index, username in enumerate(_OSS_BASELINE_USERS, start=1)
    )
    if (
        tenant_state,
        credential_envelope,
        {membership.user_public_id for membership in memberships},
        membership_state,
    ) != (
        expected_tenant_state,
        {"initial_balance": "10000.0"},
        user_public_ids,
        expected_membership_state,
    ):
        raise PnlUatFixtureError(_OSS_BASELINE_REFUSAL)


async def _require_seed_preconditions(
    session: AsyncSession,
    dialect_name: str,
) -> None:
    """Validate one stable target cut before fixture mutation."""
    await _require_schema_head(session)
    await _lock_fixture_tables_for_stable_cut(session, dialect_name)
    await _require_paper_only_credentials(session)
    await _require_pristine_namespace(session)
    await _require_fresh_oss_seed_baseline(session)


async def _require_admin_user(session: AsyncSession) -> str:
    """Return the active seeded admin identity required for manual lineage."""
    row = (
        await session.execute(
            select(User.public_id, User.known_to).where(
                User.username == "admin",
                User.is_active.is_(True),
            )
        )
    ).one_or_none()
    if row is None:
        raise PnlUatFixtureError("active admin seed user is required before P&L UAT fixture")
    public_id, known_to = row
    if not isinstance(public_id, str) or known_to != KNOWN_TO_MAX:
        raise PnlUatFixtureError("active admin seed user is required before P&L UAT fixture")
    return public_id


async def _prepare_symbols(
    session: AsyncSession,
    valid_from: datetime,
) -> dict[str, str]:
    """Validate and backdate canonical schema-seed symbols for fixture history."""
    as_of = datetime.now(UTC)
    expected = {
        "EUR-PLN": ("EUR", "PLN", "forex"),
        "BTC-USD": ("BTC", "USD", "crypto"),
        "ETH-USD": ("ETH", "USD", "crypto"),
        "USD-PLN": ("USD", "PLN", "forex"),
        "EUR-USD": ("EUR", "USD", "forex"),
    }
    rows = (
        (await session.execute(select(Symbol).where(Symbol.native_symbol.in_(tuple(expected)))))
        .scalars()
        .all()
    )
    symbols = {row.native_symbol: row for row in rows}
    if len(rows) != len(expected) or set(symbols) != set(expected):
        raise PnlUatFixtureError("canonical P&L UAT symbols are missing from the schema seed")
    for native_symbol, (base, quote, asset_type) in expected.items():
        symbol = symbols[native_symbol]
        if (
            symbol.base != base
            or symbol.quote != quote
            or symbol.asset_type != asset_type
            or symbol.timestamp > as_of
            or symbol.known_to <= as_of
        ):
            raise PnlUatFixtureError(
                f"canonical symbol metadata is incompatible for {native_symbol}"
            )
        symbol.created_at = min(symbol.created_at, valid_from)
        symbol.timestamp = min(symbol.timestamp, valid_from)
    return {native_symbol: symbols[native_symbol].public_id for native_symbol in expected}


def _wallet(public_id: str, label: str, authored_at: datetime, sequence_id: int) -> Wallet:
    """Build one paper wallet without a credential."""
    return Wallet(
        public_id=public_id,
        label=label,
        description="Deterministic paper-only P&L browser UAT",
        is_paper=True,
        timestamp=authored_at,
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION_ID,
        sequence_id=sequence_id,
    )


def _instrument(
    public_id: str,
    symbol_public_id: str,
    exchange: str,
    source_exchange: str | None,
    authored_at: datetime,
    sequence_id: int,
) -> Instrument:
    """Build one source or paper instrument."""
    return Instrument(
        public_id=public_id,
        symbol_public_id=symbol_public_id,
        exchange=exchange,
        source_exchange=source_exchange,
        requires_ai_review=False,
        timestamp=authored_at,
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION_ID,
        sequence_id=sequence_id,
    )


def _order(
    public_id: str,
    instrument_public_id: str,
    wallet_public_id: str,
    client_order_id: str,
    exchange_order_id: str,
    size: float,
    price: float,
    created_at: datetime,
    sequence_id: int,
) -> Order:
    """Build one filled paper order."""
    return Order(
        public_id=public_id,
        instrument_public_id=instrument_public_id,
        wallet_public_id=wallet_public_id,
        operator_public_id=None,
        mode="paper",
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
        created_at=created_at,
        updated_at=created_at,
        side="buy",
        order_type="market",
        price=price,
        size=size,
        status="filled",
        time_in_force=None,
        filled_size=size,
        average_price=price,
        error=None,
        leverage=None,
        reduce_only=False,
        plan_public_id=None,
        timestamp=created_at,
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION_ID,
        sequence_id=sequence_id,
    )


def _execution(
    public_id: str,
    order_public_id: str,
    wallet_public_id: str,
    scope_sequence: int,
    size: float,
    price: float,
    fee: float,
    fee_asset: str,
    executed_at: datetime,
    sequence_id: int,
) -> Execution:
    """Build one append-only paper fill."""
    return Execution(
        public_id=public_id,
        order_public_id=order_public_id,
        wallet_public_id=wallet_public_id,
        operator_public_id=None,
        exchange="paper",
        mode="paper",
        scope_sequence=scope_sequence,
        exec_id=f"pnl-uat-exec-{scope_sequence}-{public_id[-4:]}",
        trade_id=f"pnl-uat-trade-{scope_sequence}-{public_id[-4:]}",
        side="buy",
        status="filled",
        price=price,
        size=size,
        fee=fee,
        fee_asset=fee_asset,
        price_decimal=None,
        size_decimal=None,
        fee_decimal=None,
        counter_amount_decimal=None,
        numeric_provenance=None,
        executed_at=executed_at,
        liquidity_role="maker",
        timestamp=executed_at,
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION_ID,
        sequence_id=sequence_id,
    )


def _fill_event(
    order: Order,
    execution: Execution,
    instrument: str,
    command_public_id: str | None,
    strategy_tag: str | None,
) -> VenueEvent:
    """Build canonical append-only fill evidence for one fixture execution."""
    event_time = execution.executed_at or execution.timestamp
    return VenueEvent(
        public_id=str(uuid5(_VENUE_EVENT_NAMESPACE, execution.public_id)),
        event_type="fill_observed",
        shard_key=compute_shard_key(
            instrument=instrument,
            exchange=ExchangeEnum.PAPER,
            mode=ExecutionModeEnum.PAPER,
            wallet_public_id=execution.wallet_public_id,
            strategy_tag=strategy_tag,
        ),
        wallet_public_id=execution.wallet_public_id,
        command_public_id=command_public_id,
        exchange="paper",
        instrument=instrument,
        mode="paper",
        exchange_order_id=order.exchange_order_id,
        client_order_id=order.client_order_id,
        venue_client_id=f"venue-{order.client_order_id}",
        side=execution.side,
        status=execution.status,
        fill_price=execution.price,
        fill_size=execution.size,
        cum_fill_size=execution.size,
        fee=execution.fee,
        fee_asset=execution.fee_asset,
        exec_id=execution.exec_id,
        trade_id=execution.trade_id,
        error=None,
        venue_timestamp=event_time,
        received_at=event_time,
        payload_json=None,
        liquidity_role=execution.liquidity_role,
        paired_group_id=None,
        timestamp=event_time,
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION_ID,
        sequence_id=900 + execution.sequence_id,
    )


def _signal(
    public_id: str,
    wallet_public_id: str,
    fired_at: datetime,
    strategy_name: str,
    reason: str,
    sequence_id: int,
) -> Signal:
    """Build one BTC-USD signal marker source."""
    return Signal(
        public_id=public_id,
        instrument_public_id=_BTC_USD_PAPER_INSTRUMENT_ID,
        wallet_public_id=wallet_public_id,
        operator_public_id=None,
        fired_at=fired_at,
        side="buy",
        strength=0.75,
        reason=reason,
        strategy_name=strategy_name,
        price=100.0,
        paired_group_id=None,
        timestamp=fired_at,
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION_ID,
        sequence_id=sequence_id,
    )


def _trade_command(
    public_id: str,
    wallet_public_id: str,
    user_public_id: str | None,
    instrument: str,
    client_order_id: str,
    exchange_order_id: str,
    size: float,
    price: float,
    created_at: datetime,
    source_surface: str,
    strategy_id: str,
    signal_public_id: str | None,
    correlation_id: str,
    sequence_id: int,
) -> TradeCommand:
    """Build one immutable initiating-command lineage row."""
    strategy_tag = strategy_id if source_surface == "strategy" else None
    return TradeCommand(
        public_id=public_id,
        command_type="submit",
        shard_key=compute_shard_key(
            instrument=instrument,
            exchange=ExchangeEnum.PAPER,
            mode=ExecutionModeEnum.PAPER,
            wallet_public_id=wallet_public_id,
            strategy_tag=strategy_tag,
        ),
        wallet_public_id=wallet_public_id,
        operator_public_id=None,
        user_public_id=user_public_id,
        exchange="paper",
        instrument=instrument,
        mode="paper",
        strategy_id=strategy_id,
        client_order_id=client_order_id,
        venue_client_id=f"venue-{client_order_id}",
        idempotency_key=f"idempotency-{client_order_id}",
        side="buy",
        order_type="market",
        quantity=size,
        price=price,
        stop_price=None,
        leverage=None,
        reduce_only=False,
        status="filled",
        attempt_count=1,
        last_error=None,
        created_at=created_at,
        dispatched_at=created_at,
        acked_at=created_at,
        terminal_at=created_at,
        exchange_order_id=exchange_order_id,
        supersedes_command_id=None,
        correlation_id=correlation_id,
        plan_public_id=None,
        source_surface=source_surface,
        signal_public_id=signal_public_id,
        ai_review_public_id=None,
        submitted_notional_usd=None,
        origin="live",
        replay_window_start=None,
        replay_window_end=None,
        timestamp=created_at,
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION_ID,
        sequence_id=sequence_id,
    )


def _ai_rows(
    admin_user_public_id: str,
    anchor: datetime,
) -> tuple[AiReview, AiReviewEvent]:
    """Build one rejected AI decision and its append-only marker event."""
    occurred_at = anchor - timedelta(minutes=20)
    created_at = anchor - timedelta(minutes=30)
    envelope: JsonObject = {
        "instrument": "BTC-USD",
        "mode": "paper",
        "side": "buy",
        "strategy": "momentum",
    }
    metadata: JsonObject = {"last_price": 100.0, "source": "synthetic"}
    snapshot_hash = hashlib.sha256(
        json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    review = AiReview(
        public_id=_AI_REVIEW_ID,
        session_id=_SESSION_ID,
        sequence_id=801,
        user_public_id=admin_user_public_id,
        operator_public_id=_AI_OPERATOR_ID,
        wallet_public_id=_HAPPY_WALLET_ID,
        instrument_public_id=_BTC_USD_PAPER_INSTRUMENT_ID,
        strategy_public_id=_AI_STRATEGY_ID,
        selected_delegate_public_id=_AI_DELEGATE_ID,
        responding_delegate_public_id=_AI_DELEGATE_ID,
        resolution_mode="pick_one_primary",
        status="resolved_rejected",
        signal_envelope=envelope,
        signal_snapshot_hash=snapshot_hash,
        instrument_metadata=metadata,
        deadline=created_at + timedelta(days=1),
        fanout_after=created_at + timedelta(minutes=1),
        decision="reject",
        rationale="Deterministic P&L UAT rejection",
        dispatch_version=0,
        counter_decremented_at=occurred_at,
        created_at=created_at,
        updated_at=occurred_at,
        resolved_at=occurred_at,
    )
    payload: JsonObject = {
        "decision": "reject",
        "rationale": "Deterministic P&L UAT rejection",
    }
    event = AiReviewEvent(
        public_id=_AI_EVENT_ID,
        review_public_id=_AI_REVIEW_ID,
        event_type="decision_recorded",
        actor_delegate_public_id=_AI_DELEGATE_ID,
        previous_status="pending",
        new_status="resolved_rejected",
        payload=payload,
        occurred_at=occurred_at,
    )
    return review, event


def _candle_close(
    instrument_public_id: str,
    close_minute: datetime,
    eur_fill_at: datetime,
    anchor: datetime,
) -> float:
    """Return one exact source close for a fixture series and close minute."""
    if instrument_public_id == _BTC_USD_SOURCE_INSTRUMENT_ID:
        return 100.0
    if instrument_public_id == _USD_PLN_SOURCE_INSTRUMENT_ID:
        return 4.0
    elapsed = max(0.0, (close_minute - eur_fill_at).total_seconds())
    span = (anchor - eur_fill_at).total_seconds()
    eur_pln = 4.0 + min(1.0, elapsed / span)
    if instrument_public_id == _EUR_PLN_SOURCE_INSTRUMENT_ID:
        return eur_pln
    if instrument_public_id == _EUR_USD_SOURCE_INSTRUMENT_ID:
        return eur_pln / 4.0
    raise PnlUatFixtureError(f"unsupported candle fixture instrument: {instrument_public_id}")


def _candles(anchor: datetime) -> list[Candle]:
    """Build complete 1m source candles covering every requested close minute."""
    window_from = anchor - _WINDOW
    eur_fill_at = anchor - _EUR_FILL_OFFSET
    source_instruments = (
        _EUR_PLN_SOURCE_INSTRUMENT_ID,
        _BTC_USD_SOURCE_INSTRUMENT_ID,
        _USD_PLN_SOURCE_INSTRUMENT_ID,
        _EUR_USD_SOURCE_INSTRUMENT_ID,
    )
    rows: list[Candle] = []
    sequence_id = 1000
    for source_instrument in source_instruments:
        open_at = window_from - timedelta(minutes=1)
        while open_at < anchor:
            close_minute = open_at + timedelta(minutes=1)
            close = _candle_close(source_instrument, close_minute, eur_fill_at, anchor)
            rows.append(
                Candle(
                    public_id=str(
                        uuid5(
                            _CANDLE_NAMESPACE,
                            f"{source_instrument}:{open_at.isoformat()}",
                        )
                    ),
                    instrument_public_id=source_instrument,
                    open_at=open_at,
                    timeframe="1m",
                    open=close,
                    high=close,
                    low=close,
                    close=close,
                    volume=1.0,
                    vwap=close,
                    trades=1,
                    source="synthesized",
                    complete=True,
                    timestamp=close_minute,
                    known_to=KNOWN_TO_MAX,
                    session_id=_SESSION_ID,
                    sequence_id=sequence_id,
                )
            )
            sequence_id += 1
            open_at += timedelta(minutes=1)
    return rows


def _fixture_rows(
    admin_user_public_id: str,
    symbols: dict[str, str],
    anchor: datetime,
) -> list[object]:
    """Build every non-candle fixture row in dependency-neutral order."""
    authored_at = anchor - timedelta(days=2)
    eur_fill_at = anchor - _EUR_FILL_OFFSET
    incomplete_fill_at = anchor - _INCOMPLETE_FILL_OFFSET
    ai_review, ai_event = _ai_rows(admin_user_public_id, anchor)
    eur_order = _order(
        _EUR_PLN_ORDER_ID,
        _EUR_PLN_PAPER_INSTRUMENT_ID,
        _HAPPY_WALLET_ID,
        _MANUAL_CLIENT_ORDER_ID,
        "pnl-uat-exchange-manual",
        20.04,
        4.0,
        eur_fill_at,
        401,
    )
    btc_order = _order(
        _BTC_USD_ORDER_ID,
        _BTC_USD_PAPER_INSTRUMENT_ID,
        _HAPPY_WALLET_ID,
        _SYSTEM_CLIENT_ORDER_ID,
        "pnl-uat-exchange-system",
        1.0,
        100.0,
        anchor,
        402,
    )
    eth_order = _order(
        _ETH_USD_ORDER_ID,
        _ETH_USD_PAPER_INSTRUMENT_ID,
        _INCOMPLETE_WALLET_ID,
        _INCOMPLETE_CLIENT_ORDER_ID,
        "pnl-uat-exchange-incomplete",
        1.0,
        100.0,
        incomplete_fill_at,
        403,
    )
    eur_execution = _execution(
        _EUR_PLN_EXECUTION_ID,
        _EUR_PLN_ORDER_ID,
        _HAPPY_WALLET_ID,
        1,
        20.04,
        4.0,
        0.04,
        "EUR",
        eur_fill_at,
        501,
    )
    btc_execution = _execution(
        _BTC_USD_EXECUTION_ID,
        _BTC_USD_ORDER_ID,
        _HAPPY_WALLET_ID,
        2,
        1.0,
        100.0,
        0.0,
        "USD",
        anchor,
        502,
    )
    eth_execution = _execution(
        _ETH_USD_EXECUTION_ID,
        _ETH_USD_ORDER_ID,
        _INCOMPLETE_WALLET_ID,
        1,
        1.0,
        100.0,
        0.0,
        "USD",
        incomplete_fill_at,
        503,
    )
    return [
        _wallet(_HAPPY_WALLET_ID, _HAPPY_WALLET_LABEL, authored_at, 101),
        _wallet(_INCOMPLETE_WALLET_ID, _INCOMPLETE_WALLET_LABEL, authored_at, 102),
        _instrument(
            _EUR_PLN_SOURCE_INSTRUMENT_ID,
            symbols["EUR-PLN"],
            "walutomat",
            None,
            authored_at,
            301,
        ),
        _instrument(
            _BTC_USD_SOURCE_INSTRUMENT_ID,
            symbols["BTC-USD"],
            "kraken",
            None,
            authored_at,
            302,
        ),
        _instrument(
            _ETH_USD_SOURCE_INSTRUMENT_ID,
            symbols["ETH-USD"],
            "kraken",
            None,
            authored_at,
            303,
        ),
        _instrument(
            _USD_PLN_SOURCE_INSTRUMENT_ID,
            symbols["USD-PLN"],
            "walutomat",
            None,
            authored_at,
            304,
        ),
        _instrument(
            _EUR_USD_SOURCE_INSTRUMENT_ID,
            symbols["EUR-USD"],
            "walutomat",
            None,
            authored_at,
            305,
        ),
        _instrument(
            _EUR_PLN_PAPER_INSTRUMENT_ID,
            symbols["EUR-PLN"],
            "paper",
            "walutomat",
            authored_at,
            311,
        ),
        _instrument(
            _BTC_USD_PAPER_INSTRUMENT_ID,
            symbols["BTC-USD"],
            "paper",
            "kraken",
            authored_at,
            312,
        ),
        _instrument(
            _ETH_USD_PAPER_INSTRUMENT_ID,
            symbols["ETH-USD"],
            "paper",
            "kraken",
            authored_at,
            313,
        ),
        _signal(
            _EXECUTED_SIGNAL_ID,
            _HAPPY_WALLET_ID,
            anchor,
            "momentum",
            "Deterministic executed signal",
            701,
        ),
        _signal(
            _NO_FILL_SIGNAL_ID,
            _HAPPY_WALLET_ID,
            anchor - timedelta(minutes=30),
            "mean-reversion",
            "Deterministic no-fill signal",
            702,
        ),
        _trade_command(
            _MANUAL_COMMAND_ID,
            _HAPPY_WALLET_ID,
            admin_user_public_id,
            "EUR-PLN",
            _MANUAL_CLIENT_ORDER_ID,
            "pnl-uat-exchange-manual",
            20.04,
            4.0,
            eur_fill_at,
            "rest",
            "manual",
            None,
            _MANUAL_CORRELATION_ID,
            601,
        ),
        _trade_command(
            _SYSTEM_COMMAND_ID,
            _HAPPY_WALLET_ID,
            None,
            "BTC-USD",
            _SYSTEM_CLIENT_ORDER_ID,
            "pnl-uat-exchange-system",
            1.0,
            100.0,
            anchor,
            "strategy",
            "momentum",
            _EXECUTED_SIGNAL_ID,
            _SYSTEM_CORRELATION_ID,
            602,
        ),
        eur_order,
        btc_order,
        eth_order,
        eur_execution,
        btc_execution,
        eth_execution,
        _fill_event(
            eur_order,
            eur_execution,
            "EUR-PLN",
            _MANUAL_COMMAND_ID,
            None,
        ),
        _fill_event(
            btc_order,
            btc_execution,
            "BTC-USD",
            _SYSTEM_COMMAND_ID,
            "momentum",
        ),
        _fill_event(
            eth_order,
            eth_execution,
            "ETH-USD",
            None,
            None,
        ),
        ai_review,
        ai_event,
    ]


async def _seed_fixture_database_unchecked(
    db_url: URL,
    anchor: datetime,
) -> PnlUatManifest:
    """Seed a head-migrated database after the caller has fenced its location.

    This internal function exists so isolated SQLite tests can exercise the real
    schema and P&L service. The CLI reaches it only through
    :func:`seed_fixture_database`, which applies the PostgreSQL loopback and
    database-prefix guards first.

    Args:
        db_url: Parsed async SQLAlchemy URL.
        anchor: Inclusive P&L window endpoint.

    Returns:
        Manifest matching the committed fixture.

    Raises:
        PnlUatFixtureError: If any precondition or insert fails.
    """
    manifest = build_manifest(anchor)
    engine: AsyncEngine | None = None
    try:
        engine = create_async_engine(db_url, poolclass=NullPool)
        async with (
            AsyncSession(engine, expire_on_commit=False) as session,
            session.begin(),
        ):
            await _require_seed_preconditions(session, engine.dialect.name)
            symbols = await _prepare_symbols(session, anchor - timedelta(days=2))
            admin_user_public_id = await _require_admin_user(session)
            session.add_all(_fixture_rows(admin_user_public_id, symbols, anchor))
            session.add_all(_candles(anchor))
    except PnlUatFixtureError:
        raise
    except (OSError, SQLAlchemyError, ValueError) as exc:
        raise PnlUatFixtureError(
            "fixture database operation failed; no transaction committed"
        ) from exc
    finally:
        if engine is not None:
            await engine.dispose()
    await _seed_activation_anchors(db_url, manifest)
    return manifest


async def _seed_activation_anchors(
    db_url: URL,
    manifest: PnlUatManifest,
) -> None:
    """Create all six anchors after fixture rows commit through production code."""
    repository = SQLAlchemyRepository(db_url.render_as_string(hide_password=False))
    try:
        for wallet_public_id in (
            manifest.ids.happy_wallet_public_id,
            manifest.ids.incomplete_wallet_public_id,
        ):
            for valuation_ccy in ("USD", "PLN", "EUR"):
                await ensure_wallet_pnl_anchor(
                    repository,
                    wallet_public_id,
                    manifest.mode,
                    valuation_ccy,
                    manifest.times.window_from,
                    manifest.times.anchor,
                )
    except (OSError, SQLAlchemyError, RuntimeError, ValueError) as exc:
        raise PnlUatFixtureError(
            "activation-anchor creation failed after fixture commit; "
            "discard the one-shot database"
        ) from exc
    finally:
        await repository.engine.dispose()


async def seed_fixture_database(
    db_url: str | URL,
    anchor: datetime,
) -> PnlUatManifest:
    """Seed one disposable local PostgreSQL database after URL validation.

    Args:
        db_url: Raw or parsed URL to validate again at this public boundary.
        anchor: Inclusive P&L window endpoint.

    Returns:
        Manifest matching the committed fixture.
    """
    validated_url = validate_target_database(db_url)
    return await _seed_fixture_database_unchecked(validated_url, anchor)


def _validated_manifest_path(path: Path) -> Path:
    """Return a canonical new manifest path or fail before database access."""
    try:
        safe_path = resolve_operator_file(path, must_exist=False)
    except UnsafePathError as exc:
        raise PnlUatFixtureError(
            "manifest parent directory does not exist or manifest path is unsafe"
        ) from exc
    if safe_path.exists():
        raise PnlUatFixtureError("manifest path already exists; refusing to overwrite it")
    return safe_path


def write_manifest(path: Path, manifest: PnlUatManifest) -> None:
    """Create a manifest without overwriting an existing artifact.

    Args:
        path: New manifest file path.
        manifest: Credential-free fixture manifest.

    Raises:
        PnlUatFixtureError: If the path is unsafe or cannot be created.
    """
    safe_path = _validated_manifest_path(path)
    try:
        with safe_path.open("x", encoding="utf-8") as handle:
            handle.write(manifest.model_dump_json(indent=2))
            handle.write("\n")
    except OSError as exc:
        raise PnlUatFixtureError(
            "manifest creation failed; discard the one-shot database before retrying"
        ) from exc


async def run_fixture(db_url: str, anchor: datetime, manifest_path: Path) -> PnlUatManifest:
    """Seed the database and emit its manifest.

    Args:
        db_url: Async SQLAlchemy PostgreSQL URL.
        anchor: Inclusive P&L window endpoint.
        manifest_path: New manifest path.

    Returns:
        The committed fixture manifest.
    """
    safe_manifest_path = _validated_manifest_path(manifest_path)
    validated_url = validate_target_database(db_url)
    manifest = await seed_fixture_database(validated_url, anchor)
    write_manifest(safe_manifest_path, manifest)
    return manifest


def _argument_parser() -> argparse.ArgumentParser:
    """Build the credential-free command-line parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--anchor")
    return parser


def main() -> int:
    """Run the guarded one-shot fixture command.

    Returns:
        Zero after a committed fixture and manifest, otherwise two.
    """
    args = _argument_parser().parse_args()
    db_url = os.environ.get("DB_URL")
    if db_url is None:
        print("error: DB_URL is required", file=sys.stderr)
        return 2
    try:
        anchor = parse_anchor(args.anchor)
        manifest = asyncio.run(run_fixture(db_url, anchor, args.manifest))
    except PnlUatFixtureError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(
        f"P&L UAT fixture seeded at {manifest.times.anchor.isoformat()} "
        f"with manifest {args.manifest}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
