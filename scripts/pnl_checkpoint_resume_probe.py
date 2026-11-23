"""Read-only production probe comparing cold replay vs in-memory checkpoint resume.

Usage:
    PYTHONPATH=src python scripts/pnl_checkpoint_resume_probe.py \
        --wallet <WALLET_PUBLIC_ID> --mode live --valuation-ccy USD --window-minutes 60

Safety architecture:
    * Builds its own isolated engine before any query executes. On PostgreSQL,
      asyncpg sets ``default_transaction_read_only='on'``, ``statement_timeout``,
      ``lock_timeout='1000'``, ``idle_in_transaction_session_timeout='10000'``,
      and ``application_name='pnl_checkpoint_resume_probe'`` at connection startup.
      On SQLite, ``PRAGMA query_only = ON`` and ``PRAGMA busy_timeout = 1000``
      are applied on connect.
    * Verifies read-only mode and timeout bounds on every pooled checkout.
    * Uses PostgreSQL ``has_table_privilege`` to refuse roles with ``INSERT``,
      ``UPDATE``, or ``DELETE`` privileges on key P&L tables. Use a dedicated
      read-only role such as ``snapper_ro``.
    * Note: ``lock_timeout`` (1 s) bounds lock acquisition only; statement execution
      and idle lock retention are bounded by ``statement_timeout``,
      ``idle_in_transaction_session_timeout``, and ``--max-runtime-seconds``.
      The probe rechecks the monotonic deadline before reporting a verdict.
    * RSS checks sample memory growth and Linux ``RLIMIT_AS`` limits virtual
      memory. Use a container memory limit (``docker run --memory``) and an
      outer timeout to enforce hard memory and runtime bounds.
    * The ``do_orm_execute`` listener is class-global. Use it only in the
      standalone probe process, never inside the application process.
    * Redacts credentials/DSNs from all error and CLI output.

Row budgets:
    * Each budgeted SELECT adds up to one bounded scan before the load. Size
      ``--max-runtime-seconds`` and the outer timeout for about twice the candle
      scan work.
    * Execution rows include the spot execution chain tip fold and the
      timeline execution prefix, frozen prefix, and frozen suffix reads.
      Checkpoint verification's prefix count is an aggregate. Its chain-tip
      folds and frozen prefix read run in separate transactions.
    * Fill rows include timeline fill lineage, execution annulment
      contradiction witnesses, and the scope fill prefix read for the gap
      check. Annulment rows come from the execution annulment manifest.
    * Accrual rows come from ``get_accruals_for_pnl``. Candle rows come from
      ``get_pnl_timeline_candles``, ``get_pnl_fx_rate_candles``, and
      ``get_pnl_fx_rate_candles_at``.
    * The probe counts candle rows across the replay's batches and counts
      other rows per transaction. If a frozen suffix read loads candidates and
      falls back to a full prefix in the same transaction, both buffers count.

Exit codes:
    * ``0``: ``RESULT: PASS`` (cold and resumed outputs match bit-for-bit)
    * ``1``: ``RESULT: FAIL <mismatch_detail>`` (divergence detected)
    * ``2``: ``RESULT: FAIL <reason>`` (error, missing state, safety refusal, timeout, interrupt)
    * ``3``: ``RESULT: REFUSED <reason>`` (closed checkpoint, scope, or cap refusal)
"""

import argparse
import asyncio
import math
import os
import re
import resource
import sys
import time
from collections.abc import Iterator
from collections.abc import Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from dataclasses import fields
from dataclasses import is_dataclass
from datetime import datetime
from datetime import timedelta
from itertools import pairwise
from pathlib import Path
from typing import Final
from typing import Literal
from typing import NoReturn
from typing import Protocol
from typing import cast
from weakref import WeakKeyDictionary

from aiosqlite import Connection as SQLiteConnection
from sqlalchemy import Select
from sqlalchemy import event
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.engine import Engine
from sqlalchemy.engine import Result
from sqlalchemy.engine import make_url
from sqlalchemy.engine.result import null_result
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import ORMExecuteState
from sqlalchemy.orm import Session
from sqlalchemy.orm import SessionTransaction
from sqlalchemy.pool import StaticPool
from sqlalchemy.sql.elements import TextClause

from snapper.application.portfolio.fx_conversion_shadow import POINTWISE_FX_ELECTION_POLICY_VERSION
from snapper.application.portfolio.pnl_anchor_identity import normalize_portfolio_pnl_valuation_ccy
from snapper.application.portfolio.pnl_anchor_identity import (
    normalize_portfolio_pnl_wallet_public_id,
)
from snapper.application.portfolio.pnl_opening_checkpoint_capture import PnlCheckpointCaptureRequest
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    execution_manifest_digest,
)
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import PnlCheckpointFallback
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import PnlCheckpointResume
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import (
    PnlCheckpointResumeEligibility,
)
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import (
    verify_checkpoint_for_resume,
)
from snapper.application.portfolio.pnl_snapshot_planner import FINALIZATION_LAG
from snapper.application.portfolio.pnl_snapshotter_checkpoint import checkpoint_anchor_identity
from snapper.application.portfolio.pnl_timeline_service import PNL_TIMELINE_CALC_VERSION
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReadPolicy
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayOptions
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_series
from snapper.config.settings import get_bootstrap_settings
from snapper.core.json_types import JsonValue
from snapper.data.models import AccrualLedger
from snapper.data.models import Candle
from snapper.data.models import Execution
from snapper.data.models import ExecutionAnnulment
from snapper.data.models import VenueEvent
from snapper.data.pnl_correction_state import PnlCorrectionFrontier
from snapper.data.pnl_correction_state import PnlOpeningCheckpointRecord
from snapper.data.repository import PortfolioPnlSampleScope
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PnlTimelineExecutionPrefix
from snapper.data.repository_types import PnlTimelineFillWitnessCut
from snapper.data.repository_types import PortfolioPnlAnchorRow

_MINUTE = timedelta(minutes=1)
DEFAULT_STATEMENT_TIMEOUT_MS: Final[int] = 30_000
MAX_STATEMENT_TIMEOUT_MS: Final[int] = 60_000
LOCK_TIMEOUT_MS: Final[int] = 1_000
IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS: Final[int] = 10_000
DEFAULT_MAX_RUNTIME_SECONDS: Final[float] = 120.0
MAX_RUNTIME_SECONDS: Final[float] = 600.0
DEFAULT_MAX_EXECUTIONS: Final[int] = 200_000
DEFAULT_MAX_ACCRUALS: Final[int] = 200_000
DEFAULT_MAX_CANDLES: Final[int] = 2_000_000
DEFAULT_MAX_RSS_MB: Final[int] = 2048
ADDRESS_SPACE_HEADROOM_FACTOR: Final[int] = 4
PROBE_APPLICATION_NAME: Final[str] = "pnl_checkpoint_resume_probe"
PROBE_DB_URL_ENV_VAR: Final[str] = "PNL_PROBE_DB_URL"
READONLY_ROLE_WARNING: Final[str] = (
    "WARNING: neither --db-url nor PNL_PROBE_DB_URL is set; using app DB_URL. "
    "Running under a dedicated read-only role (e.g. snapper_ro) is recommended."
)

_SQL_IDENTIFIER_START = r"[A-Za-z_\x80-\U0010ffff]"
_SQL_IDENTIFIER_CONTINUE = r"[A-Za-z_0-9\x80-\U0010ffff]"
_SQL_TOKEN_RE = re.compile(
    r"(?P<space>[ \t\r\n\f\v]+)|(?P<line_comment>--)|(?P<block_comment>/\*)"
    r"""|(?P<quote>[Ee]'|[Uu]&['"]|['"`\[])"""
    rf"|(?P<dollar_quote>\$(?:{_SQL_IDENTIFIER_START}{_SQL_IDENTIFIER_CONTINUE}*)?\$)"
    rf"|(?P<word>{_SQL_IDENTIFIER_START}(?:{_SQL_IDENTIFIER_CONTINUE}|\$)*)"
    r"|(?P<symbol>\$[0-9]+|[0-9]+|[.,;()\]+*/<>=~!@#%^&|?:-])"
)
_SQL_BLOCK_MARKER_RE = re.compile(r"/\*|\*/")
_SQL_PG_LINE_END_RE = re.compile(r"[\r\n]")
_SQL_SQLITE_LINE_END_RE = re.compile(r"\n")
_UNCLASSIFIED_SQL = "write_sql_refused:unclassified_sql"
_VMSIZE_LINE_RE = re.compile(r"^VmSize:\s*(\d+)\s*kB\s*$", re.MULTILINE)
_FOR_LOCK_RE = re.compile(r"\bFOR\s+(?:(?:NO\s+KEY\s+)?UPDATE|(?:KEY\s+)?SHARE)\b")
_READ_WRITE_RE = re.compile(r"\bREAD\s+WRITE\b")
_SELECT_INTO_RE = re.compile(r"\bSELECT\b[\s\S]*\bINTO\b")
_EXPLAIN_ANALYZE_RE = re.compile(r"\bANALYZE\b")
_COPY_PROGRAM_RE = re.compile(r"\bCOPY\b.*\b(?:FROM|TO)\s+PROGRAM\b")
_FORBIDDEN_SQL_NAME_RE = re.compile(
    r"SET_CONFIG|SETVAL|NEXTVAL|DBLINK(?:_.*)?|LO_.*|LOREAD|LOWRITE"
    r"|PG_(?:(?:TRY_)?ADVISORY(?:_[A-Z]+)*|SLEEP(?:_FOR|_UNTIL)?|NOTIFY"
    r"|TERMINATE_BACKEND|CANCEL_BACKEND|RELOAD_CONF|ROTATE_LOGFILE"
    r"|FILE_(?:WRITE|SYNC|RENAME|UNLINK)|LOGDIR_LS)"
)
_BOUND_TX_CONFIG_RE = re.compile(
    r"^SELECT\s+set_config\('statement_timeout',\s*CASE\s+WHEN\s+"
    r"current_setting\('statement_timeout'\)::interval\s*=\s*interval\s*'0'\s+OR\s+"
    r"current_setting\('statement_timeout'\)::interval\s*>\s*interval\s*('[^']+')\s+"
    r"THEN\s+('[^']+')\s+ELSE\s+current_setting\('statement_timeout'\)\s+END,\s*true\),\s*"
    r"set_config\('lock_timeout',\s*CASE\s+WHEN\s+"
    r"current_setting\('lock_timeout'\)::interval\s*=\s*interval\s*'0'\s+OR\s+"
    r"current_setting\('lock_timeout'\)::interval\s*>\s*interval\s*('[^']+')\s+"
    r"THEN\s+('[^']+')\s+ELSE\s+current_setting\('lock_timeout'\)\s+END,\s*true\)$",
    re.IGNORECASE,
)
_LOCAL_TIMEOUT_STMT_RE = re.compile(
    r"^SET\s+LOCAL\s+(STATEMENT_TIMEOUT|LOCK_TIMEOUT)\b(?:\s*(?:=|TO)\s*(.*))?$",
    re.IGNORECASE,
)
_LOCAL_TIMEOUT_VAL_RE = re.compile(
    r"^(?:(\d+)|'(\d+)\s*(ms|seconds?|s|minutes?|min)?')$",
    re.IGNORECASE,
)
_LOCAL_TIMEOUT_UNITS: Final[dict[str, int]] = {
    "ms": 1,
    "s": 1_000,
    "second": 1_000,
    "seconds": 1_000,
    "min": 60_000,
    "minute": 60_000,
    "minutes": 60_000,
}
_PG_DURATION_TOKEN_RE = re.compile(r"(?:(\d+)\s*(ms|s|min|h))\s*")
_PG_UNIT_MS: Final[dict[str, int]] = {
    "ms": 1,
    "s": 1_000,
    "min": 60_000,
    "h": 3_600_000,
}
_DSN_USERINFO_RE = re.compile(r"([a-zA-Z][a-zA-Z0-9+.-]*://)([^\s'\"@/][^\s'\"@]*)(@)")
_SECRET_KV_RE = re.compile(
    r"(?i)(\b(?:password|passwd|pwd|secret|token|api_key)\s*=\s*)(?:'[^']*'|\"[^\"]*\"|[^\s&'\",;)]+)"
)
_KEY_PNL_TABLES: Final[tuple[str, ...]] = (
    "portfolio_pnl_points",
    "portfolio_pnl_correction_frontiers",
    "portfolio_pnl_opening_checkpoints",
    "executions",
    "execution_annulments",
    "venue_events",
    "accrual_ledger",
    "candles",
)
_WRITE_PRIVILEGES: Final[tuple[str, ...]] = ("INSERT", "UPDATE", "DELETE")
_ROLE_PRIVILEGE_QUERY = text(
    "SELECT checks.table_name, checks.privilege "
    "FROM (VALUES "
    + ", ".join(f"('{table}', '{priv}')" for table in _KEY_PNL_TABLES for priv in _WRITE_PRIVILEGES)
    + ") AS checks(table_name, privilege) "
    "WHERE has_table_privilege(current_user, checks.table_name, checks.privilege)"
)
_READ_ONLY_VERBS = frozenset(
    {
        "SELECT",
        "WITH",
        "SHOW",
        "VALUES",
        "EXPLAIN",
        "BEGIN",
        "COMMIT",
        "END",
        "ROLLBACK",
        "SAVEPOINT",
        "RELEASE",
    }
)
_WRITE_KEYWORDS = frozenset(
    {
        "INSERT",
        "UPDATE",
        "DELETE",
        "REPLACE",
        "MERGE",
        "UPSERT",
        "CREATE",
        "ALTER",
        "DROP",
        "TRUNCATE",
        "VACUUM",
        "ATTACH",
        "DETACH",
        "GRANT",
        "REVOKE",
        "COPY",
        "CALL",
        "DO",
        "LOCK",
    }
)
_CLOSED_REFUSAL_EXACT = frozenset(
    {
        "cut_moved",
        "frontier_has_fill_gap",
        "frontier_incomplete_cut",
        "frontier_window_too_short",
        "capture_fill_gap",
        "missing_capture_fill_witness_cut",
    }
)
_CLOSED_REFUSAL_PREFIXES: Final[tuple[str, ...]] = (
    "caps_exceeded:",
    "checkpoint_capture_refused:",
    "checkpoint_verify_fallback:",
    "checkpoint_resume_fallback:",
)


class CheckpointResumeProbeError(RuntimeError):
    """Explicit failure when read-only invariants, bounds, or resume equivalence fail."""


class _CursorProtocol(Protocol):
    """Minimal DBAPI cursor surface used for connection initialization and verification."""

    def execute(self, statement: str) -> object:
        """Execute a session read-only or verification statement on the connection."""
        ...

    def fetchone(self) -> Sequence[object] | None:
        """Fetch a single verification scalar row returned by ``execute``."""
        ...

    def close(self) -> object:
        """Close the temporary cursor."""
        ...


class _DbapiConnectionProtocol(Protocol):
    """Minimal DBAPI connection surface providing a cursor."""

    def cursor(self) -> _CursorProtocol:
        """Open a DBAPI cursor for connection initialization or checkout checks."""
        ...


class _CursorExecuteHook(Protocol):
    """Typed callable signature registered on ``before_cursor_execute``."""

    def __call__(
        self,
        _conn: object,
        _cursor: object,
        statement: str,
        *_rest: object,
    ) -> None:
        """Validate ``statement`` before DBAPI execution."""
        ...


_CURSOR_GUARDS: WeakKeyDictionary[Engine, _CursorExecuteHook] = WeakKeyDictionary()


@dataclass(frozen=True)
class CheckpointResumeProbeConfig:
    """Operator parameters selecting one wallet/scope window and resource bounds."""

    wallet_public_id: str
    mode: str = "live"
    valuation_ccy: str = "USD"
    window_minutes: int = 60
    db_url: str | None = None
    statement_timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS
    max_runtime_seconds: float = DEFAULT_MAX_RUNTIME_SECONDS
    max_executions: int = DEFAULT_MAX_EXECUTIONS
    max_accruals: int = DEFAULT_MAX_ACCRUALS
    max_candles: int = DEFAULT_MAX_CANDLES
    max_rss_mb: int = DEFAULT_MAX_RSS_MB


@dataclass(frozen=True)
class CheckpointResumeProbeReport:
    """Measured timings and bit-exact comparison verdict for one probed window."""

    wallet_public_id: str
    mode: str
    valuation_ccy: str
    calc_version: str
    checkpoint_minute: datetime
    frontier_tip: datetime
    point_count: int
    rate_source_count: int
    cold_seconds: float
    capture_seconds: float
    verify_seconds: float
    resume_seconds: float
    matched: bool
    mismatch_detail: str | None = None


@dataclass(frozen=True)
class _PreparedScope:
    """Validated anchor, frontier, and checkpoint/suffix window bounds."""

    scope: PortfolioPnlSampleScope
    anchor: PortfolioPnlAnchorRow
    frontier: PnlCorrectionFrontier
    checkpoint_minute: datetime
    capture_from: datetime
    suffix_from: datetime
    capture_as_of: datetime


def _redact_secrets(raw_text: str) -> str:
    """Redact DSN userinfo and key-value passwords from diagnostic strings.

    Args:
        raw_text: Untrusted error, exception, or DSN-bearing diagnostic text.

    Returns:
        Text with URL credentials and password/token values replaced by ``***``.
    """
    redacted = _DSN_USERINFO_RE.sub(r"\1***:***\3", raw_text)
    return _SECRET_KV_RE.sub(r"\1***", redacted)


def _require(condition: bool, reason: str) -> None:
    """Raise ``CheckpointResumeProbeError`` with redacted ``reason`` unless ``condition`` holds.

    Args:
        condition: Invariant condition required for safe probing.
        reason: Failure code or message to surface when ``condition`` is ``False``.

    Raises:
        CheckpointResumeProbeError: When ``condition`` evaluates to ``False``.
    """
    if not condition:
        raise CheckpointResumeProbeError(_redact_secrets(reason))


@dataclass(frozen=True, slots=True)
class _SqlToken:
    """Retain the source spelling and the name of an unquoted or quoted identifier."""

    kind: str
    raw: str
    name: str = ""


def _sql_block_comment_end(statement: str, start: int, dialect_name: str) -> int:
    """Find the end of a block comment, including PostgreSQL's nested comments."""
    if dialect_name == "sqlite":
        end = statement.find("*/", start)
        if end >= 0:
            return end + 2
    else:
        depth = 1
        for marker in _SQL_BLOCK_MARKER_RE.finditer(statement, start):
            depth += 1 if marker.group() == "/*" else -1
            if depth == 0:
                return marker.end()
    raise CheckpointResumeProbeError(_UNCLASSIFIED_SQL)


def _sql_quote_end(
    statement: str, start: int, quote: str, backslashes: Literal["escape", "reject", "literal"]
) -> int:
    """Scan through a closing delimiter without interpreting doubled quotes as a close."""
    pos = start
    while pos < len(statement):
        char = statement[pos]
        pos += 1
        if char == "\\" and backslashes != "literal":
            _require(backslashes == "escape", _UNCLASSIFIED_SQL)
            pos += 1
        elif char == quote:
            if quote == "]" or pos == len(statement) or statement[pos] != quote:
                return pos
            pos += 1
    raise CheckpointResumeProbeError(_UNCLASSIFIED_SQL)


def _sql_quoted_token(
    statement: str, match: re.Match[str], dialect_name: str
) -> tuple[_SqlToken, int]:
    """Read quoted strings and identifiers using the active PostgreSQL or SQLite syntax.

    The guard refuses ordinary PostgreSQL string backslashes because their meaning
    depends on the session setting. It accepts Unicode identifier quoting when the
    spelling needs no escape decoding, and refuses ``UESCAPE`` separately.
    """
    opener = match.group().upper()
    if dialect_name == "sqlite":
        _require(opener in {"'", '"', "[", "`"}, _UNCLASSIFIED_SQL)
    elif opener == "[":
        return _SqlToken("symbol", "["), match.end()
    else:
        _require(opener != "`", _UNCLASSIFIED_SQL)
    quote = "]" if opener == "[" else opener[-1]
    backslashes: Literal["escape", "reject", "literal"] = "escape" if opener == "E'" else "literal"
    if opener == "'" and dialect_name == "postgresql":
        backslashes = "reject"
    end = _sql_quote_end(statement, match.end(), quote, backslashes)
    raw = statement[match.start() : end]
    if quote == "'":
        return _SqlToken("string", raw), end
    name = statement[match.end() : end - 1].replace(quote * 2, quote)
    if opener == 'U&"':
        _require("\\" not in name, _UNCLASSIFIED_SQL)
    return _SqlToken("identifier", raw, name.upper()), end


def _read_sql_token(
    statement: str, match: re.Match[str], dialect_name: str
) -> tuple[_SqlToken, int]:
    """Read one lexeme, replacing comments and whitespace with separator tokens."""
    kind = match.lastgroup
    raw = match.group()
    end = match.end()
    if kind == "line_comment":
        line_end_re = _SQL_SQLITE_LINE_END_RE if dialect_name == "sqlite" else _SQL_PG_LINE_END_RE
        line_end = line_end_re.search(statement, end)
        return _SqlToken("space", " "), line_end.start() if line_end else len(statement)
    if kind == "block_comment":
        return _SqlToken("space", " "), _sql_block_comment_end(statement, end, dialect_name)
    if kind == "space":
        return _SqlToken("space", " "), end
    if kind == "quote":
        return _sql_quoted_token(statement, match, dialect_name)
    if kind == "dollar_quote":
        _require(dialect_name == "postgresql", _UNCLASSIFIED_SQL)
        close = statement.find(raw, end)
        _require(close >= 0, _UNCLASSIFIED_SQL)
        end = close + len(raw)
        return _SqlToken("string", statement[match.start() : end]), end
    if kind == "word":
        name = raw.upper()
        _require(name != "UESCAPE", _UNCLASSIFIED_SQL)
        return _SqlToken("word", raw, name), end
    if dialect_name == "sqlite":
        _require(not raw.startswith(("$", ":", "@", "#")), _UNCLASSIFIED_SQL)
    return _SqlToken("symbol", raw), end


def _sql_fragments(statement: str, dialect_name: str) -> Iterator[list[_SqlToken]]:
    """Yield statements separated by semicolons outside quoted tokens and comments."""
    pos = 0
    fragment: list[_SqlToken] = []
    while pos < len(statement):
        match = _SQL_TOKEN_RE.match(statement, pos)
        if match is None:
            raise CheckpointResumeProbeError(_UNCLASSIFIED_SQL)
        token, pos = _read_sql_token(statement, match, dialect_name)
        if token.raw == ";":
            yield fragment
            fragment = []
        elif token.kind != "space" or (fragment and fragment[-1].kind != "space"):
            fragment.append(token)
    yield fragment


def _is_read_only_pragma(normalized: str) -> bool:
    """Return whether a SQLite ``PRAGMA`` statement performs no mutation.

    Args:
        normalized: Whitespace-collapsed uppercase SQL fragment starting with ``PRAGMA``.

    Returns:
        ``True`` only for read-only introspection pragmas or allowed read/timeout settings.
    """
    if "=" not in normalized:
        return True
    compact = normalized.replace(" ", "")
    return compact in {
        "PRAGMAQUERY_ONLY=ON",
        "PRAGMAQUERY_ONLY=1",
        "PRAGMAQUERY_ONLY=TRUE",
        f"PRAGMABUSY_TIMEOUT={LOCK_TIMEOUT_MS}",
        "PRAGMAFOREIGN_KEYS=ON",
    }


def _is_read_only_set(normalized: str) -> bool:
    """Return whether a non-timeout PostgreSQL ``SET`` statement is read-only.

    Args:
        normalized: Whitespace-collapsed uppercase SQL fragment starting with ``SET``.

    Returns:
        ``True`` only for read-only characteristics, isolation, or local JIT settings.
    """
    if normalized == "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY":
        return True
    if re.fullmatch(
        r"SET TRANSACTION (?:READ ONLY|ISOLATION LEVEL "
        r"(?:READ COMMITTED|REPEATABLE READ|SERIALIZABLE)(?:\s*,\s*READ ONLY| READ ONLY)?)",
        normalized,
    ):
        return True
    return re.fullmatch(r"SET LOCAL JIT\s*(?:=|TO)\s*(?:ON|OFF|TRUE|FALSE)", normalized) is not None


def _parse_local_timeout_ms(raw_value: str) -> int | None:
    """Parse an unquoted ms integer or quoted ms/s/min timeout value into ms.

    Args:
        raw_value: Original unmasked right-hand side of a ``SET LOCAL`` timeout statement.

    Returns:
        Non-negative milliseconds when syntax matches valid units, else ``None``.
    """
    match = _LOCAL_TIMEOUT_VAL_RE.fullmatch(raw_value.strip())
    if match is None:
        return None
    if match.group(1) is not None:
        return int(match.group(1))
    multiplier = _LOCAL_TIMEOUT_UNITS[(match.group(3) or "ms").lower()]
    return int(match.group(2)) * multiplier


def _assert_read_only_set(raw_fragment: str, statement_timeout_ms: int) -> None:
    """Validate ``SET`` fragments while strictly capping ``SET LOCAL`` timeouts.

    Args:
        raw_fragment: Comment-free SQL fragment with original literal contents.
        statement_timeout_ms: Probe configured statement-timeout ceiling in milliseconds.

    Raises:
        CheckpointResumeProbeError: When timeout bounds fail or ``SET`` mutates session state.
    """
    local_timeout = _LOCAL_TIMEOUT_STMT_RE.fullmatch(raw_fragment)
    if local_timeout is not None:
        setting = local_timeout.group(1).lower()
        cap_ms = statement_timeout_ms if setting == "statement_timeout" else LOCK_TIMEOUT_MS
        parsed_ms = _parse_local_timeout_ms(local_timeout.group(2) or "")
        if parsed_ms is None or parsed_ms <= 0 or parsed_ms > cap_ms:
            raise CheckpointResumeProbeError(f"write_or_unbounded_sql:SET_LOCAL_{setting}")
        return
    if not _is_read_only_set(raw_fragment.upper()):
        raise CheckpointResumeProbeError("write_sql_refused:SET")


def _allow_bound_pg_timeout_select(raw_fragment: str) -> bool:
    """Allow only the repository's downward timeout clamp within hard safety caps.

    Args:
        raw_fragment: Whitespace-collapsed literal-restored SQL fragment.

    Returns:
        ``True`` when the fragment matches the exact timeout clamp query within bounds, else ``False``.

    Raises:
        CheckpointResumeProbeError: When the exact query specifies invalid or over-cap limits.
    """
    match = _BOUND_TX_CONFIG_RE.fullmatch(raw_fragment)
    if match is None:
        return False
    stmt_cmp = _parse_local_timeout_ms(match.group(1))
    stmt_then = _parse_local_timeout_ms(match.group(2))
    lock_cmp = _parse_local_timeout_ms(match.group(3))
    lock_then = _parse_local_timeout_ms(match.group(4))
    if (
        stmt_cmp is None
        or stmt_then is None
        or lock_cmp is None
        or lock_then is None
        or stmt_cmp != stmt_then
        or lock_cmp != lock_then
        or stmt_then <= 0
        or stmt_then > MAX_STATEMENT_TIMEOUT_MS
        or lock_then <= 0
        or lock_then > LOCK_TIMEOUT_MS
    ):
        raise CheckpointResumeProbeError("write_or_unbounded_sql:set_config_timeout")
    return True


def _assert_query_read_only(keywords: str, verb: str) -> None:
    """Reject locking clauses and write syntax outside quoted SQL tokens."""
    if verb in {"BEGIN", "START", "SET"} and _READ_WRITE_RE.search(keywords):
        raise CheckpointResumeProbeError("write_sql_refused:READ_WRITE")
    if _FOR_LOCK_RE.search(keywords):
        raise CheckpointResumeProbeError("write_sql_refused:FOR_UPDATE")
    if _COPY_PROGRAM_RE.search(keywords):
        raise CheckpointResumeProbeError("write_sql_refused:COPY_PROGRAM")
    if _SELECT_INTO_RE.search(keywords):
        raise CheckpointResumeProbeError("write_sql_refused:SELECT_INTO")
    if verb == "EXPLAIN" and _EXPLAIN_ANALYZE_RE.search(keywords):
        raise CheckpointResumeProbeError("write_sql_refused:EXPLAIN_ANALYZE")
    if verb == "WITH" and _WRITE_KEYWORDS.intersection(keywords.split()):
        raise CheckpointResumeProbeError("write_sql_refused:WITH")


def _sql_keyword(token: _SqlToken) -> str:
    """Keep unquoted words in clause checks and mark other tokens as separators."""
    if token.kind == "word" and token.raw.isascii() and "$" not in token.raw:
        return token.name
    return "?"


def _assert_no_forbidden_calls(tokens: Sequence[_SqlToken]) -> None:
    """Refuse side-effecting function names before a call's opening parenthesis."""
    for name, opening in pairwise(tokens):
        if opening.raw == "(" and _FORBIDDEN_SQL_NAME_RE.fullmatch(name.name):
            raise CheckpointResumeProbeError("write_sql_refused:forbidden_function")


def _assert_fragment_read_only(
    fragment: Sequence[_SqlToken],
    statement_timeout_ms: int,
) -> None:
    """Reject any SQL fragment capable of mutating, locking, or unbounding database state.

    Args:
        fragment: Lexed SQL fragment without statement-delimiting semicolons.
        statement_timeout_ms: Probe configured statement-timeout ceiling in milliseconds.

    Raises:
        CheckpointResumeProbeError: When the fragment violates read-only or timeout invariants.
    """
    tokens = [token for token in fragment if token.kind != "space"]
    if not tokens:
        return
    verb = _sql_keyword(tokens[0])
    _require(verb != "?", _UNCLASSIFIED_SQL)
    raw_fragment = "".join(token.raw for token in fragment).strip(" ")
    if verb == "SELECT" and _allow_bound_pg_timeout_select(raw_fragment):
        return
    _assert_no_forbidden_calls(tokens)
    _assert_query_read_only(" ".join(map(_sql_keyword, tokens)), verb)
    if verb in _READ_ONLY_VERBS:
        return
    if verb == "PRAGMA" and _is_read_only_pragma(raw_fragment.upper()):
        return
    if verb == "SET":
        _assert_read_only_set(raw_fragment, statement_timeout_ms)
        return
    raise CheckpointResumeProbeError(f"write_sql_refused:{verb}")


def _assert_read_only_sql(
    statement: str,
    statement_timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS,
    *,
    dialect_name: str = "postgresql",
) -> None:
    """Validate every semicolon-delimited statement in ``statement`` is strictly bounded read-only.

    Args:
        statement: Raw SQL string submitted to SQLAlchemy cursor execution.
        statement_timeout_ms: Probe configured statement-timeout ceiling in milliseconds.
        dialect_name: SQLAlchemy dialect controlling quoting and comment syntax.
    """
    _require(
        dialect_name in {"postgresql", "sqlite"} and "\x00" not in statement, _UNCLASSIFIED_SQL
    )
    for fragment in _sql_fragments(statement, dialect_name):
        _assert_fragment_read_only(fragment, statement_timeout_ms)


def _parse_pg_duration_ms(raw: str) -> int | None:
    """Parse a PostgreSQL timeout duration string from ``SHOW`` into milliseconds.

    Args:
        raw: Raw duration string returned by PostgreSQL ``SHOW``.

    Returns:
        Timeout in integer milliseconds, or ``None`` when unparseable.
    """
    cleaned = raw.strip().lower()
    if cleaned.isdigit():
        return int(cleaned)
    if not cleaned or not re.fullmatch(r"(?:\d+\s*(?:ms|s|min|h)\s*)+", cleaned):
        return None
    return sum(int(qty) * _PG_UNIT_MS[unit] for qty, unit in _PG_DURATION_TOKEN_RE.findall(cleaned))


def _apply_sqlite_connection_pragmas(cursor: _CursorProtocol) -> None:
    """Enable SQLite foreign keys, query-only read enforcement, and lock busy timeout."""
    cursor.execute("PRAGMA foreign_keys = ON")
    cursor.execute("PRAGMA query_only = ON")
    cursor.execute(f"PRAGMA busy_timeout = {LOCK_TIMEOUT_MS}")


def _enforce_connection_read_only(
    dbapi_connection: object,
    dialect_name: str,
    statement_timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS,
) -> None:
    """Enable read-only transaction and timeout bounds on a DBAPI connection.

    Args:
        dbapi_connection: Raw driver connection opened or checked out by SQLAlchemy.
        dialect_name: SQLAlchemy dialect name (``sqlite`` or ``postgresql``).
        statement_timeout_ms: PostgreSQL per-statement timeout in milliseconds.

    Raises:
        CheckpointResumeProbeError: For unsupported dialects or invalid timeout bounds.
    """
    conn = cast(_DbapiConnectionProtocol, dbapi_connection)
    cursor = conn.cursor()
    try:
        if dialect_name == "sqlite":
            _apply_sqlite_connection_pragmas(cursor)
        elif dialect_name == "postgresql":
            _require(
                1 <= statement_timeout_ms <= MAX_STATEMENT_TIMEOUT_MS,
                f"invalid_statement_timeout_ms:{statement_timeout_ms}",
            )
            cursor.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
            cursor.execute(f"SET SESSION statement_timeout = {int(statement_timeout_ms)}")
            cursor.execute(f"SET SESSION lock_timeout = {LOCK_TIMEOUT_MS}")
            cursor.execute(
                "SET SESSION idle_in_transaction_session_timeout = "
                f"{IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS}"
            )
        else:
            raise CheckpointResumeProbeError(f"unsupported_dialect:{dialect_name}")
    finally:
        cursor.close()


def _scalar_from_cursor(cursor: _CursorProtocol, query: str) -> object:
    """Execute one single-row query on a raw DBAPI cursor and return column zero."""
    cursor.execute(query)
    row = cursor.fetchone()
    return None if row is None else row[0]


def _verify_sqlite_cursor_state(cursor: _CursorProtocol) -> None:
    """Verify read-only mode and lock timeout on one checked-out SQLite connection."""
    _require(_scalar_from_cursor(cursor, "PRAGMA query_only") == 1, "database_write_would_commit")
    _require(
        _scalar_from_cursor(cursor, "PRAGMA busy_timeout") == LOCK_TIMEOUT_MS,
        "database_lock_timeout_mismatch",
    )


def _verify_postgres_cursor_state(
    cursor: _CursorProtocol,
    statement_timeout_ms: int,
) -> None:
    """Verify read-only mode and timeout bounds on one checked-out Postgres connection."""
    read_only = _scalar_from_cursor(cursor, "SHOW transaction_read_only")
    _require(str(read_only).lower() == "on", "database_write_would_commit")
    stmt_raw = _scalar_from_cursor(cursor, "SHOW statement_timeout")
    _require(
        _parse_pg_duration_ms(str(stmt_raw)) == statement_timeout_ms,
        "database_statement_timeout_mismatch",
    )
    lock_raw = _scalar_from_cursor(cursor, "SHOW lock_timeout")
    _require(
        _parse_pg_duration_ms(str(lock_raw)) == LOCK_TIMEOUT_MS,
        "database_lock_timeout_mismatch",
    )
    idle_raw = _scalar_from_cursor(cursor, "SHOW idle_in_transaction_session_timeout")
    _require(
        _parse_pg_duration_ms(str(idle_raw)) == IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS,
        "database_idle_timeout_mismatch",
    )


def _verify_dbapi_checkout_state(
    dbapi_connection: object,
    dialect_name: str,
    statement_timeout_ms: int,
) -> None:
    """Verify read-only mode and timeouts on every pooled DBAPI connection checkout.

    Args:
        dbapi_connection: Checked-out DBAPI connection handle.
        dialect_name: Target SQLAlchemy dialect name.
        statement_timeout_ms: Expected PostgreSQL ``statement_timeout`` in milliseconds.
    """
    conn = cast(_DbapiConnectionProtocol, dbapi_connection)
    cursor = conn.cursor()
    try:
        if dialect_name == "sqlite":
            _verify_sqlite_cursor_state(cursor)
        elif dialect_name == "postgresql":
            _verify_postgres_cursor_state(cursor, statement_timeout_ms)
        else:
            raise CheckpointResumeProbeError(f"unsupported_dialect:{dialect_name}")
    finally:
        cursor.close()


def _guard_session_write(session: object, *_args: object) -> None:
    """Refuse ORM flushes or commits whenever any pending mutations are staged.

    Args:
        session: SQLAlchemy ORM session entering ``before_flush`` or ``before_commit``.
        *_args: Additional SQLAlchemy event hook arguments (unused).

    Raises:
        CheckpointResumeProbeError: When ``session`` carries new, dirty, or deleted state.
    """
    if isinstance(session, Session) and (session.new or session.dirty or session.deleted):
        raise CheckpointResumeProbeError("write_commit_detected")


@contextmanager
def _read_only_guards(
    repo: Repository,
    *,
    statement_timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS,
) -> Iterator[SQLAlchemyRepository]:
    """Attach DBAPI read-only/timeout hooks, SQL filters, and ORM commit guards.

    Args:
        repo: Candidate repository instance supplied to the probe.
        statement_timeout_ms: PostgreSQL per-statement timeout in milliseconds.

    Yields:
        Verified ``SQLAlchemyRepository`` with active read-only event guards.

    Raises:
        CheckpointResumeProbeError: If ``repo`` is not a ``SQLAlchemyRepository``.
    """
    _require(isinstance(repo, SQLAlchemyRepository), "sqlalchemy_repository_required")
    sa_repo = cast(SQLAlchemyRepository, repo)
    sync_engine = sa_repo.engine.sync_engine
    dialect_name = sa_repo.dialect_name

    def _on_connect(dbapi_connection: object, _connection_record: object) -> None:
        """Apply DBAPI read-only mode and timeout bounds on newly opened connections."""
        _enforce_connection_read_only(dbapi_connection, dialect_name, statement_timeout_ms)

    def _on_checkout(
        dbapi_connection: object,
        _connection_record: object,
        _connection_proxy: object,
    ) -> None:
        """Enforce and verify read-only mode and timeout bounds on every pooled checkout."""
        if dialect_name == "sqlite":
            _enforce_connection_read_only(dbapi_connection, dialect_name, statement_timeout_ms)
        _verify_dbapi_checkout_state(dbapi_connection, dialect_name, statement_timeout_ms)

    def _on_cursor_execute(
        _conn: object,
        _cursor: object,
        statement: str,
        *_rest: object,
    ) -> None:
        """Refuse non-read-only or unbounded SQL before execution reaches the driver."""
        _assert_read_only_sql(statement, statement_timeout_ms, dialect_name=dialect_name)

    event.listen(sync_engine, "connect", _on_connect)
    event.listen(sync_engine, "checkout", _on_checkout)
    event.listen(sync_engine, "before_cursor_execute", _on_cursor_execute)
    _CURSOR_GUARDS[sync_engine] = _on_cursor_execute
    event.listen(Session, "before_flush", _guard_session_write)
    event.listen(Session, "before_commit", _guard_session_write)
    try:
        yield sa_repo
    finally:
        event.remove(Session, "before_commit", _guard_session_write)
        event.remove(Session, "before_flush", _guard_session_write)
        _CURSOR_GUARDS.pop(sync_engine, None)
        event.remove(sync_engine, "before_cursor_execute", _on_cursor_execute)
        event.remove(sync_engine, "checkout", _on_checkout)
        event.remove(sync_engine, "connect", _on_connect)


async def _verify_connection_state(
    session: AsyncSession,
    dialect_name: str,
    statement_timeout_ms: int,
) -> None:
    """Verify read-only mode and timeout bounds via ``AsyncSession`` queries.

    Args:
        session: Open async session checked out under read-only hooks.
        dialect_name: Target SQLAlchemy dialect name.
        statement_timeout_ms: Expected PostgreSQL ``statement_timeout`` in milliseconds.

    Raises:
        CheckpointResumeProbeError: When read-only mode or timeout bounds are absent.
    """
    if dialect_name == "sqlite":
        query_only = (await session.execute(text("PRAGMA query_only"))).scalar_one()
        _require(query_only == 1, "database_write_would_commit")
        busy_timeout = (await session.execute(text("PRAGMA busy_timeout"))).scalar_one()
        _require(busy_timeout == LOCK_TIMEOUT_MS, "database_lock_timeout_mismatch")
        return
    if dialect_name == "postgresql":
        read_only = (await session.execute(text("SHOW transaction_read_only"))).scalar_one()
        _require(str(read_only).lower() == "on", "database_write_would_commit")
        stmt_raw = (await session.execute(text("SHOW statement_timeout"))).scalar_one()
        _require(
            _parse_pg_duration_ms(str(stmt_raw)) == statement_timeout_ms,
            "database_statement_timeout_mismatch",
        )
        lock_raw = (await session.execute(text("SHOW lock_timeout"))).scalar_one()
        _require(
            _parse_pg_duration_ms(str(lock_raw)) == LOCK_TIMEOUT_MS,
            "database_lock_timeout_mismatch",
        )
        idle_raw = (
            await session.execute(text("SHOW idle_in_transaction_session_timeout"))
        ).scalar_one()
        _require(
            _parse_pg_duration_ms(str(idle_raw)) == IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS,
            "database_idle_timeout_mismatch",
        )
        return
    raise CheckpointResumeProbeError(f"unsupported_dialect:{dialect_name}")


async def _verify_postgres_role_privileges(session: AsyncSession) -> None:
    """Refuse PostgreSQL roles with write privileges on key P&L tables."""
    writable = (await session.execute(_ROLE_PRIVILEGE_QUERY)).first()
    if writable is not None:
        raise CheckpointResumeProbeError(f"writable_role_refused:{writable[0]}:{writable[1]}")


async def _preflight_read_only(
    repo: SQLAlchemyRepository,
    policy: PnlSeriesReadPolicy,
    *,
    statement_timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS,
) -> None:
    """Verify read policy, SQL/ORM hooks, session settings, and DB role privileges.

    Args:
        repo: Guarded SQLAlchemy repository instance.
        policy: Series read policy that will govern all probe service calls.
        statement_timeout_ms: Expected PostgreSQL ``statement_timeout`` in milliseconds.

    Raises:
        CheckpointResumeProbeError: When any write hazard or missing guard is detected.
    """
    _require(not policy.allow_anchor_creation, "write_policy_allow_anchor_creation")
    sync_engine = repo.engine.sync_engine
    cursor_hook = _CURSOR_GUARDS.get(sync_engine)
    _require(
        cursor_hook is not None
        and event.contains(sync_engine, "before_cursor_execute", cursor_hook)
        and event.contains(Session, "before_commit", _guard_session_write),
        "read_only_guard_missing",
    )
    async with repo.session() as session:
        await _verify_connection_state(session, repo.dialect_name, statement_timeout_ms)
        if repo.dialect_name == "postgresql":
            await _verify_postgres_role_privileges(session)


_PROC_STATM_PATH: Final[Path] = Path("/proc/self/statm")
_PROC_STATUS_PATH: Final[Path] = Path("/proc/self/status")


def _current_rss_mb() -> float:
    """Return current resident memory in MiB (Linux statm, macOS ``ru_maxrss``, or ``0.0``)."""
    if sys.platform.startswith("linux"):
        fields_raw = _PROC_STATM_PATH.read_text(encoding="utf-8").split()
        resident_pages = int(fields_raw[1]) if len(fields_raw) > 1 else 0
        page_size = os.sysconf("SC_PAGE_SIZE")
        return float(resident_pages * page_size) / (1024.0 * 1024.0)
    if sys.platform == "darwin":
        return float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / (1024.0 * 1024.0)
    return 0.0


def _read_linux_vmsize_bytes() -> int | None:
    """Read current Linux virtual address space size in bytes from ``/proc/self/status``."""
    try:
        status_text = _PROC_STATUS_PATH.read_text(encoding="utf-8")
    except OSError:
        return None
    match = _VMSIZE_LINE_RE.search(status_text)
    if match is None:
        return None
    return int(match.group(1)) * 1024


def _check_rss_mb(max_rss_mb: int, baseline_rss_mb: float = 0.0) -> None:
    """Abort the probe if RSS growth over ``baseline_rss_mb`` exceeds ``max_rss_mb``.

    Args:
        max_rss_mb: Maximum allowed RSS growth in MiB over startup baseline.
        baseline_rss_mb: Resident memory reading captured at probe start in MiB.
    """
    growth_mb = max(0.0, _current_rss_mb() - baseline_rss_mb)
    _require(growth_mb <= max_rss_mb, f"max_rss_exceeded:{growth_mb:.1f}>{max_rss_mb}")


def _apply_address_space_limit(max_rss_mb: int) -> None:
    """Lower soft ``RLIMIT_AS`` on Linux relative to current ``VmSize`` with allocator headroom."""
    rlimit_as = getattr(resource, "RLIMIT_AS", None)
    if rlimit_as is None or not sys.platform.startswith("linux"):
        return
    current_vmsize = _read_linux_vmsize_bytes()
    if current_vmsize is None:
        return
    headroom_mb = max(max_rss_mb, ADDRESS_SPACE_HEADROOM_FACTOR * max_rss_mb)
    desired_soft = current_vmsize + headroom_mb * 1024 * 1024
    try:
        inherited_soft, hard = resource.getrlimit(rlimit_as)
        capped = (
            desired_soft
            if inherited_soft == resource.RLIM_INFINITY
            else min(inherited_soft, desired_soft)
        )
        if capped != inherited_soft:
            resource.setrlimit(rlimit_as, (capped, hard))
    except (ValueError, OSError):
        return


def _validate_runtime_bounds(config: CheckpointResumeProbeConfig) -> None:
    """Validate hard timeout, execution, and memory bounds before opening connections."""
    _require(
        1 <= config.statement_timeout_ms <= MAX_STATEMENT_TIMEOUT_MS,
        f"invalid_statement_timeout_ms:{config.statement_timeout_ms}",
    )
    _require(
        math.isfinite(config.max_runtime_seconds)
        and 0.0 < config.max_runtime_seconds <= MAX_RUNTIME_SECONDS,
        f"invalid_max_runtime_seconds:{config.max_runtime_seconds}",
    )
    _require(config.max_executions >= 1, f"invalid_max_executions:{config.max_executions}")
    _require(config.max_accruals >= 1, f"invalid_max_accruals:{config.max_accruals}")
    _require(config.max_candles >= 1, f"invalid_max_candles:{config.max_candles}")
    _require(config.max_rss_mb >= 1, f"invalid_max_rss_mb:{config.max_rss_mb}")


def _validate_config(config: CheckpointResumeProbeConfig) -> tuple[str, str, str]:
    """Validate and normalize operator wallet, trading mode, and valuation currency.

    Args:
        config: Raw operator configuration parameters.

    Returns:
        Normalized ``(wallet_public_id, mode, valuation_ccy)`` tuple.
    """
    _validate_runtime_bounds(config)
    wallet = normalize_portfolio_pnl_wallet_public_id(config.wallet_public_id)
    _require(config.mode in ("live", "paper"), f"invalid_mode:{config.mode}")
    ccy = normalize_portfolio_pnl_valuation_ccy(config.valuation_ccy)
    _require(config.window_minutes >= 1, f"invalid_window_minutes:{config.window_minutes}")
    return wallet, config.mode, ccy


_REPLAY_ROW_COLUMNS = (
    ("candles", Candle.__table__.c.close),
    ("accruals", AccrualLedger.__table__.c.id),
    ("fills", VenueEvent.__table__.c.public_id),
    ("annulments", ExecutionAnnulment.__table__.c.id),
    ("executions", Execution.__table__.c.id),
)
_PG_READ_SNAPSHOT = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
_ACTIVE_ROW_BUDGET: ContextVar[_ReplayRowBudget | None] = ContextVar(
    "_ACTIVE_ROW_BUDGET", default=None
)


class _ReplayRowBudget:
    """Check SQL row counts in the same read snapshot before buffering replay data.

    The projected columns identify materialized reads without counting aggregate
    fingerprints or guessing the set of mark and FX inputs before replay plans them.
    """

    def __init__(
        self,
        engine: Engine,
        config: CheckpointResumeProbeConfig,
        fresh_cut: bool,
    ) -> None:
        """Track candle rows across replay batches and prefix rows within each read."""
        self.engine = engine
        self.limits = {
            "candles": config.max_candles,
            "accruals": config.max_accruals,
            "executions": config.max_executions,
            "fills": config.max_executions,
            "annulments": config.max_executions,
        }
        self.execution_reason = "cut_moved" if fresh_cut else "caps_exceeded:executions"
        self.snapshot_sql = _PG_READ_SNAPSHOT if engine.dialect.name == "postgresql" else "BEGIN"
        self.read_counts: WeakKeyDictionary[SessionTransaction, dict[str, int]] = (
            WeakKeyDictionary()
        )
        self.candles = 0

    def after_begin(
        self, _session: Session, transaction: SessionTransaction, connection: Connection
    ) -> None:
        """Pin the outer transaction before its first statement and retain it across savepoints."""
        if (
            _ACTIVE_ROW_BUDGET.get() is not self
            or connection.engine is not self.engine
            or transaction.parent is not None
        ):
            return
        if (
            self.snapshot_sql != "BEGIN"
            or not cast(SQLiteConnection, connection.connection.driver_connection).in_transaction
        ):
            connection.exec_driver_sql(self.snapshot_sql)
        self.read_counts[transaction] = {}

    def before_execute(self, state: ORMExecuteState) -> Result[tuple[()]] | None:
        """Coalesce the repository's own snapshot pin and meter the actual replay SELECTs."""
        if (
            _ACTIVE_ROW_BUDGET.get() is not self
            or state.session.get_bind().engine is not self.engine
        ):
            return None
        statement = state.statement
        if isinstance(statement, TextClause) and statement.text == self.snapshot_sql:
            self._pinned_connection(state.session)
            return null_result()
        if isinstance(statement, Select):
            self._check_select(state, statement)
        return None

    def _pinned_connection(self, session: Session) -> tuple[Connection, dict[str, int]]:
        """Acquire the connection and require a snapshot recorded by this budget."""
        connection = session.connection()
        transaction = session.get_transaction()
        _require(transaction is not None, "snapshot_not_pinned")
        counts = self.read_counts.get(cast(SessionTransaction, transaction))
        _require(counts is not None, "snapshot_not_pinned")
        return connection, cast(dict[str, int], counts)

    def _check_select[*RowT](self, state: ORMExecuteState, statement: Select[*RowT]) -> None:
        """Refuse an over-cap query before it executes in the pinned read transaction."""
        connection, counts = self._pinned_connection(state.session)
        for kind, column in _REPLAY_ROW_COLUMNS:
            if statement.selected_columns.corresponding_column(column) is not None:
                used = self.candles if kind == "candles" else counts.get(kind, 0)
                remaining = self.limits[kind] - used
                rows = statement.order_by(None).subquery()
                bounded = select(1).select_from(rows).limit(remaining + 1).subquery()
                count = connection.execute(
                    select(func.count()).select_from(bounded), state.parameters
                ).scalar_one()
                _require(count <= remaining, self._refusal(kind))
                counts[kind] = used + count
                if kind == "candles":
                    self.candles = counts[kind]
                return

    def _refusal(self, kind: str) -> str:
        """Map fill and annulment buffers to the execution-cap exit reason."""
        if kind in ("candles", "accruals"):
            return f"caps_exceeded:{kind}"
        return self.execution_reason


@contextmanager
def _guard_replay_rows(
    repo: SQLAlchemyRepository,
    config: CheckpointResumeProbeConfig,
    *,
    fresh_cut: bool = False,
) -> Iterator[None]:
    """Install one budget around a replay, checkpoint verification, or fresh cut read."""
    budget = _ReplayRowBudget(repo.engine.sync_engine, config, fresh_cut)
    after_begin = budget.after_begin
    before_execute = budget.before_execute
    event.listen(Session, "after_begin", after_begin)
    event.listen(Session, "do_orm_execute", before_execute)
    token = _ACTIVE_ROW_BUDGET.set(budget)
    try:
        yield
    finally:
        _ACTIVE_ROW_BUDGET.reset(token)
        event.remove(Session, "do_orm_execute", before_execute)
        event.remove(Session, "after_begin", after_begin)


async def _load_prepared_scope(
    repo: SQLAlchemyRepository,
    config: CheckpointResumeProbeConfig,
) -> _PreparedScope:
    """Load and validate the anchor, correction frontier, and output window bounds.

    Args:
        repo: Guarded read-only repository.
        config: Validated operator probe parameters.

    Returns:
        Prepared scope, anchor, frontier, and capture and suffix minute bounds.
    """
    wallet, mode, ccy = _validate_config(config)
    anchor = await repo.get_portfolio_pnl_anchor(wallet, mode, ccy, None)
    _require(anchor is not None, "no_anchor")
    assert anchor is not None
    t0 = anchor["point_time"]
    scope = PortfolioPnlSampleScope(
        wallet_public_id=wallet,
        mode=mode,
        valuation_ccy=ccy,
        epoch_public_id=anchor["epoch_public_id"],
        anchor_point_time=t0,
    )
    frontier = await repo.get_portfolio_pnl_correction_frontier(scope)
    _require(frontier is not None, "no_frontier")
    assert frontier is not None
    _require(frontier.has_fill_gap is False, "frontier_has_fill_gap")
    _require(
        frontier.raw_execution_watermarks is not None
        and frontier.execution_manifest_digest is not None
        and frontier.expected_venues is not None
        and frontier.venue_event_watermarks is not None
        and frontier.fill_witness_digest is not None,
        "frontier_incomplete_cut",
    )
    earliest_checkpoint = t0 + _MINUTE
    checkpoint_minute = max(
        earliest_checkpoint,
        frontier.tip - timedelta(minutes=config.window_minutes),
    )
    _require(frontier.tip > checkpoint_minute, "frontier_window_too_short")
    capture_as_of = min(frontier.as_of, checkpoint_minute + FINALIZATION_LAG)
    return _PreparedScope(
        scope=scope,
        anchor=anchor,
        frontier=frontier,
        checkpoint_minute=checkpoint_minute,
        capture_from=max(
            earliest_checkpoint, checkpoint_minute - (config.window_minutes - 1) * _MINUTE
        ),
        suffix_from=checkpoint_minute + _MINUTE,
        capture_as_of=capture_as_of,
    )


def _frontier_witness_cut(frontier: PnlCorrectionFrontier) -> PnlTimelineFillWitnessCut:
    """Extract the typed frozen fill-witness cut from ``frontier``.

    Args:
        frontier: Frontier previously validated to contain complete cut metadata.

    Returns:
        Frozen fill-witness cut mapping for replay options.
    """
    assert frontier.venue_event_watermarks is not None
    assert frontier.fill_witness_digest is not None
    return {
        "watermarks": dict(frontier.venue_event_watermarks),
        "digest": frontier.fill_witness_digest,
    }


async def _capture_and_verify_in_memory(
    repo: SQLAlchemyRepository,
    prepared: _PreparedScope,
    read_policy: PnlSeriesReadPolicy,
    config: CheckpointResumeProbeConfig,
    rss_baseline_mb: float,
) -> tuple[PnlCheckpointResume, float, float]:
    """Capture a checkpoint at C purely in memory and verify it against frontier T.

    Args:
        repo: Guarded read-only repository.
        prepared: Prepared scope, frontier, and window bounds.
        read_policy: Read policy with ``allow_anchor_creation=False``.
        config: Operator configuration holding RSS and execution bounds.
        rss_baseline_mb: Resident memory baseline captured at probe start in MiB.

    Returns:
        Tuple of verified resume state, capture seconds, and verification seconds.
    """
    scope, anchor, frontier = prepared.scope, prepared.anchor, prepared.frontier
    with _guard_replay_rows(repo, config, fresh_cut=True):
        capture_prefix: PnlTimelineExecutionPrefix = await repo.get_pnl_timeline_execution_prefix(
            scope.wallet_public_id,
            scope.mode,
            prepared.capture_as_of,
            capture_fill_witness_cut=True,
        )
        _require(
            not await repo.pnl_timeline_scope_has_fill_gap(
                scope.wallet_public_id,
                scope.mode,
                prepared.capture_as_of,
                capture_prefix,
            ),
            "capture_fill_gap",
        )
    witness = capture_prefix.get("fill_witness_cut")
    _require(witness is not None, "missing_capture_fill_witness_cut")
    assert witness is not None
    assert frontier.expected_venues is not None
    anchor_public_id, anchor_digest = checkpoint_anchor_identity(anchor)
    capture_request = PnlCheckpointCaptureRequest(
        key=frontier.key,
        source_generation=frontier.generation,
        raw_execution_watermarks=dict(capture_prefix["watermarks"]),
        execution_manifest_digest=execution_manifest_digest(capture_prefix["annulments"]),
        expected_venues=frontier.expected_venues,
        venue_event_watermarks=dict(witness["watermarks"]),
        fill_witness_digest=witness["digest"],
        anchor_public_id=anchor_public_id,
        anchor_digest=anchor_digest,
        calc_version=frontier.key.calc_version,
    )
    capture_started = time.perf_counter()
    with _guard_replay_rows(repo, config):
        capture_result = await build_wallet_pnl_series(
            repo,
            scope.wallet_public_id,
            scope.mode,
            prepared.capture_from,
            prepared.checkpoint_minute,
            "1m",
            prepared.capture_as_of,
            valuation_ccy=scope.valuation_ccy,
            policy=read_policy,
            options=PnlSeriesReplayOptions(
                baseline_watermarks=dict(frontier.baseline_watermarks),
                frozen_execution_watermarks=capture_prefix["watermarks"],
                frozen_execution_manifest_digest=capture_request.execution_manifest_digest,
                frozen_has_fill_gap=False,
                frozen_fill_witness_cut=witness,
                replay_generation_tip=prepared.checkpoint_minute,
                pointwise_exact_fx=True,
                checkpoint_capture=capture_request,
            ),
        )
    capture_seconds = time.perf_counter() - capture_started
    _require(
        capture_result.checkpoint_write is not None,
        f"checkpoint_capture_refused:{capture_result.checkpoint_capture_refusal}",
    )
    _check_rss_mb(config.max_rss_mb, rss_baseline_mb)
    assert capture_result.checkpoint_write is not None
    write = capture_result.checkpoint_write
    record = PnlOpeningCheckpointRecord(
        certificate=write.certificate,
        state_json=write.state_json,
        updated_at=prepared.capture_as_of,
    )
    verify_started = time.perf_counter()
    with _guard_replay_rows(repo, config):
        verified = await verify_checkpoint_for_resume(
            repo,
            record,
            scope,
            frontier.as_of,
            PnlCheckpointResumeEligibility(
                mode="active",
                calc_version=frontier.key.calc_version,
                timeline_calc_version=PNL_TIMELINE_CALC_VERSION,
                fx_policy_version=POINTWISE_FX_ELECTION_POLICY_VERSION,
                expected_anchor_public_id=anchor_public_id,
                expected_anchor_digest=anchor_digest,
                pointwise_exact_fx=True,
                replay_generation_tip=frontier.tip,
                metadata_only=False,
                from_time=prepared.suffix_from,
                frontier_tip=frontier.tip,
                has_fill_gap=False,
                expected_execution_manifest_digest=frontier.execution_manifest_digest,
            ),
        )
    verify_seconds = time.perf_counter() - verify_started
    if isinstance(verified, PnlCheckpointFallback):
        raise CheckpointResumeProbeError(f"checkpoint_verify_fallback:{verified.reason}")
    _check_rss_mb(config.max_rss_mb, rss_baseline_mb)
    return verified, capture_seconds, verify_seconds


async def _run_window_series(
    repo: SQLAlchemyRepository,
    prepared: _PreparedScope,
    read_policy: PnlSeriesReadPolicy,
    resume: PnlCheckpointResume | None,
) -> tuple[PnlWalletSeriesResult, float]:
    """Execute one timed cold or resumed replay over ``[suffix_from, frontier.tip]``.

    Args:
        repo: Guarded read-only repository.
        prepared: Prepared scope, frontier, and window bounds.
        read_policy: Read policy with ``allow_anchor_creation=False``.
        resume: Verified opening-checkpoint resume state, or ``None`` for cold replay.

    Returns:
        Series result paired with elapsed wall-clock seconds.
    """
    scope, frontier = prepared.scope, prepared.frontier
    options = PnlSeriesReplayOptions(
        baseline_watermarks=dict(frontier.baseline_watermarks),
        frozen_execution_watermarks=frontier.raw_execution_watermarks,
        frozen_execution_manifest_digest=frontier.execution_manifest_digest,
        frozen_has_fill_gap=False,
        frozen_fill_witness_cut=_frontier_witness_cut(frontier),
        replay_generation_tip=frontier.tip,
        pointwise_exact_fx=True,
        checkpoint_resume=resume,
    )
    started = time.perf_counter()
    result = await build_wallet_pnl_series(
        repo,
        scope.wallet_public_id,
        scope.mode,
        prepared.suffix_from,
        frontier.tip,
        "1m",
        frontier.as_of,
        valuation_ccy=scope.valuation_ccy,
        policy=read_policy,
        options=options,
    )
    elapsed = time.perf_counter() - started
    return result, elapsed


def _canonical(value: object) -> JsonValue:
    """Convert point or rate-source values to float-hex, NaN-aware JSON primitives.

    Args:
        value: Scalar, datetime, tuple, or frozen dataclass field value.

    Returns:
        Canonical JSON structure preserving float bit representations.
    """
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return "nan" if math.isnan(value) else value.hex()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, tuple):
        return [_canonical(item) for item in value]
    if is_dataclass(value) and not isinstance(value, type):
        return {item.name: _canonical(getattr(value, item.name)) for item in fields(value)}
    raise TypeError(f"Unsupported probe comparison value: {type(value).__name__}")


def _compare_sequences(
    label: str,
    left: Sequence[object],
    right: Sequence[object],
) -> str | None:
    """Compare two sequences element-wise using bit-exact float serialization.

    Args:
        label: Result field group name (``points`` or ``rate_sources``).
        left: Cold series sequence.
        right: Resumed series sequence.

    Returns:
        Mismatch descriptor string, or ``None`` when bit-identical.
    """
    if len(left) != len(right):
        return f"{label}.length:{len(left)}!={len(right)}"
    for index, (left_item, right_item) in enumerate(zip(left, right, strict=True)):
        if _canonical(left_item) != _canonical(right_item):
            return f"{label}[{index}]"
    return None


def _compare_series(
    cold: PnlWalletSeriesResult,
    resumed: PnlWalletSeriesResult,
) -> str | None:
    """Compare ``points`` and ``rate_sources`` bit-for-bit across cold and resumed results.

    Args:
        cold: Cold anchor replay result over ``[suffix_from, frontier.tip]``.
        resumed: Checkpoint-resumed replay result over ``[suffix_from, frontier.tip]``.

    Returns:
        First mismatch location, or ``None`` when both ``points`` and ``rate_sources`` match.
    """
    points_mismatch = _compare_sequences("points", cold.points, resumed.points)
    if points_mismatch is not None:
        return points_mismatch
    return _compare_sequences("rate_sources", cold.rate_sources, resumed.rate_sources)


def _postgres_connect_args(statement_timeout_ms: int) -> dict[str, dict[str, str]]:
    """Build startup ``server_settings`` so asyncpg transaction #1 starts read-only and bounded."""
    return {
        "server_settings": {
            "timezone": "UTC",
            "default_transaction_read_only": "on",
            "statement_timeout": str(int(statement_timeout_ms)),
            "lock_timeout": str(LOCK_TIMEOUT_MS),
            "idle_in_transaction_session_timeout": str(IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS),
            "application_name": PROBE_APPLICATION_NAME,
        }
    }


def _build_probe_engine(db_url: str, statement_timeout_ms: int) -> AsyncEngine:
    """Construct the probe's dedicated read-only SQLAlchemy async engine.

    Args:
        db_url: Target database URL.
        statement_timeout_ms: PostgreSQL statement timeout cap in milliseconds.

    Returns:
        Dedicated ``AsyncEngine`` with startup read-only and timeout settings.

    Raises:
        CheckpointResumeProbeError: When the URL dialect is unsupported or malformed.
    """
    try:
        backend = make_url(db_url).get_backend_name()
        if backend == "postgresql":
            return create_async_engine(
                db_url,
                connect_args=_postgres_connect_args(statement_timeout_ms),
                future=True,
                pool_pre_ping=True,
                pool_recycle=3600,
            )
        if backend == "sqlite":
            if ":memory:" in db_url:
                engine = create_async_engine(
                    db_url,
                    connect_args={"timeout": 1, "check_same_thread": False},
                    future=True,
                    pool_pre_ping=True,
                    pool_recycle=3600,
                    poolclass=StaticPool,
                )
            else:
                engine = create_async_engine(
                    db_url,
                    connect_args={"timeout": 1, "check_same_thread": False},
                    future=True,
                    pool_pre_ping=True,
                    pool_recycle=3600,
                )

            def _sqlite_startup(dbapi_connection: object, _record: object) -> None:
                """Pin ``PRAGMA query_only = ON`` and lock timeout at SQLite connect time."""
                conn = cast(_DbapiConnectionProtocol, dbapi_connection)
                cursor = conn.cursor()
                try:
                    _apply_sqlite_connection_pragmas(cursor)
                finally:
                    cursor.close()

            event.listen(engine.sync_engine, "connect", _sqlite_startup)
            return engine
    except (SQLAlchemyError, ValueError, ImportError) as exc:
        raise CheckpointResumeProbeError(
            _redact_secrets(f"invalid_database_url:{type(exc).__name__}:{exc}")
        ) from None
    raise CheckpointResumeProbeError(f"unsupported_dialect:{backend}")


async def _execute_probe_pipeline(
    sa_repo: SQLAlchemyRepository,
    config: CheckpointResumeProbeConfig,
    policy: PnlSeriesReadPolicy,
) -> CheckpointResumeProbeReport:
    """Run guarded preflight, cold replay, in-memory capture, resume, and compare."""
    rss_baseline_mb = _current_rss_mb()
    _check_rss_mb(config.max_rss_mb, rss_baseline_mb)
    await _preflight_read_only(
        sa_repo,
        policy,
        statement_timeout_ms=config.statement_timeout_ms,
    )
    prepared = await _load_prepared_scope(sa_repo, config)
    _check_rss_mb(config.max_rss_mb, rss_baseline_mb)
    with _guard_replay_rows(sa_repo, config):
        cold_result, cold_seconds = await _run_window_series(sa_repo, prepared, policy, None)
    _check_rss_mb(config.max_rss_mb, rss_baseline_mb)
    verified, capture_seconds, verify_seconds = await _capture_and_verify_in_memory(
        sa_repo,
        prepared,
        policy,
        config,
        rss_baseline_mb,
    )
    with _guard_replay_rows(sa_repo, config):
        resumed_result, resume_seconds = await _run_window_series(
            sa_repo,
            prepared,
            policy,
            verified,
        )
    _require(
        resumed_result.checkpoint_resume_fallback is None,
        f"checkpoint_resume_fallback:{resumed_result.checkpoint_resume_fallback}",
    )
    _check_rss_mb(config.max_rss_mb, rss_baseline_mb)
    mismatch = _compare_series(cold_result, resumed_result)
    return CheckpointResumeProbeReport(
        wallet_public_id=prepared.scope.wallet_public_id,
        mode=prepared.scope.mode,
        valuation_ccy=prepared.scope.valuation_ccy,
        calc_version=prepared.frontier.key.calc_version,
        checkpoint_minute=prepared.checkpoint_minute,
        frontier_tip=prepared.frontier.tip,
        point_count=len(cold_result.points),
        rate_source_count=len(cold_result.rate_sources),
        cold_seconds=cold_seconds,
        capture_seconds=capture_seconds,
        verify_seconds=verify_seconds,
        resume_seconds=resume_seconds,
        matched=mismatch is None,
        mismatch_detail=mismatch,
    )


async def probe_with_repository(
    repo: Repository,
    config: CheckpointResumeProbeConfig,
    *,
    read_policy: PnlSeriesReadPolicy | None = None,
) -> CheckpointResumeProbeReport:
    """Run read-only preflight, cold replay, in-memory capture, resume, and comparison.

    Args:
        repo: Database repository (must be ``SQLAlchemyRepository``).
        config: Operator wallet, mode, currency, suffix-window, and safety limits.
        read_policy: Optional override read policy (defaults to non-mutating policy).

    Returns:
        Probe timings and bit-exact comparison report.

    Raises:
        CheckpointResumeProbeError: When any read-only check, cap, capture, or resume refuses.
    """
    _validate_runtime_bounds(config)
    deadline = time.monotonic() + config.max_runtime_seconds
    policy = read_policy or PnlSeriesReadPolicy(
        allow_anchor_creation=False,
        current_truth_horizon=False,
    )
    with _read_only_guards(repo, statement_timeout_ms=config.statement_timeout_ms) as sa_repo:
        try:
            async with asyncio.timeout(config.max_runtime_seconds):
                report = await _execute_probe_pipeline(sa_repo, config, policy)
        except TimeoutError as exc:
            raise CheckpointResumeProbeError("timeout") from exc
    _require(time.monotonic() <= deadline, "timeout")
    return report


def _has_probe_db_override(explicit_url: str | None) -> bool:
    """Return ``True`` when ``--db-url`` or non-empty ``PNL_PROBE_DB_URL`` is supplied."""
    if explicit_url is not None:
        return True
    env_url = os.environ.get(PROBE_DB_URL_ENV_VAR)
    return bool(env_url and env_url.strip())


def _resolve_database_url(explicit_url: str | None) -> str:
    """Resolve ``--db-url``, ``PNL_PROBE_DB_URL`` fallback, or bootstrap ``DB_URL``."""
    if explicit_url is not None:
        return explicit_url
    env_url = os.environ.get(PROBE_DB_URL_ENV_VAR)
    if env_url is not None and env_url.strip():
        return env_url.strip()
    try:
        return str(get_bootstrap_settings().db_url)
    except Exception as exc:
        raise CheckpointResumeProbeError(
            _redact_secrets(f"bootstrap_config_error:{type(exc).__name__}:{exc}")
        ) from None


async def probe_wallet_checkpoint_resume(
    config: CheckpointResumeProbeConfig,
) -> CheckpointResumeProbeReport:
    """Build the probe's isolated read-only engine and run the resume probe.

    Args:
        config: Operator probe parameters including optional explicit database URL.

    Returns:
        Probe timings and bit-exact comparison report.
    """
    _validate_runtime_bounds(config)
    deadline = time.monotonic() + config.max_runtime_seconds
    url = _resolve_database_url(config.db_url)
    probe_engine = _build_probe_engine(url, config.statement_timeout_ms)
    try:
        repo = SQLAlchemyRepository(url)
        default_engine = repo.engine
        try:
            repo.engine = probe_engine
            repo.session_factory = async_sessionmaker(
                probe_engine,
                expire_on_commit=False,
                class_=AsyncSession,
            )
            report = await probe_with_repository(repo, config)
        finally:
            await asyncio.shield(default_engine.dispose())
    finally:
        await asyncio.shield(probe_engine.dispose())
    _require(time.monotonic() <= deadline, "timeout")
    return report


def _classify_failure(failure: Exception) -> tuple[int, str]:
    """Map an exception caught by ``main`` to ``(exit_code, RESULT line)`` cleanly.

    Args:
        failure: Runtime error, closed refusal, timeout, or interrupt.

    Returns:
        Pair of CLI exit code (2 for errors, 3 for closed refusals) and single-line redacted RESULT.
    """
    if isinstance(failure, TimeoutError):
        return 2, "RESULT: FAIL timeout"
    reason = " ".join(_redact_secrets(str(failure)).split()) or type(failure).__name__
    if reason == "timeout":
        return 2, "RESULT: FAIL timeout"
    if reason == "no_frontier":
        return 2, "RESULT: FAIL no_frontier"
    if isinstance(failure, CheckpointResumeProbeError) and (
        reason in _CLOSED_REFUSAL_EXACT or reason.startswith(_CLOSED_REFUSAL_PREFIXES)
    ):
        return 3, f"RESULT: REFUSED {reason}"
    return 2, f"RESULT: FAIL {reason}"


def _format_report(report: CheckpointResumeProbeReport) -> str:
    """Format probe timings and comparison verdict as operator-readable stdout lines.

    Args:
        report: Completed probe report returned by :func:`probe_wallet_checkpoint_resume`.

    Returns:
        Multi-line summary containing scope, timings, counts, and final ``RESULT:`` line.
    """
    verdict = "RESULT: PASS" if report.matched else f"RESULT: FAIL {report.mismatch_detail}"
    return "\n".join(
        (
            (
                f"wallet={report.wallet_public_id} mode={report.mode} "
                f"ccy={report.valuation_ccy} calc_version={report.calc_version}"
            ),
            (
                f"checkpoint_minute={report.checkpoint_minute.isoformat()} "
                f"frontier_tip={report.frontier_tip.isoformat()}"
            ),
            f"points={report.point_count} rate_sources={report.rate_source_count}",
            (
                f"cold_seconds={report.cold_seconds:.6f} "
                f"resume_seconds={report.resume_seconds:.6f} "
                f"capture_seconds={report.capture_seconds:.6f} "
                f"verify_seconds={report.verify_seconds:.6f}"
            ),
            f"bit_identical={report.matched} mismatch_detail={report.mismatch_detail or 'none'}",
            verdict,
        )
    )


class _RedactingArgumentParser(argparse.ArgumentParser):
    """Argument parser that scrubs credentials from all ``error()`` and ``exit()`` output."""

    def exit(self, status: int = 0, message: str | None = None) -> NoReturn:
        """Exit CLI cleanly after redacting any secrets from ``message``."""
        redacted = _redact_secrets(message) if message is not None else None
        super().exit(status, redacted)

    def error(self, message: str) -> NoReturn:
        """Abort parsing cleanly after redacting any secrets from ``message``."""
        super().error(_redact_secrets(message))


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser for the read-only checkpoint resume probe.

    Returns:
        Configured argument parser accepting wallet, scope, window, and probe bounds.
    """
    parser = _RedactingArgumentParser(description=__doc__)
    parser.add_argument("--wallet", required=True, help="Wallet public ID to probe.")
    parser.add_argument("--mode", default="live", choices=("live", "paper"))
    parser.add_argument("--valuation-ccy", default="USD")
    parser.add_argument(
        "--window-minutes", type=int, default=60, help="Maximum output minutes per replay."
    )
    parser.add_argument(
        "--db-url",
        default=None,
        help=(
            "Database URL overriding PNL_PROBE_DB_URL and app DB_URL for the probe's own engine. "
            "PostgreSQL roles must lack INSERT, UPDATE, and DELETE on key P&L tables."
        ),
    )
    parser.add_argument(
        "--statement-timeout-ms",
        type=int,
        default=DEFAULT_STATEMENT_TIMEOUT_MS,
        help="Per-statement timeout in ms (1..60000; lock_timeout stays 1000ms for acquisition).",
    )
    parser.add_argument(
        "--max-runtime-seconds",
        type=float,
        default=DEFAULT_MAX_RUNTIME_SECONDS,
        help=(
            "Overall probe runtime wall-clock ceiling in seconds (0 < s <= 600); "
            "use an outer timeout to terminate overrunning CPU-bound work."
        ),
    )
    parser.add_argument("--max-executions", type=int, default=DEFAULT_MAX_EXECUTIONS)
    parser.add_argument("--max-accruals", type=int, default=DEFAULT_MAX_ACCRUALS)
    parser.add_argument("--max-candles", type=int, default=DEFAULT_MAX_CANDLES)
    parser.add_argument(
        "--max-rss-mb",
        type=int,
        default=DEFAULT_MAX_RSS_MB,
        help=(
            "Sampled RSS growth limit in MiB over probe startup baseline (default: 2048). "
            "On Linux, can lower soft RLIMIT_AS to current VmSize + "
            f"{ADDRESS_SPACE_HEADROOM_FACTOR} * max_rss_mb headroom. "
            "Use a container memory limit for hard enforcement."
        ),
    )
    parser.add_argument(
        "--require-readonly-role",
        action="store_true",
        help="Accepted for compatibility. PostgreSQL write privileges are always checked.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the read-only checkpoint resume probe CLI and print timings and RESULT status.

    Args:
        argv: Optional command-line arguments overriding ``sys.argv[1:]``.

    Returns:
        0 on match, 1 on mismatch, 2 on error/timeout/interrupt, 3 on closed refusal.
    """
    arguments = build_parser().parse_args(list(argv) if argv is not None else None)
    config = CheckpointResumeProbeConfig(
        wallet_public_id=arguments.wallet,
        mode=arguments.mode,
        valuation_ccy=arguments.valuation_ccy,
        window_minutes=arguments.window_minutes,
        db_url=arguments.db_url,
        statement_timeout_ms=arguments.statement_timeout_ms,
        max_runtime_seconds=arguments.max_runtime_seconds,
        max_executions=arguments.max_executions,
        max_accruals=arguments.max_accruals,
        max_candles=arguments.max_candles,
        max_rss_mb=arguments.max_rss_mb,
    )
    if not _has_probe_db_override(config.db_url):
        print(READONLY_ROLE_WARNING)
    try:
        deadline = time.monotonic() + config.max_runtime_seconds
        _apply_address_space_limit(config.max_rss_mb)
        report = asyncio.run(probe_wallet_checkpoint_resume(config))
        formatted = _format_report(report)
        _require(time.monotonic() <= deadline, "timeout")
    except KeyboardInterrupt:
        print("RESULT: FAIL interrupted")
        return 2
    except Exception as exc:
        code, result_line = _classify_failure(exc)
        print(result_line)
        return code
    print(formatted)
    return 0 if report.matched else 1


if __name__ == "__main__":
    raise SystemExit(main())
