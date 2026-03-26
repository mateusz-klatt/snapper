"""Tests for archive symbol resolution and filesystem path safety."""

from datetime import UTC
from datetime import datetime

from sqlalchemy import select as sa_select
from sqlalchemy import update as sa_update

from snapper.data.archive_symbols import resolve_archive_symbols
from snapper.data.archive_symbols import safe_path
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Base
from snapper.data.models import Symbol
from snapper.data.repository import DatabaseRepository


def test_safe_path_preserves_canonical_symbol() -> None:
    """BTC-USD is already clean and passes through unchanged."""
    assert safe_path("BTC-USD") == "BTC-USD"


def test_safe_path_uppercases() -> None:
    """Lowercase input is normalized to uppercase."""
    assert safe_path("btc-usd") == "BTC-USD"


def test_safe_path_replaces_colon() -> None:
    """Polygon-style colon delimiter replaced with underscore."""
    assert safe_path("X:BTCUSD") == "X_BTCUSD"


def test_safe_path_replaces_slash() -> None:
    """Slash replaced with underscore."""
    assert safe_path("eth/usd") == "ETH_USD"


def test_safe_path_replaces_space() -> None:
    """Space replaced with underscore."""
    assert safe_path("BTC USD") == "BTC_USD"


def test_safe_path_collapses_multiple_underscores() -> None:
    """Multiple consecutive underscores collapse to one."""
    assert safe_path("A::B") == "A_B"


def test_safe_path_strips_leading_trailing_underscores() -> None:
    """Leading and trailing underscores removed after normalization."""
    assert safe_path(":BTC:") == "BTC"


def test_safe_path_strips_whitespace() -> None:
    """Leading and trailing whitespace stripped."""
    assert safe_path("  BTC-USD  ") == "BTC-USD"


def test_safe_path_case_insensitive_fs_safety() -> None:
    """Mixed-case inputs normalize to the same uppercase form."""
    assert safe_path("APPLx") == "APPLX"
    assert safe_path("APPLX") == "APPLX"
    assert safe_path("applx") == "APPLX"


def test_safe_path_windows_reserved_names() -> None:
    """Windows reserved names get underscore suffix."""
    assert safe_path("CON") == "CON_"
    assert safe_path("PRN") == "PRN_"
    assert safe_path("NUL") == "NUL_"
    assert safe_path("COM1") == "COM1_"
    assert safe_path("LPT9") == "LPT9_"


def test_safe_path_windows_reserved_case_insensitive() -> None:
    """Windows reserved names detected after uppercasing."""
    assert safe_path("con") == "CON_"
    assert safe_path("Nul") == "NUL_"


def test_resolve_archive_symbols_basic() -> None:
    """Basic mapping without collisions."""
    rows = [
        ("pub-1", "BTC-USD"),
        ("pub-2", "ETH-USD"),
    ]
    result = resolve_archive_symbols(rows)
    assert result == {"pub-1": "BTC-USD", "pub-2": "ETH-USD"}


def test_resolve_archive_symbols_seniority_collision() -> None:
    """Older symbol gets clean name, younger gets -2 suffix."""
    rows = [
        ("pub-old", "BTC-USD"),
        ("pub-new", "BTC-USD"),
    ]
    result = resolve_archive_symbols(rows)
    assert result == {"pub-old": "BTC-USD", "pub-new": "BTC-USD-2"}


def test_resolve_archive_symbols_triple_collision() -> None:
    """Three-way collision produces -2 and -3 suffixes."""
    rows = [
        ("pub-1", "BTC-USD"),
        ("pub-2", "BTC-USD"),
        ("pub-3", "BTC-USD"),
    ]
    result = resolve_archive_symbols(rows)
    assert result == {
        "pub-1": "BTC-USD",
        "pub-2": "BTC-USD-2",
        "pub-3": "BTC-USD-3",
    }


def test_resolve_archive_symbols_case_collision() -> None:
    """Different casing normalizes to same safe_path, triggers seniority."""
    rows = [
        ("pub-1", "APPLX"),
        ("pub-2", "applx"),
    ]
    result = resolve_archive_symbols(rows)
    assert result == {"pub-1": "APPLX", "pub-2": "APPLX-2"}


def test_resolve_archive_symbols_empty() -> None:
    """Empty input returns empty dict."""
    assert resolve_archive_symbols([]) == {}


def test_resolve_archive_symbols_normalization_collision() -> None:
    """Symbols that differ only by non-alphanum characters collide after safe_path."""
    rows = [
        ("pub-1", "X:BTCUSD"),
        ("pub-2", "X/BTCUSD"),
    ]
    result = resolve_archive_symbols(rows)
    assert result == {"pub-1": "X_BTCUSD", "pub-2": "X_BTCUSD-2"}


def _make_repo() -> DatabaseRepository:
    """Create an in-memory DatabaseRepository with schema."""
    repo = DatabaseRepository("sqlite:///")
    Base.metadata.create_all(repo.engine)
    return repo


def _insert_symbol(
    repo: DatabaseRepository,
    public_id: str,
    native_symbol: str,
    ts: datetime,
    close_previous: bool = False,
) -> None:
    """Insert a Symbol row into the test database.

    When close_previous is True, closes the existing active row for
    the same public_id before inserting (SCD2 pattern).
    """
    with repo.get_session() as session:
        if close_previous:
            session.execute(
                sa_update(Symbol)
                .where(Symbol.public_id == public_id, Symbol.known_to == KNOWN_TO_MAX)
                .values(known_to=ts)
            )
        sym = Symbol(
            public_id=public_id,
            native_symbol=native_symbol,
            base=native_symbol.split("-", maxsplit=1)[0] if "-" in native_symbol else native_symbol,
            quote=native_symbol.split("-")[1] if "-" in native_symbol else "",
            asset_type="crypto",
            session_id="test-session",
            sequence_id=1,
            timestamp=ts,
            created_at=ts,
        )
        session.add(sym)
        session.commit()


def test_get_archive_symbols_basic() -> None:
    """DatabaseRepository.get_archive_symbols returns correct mapping."""
    repo = _make_repo()
    ts = datetime(2024, 1, 1, tzinfo=UTC)
    _insert_symbol(repo, "pub-btc", "BTC-USD", ts)
    _insert_symbol(repo, "pub-eth", "ETH-USD", datetime(2024, 1, 2, tzinfo=UTC))
    result = repo.get_archive_symbols()
    assert result == {"pub-btc": "BTC-USD", "pub-eth": "ETH-USD"}


def test_get_archive_symbols_uses_first_version() -> None:
    """Archive symbol uses native_symbol from first version, not latest."""
    repo = _make_repo()
    _insert_symbol(repo, "pub-btc", "BTC-USD", datetime(2024, 1, 1, tzinfo=UTC))
    _insert_symbol(
        repo, "pub-btc", "BITCOIN-USD", datetime(2024, 6, 1, tzinfo=UTC), close_previous=True
    )
    result = repo.get_archive_symbols()
    assert result["pub-btc"] == "BTC-USD"


def test_get_archive_symbols_empty_db() -> None:
    """Empty database returns empty mapping."""
    repo = _make_repo()
    assert repo.get_archive_symbols() == {}


def test_get_archive_symbols_seniority_collision() -> None:
    """Older symbol gets clean name, younger duplicate gets -2."""
    repo = _make_repo()
    _insert_symbol(repo, "pub-old", "BTC-USD", datetime(2024, 1, 1, tzinfo=UTC))
    with repo.get_session() as session:
        session.execute(
            sa_update(Symbol)
            .where(Symbol.public_id == "pub-old", Symbol.known_to == KNOWN_TO_MAX)
            .values(known_to=datetime(2025, 1, 1, tzinfo=UTC))
        )
        session.commit()
    _insert_symbol(repo, "pub-new", "BTC-USD", datetime(2025, 1, 1, tzinfo=UTC))
    result = repo.get_archive_symbols()
    assert result["pub-old"] == "BTC-USD"
    assert result["pub-new"] == "BTC-USD-2"


def test_get_symbol_anchor_ids_basic() -> None:
    """DatabaseRepository.get_symbol_anchor_ids returns correct row IDs."""
    repo = _make_repo()
    _insert_symbol(repo, "pub-btc", "BTC-USD", datetime(2024, 1, 1, tzinfo=UTC))
    _insert_symbol(
        repo, "pub-btc", "BITCOIN-USD", datetime(2024, 6, 1, tzinfo=UTC), close_previous=True
    )
    _insert_symbol(repo, "pub-eth", "ETH-USD", datetime(2024, 1, 2, tzinfo=UTC))
    anchor_ids = repo.get_symbol_anchor_ids()
    assert len(anchor_ids) == 2
    with repo.get_session() as session:

        all_rows = session.execute(
            sa_select(Symbol.id, Symbol.public_id, Symbol.native_symbol).order_by(Symbol.timestamp)
        ).all()
    first_btc_id = next(r[0] for r in all_rows if r[2] == "BTC-USD")
    eth_id = next(r[0] for r in all_rows if r[2] == "ETH-USD")
    assert first_btc_id in anchor_ids
    assert eth_id in anchor_ids


def test_get_symbol_anchor_ids_empty_db() -> None:
    """Empty database returns empty set."""
    repo = _make_repo()
    assert repo.get_symbol_anchor_ids() == set()
