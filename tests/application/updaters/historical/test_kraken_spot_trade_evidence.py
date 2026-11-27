"""Test offline parsing of held Kraken Spot trade evidence."""

import csv
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from io import StringIO
from pathlib import Path

import pytest

from snapper.application.updaters.historical import kraken_spot_trade_evidence
from snapper.application.updaters.historical.kraken_spot_trade_evidence import (
    KrakenSpotTradeEvidence,
)
from snapper.application.updaters.historical.kraken_spot_trade_evidence import confined_output
from snapper.application.updaters.historical.kraken_spot_trade_evidence import (
    read_kraken_spot_trade_evidence,
)

_HEADER = (
    "exchange,symbol,instrument_public_id,exchange_trade_id,executed_at_utc,exchange_unix_seconds,"
    "price,size,exchange_side,exchange_order_type,exchange_misc\n"
)
_ROW = (
    "kraken",
    "BTC-USD",
    "00000000-0000-4000-8000-000000000001",
    "999000001",
    "2026-01-01T00:00:00.123456Z",
    "1767225600.1234561",
    "12345.10000",
    "0.01000000",
    "b",
    "l",
    "",
)


def _csv(*rows: list[str]) -> StringIO:
    """Build a text stream with the actual evidence export's header."""
    stream = StringIO(newline="")
    stream.write(_HEADER)
    csv.writer(stream).writerows(rows)
    stream.seek(0)
    return stream


def test_confines_new_receipts_under_the_input_root(tmp_path: Path) -> None:
    """Resolve new receipt names under the root without creating files.

    Given: A directory inside the input root with an unused receipt name.
    When: An operator supplies its relative path or its absolute path.
    Then: Both resolve to the same unused path inside the root.
    """
    root = tmp_path.resolve()
    relative = Path("receipts/receipt.json")
    path = root / relative
    path.parent.mkdir()

    assert confined_output(root, relative) == confined_output(root, path) == path
    assert not path.exists()


@pytest.mark.parametrize(
    "candidate",
    [
        "../receipt.json",
        "outside/receipt.json",
        "missing/receipt.json",
        "regular.json/receipt.json",
        ".",
        "nested/..",
        pytest.param("a" * 1024, id="oversize-file-name"),
    ],
)
def test_rejects_receipts_with_invalid_paths(tmp_path: Path, candidate: str) -> None:
    """Reject receipt paths that escape the root or cannot identify a new file.

    Given: An input root with a nested directory, a regular file, and a link outside.
    When: An operator supplies a path with an invalid destination.
    Then: Confinement rejects the receipt.
    """
    root = tmp_path.resolve() / "inputs"
    root.mkdir()
    (root / "nested").mkdir()
    (root / "outside").symlink_to(tmp_path, target_is_directory=True)
    (root / "regular.json").write_text("existing file", encoding="utf-8")

    with pytest.raises(ValueError, match="Receipt path must be a new file inside the input root"):
        confined_output(root, Path(candidate))


@pytest.mark.parametrize("dangling_symlink", [False, True])
def test_rejects_occupied_receipt_names(tmp_path: Path, dangling_symlink: bool) -> None:
    """Require unused names for receipts.

    Given: A receipt name occupied by a regular file or a dangling symlink.
    When: An operator supplies that destination.
    Then: Confinement rejects the name and preserves the existing entry.
    """
    root = tmp_path.resolve()
    path = root / "receipt.json"
    if dangling_symlink:
        path.symlink_to(root / "missing.json")
    else:
        path.write_text("existing receipt", encoding="utf-8")

    with pytest.raises(ValueError, match="Receipt path must be a new file inside the input root"):
        confined_output(root, path)

    assert path.exists(follow_symlinks=False)


def test_preserves_decimal_precision_order_and_duplicate_rows() -> None:
    """Retain both sides and original prices, sizes and repeated identities.

    Given: An export with a buy, a sell and a repeated buy.
    When: The evidence reader parses the export.
    Then: It preserves the decimal representation and row order.
    """
    buy = list(_ROW)
    sell = list(_ROW)
    sell[3] = "999000002"
    sell[6] = "12345.100000000000000001"
    sell[7] = "0.00000001234567890123456789"
    sell[8:11] = ["s", "m", "quoted, miscellaneous"]
    expected_buy = KrakenSpotTradeEvidence(
        symbol="BTC-USD",
        instrument_public_id="00000000-0000-4000-8000-000000000001",
        trade_id="999000001",
        executed_at=datetime(2026, 1, 1, 0, 0, 0, 123_000, tzinfo=UTC),
        price=Decimal("12345.10000"),
        size=Decimal("0.01000000"),
        side="buy",
    )
    expected_sell = KrakenSpotTradeEvidence(
        symbol="BTC-USD",
        instrument_public_id="00000000-0000-4000-8000-000000000001",
        trade_id="999000002",
        executed_at=datetime(2026, 1, 1, 0, 0, 0, 123_000, tzinfo=UTC),
        price=Decimal("12345.100000000000000001"),
        size=Decimal("0.00000001234567890123456789"),
        side="sell",
    )

    result = read_kraken_spot_trade_evidence(_csv(buy, sell, buy))

    assert result == [expected_buy, expected_sell, expected_buy]
    assert result[0].price.as_tuple() == expected_buy.price.as_tuple()
    assert result[0].size.as_tuple() == expected_buy.size.as_tuple()


@pytest.mark.parametrize("symbol", ["BTC/USD", "BTC-USD"])
def test_normalizes_evidence_symbol_to_catalog_spelling(symbol: str) -> None:
    """Normalize supported evidence spellings to the catalog spelling.

    Given: A Spot trade with a slash-delimited or native symbol.
    When: The evidence reader parses the trade.
    Then: The resulting symbol uses the catalog's dash separator.
    """
    row = list(_ROW)
    row[1] = symbol

    [trade] = read_kraken_spot_trade_evidence(_csv(row))

    assert trade.symbol == "BTC-USD"


@pytest.mark.parametrize(
    ("iso", "seconds", "normalized_microsecond"),
    [
        ("2026-01-01T00:00:00.123456Z", "1767225600.1234559", 123_000),
        ("2026-01-01T00:00:00.124000Z", "1767225600.1239999", 123_000),
        ("2026-01-01T00:00:01.000000Z", "1767225600.9999999", 999_000),
        ("2026-01-01T00:00:00+00:00", "1767225600", 0),
    ],
)
def test_normalizes_the_raw_epoch_when_iso_rounds_up(
    iso: str, seconds: str, normalized_microsecond: int
) -> None:
    """Preserve the source millisecond when the ISO display rounds up.

    Given: A raw epoch and an ISO display that can round across a boundary.
    When: The evidence reader normalizes execution time.
    Then: The result uses the exact raw epoch's millisecond.
    """
    row = list(_ROW)
    row[4:6] = [iso, seconds]

    [trade] = read_kraken_spot_trade_evidence(_csv(row))

    assert trade.executed_at == datetime(2026, 1, 1, microsecond=normalized_microsecond, tzinfo=UTC)


@pytest.mark.parametrize("header", ["", "exchange,symbol\n", _HEADER.replace("size", "amount")])
def test_rejects_missing_or_inexact_header(header: str) -> None:
    """Require the exact evidence header.

    Given: A missing or altered CSV header.
    When: The evidence reader parses the input.
    Then: It reports the header error.
    """
    with pytest.raises(ValueError, match="Invalid Kraken Spot evidence CSV header"):
        read_kraken_spot_trade_evidence(StringIO(header))


def test_rejects_header_without_rows() -> None:
    """Require at least one evidence row.

    Given: A CSV containing only the exact header.
    When: The evidence reader parses the input.
    Then: It reports the missing data rows.
    """
    with pytest.raises(ValueError, match="contains no rows"):
        read_kraken_spot_trade_evidence(StringIO(_HEADER))


def test_rejects_invalid_csv_quoting() -> None:
    """Report the source line of an unterminated quoted field.

    Given: A CSV with an unterminated quoted field in its first record.
    When: The evidence reader parses the input.
    Then: It reports a malformed CSV at line two.
    """
    with pytest.raises(ValueError, match="Malformed Kraken Spot evidence CSV at line 2"):
        read_kraken_spot_trade_evidence(StringIO(_HEADER + '"unterminated'))


@pytest.mark.parametrize("row", [list(_ROW[:-1]), [*_ROW, "extra"], []])
def test_rejects_wrong_number_of_columns(row: list[str]) -> None:
    """Require the fixed number of fields on each evidence row.

    Given: A data record with missing or extra fields.
    When: The evidence reader parses the record.
    Then: It reports the exact expected field count.
    """
    with pytest.raises(ValueError, match="CSV line 2: Expected 11 fields"):
        read_kraken_spot_trade_evidence(_csv(row))


@pytest.mark.parametrize(
    ("column", "replacement", "message"),
    [
        (0, "kraken-futures", "Expected kraken exchange"),
        (1, "BTC/USD:USD", "Invalid Kraken Spot native symbol"),
        (2, "AAAAAAAA-0000-4000-8000-000000000001", "canonical instrument_public_id UUID"),
        (3, "123.0", "positive numeric exchange_trade_id"),
        (3, "00123", "positive numeric exchange_trade_id"),
        (3, "0", "positive numeric exchange_trade_id"),
        (3, "1" * 65, "positive numeric exchange_trade_id"),
        (4, "2026-01-01T01:00:00.123456+01:00", "Expected executed_at_utc"),
        (4, "2026-01-01T00:00:00.123456", "Expected executed_at_utc"),
        (4, "2026-01-35T00:00:00.123456Z", "CSV line 2"),
        (5, "1.767225600e9", "Expected decimal exchange_unix_seconds"),
        (5, "1767225600.123455000", "disagree"),
        (5, "1767225600.1234571", "disagree"),
        (6, "NaN", "finite positive price"),
        (6, "Inf", "finite positive price"),
        (6, "-1", "finite positive price"),
        (6, "0", "finite positive price"),
        (6, "1e309", "finite positive price"),
        (6, "1e-400", "finite positive price"),
        (6, "1e999999999999999999999", "finite positive price"),
        (7, "1_000", "finite positive size"),
        (8, "buy", "Expected exchange_side b or s"),
        (9, "market", "Expected exchange_order_type l or m"),
    ],
)
def test_rejects_invalid_trade_fields(column: int, replacement: str, message: str) -> None:
    """Reject ambiguous identities, conflicting times and invalid payloads.

    Given: An otherwise valid evidence row with one invalid field.
    When: The evidence reader parses the row.
    Then: It reports the failure with the affected field or source line.
    """
    row = list(_ROW)
    row[column] = replacement

    with pytest.raises(ValueError, match=message):
        read_kraken_spot_trade_evidence(_csv(row))


def test_enforces_row_limit_without_deduplicating(monkeypatch: pytest.MonkeyPatch) -> None:
    """Count repeated trade IDs toward the row limit.

    Given: A two-row limit and an evidence CSV with repeated trade IDs.
    When: The evidence reader parses two and then three rows.
    Then: It accepts the first CSV and rejects the second as oversized.
    """
    monkeypatch.setattr(kraken_spot_trade_evidence, "MAX_KRAKEN_SPOT_TRADE_EVIDENCE_ROWS", 2)
    row = list(_ROW)

    assert len(read_kraken_spot_trade_evidence(_csv(row, row))) == 2
    with pytest.raises(ValueError, match="Kraken Spot evidence exceeds 2 rows"):
        read_kraken_spot_trade_evidence(_csv(row, row, row))
