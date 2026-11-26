"""Exercise the offline Kraken Spot recovery planner through its command interface."""

import hashlib
import json
from pathlib import Path

import pytest

from scripts import plan_kraken_spot_trade_recovery as planner

_BTC = "00000000-0000-4000-8000-000000000001"
_ETH = "00000000-0000-4000-8000-000000000002"
_EVIDENCE_HEADER = (
    "exchange,symbol,instrument_public_id,exchange_trade_id,executed_at_utc,"
    "exchange_unix_seconds,price,size,exchange_side,exchange_order_type,exchange_misc\n"
)
_BTC_EVIDENCE = (
    f"kraken,BTC-USD,{_BTC},999001,2026-01-01T00:00:00.123456Z,"
    "1767225600.1234561,12345.10000,0.01000000,b,l,\n"
)
_ETH_EVIDENCE = (
    f"kraken,ETH-USD,{_ETH},999001,2026-01-01T00:00:01.123456Z,"
    "1767225601.1234561,2345.10000,0.02000000,s,m,\n"
)
_STORED_HEADER = "instrument_public_id,trade_id,executed_at,price,size,side\n"
_BTC_STORED = f"{_BTC},999001,2026-01-01 01:00:00.123+01:00,12345.1,0.01,buy\n"
_ETH_STORED = f"{_ETH},999001,2026-01-01 00:00:01.123+00,2345.1,0.02,sell\n"


def _args(tmp_path: Path, evidence: str, stored: str) -> list[str]:
    """Write inputs and return the arguments passed by an operator."""
    evidence_path = tmp_path / "evidence.csv"
    evidence_bytes = (_EVIDENCE_HEADER + evidence).encode()
    evidence_path.write_bytes(evidence_bytes)
    stored_path = tmp_path / "stored.csv"
    stored_path.write_text(stored, encoding="utf-8")
    return [
        "--evidence",
        str(evidence_path),
        "--evidence-sha256",
        hashlib.sha256(evidence_bytes).hexdigest().upper(),
        "--stored-trades",
        str(stored_path),
        "--start",
        "2026-01-01T00:00:00Z",
        "--end",
        "2026-01-01T00:01:00Z",
    ]


def test_reports_present_and_missing_without_touching_inputs(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Compare equal public trade IDs on different instruments independently.

    Given: Two instruments with the same public trade ID and one stored row.
    When: The planner compares the two CSVs.
    Then: Only the other instrument's trade is missing and both files are intact.
    """
    argv = _args(tmp_path, _BTC_EVIDENCE + _ETH_EVIDENCE, _STORED_HEADER + _BTC_STORED)
    before = {file.name: file.read_bytes() for file in tmp_path.iterdir()}

    assert planner.main(argv) == 0
    output = capsys.readouterr()
    assert json.loads(output.out) == {
        "already_present": 1,
        "evidence_rows": 2,
        "missing_from_snapshot": 1,
        "missing_trade_ids_by_symbol": {"ETH-USD": ["999001"]},
    }
    assert output.err == ""
    assert {file.name: file.read_bytes() for file in tmp_path.iterdir()} == before


def _fixture_rows(template: str, count: int) -> str:
    """Expand one synthetic fixture row into a sequence of distinct venue IDs."""
    return "".join(template.replace("999001", str(999001 + i)) for i in range(count))


@pytest.mark.parametrize(("btc_count", "eth_count"), [(672, 192), (342, 83)])
def test_reclassifies_both_incident_size_fixtures_on_replay(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], btc_count: int, eth_count: int
) -> None:
    """Reclassify both incident-sized candidate sets after simulated recovery.

    Given: Synthetic candidate sets sized like the two observed incidents.
    When: The planner compares against an empty and then fully matched snapshot.
    Then: Every candidate is missing initially and present after simulated recovery.
    """
    evidence = _fixture_rows(_BTC_EVIDENCE, btc_count) + _fixture_rows(_ETH_EVIDENCE, eth_count)
    total = btc_count + eth_count
    snapshot = _STORED_HEADER + _fixture_rows(_BTC_STORED, btc_count)
    snapshot += _fixture_rows(_ETH_STORED, eth_count)

    assert planner.main(_args(tmp_path, evidence, _STORED_HEADER)) == 0
    before = json.loads(capsys.readouterr().out)
    assert before["already_present"] == 0
    assert before["missing_from_snapshot"] == total
    assert len(before["missing_trade_ids_by_symbol"]["BTC-USD"]) == btc_count
    assert len(before["missing_trade_ids_by_symbol"]["ETH-USD"]) == eth_count

    assert planner.main(_args(tmp_path, evidence, snapshot)) == 0
    after = json.loads(capsys.readouterr().out)
    assert after["already_present"] == total
    assert after["missing_from_snapshot"] == 0
    assert after["missing_trade_ids_by_symbol"] == {}


@pytest.mark.parametrize(
    ("replacement_index", "replacement", "message"),
    [
        (3, "0" * 64, "Evidence SHA-256"),
        (7, "2026-01-01T00:00:00", "millisecond-aligned"),
        (9, "2026-01-01T00:01:00.000001Z", "millisecond-aligned"),
        (9, "2026-01-01T00:00:00Z", "start must precede end"),
        (9, "2026-01-01T00:00:00.123Z", "outside the requested window"),
    ],
)
def test_rejects_untrusted_evidence_or_window(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    replacement_index: int,
    replacement: str,
    message: str,
) -> None:
    """Require an exact evidence digest and a valid UTC window.

    Given: A valid evidence file and an invalid digest or scope.
    When: The planner validates the file against the arguments.
    Then: It returns an error without printing a plan.
    """
    argv = _args(tmp_path, _BTC_EVIDENCE, _STORED_HEADER)
    argv[replacement_index] = replacement

    assert planner.main(argv) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert message in output.err


@pytest.mark.parametrize(
    "other", [_ETH_EVIDENCE.replace("ETH-USD", "BTC-USD"), _ETH_EVIDENCE.replace(_ETH, _BTC)]
)
def test_rejects_ambiguous_instrument_assignment(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], other: str
) -> None:
    """Reject ambiguous instrument identity in the evidence.

    Given: Two evidence rows with an ambiguous symbol to instrument mapping.
    When: The planner validates their scope.
    Then: It returns an error without classifying the trades.
    """
    assert planner.main(_args(tmp_path, _BTC_EVIDENCE + other, _STORED_HEADER)) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert "ambiguous symbol or instrument assignment" in output.err


@pytest.mark.parametrize(
    ("stored", "message"),
    [
        ("wrong\n", "snapshot header"),
        (_STORED_HEADER + "broken\n", "Invalid stored trade on CSV line 2"),
        (_STORED_HEADER + _BTC_STORED.replace("+01:00", ""), "Invalid stored trade on CSV line 2"),
        (_STORED_HEADER + _BTC_STORED.replace(_BTC, "invalid"), "Invalid stored trade"),
        (_STORED_HEADER + _BTC_STORED.replace(_BTC, f"{_BTC[:-1]}A"), "Invalid stored trade"),
        (_STORED_HEADER + _BTC_STORED.replace("999001", ""), "Invalid stored trade"),
        (_STORED_HEADER + _BTC_STORED.replace("999001", " 999001"), "Invalid stored trade"),
        (_STORED_HEADER + _BTC_STORED.replace("12345.1", "NaN"), "Invalid stored trade"),
        (_STORED_HEADER + _BTC_STORED.replace("0.01", "0"), "Invalid stored trade"),
        (_STORED_HEADER + _BTC_STORED.replace("buy", "b"), "Invalid stored trade"),
        (_STORED_HEADER + _BTC_STORED.replace(".123", ".123456"), "999001"),
    ],
)
def test_rejects_invalid_or_conflicting_stored_rows(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], stored: str, message: str
) -> None:
    """Refuse to report absence against invalid or conflicting stored data.

    Given: A stored snapshot with an invalid or conflicting trade.
    When: The planner compares it with the verified evidence.
    Then: It refuses the plan and describes the validation failure.
    """
    assert planner.main(_args(tmp_path, _BTC_EVIDENCE, stored)) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert message in output.err


def test_rejects_oversize_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Reject evidence over the size cap before parsing it.

    Given: Verified evidence larger than the supported size.
    When: The planner reads the file.
    Then: It refuses to compare the oversized export.
    """
    argv = _args(tmp_path, _BTC_EVIDENCE, _STORED_HEADER)
    monkeypatch.setattr(planner, "_MAX_EVIDENCE_BYTES", 16)

    assert planner.main(argv) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert "Evidence file exceeds the size limit" in output.err
