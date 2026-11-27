"""Exercise the offline Kraken Spot recovery planner through its command interface."""

import hashlib
import json
from pathlib import Path
from unittest.mock import Mock

import pytest
from typer.testing import CliRunner

from snapper.application.updaters.historical import kraken_spot_trade_evidence
from snapper.cli import kraken_spot_trade_recovery as planner
from snapper.cli.app import app

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
        "--input-root",
        str(tmp_path),
    ]


def test_planner_command_exposes_argparse_help() -> None:
    """Expose the offline planner's argparse help through the Snapper CLI.

    Given: The operator invokes the planner command with --help.
    When: Typer forwards the request to argparse.
    Then: The command documents the stored snapshot argument and exits successfully.
    """
    result = CliRunner().invoke(app, ["plan-kraken-spot-trade-recovery", "--help"])

    assert result.exit_code == 0
    assert result.stdout.startswith("usage: snapper plan-kraken-spot-trade-recovery ")
    assert "--stored-trades" in result.stdout
    assert "Stored trade snapshot CSV" in result.stdout


def test_reports_present_and_missing_without_touching_inputs(tmp_path: Path) -> None:
    """Compare equal public trade IDs on different instruments independently.

    Given: Two instruments with the same public trade ID and one stored row.
    When: The planner compares the two CSVs.
    Then: Only the other instrument's trade is missing and both files are intact.
    """
    argv = _args(tmp_path, _BTC_EVIDENCE + _ETH_EVIDENCE, _STORED_HEADER + _BTC_STORED)
    before = {file.name: file.read_bytes() for file in tmp_path.iterdir()}

    result = CliRunner().invoke(app, ["plan-kraken-spot-trade-recovery", *argv])

    assert result.exit_code == 0
    assert json.loads(result.stdout) == {
        "already_present": 1,
        "evidence_rows": 2,
        "missing_from_snapshot": 1,
        "missing_trade_ids_by_symbol": {"ETH-USD": ["999001"]},
    }
    assert result.stderr == ""
    assert {file.name: file.read_bytes() for file in tmp_path.iterdir()} == before


def test_accepts_relative_inputs_under_the_working_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Use the working directory as the default input root.

    Given: Relative CSV paths in a subdirectory of the working directory.
    When: The planner runs without an explicit input root.
    Then: It reads both files and reports the stored trade.
    """
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    argv = _args(inputs, _BTC_EVIDENCE, _STORED_HEADER + _BTC_STORED)
    argv[argv.index("--evidence") + 1] = "inputs/evidence.csv"
    argv[argv.index("--stored-trades") + 1] = "inputs/stored.csv"
    root_index = argv.index("--input-root")
    del argv[root_index : root_index + 2]
    monkeypatch.chdir(tmp_path)

    assert planner.main(argv) == 0
    output = capsys.readouterr()
    assert json.loads(output.out)["already_present"] == 1
    assert output.err == ""


@pytest.mark.parametrize("option", ["--evidence", "--stored-trades"])
@pytest.mark.parametrize(
    ("candidate", "message"),
    [
        ("../inputs-other/outside.csv", "Input path must be a regular file inside the input root"),
        ("linked.csv", "Input path must be a regular file inside the input root"),
        ("directory", "Input path must be a regular file inside the input root"),
        ("missing.csv", "missing.csv"),
    ],
)
def test_rejects_invalid_paths_before_opening_either_input(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], option: str, candidate: str, message: str
) -> None:
    """Require each input path to identify a regular file inside the root.

    Given: Either CSV argument escapes the root or names a directory or missing file.
    When: The planner validates both paths.
    Then: It reports the failure and exits without opening either input.
    """
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    sibling = tmp_path / "inputs-other"
    sibling.mkdir()
    outside = sibling / "outside.csv"
    outside.write_text("outside\n", encoding="utf-8")
    (inputs / "linked.csv").symlink_to(outside)
    (inputs / "directory").mkdir()
    argv = _args(inputs, _BTC_EVIDENCE, _STORED_HEADER)
    argv[argv.index(option) + 1] = candidate

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(Path, "open", Mock(side_effect=AssertionError("An input was opened")))
        assert planner.main(argv) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert message in output.err


def test_rejects_a_file_as_the_input_root(tmp_path: Path) -> None:
    """Require the input root to resolve to a directory.

    Given: A regular file supplied as the input root.
    When: The planner resolves the root.
    Then: It reports the invalid root and exits without a plan.
    """
    argv = _args(tmp_path, _BTC_EVIDENCE, _STORED_HEADER)
    argv[argv.index("--input-root") + 1] = str(tmp_path / "evidence.csv")

    result = CliRunner().invoke(app, ["plan-kraken-spot-trade-recovery", *argv])

    assert result.exit_code == 1
    assert result.stdout == ""
    assert "Input root must be a directory" in result.stderr


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
    monkeypatch.setattr(kraken_spot_trade_evidence, "_MAX_EVIDENCE_BYTES", 16)

    assert planner.main(argv) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert "Evidence file exceeds the size limit" in output.err
