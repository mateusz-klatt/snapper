"""Tests for sonar_report module."""

from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

import scripts.sonar_report as sonar_report


def _quality_gate(status: str = "OK") -> sonar_report.QualityGateReport:
    """Return a minimal quality gate payload for tests."""
    return {
        "status": status,
        "period_mode": "previous_version",
        "period_date": "2026-02-01T16:42:42+0000",
        "conditions": [],
        "failing_conditions": [],
    }


class FrozenDateTime(datetime):
    """Deterministic datetime for tests."""

    @classmethod
    def now(cls, tz: Any = None) -> datetime:
        """Return a fixed UTC datetime."""
        return datetime(2026, 2, 1, 17, 21, tzinfo=UTC)


class TestGetToken:
    """Tests for get_token."""

    def test_returns_token_when_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Returns env token when SONAR_TOKEN is set."""
        monkeypatch.setenv("SONAR_TOKEN", "abc")
        assert sonar_report.get_token() == "abc"

    def test_exits_with_message_when_missing(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Exits with code 1 and prints an error when SONAR_TOKEN is missing."""
        monkeypatch.delenv("SONAR_TOKEN", raising=False)
        with pytest.raises(SystemExit) as exc:
            sonar_report.get_token()

        assert exc.value.code == 1
        captured = capsys.readouterr()
        assert "SONAR_TOKEN not set" in captured.out


class TestRatingLabel:
    """Tests for rating_label."""

    def test_known_values_convert_to_letters(self) -> None:
        """Maps rating numbers to letter grades."""
        assert sonar_report.rating_label("1.0") == "A"
        assert sonar_report.rating_label("5.0") == "E"

    def test_unknown_value_returns_input(self) -> None:
        """Returns input when mapping is unknown."""
        assert sonar_report.rating_label("?") == "?"


class TestFetchMetrics:
    """Tests for fetch_metrics."""

    def test_fetches_and_builds_metric_mapping(self) -> None:
        """Builds metric dict from Sonar API payload."""
        response = MagicMock()
        response.raise_for_status = MagicMock()
        response.json = MagicMock(
            return_value={
                "component": {
                    "measures": [
                        {"metric": "bugs", "value": "0"},
                        {"metric": "coverage", "value": "100"},
                    ]
                }
            }
        )

        with patch("scripts.sonar_report.httpx.get", return_value=response) as mock_get:
            result = sonar_report.fetch_metrics("t")

        assert result == {"bugs": "0", "coverage": "100"}
        assert mock_get.call_args.kwargs["auth"] == ("t", "")
        assert mock_get.call_args.kwargs["timeout"] == 30
        assert "measures/component" in mock_get.call_args.args[0]


class TestFetchAllIssues:
    """Tests for fetch_all_issues."""

    def test_paginates_until_total_reached(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Fetches multiple pages when total requires it."""
        response_1 = MagicMock()
        response_1.raise_for_status = MagicMock()
        response_1.json = MagicMock(
            return_value={
                "issues": [
                    {"key": "i1", "severity": "MAJOR"},
                    {"key": "i2", "severity": "MINOR"},
                ],
                "total": 3,
            }
        )

        response_2 = MagicMock()
        response_2.raise_for_status = MagicMock()
        response_2.json = MagicMock(
            return_value={"issues": [{"key": "i3", "severity": "CRITICAL"}], "total": 3}
        )

        with patch(
            "scripts.sonar_report.httpx.get", side_effect=[response_1, response_2]
        ) as mock_get:
            issues = sonar_report.fetch_all_issues("t")

        assert [i["key"] for i in issues] == ["i1", "i2", "i3"]
        assert mock_get.call_count == 2
        assert mock_get.call_args_list[0].kwargs["params"]["p"] == 1
        assert mock_get.call_args_list[1].kwargs["params"]["p"] == 2

        captured = capsys.readouterr()
        assert "page 1" in captured.out
        assert "page 2" in captured.out

    def test_stops_when_issues_empty(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Stops early if API returns no issues."""
        response = MagicMock()
        response.raise_for_status = MagicMock()
        response.json = MagicMock(return_value={"issues": [], "total": 10})

        with patch("scripts.sonar_report.httpx.get", return_value=response) as mock_get:
            issues = sonar_report.fetch_all_issues("t")

        assert issues == []
        assert mock_get.call_count == 1
        captured = capsys.readouterr()
        assert "page 1" in captured.out


class TestPrintReport:
    """Tests for print_report."""

    def test_prints_none_when_no_critical_issues(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Prints '(none)' in critical section when no critical issues exist."""
        issues: list[dict[str, Any]] = [
            {
                "severity": "MAJOR",
                "type": "BUG",
                "rule": "python:S123",
                "component": f"{sonar_report.PROJECT_KEY}:main.py",
                "line": 10,
                "message": "Something",
            }
        ]
        metrics = {"ncloc": "1", "sqale_index": "0"}
        quality_gate = _quality_gate()

        with patch.object(sonar_report, "datetime", FrozenDateTime):
            sonar_report.print_report(issues, metrics, quality_gate)

        captured = capsys.readouterr()
        assert "BLOCKER + CRITICAL issues" in captured.out
        assert "(none)" in captured.out

    def test_prints_critical_details_and_directory_summary(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Prints details for critical issues and includes directory aggregation."""
        issues: list[dict[str, Any]] = [
            {
                "severity": "CRITICAL",
                "type": "VULNERABILITY",
                "rule": "python:S999",
                "component": f"{sonar_report.PROJECT_KEY}:src/app.py",
                "line": 7,
                "message": "A" * 200,
            },
            {
                "severity": "MINOR",
                "type": "CODE_SMELL",
                "rule": "python:S111",
                "component": f"{sonar_report.PROJECT_KEY}:src/lib/util.py",
                "line": 3,
                "message": "Minor",
            },
        ]
        metrics = {
            "ncloc": "123",
            "bugs": "0",
            "vulnerabilities": "1",
            "code_smells": "2",
            "security_hotspots": "0",
            "coverage": "100",
            "duplicated_lines_density": "0",
            "sqale_index": "61",
            "reliability_rating": "1.0",
            "security_rating": "2.0",
            "sqale_rating": "3.0",
        }
        quality_gate = {
            "status": "ERROR",
            "period_mode": "previous_version",
            "period_date": "2026-02-01T16:42:42+0000",
            "conditions": [
                {
                    "status": "ERROR",
                    "metric_key": "new_duplicated_lines_density",
                    "comparator": "GT",
                    "error_threshold": "3",
                    "actual_value": "5.4",
                    "period_index": 1,
                }
            ],
            "failing_conditions": [
                {
                    "status": "ERROR",
                    "metric_key": "new_duplicated_lines_density",
                    "comparator": "GT",
                    "error_threshold": "3",
                    "actual_value": "5.4",
                    "period_index": 1,
                }
            ],
        }

        with patch.object(sonar_report, "datetime", FrozenDateTime):
            sonar_report.print_report(issues, metrics, quality_gate)

        captured = capsys.readouterr()
        assert "CRITICAL" in captured.out
        assert "src/app.py:7" in captured.out
        assert "Tech debt:" in captured.out
        assert "Maintainability:" in captured.out
        assert "QUALITY GATE DETAILS" in captured.out
        assert "New duplication" in captured.out
        assert "Top 20 directories:" in captured.out
        assert "src" in captured.out


class TestSaveJson:
    """Tests for save_json."""

    def test_writes_json_with_deterministic_name(self, tmp_path: Path) -> None:
        """Writes JSON file and returns its path."""
        output_dir = tmp_path / "out"
        issues: list[dict[str, Any]] = [{"key": "i1"}]
        metrics: dict[str, str] = {"bugs": "0"}
        quality_gate = _quality_gate()

        with patch.object(sonar_report, "datetime", FrozenDateTime):
            path = sonar_report.save_json(issues, metrics, quality_gate, output_dir)

        assert path.exists()
        assert path.name == "sonar_report_20260201_1721.json"
        payload = path.read_text()
        assert '"project"' in payload
        assert '"issues"' in payload
        assert '"quality_gate"' in payload


class TestMain:
    """Tests for main."""

    def test_main_orchestrates_and_saves_report(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Calls fetchers, prints report, and saves JSON."""
        json_path = tmp_path / "x.json"
        json_path.write_text("{}")

        with (
            patch("scripts.sonar_report.get_token", return_value="t") as mock_token,
            patch(
                "scripts.sonar_report.fetch_metrics",
                return_value={"ncloc": "0", "sqale_index": "0"},
            ) as mock_metrics,
            patch(
                "scripts.sonar_report.fetch_quality_gate",
                return_value=_quality_gate(),
            ) as mock_quality_gate,
            patch("scripts.sonar_report.fetch_all_issues", return_value=[]) as mock_issues,
            patch("scripts.sonar_report.fetch_source_cache", return_value={}) as mock_source_cache,
            patch("scripts.sonar_report.print_report") as mock_print,
            patch("scripts.sonar_report.save_json", return_value=json_path) as mock_save,
        ):
            sonar_report.main()

        assert mock_token.call_count == 1
        assert mock_metrics.call_count == 1
        assert mock_quality_gate.call_count == 1
        assert mock_issues.call_count == 1
        assert mock_source_cache.call_count == 1
        assert mock_print.call_count == 1
        assert mock_save.call_count == 1

        captured = capsys.readouterr()
        assert "Raw JSON saved to" in captured.out
