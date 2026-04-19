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


class TestHelperFunctions:
    """Tests for Sonar report helper functions."""

    def test_value_and_label_helpers_cover_fallback_paths(self) -> None:
        """Helpers normalize missing values, markup, labels, and metric formatting."""
        assert (
            sonar_report._component_path(f"{sonar_report.PROJECT_KEY}:src/app.py") == "src/app.py"
        )
        assert sonar_report._measure_value({"value": "7"}) == "7"
        assert sonar_report._measure_value({"periods": [{"value": "8.5"}]}) == "8.5"
        assert sonar_report._measure_value({"periods": [{"other": "x"}]}) == "?"
        assert sonar_report._measure_value({}) == "?"
        assert sonar_report._optional_str(None) is None
        assert sonar_report._optional_str(12) == "12"
        assert sonar_report._optional_int(None) is None
        assert sonar_report._optional_int("14") == 14
        assert sonar_report._strip_code_markup("<span>alpha &gt; beta</span>") == "alpha > beta"
        assert (
            sonar_report._quality_metric_label("new_duplicated_lines_density") == "New duplication"
        )
        assert sonar_report._quality_metric_label("custom_metric") == "custom_metric"
        assert sonar_report._format_metric_value("new_security_rating", "1.0") == "A"
        assert sonar_report._format_metric_value("new_coverage", "95.73") == "95.7%"
        assert sonar_report._format_metric_value("custom_metric", "42") == "42"
        assert sonar_report._format_metric_value("custom_metric", None) == "?"

    def test_extract_enrich_and_print_issue_snippet(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Snippet helpers annotate issues and render marked source context."""
        component = f"{sonar_report.PROJECT_KEY}:src/app.py"
        flow_component = f"{sonar_report.PROJECT_KEY}:src/lib.py"
        source_cache: dict[str, list[sonar_report.SourceLine]] = {
            component: [
                {"line": 8, "code": "before", "duplicated": False, "is_new": False},
                {"line": 9, "code": "target", "duplicated": True, "is_new": True},
                {"line": 10, "code": "after", "duplicated": False, "is_new": True},
            ],
            flow_component: [
                {"line": 3, "code": "helper", "duplicated": False, "is_new": False},
            ],
        }
        snippet = sonar_report._extract_snippet(source_cache, component, 9, 9)
        assert [line["line"] for line in snippet] == [8, 9, 10]
        assert sonar_report._extract_snippet(source_cache, "missing", 1, 1) == []

        issue: dict[str, Any] = {
            "component": component,
            "line": 9,
            "textRange": {"startLine": 9, "endLine": 9, "startOffset": 0, "endOffset": 6},
            "flows": [
                {
                    "locations": [
                        {
                            "component": flow_component,
                            "textRange": {
                                "startLine": 3,
                                "endLine": 3,
                                "startOffset": 1,
                                "endOffset": 5,
                            },
                            "msg": "+1",
                        }
                    ]
                }
            ],
        }
        enriched = sonar_report._enrich_issue(issue, source_cache)
        assert enriched["component_path"] == "src/app.py"
        assert len(enriched["primary_snippet"]) == 3
        assert enriched["flow_details"][0]["component_path"] == "src/lib.py"
        assert enriched["flow_details"][0]["snippet"][0]["code"] == "helper"

        sonar_report._print_issue_snippet(snippet, 9, 9, "  ")
        captured = capsys.readouterr()
        assert ">     9 | target [dup new]" in captured.out


class TestFetchQualityGate:
    """Tests for fetch_quality_gate."""

    def test_fetches_and_normalizes_quality_gate(self) -> None:
        """Builds quality gate data including failing conditions."""
        response = MagicMock()
        response.raise_for_status = MagicMock()
        response.json = MagicMock(
            return_value={
                "projectStatus": {
                    "status": "ERROR",
                    "periods": [{"mode": "previous_version", "date": "2026-02-01T16:42:42+0000"}],
                    "conditions": [
                        {
                            "status": "OK",
                            "metricKey": "new_coverage",
                            "comparator": "LT",
                            "errorThreshold": "80",
                            "actualValue": "95.7",
                            "periodIndex": 1,
                        },
                        {
                            "status": "ERROR",
                            "metricKey": "new_duplicated_lines_density",
                            "comparator": "GT",
                            "errorThreshold": "3",
                            "actualValue": "5.4",
                            "periodIndex": 1,
                        },
                    ],
                }
            }
        )

        with patch("scripts.sonar_report.httpx.get", return_value=response) as mock_get:
            result = sonar_report.fetch_quality_gate("t")

        assert result["status"] == "ERROR"
        assert result["period_mode"] == "previous_version"
        assert result["period_date"] == "2026-02-01T16:42:42+0000"
        assert len(result["conditions"]) == 2
        assert len(result["failing_conditions"]) == 1
        assert result["failing_conditions"][0]["metric_key"] == "new_duplicated_lines_density"
        assert "qualitygates/project_status" in mock_get.call_args.args[0]


class TestFetchSourceCache:
    """Tests for fetch_source_cache."""

    def test_fetches_source_lines_for_issue_and_flow_components(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Loads and normalizes source lines for primary and secondary components."""
        response_main = MagicMock()
        response_main.raise_for_status = MagicMock()
        response_main.json = MagicMock(
            return_value={
                "sources": [
                    {
                        "line": 7,
                        "code": "<span>alpha</span>",
                        "duplicated": True,
                        "isNew": False,
                    }
                ]
            }
        )
        response_flow = MagicMock()
        response_flow.raise_for_status = MagicMock()
        response_flow.json = MagicMock(
            return_value={
                "sources": [
                    {
                        "line": 3,
                        "code": "<span>beta &amp; gamma</span>",
                        "duplicated": False,
                        "isNew": True,
                    }
                ]
            }
        )
        issues: list[dict[str, Any]] = [
            {
                "component": f"{sonar_report.PROJECT_KEY}:src/app.py",
                "flows": [{"locations": [{"component": f"{sonar_report.PROJECT_KEY}:src/lib.py"}]}],
            }
        ]

        with patch(
            "scripts.sonar_report.httpx.get", side_effect=[response_main, response_flow]
        ) as mock_get:
            cache = sonar_report.fetch_source_cache("t", issues)

        assert sorted(cache) == [
            f"{sonar_report.PROJECT_KEY}:src/app.py",
            f"{sonar_report.PROJECT_KEY}:src/lib.py",
        ]
        assert cache[f"{sonar_report.PROJECT_KEY}:src/app.py"][0]["code"] == "alpha"
        assert cache[f"{sonar_report.PROJECT_KEY}:src/lib.py"][0]["code"] == "beta & gamma"
        assert mock_get.call_count == 2
        captured = capsys.readouterr()
        assert "source 1/2" in captured.out
        assert "src/app.py" in captured.out

    def test_ignores_missing_components_when_building_source_cache(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Skips empty primary and secondary component entries when collecting sources."""
        response = MagicMock()
        response.raise_for_status = MagicMock()
        response.json = MagicMock(return_value={"sources": []})
        issues: list[dict[str, Any]] = [
            {
                "component": "",
                "flows": [{"locations": [{"component": ""}, {}]}],
            },
            {
                "component": f"{sonar_report.PROJECT_KEY}:src/only.py",
                "flows": [],
            },
        ]

        with patch("scripts.sonar_report.httpx.get", return_value=response) as mock_get:
            cache = sonar_report.fetch_source_cache("t", issues)

        assert sorted(cache) == [f"{sonar_report.PROJECT_KEY}:src/only.py"]
        assert mock_get.call_count == 1
        captured = capsys.readouterr()
        assert "source 1/1" in captured.out


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


class TestFetchDuplicationComponents:
    """Tests for fetch_duplication_components."""

    def test_fetches_file_level_duplication_metrics_across_pages(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Loads file metrics from component_tree and normalizes defaults."""
        response_1 = MagicMock()
        response_1.raise_for_status = MagicMock()
        response_1.json = MagicMock(
            return_value={
                "components": [
                    {
                        "path": "src/app.py",
                        "measures": [
                            {"metric": "duplicated_lines_density", "value": "12.5"},
                            {"metric": "duplicated_lines", "value": "10"},
                            {
                                "metric": "new_duplicated_lines_density",
                                "periods": [{"value": "6.5"}],
                            },
                            {"metric": "new_duplicated_lines", "value": "4"},
                        ],
                    }
                ],
                "paging": {"total": 2},
            }
        )
        response_2 = MagicMock()
        response_2.raise_for_status = MagicMock()
        response_2.json = MagicMock(
            return_value={
                "components": [
                    {
                        "key": f"{sonar_report.PROJECT_KEY}:src/fallback.py",
                        "measures": [
                            {"metric": "duplicated_lines", "value": "3"},
                        ],
                    }
                ],
                "paging": {"total": 2},
            }
        )

        with patch(
            "scripts.sonar_report.httpx.get", side_effect=[response_1, response_2]
        ) as mock_get:
            result = sonar_report.fetch_duplication_components("t")

        assert result == [
            {
                "path": "src/app.py",
                "duplicated_lines_density": "12.5",
                "duplicated_lines": "10",
                "new_duplicated_lines_density": "6.5",
                "new_duplicated_lines": "4",
            },
            {
                "path": "src/fallback.py",
                "duplicated_lines_density": "0",
                "duplicated_lines": "3",
                "new_duplicated_lines_density": "0",
                "new_duplicated_lines": "0",
            },
        ]
        assert mock_get.call_args_list[0].kwargs["params"]["p"] == 1
        assert mock_get.call_args_list[1].kwargs["params"]["p"] == 2
        captured = capsys.readouterr()
        assert "page 1" in captured.out
        assert "page 2" in captured.out

    def test_stops_when_duplication_component_page_is_empty(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Stops after the first empty component_tree page."""
        response = MagicMock()
        response.raise_for_status = MagicMock()
        response.json = MagicMock(return_value={"components": [], "paging": {"total": 10}})

        with patch("scripts.sonar_report.httpx.get", return_value=response) as mock_get:
            result = sonar_report.fetch_duplication_components("t")

        assert result == []
        assert mock_get.call_count == 1
        captured = capsys.readouterr()
        assert "page 1" in captured.out


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

    def test_prints_primary_snippet_and_secondary_locations(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Prints source snippets and secondary locations for detailed issues."""
        issues: list[dict[str, Any]] = [
            {
                "severity": "CRITICAL",
                "type": "CODE_SMELL",
                "rule": "python:S3776",
                "component": f"{sonar_report.PROJECT_KEY}:src/app.py",
                "component_path": "src/app.py",
                "line": 10,
                "message": "Complex function",
                "textRange": {"startLine": 10, "endLine": 10},
                "primary_snippet": [
                    {"line": 9, "code": "before", "duplicated": False, "is_new": False},
                    {"line": 10, "code": "focus", "duplicated": False, "is_new": True},
                ],
                "flow_details": [
                    {"component_path": "src/helper.py", "start_line": 22, "message": "+1"}
                ],
            }
        ]
        metrics = {"ncloc": "10", "sqale_index": "0"}
        quality_gate = _quality_gate()

        with patch.object(sonar_report, "datetime", FrozenDateTime):
            sonar_report.print_report(issues, metrics, quality_gate)

        captured = capsys.readouterr()
        assert ">    10 | focus [new]" in captured.out
        assert "Secondary locations:" in captured.out
        assert "src/helper.py:22 +1" in captured.out

    def test_prints_duplication_hotspots_when_components_exist(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Prints ranked duplication tables when file-level metrics are present."""
        metrics = {"ncloc": "10", "sqale_index": "0"}
        quality_gate = _quality_gate()
        duplication_components: list[sonar_report.DuplicationComponent] = [
            {
                "path": "src/alpha.py",
                "duplicated_lines_density": "2.5",
                "duplicated_lines": "3",
                "new_duplicated_lines_density": "1.5",
                "new_duplicated_lines": "2",
            },
            {
                "path": "src/zero.py",
                "duplicated_lines_density": "0",
                "duplicated_lines": "0",
                "new_duplicated_lines_density": "0",
                "new_duplicated_lines": "0",
            },
        ]

        with patch.object(sonar_report, "datetime", FrozenDateTime):
            sonar_report.print_report([], metrics, quality_gate, duplication_components)

        captured = capsys.readouterr()
        assert "DUPLICATION HOTSPOTS" in captured.out
        assert "Top 20 files by duplicated lines" in captured.out
        assert "Density   Lines  File" in captured.out
        assert "src/alpha.py" in captured.out


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

    def test_writes_duplication_components_to_json(self, tmp_path: Path) -> None:
        """Serializes file-level duplication metrics alongside the issue payload."""
        output_dir = tmp_path / "out"
        duplication_components: list[sonar_report.DuplicationComponent] = [
            {
                "path": "src/report.py",
                "duplicated_lines_density": "4.0",
                "duplicated_lines": "8",
                "new_duplicated_lines_density": "2.0",
                "new_duplicated_lines": "3",
            }
        ]

        with patch.object(sonar_report, "datetime", FrozenDateTime):
            path = sonar_report.save_json(
                [], {"bugs": "0"}, _quality_gate(), output_dir, duplication_components
            )

        payload = path.read_text()
        assert '"duplication_components"' in payload
        assert '"src/report.py"' in payload


class TestDuplicationHelpers:
    """Tests for duplication-specific helper functions."""

    def test_metric_parsers_and_duplication_value_helpers(self) -> None:
        """Parses numeric metrics and returns the correct overall/new tuples."""
        component: sonar_report.DuplicationComponent = {
            "path": "src/a.py",
            "duplicated_lines_density": "3.2",
            "duplicated_lines": "4",
            "new_duplicated_lines_density": "1.1",
            "new_duplicated_lines": "2",
        }

        assert sonar_report._metric_as_float("") == pytest.approx(0.0)
        assert sonar_report._metric_as_float("?") == pytest.approx(0.0)
        assert sonar_report._metric_as_float("1.25") == pytest.approx(1.25)
        assert sonar_report._metric_as_int("") == 0
        assert sonar_report._metric_as_int("?") == 0
        assert sonar_report._metric_as_int("7.9") == 7
        assert sonar_report._duplication_values(component, False) == ("3.2", "4")
        assert sonar_report._duplication_values(component, True) == ("1.1", "2")

    def test_top_duplication_components_filters_zero_line_entries(self) -> None:
        """Sorts by density/count and excludes files with zero duplicated lines."""
        ranked = sonar_report._top_duplication_components(
            [
                {
                    "path": "src/zero.py",
                    "duplicated_lines_density": "9.9",
                    "duplicated_lines": "0",
                    "new_duplicated_lines_density": "9.9",
                    "new_duplicated_lines": "0",
                },
                {
                    "path": "src/a.py",
                    "duplicated_lines_density": "3.2",
                    "duplicated_lines": "4",
                    "new_duplicated_lines_density": "1.1",
                    "new_duplicated_lines": "2",
                },
                {
                    "path": "src/b.py",
                    "duplicated_lines_density": "5.0",
                    "duplicated_lines": "6",
                    "new_duplicated_lines_density": "0.5",
                    "new_duplicated_lines": "1",
                },
            ],
            False,
        )

        assert [component["path"] for component in ranked] == ["src/b.py", "src/a.py"]


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
            patch(
                "scripts.sonar_report.fetch_duplication_components",
                return_value=[],
            ) as mock_duplication,
            patch("scripts.sonar_report.fetch_all_issues", return_value=[]) as mock_issues,
            patch("scripts.sonar_report.fetch_source_cache", return_value={}) as mock_source_cache,
            patch("scripts.sonar_report.print_report") as mock_print,
            patch("scripts.sonar_report.save_json", return_value=json_path) as mock_save,
        ):
            sonar_report.main()

        assert mock_token.call_count == 1
        assert mock_metrics.call_count == 1
        assert mock_quality_gate.call_count == 1
        assert mock_duplication.call_count == 1
        assert mock_issues.call_count == 1
        assert mock_source_cache.call_count == 1
        assert mock_print.call_count == 1
        assert mock_save.call_count == 1

        captured = capsys.readouterr()
        assert "Raw JSON saved to" in captured.out
