"""Fetch and summarize SonarCloud issues for the snapper project."""

import html
import json
import os
import re
import sys
from collections import Counter
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any
from typing import TypedDict

import httpx

BASE_URL = "https://sonarcloud.io/api"
PROJECT_KEY = "mateusz-klatt_snapper"
PAGE_SIZE = 500
SOURCE_CONTEXT_RADIUS = 2


class SourceLine(TypedDict):
    """Normalized source line payload from SonarCloud."""

    line: int
    code: str
    duplicated: bool
    is_new: bool


class QualityGateCondition(TypedDict):
    """Single quality gate condition entry."""

    status: str
    metric_key: str
    comparator: str | None
    error_threshold: str | None
    actual_value: str | None
    period_index: int | None


class QualityGateReport(TypedDict):
    """Normalized quality gate response summary."""

    status: str
    period_mode: str | None
    period_date: str | None
    conditions: list[QualityGateCondition]
    failing_conditions: list[QualityGateCondition]


class DuplicationComponent(TypedDict):
    """File-level duplication metrics from SonarCloud."""

    path: str
    duplicated_lines_density: str
    duplicated_lines: str
    new_duplicated_lines_density: str
    new_duplicated_lines: str


def _component_path(component: str) -> str:
    """Return project-relative component path."""
    return component.replace(f"{PROJECT_KEY}:", "")


def _measure_value(measure: dict[str, Any]) -> str:
    """Extract direct or period-based metric value as string."""
    direct_value = measure.get("value")
    if direct_value is not None:
        return str(direct_value)
    periods = measure.get("periods", [])
    if periods:
        first_period = periods[0]
        if isinstance(first_period, dict) and first_period.get("value") is not None:
            return str(first_period["value"])
    return "?"


def _measure_map(measures: list[dict[str, Any]]) -> dict[str, str]:
    """Normalize SonarCloud measure payloads into a metric mapping."""
    return {
        str(measure["metric"]): _measure_value(measure)
        for measure in measures
        if measure.get("metric") is not None
    }


def _optional_str(value: object) -> str | None:
    """Return stringified value or None when missing."""
    if value is None:
        return None
    return str(value)


def _optional_int(value: object) -> int | None:
    """Return integer value or None when missing."""
    if value is None:
        return None
    return int(str(value))


def _strip_code_markup(code: str) -> str:
    """Convert SonarCloud HTML-marked source code to plain text."""
    return re.sub(r"<[^>]+>", "", html.unescape(code))


def _quality_metric_label(metric_key: str) -> str:
    """Return human-readable label for a quality gate metric key."""
    labels = {
        "new_reliability_rating": "New reliability",
        "new_security_rating": "New security",
        "new_maintainability_rating": "New maintainability",
        "new_coverage": "New coverage",
        "new_duplicated_lines_density": "New duplication",
        "new_security_hotspots_reviewed": "New hotspots reviewed",
    }
    return labels.get(metric_key, metric_key)


def _format_metric_value(metric_key: str, value: str | None) -> str:
    """Format a Sonar metric value for console output."""
    if value in (None, ""):
        return "?"
    text_value = str(value)
    if metric_key.endswith("_rating"):
        return rating_label(text_value)
    percent_metrics = {
        "coverage",
        "new_coverage",
        "duplicated_lines_density",
        "new_duplicated_lines_density",
        "new_security_hotspots_reviewed",
    }
    if metric_key in percent_metrics:
        return f"{float(text_value):.1f}%"
    return text_value


def _extract_snippet(
    source_cache: dict[str, list[SourceLine]],
    component: str,
    start_line: int,
    end_line: int,
) -> list[SourceLine]:
    """Return source snippet around the issue location."""
    source_lines = source_cache.get(component, [])
    if not source_lines:
        return []
    line_map = {entry["line"]: entry for entry in source_lines}
    snippet: list[SourceLine] = []
    for line_number in range(
        max(1, start_line - SOURCE_CONTEXT_RADIUS), end_line + SOURCE_CONTEXT_RADIUS + 1
    ):
        source_line = line_map.get(line_number)
        if source_line is not None:
            snippet.append(source_line)
    return snippet


def _enrich_issue(
    issue: dict[str, Any],
    source_cache: dict[str, list[SourceLine]],
) -> dict[str, Any]:
    """Attach normalized path and source snippets to a Sonar issue."""
    component = str(issue.get("component", ""))
    text_range = issue.get("textRange") or {}
    start_line = int(text_range.get("startLine") or issue.get("line") or 1)
    end_line = int(text_range.get("endLine") or start_line)
    issue["component_path"] = _component_path(component)
    issue["primary_snippet"] = _extract_snippet(source_cache, component, start_line, end_line)
    flow_details: list[dict[str, Any]] = []
    for flow in issue.get("flows", []):
        for location in flow.get("locations", []):
            flow_component = str(location.get("component", component))
            flow_text_range = location.get("textRange") or {}
            flow_start = int(flow_text_range.get("startLine") or 1)
            flow_end = int(flow_text_range.get("endLine") or flow_start)
            flow_details.append(
                {
                    "component": flow_component,
                    "component_path": _component_path(flow_component),
                    "start_line": flow_start,
                    "end_line": flow_end,
                    "start_offset": flow_text_range.get("startOffset"),
                    "end_offset": flow_text_range.get("endOffset"),
                    "message": location.get("msg"),
                    "snippet": _extract_snippet(source_cache, flow_component, flow_start, flow_end),
                }
            )
    issue["flow_details"] = flow_details
    return issue


def _print_issue_snippet(
    snippet: list[SourceLine],
    start_line: int,
    end_line: int,
    indent: str,
) -> None:
    """Render source snippet lines in console output."""
    for source_line in snippet:
        marker = ">" if start_line <= source_line["line"] <= end_line else " "
        flags: list[str] = []
        if source_line["duplicated"]:
            flags.append("dup")
        if source_line["is_new"]:
            flags.append("new")
        suffix = f" [{' '.join(flags)}]" if flags else ""
        print(f"{indent}{marker} {source_line['line']:>5} | {source_line['code']}{suffix}")


def get_token() -> str:
    """Return SonarCloud token from environment or exit.

    The token must be available under the SONAR_TOKEN environment variable.

    Returns:
        The SonarCloud access token.

    Raises:
        SystemExit: If SONAR_TOKEN is missing.
    """
    token = os.environ.get("SONAR_TOKEN", "")
    if not token:
        print("ERROR: SONAR_TOKEN not set. Export it or add to ~/.bashrc")
        sys.exit(1)
    return token


def fetch_all_issues(token: str) -> list[dict[str, Any]]:
    """Fetch all open issues with pagination.

    Args:
        token: SonarCloud access token.

    Returns:
        A list of issue objects as returned by the SonarCloud API.
    """
    all_issues: list[dict[str, Any]] = []
    page = 1
    while True:
        resp = httpx.get(
            f"{BASE_URL}/issues/search",
            params={
                "componentKeys": PROJECT_KEY,
                "ps": PAGE_SIZE,
                "p": page,
                "statuses": "OPEN,CONFIRMED,REOPENED",
            },
            auth=(token, ""),
            timeout=30,
        )
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        issues = data.get("issues", [])
        all_issues.extend(issues)
        total = int(data.get("total", 0))
        print(f"  page {page}: fetched {len(issues)} issues ({len(all_issues)}/{total})")
        if len(all_issues) >= total or not issues:
            break
        page += 1
    return all_issues


def fetch_metrics(token: str) -> dict[str, str]:
    """Fetch project-level metrics.

    Args:
        token: SonarCloud access token.

    Returns:
        Mapping of metric key to string value.
    """
    resp = httpx.get(
        f"{BASE_URL}/measures/component",
        params={
            "component": PROJECT_KEY,
            "metricKeys": ",".join(
                [
                    "bugs",
                    "vulnerabilities",
                    "code_smells",
                    "coverage",
                    "new_coverage",
                    "duplicated_lines_density",
                    "new_duplicated_lines_density",
                    "ncloc",
                    "security_hotspots",
                    "reliability_rating",
                    "security_rating",
                    "sqale_rating",
                    "sqale_index",
                    "alert_status",
                ]
            ),
        },
        auth=(token, ""),
        timeout=30,
    )
    resp.raise_for_status()
    payload: dict[str, Any] = resp.json()
    component: dict[str, Any] = payload.get("component", {})
    measures: list[dict[str, Any]] = component.get("measures", [])
    return _measure_map(measures)


def fetch_duplication_components(token: str) -> list[DuplicationComponent]:
    """Fetch file-level overall and new-code duplication metrics.

    Args:
        token: SonarCloud access token.

    Returns:
        File-level duplication metrics for every analyzed file.
    """
    all_components: list[DuplicationComponent] = []
    page = 1
    while True:
        resp = httpx.get(
            f"{BASE_URL}/measures/component_tree",
            params={
                "component": PROJECT_KEY,
                "metricKeys": ",".join(
                    [
                        "duplicated_lines_density",
                        "duplicated_lines",
                        "new_duplicated_lines_density",
                        "new_duplicated_lines",
                    ]
                ),
                "qualifiers": "FIL",
                "ps": PAGE_SIZE,
                "p": page,
            },
            auth=(token, ""),
            timeout=30,
        )
        resp.raise_for_status()
        payload: dict[str, Any] = resp.json()
        components: list[dict[str, Any]] = payload.get("components", [])
        for component in components:
            measure_map = _measure_map(component.get("measures", []))
            path = str(component.get("path") or _component_path(str(component.get("key", ""))))
            all_components.append(
                {
                    "path": path,
                    "duplicated_lines_density": measure_map.get("duplicated_lines_density", "0"),
                    "duplicated_lines": measure_map.get("duplicated_lines", "0"),
                    "new_duplicated_lines_density": measure_map.get(
                        "new_duplicated_lines_density", "0"
                    ),
                    "new_duplicated_lines": measure_map.get("new_duplicated_lines", "0"),
                }
            )
        paging: dict[str, Any] = payload.get("paging", {})
        total = int(paging.get("total", 0))
        print(f"  page {page}: fetched {len(components)} files ({len(all_components)}/{total})")
        if len(all_components) >= total or not components:
            break
        page += 1
    return all_components


def fetch_quality_gate(token: str) -> QualityGateReport:
    """Fetch SonarCloud quality gate status and conditions.

    Args:
        token: SonarCloud access token.

    Returns:
        Normalized quality gate status, period, and condition details.
    """
    resp = httpx.get(
        f"{BASE_URL}/qualitygates/project_status",
        params={"projectKey": PROJECT_KEY},
        auth=(token, ""),
        timeout=30,
    )
    resp.raise_for_status()
    payload: dict[str, Any] = resp.json()
    project_status: dict[str, Any] = payload.get("projectStatus", {})
    periods: list[dict[str, Any]] = project_status.get("periods", [])
    first_period = periods[0] if periods else {}
    conditions: list[QualityGateCondition] = []
    for condition in project_status.get("conditions", []):
        normalized_condition: QualityGateCondition = {
            "status": str(condition.get("status", "?")),
            "metric_key": str(condition.get("metricKey", "?")),
            "comparator": _optional_str(condition.get("comparator")),
            "error_threshold": _optional_str(condition.get("errorThreshold")),
            "actual_value": _optional_str(condition.get("actualValue")),
            "period_index": _optional_int(condition.get("periodIndex")),
        }
        conditions.append(normalized_condition)
    return {
        "status": str(project_status.get("status", "?")),
        "period_mode": _optional_str(first_period.get("mode")),
        "period_date": _optional_str(first_period.get("date")),
        "conditions": conditions,
        "failing_conditions": [
            condition for condition in conditions if condition["status"] != "OK"
        ],
    }


def _issue_components(issues: list[dict[str, Any]]) -> set[str]:
    """Collect non-empty primary and flow component keys from issues."""
    components: set[str] = set()
    for issue in issues:
        component = issue.get("component")
        if component:
            components.add(str(component))
        for flow in issue.get("flows", []):
            components.update(
                str(flow_component)
                for location in flow.get("locations", [])
                if (flow_component := location.get("component"))
            )
    return components


def _fetch_source_lines(token: str, component: str) -> tuple[list[SourceLine] | None, int]:
    """Fetch and normalize one component, returning its HTTP status."""
    response = httpx.get(
        f"{BASE_URL}/sources/lines",
        params={"key": component},
        auth=(token, ""),
        timeout=30,
    )
    if response.status_code >= 500:
        return None, response.status_code
    response.raise_for_status()
    payload: dict[str, Any] = response.json()
    return (
        [
            {
                "line": int(source_line.get("line", 0)),
                "code": _strip_code_markup(str(source_line.get("code", ""))),
                "duplicated": bool(source_line.get("duplicated", False)),
                "is_new": bool(source_line.get("isNew", False)),
            }
            for source_line in payload.get("sources", [])
        ],
        response.status_code,
    )


def fetch_source_cache(token: str, issues: list[dict[str, Any]]) -> dict[str, list[SourceLine]]:
    """Fetch source lines for every file referenced by current issues.

    Args:
        token: SonarCloud access token.
        issues: Issue payloads returned by SonarCloud.

    Returns:
        Mapping of component key to normalized source lines.
    """
    components = _issue_components(issues)
    source_cache: dict[str, list[SourceLine]] = {}
    total = len(components)
    for index, component in enumerate(sorted(components), start=1):
        lines, status_code = _fetch_source_lines(token, component)
        if lines is None:
            print(
                f"  source {index}/{total}: {_component_path(component)}"
                f" SKIPPED (SonarCloud {status_code} - issue context unavailable)"
            )
            continue
        source_cache[component] = lines
        print(f"  source {index}/{total}: {_component_path(component)}")
    return source_cache


def rating_label(value: str) -> str:
    """Convert SonarCloud rating number to a letter grade.

    Args:
        value: SonarCloud rating numeric string (for example, "1.0").

    Returns:
        Letter grade A-E when recognized, otherwise the original value.
    """
    mapping = {"1.0": "A", "2.0": "B", "3.0": "C", "4.0": "D", "5.0": "E"}
    return mapping.get(value, value)


def _metric_as_float(value: str) -> float:
    """Parse a metric value as float, defaulting missing values to zero."""
    if value in ("", "?"):
        return 0.0
    return float(value)


def _metric_as_int(value: str) -> int:
    """Parse a metric value as integer, defaulting missing values to zero."""
    if value in ("", "?"):
        return 0
    return int(float(value))


def _print_report_header() -> None:
    """Print the report header."""
    print("\n" + "=" * 70)
    print(f"  SONARCLOUD REPORT - {PROJECT_KEY}")
    print(f"  Generated: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 70)


def _print_project_metrics(metrics: dict[str, str], quality_gate: QualityGateReport) -> None:
    """Print project-wide SonarCloud metrics."""
    print("\n--- PROJECT METRICS ---")
    print(f"  Lines of code:       {int(metrics.get('ncloc', '0')):,}")
    print(f"  Bugs:                {metrics.get('bugs', '?')}")
    print(f"  Vulnerabilities:     {metrics.get('vulnerabilities', '?')}")
    print(f"  Code smells:         {metrics.get('code_smells', '?')}")
    print(f"  Security hotspots:   {metrics.get('security_hotspots', '?')}")
    print(f"  Coverage:            {metrics.get('coverage', '?')}%")
    print(f"  Duplication:         {metrics.get('duplicated_lines_density', '?')}%")
    print(
        f"  New coverage:        {_format_metric_value('new_coverage', metrics.get('new_coverage'))}"
    )
    print(
        f"  New duplication:     {_format_metric_value('new_duplicated_lines_density', metrics.get('new_duplicated_lines_density'))}"
    )
    tech_debt = metrics.get("sqale_index", "0")
    debt_minutes = int(tech_debt)
    print(f"  Tech debt:           {debt_minutes // 60}h {debt_minutes % 60}m")
    print(f"  Reliability rating:  {rating_label(metrics.get('reliability_rating', '?'))}")
    print(f"  Security rating:     {rating_label(metrics.get('security_rating', '?'))}")
    print(f"  Maintainability:     {rating_label(metrics.get('sqale_rating', '?'))}")
    print(f"  Quality gate:        {quality_gate['status']}")


def _print_quality_gate_details(quality_gate: QualityGateReport) -> None:
    """Print quality gate status and failing conditions."""
    print("\n--- QUALITY GATE DETAILS ---")
    print(f"  Period mode:         {quality_gate['period_mode'] or '?'}")
    print(f"  Period date:         {quality_gate['period_date'] or '?'}")
    failing_conditions = quality_gate["failing_conditions"]
    if not failing_conditions:
        print("  Failing conditions:  (none)")
        return
    print("  Failing conditions:")
    for condition in failing_conditions:
        actual = _format_metric_value(condition["metric_key"], condition["actual_value"])
        threshold = _format_metric_value(condition["metric_key"], condition["error_threshold"])
        print(
            f"    {_quality_metric_label(condition['metric_key']):<22} {actual} {condition['comparator'] or '?'} {threshold}"
        )


def _issue_directory(issue: dict[str, Any]) -> str:
    """Return the containing directory for an issue component."""
    component = str(issue.get("component", ""))
    parts = component.replace(f"{PROJECT_KEY}:", "").split("/")
    return "/".join(parts[:-1]) if len(parts) > 1 else "(root)"


def _issue_counters(
    issues: list[dict[str, Any]],
) -> tuple[Counter[str], Counter[str], Counter[str], Counter[str]]:
    """Aggregate issue counts by severity, type, rule, and directory."""
    severity_counts: Counter[str] = Counter()
    type_counts: Counter[str] = Counter()
    rule_counts: Counter[str] = Counter()
    dir_counts: Counter[str] = Counter()
    for issue in issues:
        severity_counts[str(issue.get("severity", "UNKNOWN"))] += 1
        type_counts[str(issue.get("type", "UNKNOWN"))] += 1
        rule_counts[str(issue.get("rule", "UNKNOWN"))] += 1
        dir_counts[_issue_directory(issue)] += 1
    return severity_counts, type_counts, rule_counts, dir_counts


def _print_issue_summary(issues: list[dict[str, Any]]) -> None:
    """Print aggregate issue counts."""
    print(f"\n--- ISSUES SUMMARY ({len(issues)} total) ---")
    severity_counts, type_counts, rule_counts, dir_counts = _issue_counters(issues)

    print("\n  By severity:")
    for sev in ["BLOCKER", "CRITICAL", "MAJOR", "MINOR", "INFO"]:
        count = severity_counts.get(sev, 0)
        if count:
            bar = "#" * min(count, 50)
            print(f"    {sev:<10} {count:>5}  {bar}")

    print("\n  By type:")
    for issue_type in ["BUG", "VULNERABILITY", "CODE_SMELL"]:
        count = type_counts.get(issue_type, 0)
        if count:
            print(f"    {issue_type:<16} {count:>5}")

    print("\n  Top 20 rules:")
    for rule, count in rule_counts.most_common(20):
        print(f"    {rule:<45} {count:>5}")

    print("\n  Top 20 directories:")
    for directory, count in dir_counts.most_common(20):
        print(f"    {directory:<55} {count:>5}")


def _duplication_values(component: DuplicationComponent, new_code: bool) -> tuple[str, str]:
    """Return density and line-count metrics for overall or new-code duplication."""
    if new_code:
        return (
            component["new_duplicated_lines_density"],
            component["new_duplicated_lines"],
        )
    return component["duplicated_lines_density"], component["duplicated_lines"]


def _top_duplication_components(
    components: list[DuplicationComponent],
    new_code: bool,
) -> list[DuplicationComponent]:
    """Return files with duplicated lines sorted by density and absolute count."""
    return sorted(
        [
            component
            for component in components
            if _metric_as_int(_duplication_values(component, new_code)[1]) > 0
        ],
        key=lambda component: (
            _metric_as_float(_duplication_values(component, new_code)[0]),
            _metric_as_int(_duplication_values(component, new_code)[1]),
            component["path"],
        ),
        reverse=True,
    )


def _print_duplication_table(
    title: str,
    components: list[DuplicationComponent],
    new_code: bool,
) -> None:
    """Print a ranked file-level duplication table."""
    print(f"\n  {title}:")
    top_components = _top_duplication_components(components, new_code)
    if not top_components:
        print("    (none)")
        return
    print("    Density   Lines  File")
    for component in top_components[:20]:
        density_metric = "new_duplicated_lines_density" if new_code else "duplicated_lines_density"
        density_value, lines_value = _duplication_values(component, new_code)
        density = _format_metric_value(density_metric, density_value)
        lines = _metric_as_int(lines_value)
        print(f"    {density:>7}  {lines:>6}  {component['path']}")


def _print_duplication_summary(components: list[DuplicationComponent]) -> None:
    """Print top duplicated files for overall and new code metrics."""
    print("\n--- DUPLICATION HOTSPOTS ---")
    _print_duplication_table(
        "Top 20 files by duplicated lines",
        components,
        False,
    )
    _print_duplication_table(
        "Top 20 files by new duplicated lines",
        components,
        True,
    )


def _print_critical_issues(issues: list[dict[str, Any]]) -> None:
    """Print details for blocker and critical issues."""
    print("\n  BLOCKER + CRITICAL issues (details):")
    critical = [issue for issue in issues if issue.get("severity") in ("BLOCKER", "CRITICAL")]
    if not critical:
        print("    (none)")
        return
    for issue in critical[:50]:
        component = issue.get(
            "component_path", issue.get("component", "").replace(f"{PROJECT_KEY}:", "")
        )
        line = issue.get("line", "?")
        print(f"    [{issue.get('severity')}] {component}:{line}")
        print(f"      {issue.get('message', '')[:120]}")
        print(f"      Rule: {issue.get('rule', '')}")
        text_range = issue.get("textRange") or {}
        start_line = int(text_range.get("startLine") or issue.get("line") or 1)
        end_line = int(text_range.get("endLine") or start_line)
        primary_snippet = issue.get("primary_snippet", [])
        if primary_snippet:
            _print_issue_snippet(primary_snippet, start_line, end_line, "      ")
        flow_details = issue.get("flow_details", [])
        if flow_details:
            print("      Secondary locations:")
            for flow_detail in flow_details[:8]:
                print(
                    f"        {flow_detail['component_path']}:{flow_detail['start_line']} {flow_detail.get('message') or ''}".rstrip()
                )
        print()


def print_report(
    issues: list[dict[str, Any]],
    metrics: dict[str, str],
    quality_gate: QualityGateReport,
    duplication_components: list[DuplicationComponent] | None = None,
) -> None:
    """Print a summary report to stdout.

    Args:
        issues: Issues returned by SonarCloud issues search API.
        metrics: Project metrics mapping.
        quality_gate: Quality gate status and condition summary.
        duplication_components: File-level duplication metrics.
    """
    report_duplication_components = duplication_components or []
    _print_report_header()
    _print_project_metrics(metrics, quality_gate)
    _print_quality_gate_details(quality_gate)
    _print_duplication_summary(report_duplication_components)
    _print_issue_summary(issues)
    _print_critical_issues(issues)
    print("=" * 70)


def save_json(
    issues: list[dict[str, Any]],
    metrics: dict[str, str],
    quality_gate: QualityGateReport,
    output_dir: Path,
    duplication_components: list[DuplicationComponent] | None = None,
) -> Path:
    """Save raw data to JSON for further analysis.

    Args:
        issues: Issues returned by SonarCloud issues search API.
        metrics: Project metrics mapping.
        quality_gate: Quality gate status and condition summary.
        duplication_components: File-level duplication metrics.
        output_dir: Directory where the report JSON should be written.

    Returns:
        Path to the written JSON report.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M")
    path = output_dir / f"sonar_report_{stamp}.json"
    report_duplication_components = duplication_components or []
    data = {
        "generated": datetime.now(UTC).isoformat(),
        "project": PROJECT_KEY,
        "metrics": metrics,
        "quality_gate": quality_gate,
        "duplication_components": report_duplication_components,
        "total_issues": len(issues),
        "issues": issues,
    }
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False))
    return path


def main() -> int:
    """Run the SonarCloud report workflow and write output JSON to reports/.

    Returns:
        Exit code (always 0 on success).
    """
    token = get_token()

    print("Fetching project metrics...")
    metrics = fetch_metrics(token)

    print("Fetching quality gate status...")
    quality_gate = fetch_quality_gate(token)

    print("Fetching file-level duplication metrics...")
    duplication_components = fetch_duplication_components(token)

    print("Fetching all issues (this may take a moment)...")
    issues = fetch_all_issues(token)

    print("Fetching source lines for issue details...")
    source_cache = fetch_source_cache(token, issues)
    detailed_issues = [_enrich_issue(issue, source_cache) for issue in issues]

    print_report(detailed_issues, metrics, quality_gate, duplication_components)

    output_dir = Path(__file__).resolve().parent.parent / "reports"
    json_path = save_json(
        detailed_issues,
        metrics,
        quality_gate,
        output_dir,
        duplication_components,
    )
    print(f"\nRaw JSON saved to: {json_path}")
    print(f"({len(detailed_issues)} issues, {json_path.stat().st_size / 1024:.0f} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
