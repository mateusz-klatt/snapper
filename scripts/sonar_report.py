"""Fetch and summarize SonarCloud issues for the snapper project."""

import json
import os
import sys
from collections import Counter
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

BASE_URL = "https://sonarcloud.io/api"
PROJECT_KEY = "mateusz-klatt_snapper"
PAGE_SIZE = 500


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
                    "duplicated_lines_density",
                    "ncloc",
                    "security_hotspots",
                    "reliability_rating",
                    "security_rating",
                    "sqale_rating",
                    "sqale_index",
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
    return {str(m["metric"]): str(m["value"]) for m in measures}


def rating_label(value: str) -> str:
    """Convert SonarCloud rating number to a letter grade.

    Args:
        value: SonarCloud rating numeric string (for example, "1.0").

    Returns:
        Letter grade A-E when recognized, otherwise the original value.
    """
    mapping = {"1.0": "A", "2.0": "B", "3.0": "C", "4.0": "D", "5.0": "E"}
    return mapping.get(value, value)


def print_report(issues: list[dict[str, Any]], metrics: dict[str, str]) -> None:
    """Print a summary report to stdout.

    Args:
        issues: Issues returned by SonarCloud issues search API.
        metrics: Project metrics mapping.
    """
    print("\n" + "=" * 70)
    print(f"  SONARCLOUD REPORT - {PROJECT_KEY}")
    print(f"  Generated: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 70)

    print("\n--- PROJECT METRICS ---")
    print(f"  Lines of code:       {int(metrics.get('ncloc', '0')):,}")
    print(f"  Bugs:                {metrics.get('bugs', '?')}")
    print(f"  Vulnerabilities:     {metrics.get('vulnerabilities', '?')}")
    print(f"  Code smells:         {metrics.get('code_smells', '?')}")
    print(f"  Security hotspots:   {metrics.get('security_hotspots', '?')}")
    print(f"  Coverage:            {metrics.get('coverage', '?')}%")
    print(f"  Duplication:         {metrics.get('duplicated_lines_density', '?')}%")
    tech_debt = metrics.get("sqale_index", "0")
    debt_minutes = int(tech_debt)
    print(f"  Tech debt:           {debt_minutes // 60}h {debt_minutes % 60}m")
    print(f"  Reliability rating:  {rating_label(metrics.get('reliability_rating', '?'))}")
    print(f"  Security rating:     {rating_label(metrics.get('security_rating', '?'))}")
    print(f"  Maintainability:     {rating_label(metrics.get('sqale_rating', '?'))}")

    print(f"\n--- ISSUES SUMMARY ({len(issues)} total) ---")

    severity_counts: Counter[str] = Counter()
    type_counts: Counter[str] = Counter()
    rule_counts: Counter[str] = Counter()
    dir_counts: Counter[str] = Counter()

    for issue in issues:
        severity_counts[issue.get("severity", "UNKNOWN")] += 1
        type_counts[issue.get("type", "UNKNOWN")] += 1
        rule_counts[issue.get("rule", "UNKNOWN")] += 1
        component = issue.get("component", "")
        parts = component.replace(f"{PROJECT_KEY}:", "").split("/")
        directory = "/".join(parts[:-1]) if len(parts) > 1 else "(root)"
        dir_counts[directory] += 1

    print("\n  By severity:")
    for sev in ["BLOCKER", "CRITICAL", "MAJOR", "MINOR", "INFO"]:
        count = severity_counts.get(sev, 0)
        if count:
            bar = "#" * min(count, 50)
            print(f"    {sev:<10} {count:>5}  {bar}")

    print("\n  By type:")
    for typ in ["BUG", "VULNERABILITY", "CODE_SMELL"]:
        count = type_counts.get(typ, 0)
        if count:
            print(f"    {typ:<16} {count:>5}")

    print("\n  Top 20 rules:")
    for rule, count in rule_counts.most_common(20):
        print(f"    {rule:<45} {count:>5}")

    print("\n  Top 20 directories:")
    for directory, count in dir_counts.most_common(20):
        print(f"    {directory:<55} {count:>5}")

    print("\n  BLOCKER + CRITICAL issues (details):")
    critical = [i for i in issues if i.get("severity") in ("BLOCKER", "CRITICAL")]
    if not critical:
        print("    (none)")
    for issue in critical[:50]:
        component = issue.get("component", "").replace(f"{PROJECT_KEY}:", "")
        line = issue.get("line", "?")
        print(f"    [{issue.get('severity')}] {component}:{line}")
        print(f"      {issue.get('message', '')[:120]}")
        print(f"      Rule: {issue.get('rule', '')}")
        print()

    print("=" * 70)


def save_json(issues: list[dict[str, Any]], metrics: dict[str, str], output_dir: Path) -> Path:
    """Save raw data to JSON for further analysis.

    Args:
        issues: Issues returned by SonarCloud issues search API.
        metrics: Project metrics mapping.
        output_dir: Directory where the report JSON should be written.

    Returns:
        Path to the written JSON report.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M")
    path = output_dir / f"sonar_report_{stamp}.json"
    data = {
        "generated": datetime.now(UTC).isoformat(),
        "project": PROJECT_KEY,
        "metrics": metrics,
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

    print("Fetching all issues (this may take a moment)...")
    issues = fetch_all_issues(token)

    print_report(issues, metrics)

    output_dir = Path(__file__).resolve().parent.parent / "reports"
    json_path = save_json(issues, metrics, output_dir)
    print(f"\nRaw JSON saved to: {json_path}")
    print(f"({len(issues)} issues, {json_path.stat().st_size / 1024:.0f} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
