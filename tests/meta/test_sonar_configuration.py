"""Pin the analysis surface of every Snapper SonarCloud project."""

import re
import subprocess
from fnmatch import fnmatchcase
from pathlib import Path
from typing import cast

import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PROJECT_SCOPES = (
    (
        Path("sonar-project.properties"),
        _REPO_ROOT,
        "tests/**,integrations/snapper-delegate/tests/**",
    ),
    (
        Path("frontend/sonar-project.properties"),
        _REPO_ROOT / "frontend",
        "e2e/**,src/test/**,**/*.test.mjs,**/*.test.ts,**/*.test.tsx,**/*.spec.ts,**/*.spec.tsx",
    ),
    (
        Path("integrations/snapper-mcp/sonar-project.properties"),
        _REPO_ROOT / "integrations" / "snapper-mcp",
        "test/**",
    ),
    (
        Path("ios/sonar-project.properties"),
        _REPO_ROOT / "ios",
        "SnapperTests/**,SnapperUITests/**",
    ),
)
_PARENT_CONFIG_PATH = Path("sonar-project.properties")
_PARENT_SUBMODULE_EXCLUSIONS = frozenset({"frontend/**", "integrations/snapper-mcp/**", "ios/**"})
_PARENT_TEMPORARY_SOURCE_EXCLUSIONS = frozenset({"src/snapper/data/repository.py"})
_PARENT_EXCLUSIONS = _PARENT_SUBMODULE_EXCLUSIONS | _PARENT_TEMPORARY_SOURCE_EXCLUSIONS
_PARENT_OWNED_INTEGRATION_WITNESS = "integrations/snapper-delegate/src/snapper_delegate/runner.py"
_SONAR_WORKFLOWS = (
    Path(".github/workflows/ci.yml"),
    Path("frontend/.github/workflows/sonarcloud.yml"),
    Path("integrations/snapper-mcp/.github/workflows/sonarcloud.yml"),
    Path("ios/.github/workflows/sonarcloud.yml"),
)
_WORKFLOW_PROJECT_ROOTS = (
    _REPO_ROOT,
    _REPO_ROOT / "frontend",
    _REPO_ROOT / "integrations" / "snapper-mcp",
    _REPO_ROOT / "ios",
)
_ALL_WORKFLOWS = tuple(
    workflow_path.relative_to(_REPO_ROOT)
    for project_root in _WORKFLOW_PROJECT_ROOTS
    for workflow_path in sorted((project_root / ".github" / "workflows").glob("*.y*ml"))
)
_SCAN_ACTION_PREFIX = "SonarSource/sonarqube-scan-action@"
_QUALITY_GATE_ACTION_PREFIX = "SonarSource/sonarqube-quality-gate-action@"
_PINNED_QUALITY_GATE_ACTION = (
    "SonarSource/sonarqube-quality-gate-action@7a5fffe8e523c40e0c740b6bc2712ab503e52efa"
)
"""v1.2.1. Bumping this is a deliberate two-step: move all four workflows, then
move this pin. The test exists so the four cannot drift apart silently, which
means a refresh that touches only the workflows is SUPPOSED to fail here."""
_PINNED_ACTION_PATTERN = re.compile(r"^[^@\s]+@[0-9a-f]{40}$")
_UNTRUSTED_SHELL_CONTEXT_PATTERN = re.compile(
    r"\$\{\{\s*(?:inputs(?:\.|\[)|github\.(?:event|ref))",
    re.IGNORECASE,
)
_NPM_CI_PATTERN = re.compile(r"\bnpm\s+ci\b")
_DOCUMENT_DIRECTORIES = frozenset({"docs", "documentation"})
_TEST_DIRECTORIES = frozenset(
    {"__tests__", "e2e", "snappertests", "snapperuitests", "test", "tests"}
)
_CLI_PROPERTY_PATTERN = re.compile(
    r"-D\s*[\"']?(?P<key>sonar\.[A-Za-z0-9_.-]+)\s*=",
    re.IGNORECASE,
)
_INLINE_SUPPRESSION_PATTERNS = (
    ("NOSONAR", re.compile(r"\bNOSONAR\b", re.IGNORECASE)),
    (
        "Sonar ignore directive",
        re.compile(r"\bsonar(?:lint)?[-_ :]+(?:disable|ignore|suppress)\b", re.IGNORECASE),
    ),
    (
        "Sonar rule suppression annotation",
        re.compile(
            r"(?:SuppressMessage|SuppressWarnings)\s*\([^)]*"
            r"(?:\bsonar(?:lint)?\b|\bsquid\s*:\s*S\d+\b|"
            r"\b[A-Za-z][A-Za-z0-9_.-]*\s*:\s*S\d+\b|"
            r"\bS\d{3,}\b|\ball\b)[^)]*\)",
            re.IGNORECASE | re.DOTALL,
        ),
    ),
)


def _read_properties(config_path: Path) -> dict[str, str]:
    """Parse active key-value properties from one Sonar configuration."""
    properties: dict[str, str] = {}
    for line in config_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        key, separator, value = stripped.partition("=")
        assert separator, f"Malformed Sonar property in {config_path}: {line}"
        properties[key.strip()] = value.strip()
    return properties


def _is_suppressive_property(key: str) -> bool:
    """Return whether a Sonar property can narrow analysis or hide findings."""
    normalized_key = key.casefold()
    return (
        normalized_key.endswith(".exclusions")
        or normalized_key.startswith("sonar.issue.ignore")
        or normalized_key.endswith(".file.suffixes")
        or (normalized_key.endswith(".inclusions") and normalized_key != "sonar.test.inclusions")
    )


def _is_document_or_test(relative_path: Path) -> bool:
    """Return whether text may legitimately discuss suppression syntax."""
    lowered_parts = tuple(part.casefold() for part in relative_path.parts)
    if any(part in _DOCUMENT_DIRECTORIES | _TEST_DIRECTORIES for part in lowered_parts[:-1]):
        return True
    filename = relative_path.name.casefold()
    return (
        relative_path.suffix.casefold() in {".md", ".rst"}
        or filename.startswith("test_")
        or filename.endswith("_test.py")
        or ".test." in filename
        or ".spec." in filename
    )


def _tracked_text_files(project_root: Path) -> list[tuple[Path, str]]:
    """Read non-test, non-documentation UTF-8 text tracked by one project."""
    tracked = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=project_root,
        check=True,
        capture_output=True,
    ).stdout
    text_files: list[tuple[Path, str]] = []
    for encoded_path in tracked.split(b"\0"):
        if not encoded_path:
            continue
        relative_path = Path(encoded_path.decode("utf-8"))
        candidate_path = project_root / relative_path
        if _is_document_or_test(relative_path) or not candidate_path.is_file():
            continue
        content = candidate_path.read_bytes()
        if b"\0" in content:
            continue
        try:
            decoded_content = content.decode("utf-8")
        except UnicodeDecodeError:
            continue
        text_files.append((candidate_path, decoded_content))
    return text_files


def _workflow_action_steps(workflow_path: Path) -> list[tuple[str, int, dict[str, object]]]:
    """Return action steps with their job and order from one workflow."""
    document: object = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    assert isinstance(document, dict)
    jobs = document.get("jobs")
    assert isinstance(jobs, dict)
    action_steps: list[tuple[str, int, dict[str, object]]] = []
    for job_name, raw_job in jobs.items():
        assert isinstance(job_name, str)
        assert isinstance(raw_job, dict)
        steps = raw_job.get("steps")
        assert isinstance(steps, list)
        for index, raw_step in enumerate(steps):
            assert isinstance(raw_step, dict)
            step = cast(dict[str, object], raw_step)
            if isinstance(step.get("uses"), str):
                action_steps.append((job_name, index, step))
    return action_steps


@pytest.mark.parametrize(
    ("relative_path", "project_root", "expected_test_inclusions"),
    _PROJECT_SCOPES,
)
def test_sonar_configs_analyze_the_full_checked_out_scope(
    relative_path: Path,
    project_root: Path,
    expected_test_inclusions: str,
) -> None:
    """Require the complete declared scope and only approved exact exclusions.

    Given: one of the four active Snapper Sonar project configurations,
    When: its source, test, and suppressive properties are inspected,
    Then: the complete checked-out project is analyzed with tests classified explicitly,
        and only the parent carries the explicitly approved exact exclusions.
    """
    assert project_root.is_dir()
    properties = _read_properties(_REPO_ROOT / relative_path)
    assert properties["sonar.sources"] == "."
    assert properties["sonar.tests"] == "."
    assert properties["sonar.test.inclusions"] == expected_test_inclusions

    suppressions = {key for key in properties if _is_suppressive_property(key)}
    if relative_path == _PARENT_CONFIG_PATH:
        exclusions = frozenset(
            item.strip() for item in properties["sonar.exclusions"].split(",") if item.strip()
        )
        assert exclusions == _PARENT_EXCLUSIONS, (
            "only independently analyzed projects and the temporary oversized repository "
            "module may be excluded; integrations/snapper-delegate must remain in parent analysis"
        )
        assert (_REPO_ROOT / _PARENT_OWNED_INTEGRATION_WITNESS).is_file()
        assert (_REPO_ROOT / next(iter(_PARENT_TEMPORARY_SOURCE_EXCLUSIONS))).is_file()
        assert not any(
            fnmatchcase(_PARENT_OWNED_INTEGRATION_WITNESS, pattern) for pattern in exclusions
        )
        suppressions.remove("sonar.exclusions")
    assert suppressions == set()


@pytest.mark.parametrize(("_relative_path", "project_root", "_test_inclusions"), _PROJECT_SCOPES)
def test_tracked_project_text_does_not_bypass_sonar_policy(
    _relative_path: Path,
    project_root: Path,
    _test_inclusions: str,
) -> None:
    """Reject command-line and inline bypasses outside docs and tests.

    Given: the tracked executable, source, and configuration text of a project,
    When: it is scanned for command-line overrides and inline suppressions,
    Then: neither a ``-Dsonar.*`` override nor a Sonar issue-suppression form exists.
    """
    violations: list[str] = []
    for candidate_path, content in _tracked_text_files(project_root):
        display_path = candidate_path.relative_to(_REPO_ROOT)
        for match in _CLI_PROPERTY_PATTERN.finditer(content):
            violations.append(f"{display_path}: CLI override {match.group('key')}")
        for label, pattern in _INLINE_SUPPRESSION_PATTERNS:
            if pattern.search(content):
                violations.append(f"{display_path}: {label}")
    assert violations == []


@pytest.mark.parametrize("relative_path", _SONAR_WORKFLOWS)
def test_sonar_workflows_block_on_the_pinned_quality_gate(relative_path: Path) -> None:
    """Require a blocking Quality Gate directly after every project scan.

    Given: the CI workflow responsible for one Snapper SonarCloud project,
    When: its action steps and Quality Gate controls are inspected,
    Then: one pinned five-minute gate follows the scan in the same job and cannot fail open.
    """
    action_steps = _workflow_action_steps(_REPO_ROOT / relative_path)
    scans = [
        item for item in action_steps if cast(str, item[2]["uses"]).startswith(_SCAN_ACTION_PREFIX)
    ]
    gates = [
        item
        for item in action_steps
        if cast(str, item[2]["uses"]).startswith(_QUALITY_GATE_ACTION_PREFIX)
    ]
    assert len(scans) == 1
    assert len(gates) == 1
    scan_job, scan_index, _scan_step = scans[0]
    gate_job, gate_index, gate_step = gates[0]
    assert (gate_job, gate_index) == (scan_job, scan_index + 1)
    assert gate_step["uses"] == _PINNED_QUALITY_GATE_ACTION
    assert gate_step["timeout-minutes"] == 5
    assert "if" not in gate_step
    assert gate_step.get("continue-on-error", False) is False
    environment = gate_step.get("env")
    assert isinstance(environment, dict)
    assert environment.get("SONAR_TOKEN") == "${{ secrets.SONAR_TOKEN }}"


def test_parent_sonar_scan_has_explicit_heap_for_declared_source_analysis() -> None:
    """Give the backend scan enough heap for its declared owned source scope.

    Given: The parent project analyzes its declared owned source scope,
    When: The pinned scanner is launched on GitHub's runner,
    Then: Its JVM has an explicit bounded heap instead of an undersized ergonomic default.
    """
    action_steps = _workflow_action_steps(_REPO_ROOT / ".github/workflows/ci.yml")
    scans = [
        step
        for _job, _index, step in action_steps
        if str(step["uses"]).startswith(_SCAN_ACTION_PREFIX)
    ]
    assert len(scans) == 1
    environment = scans[0].get("env")
    assert isinstance(environment, dict)
    assert environment.get("SONAR_SCANNER_JAVA_OPTS") == "-Xmx4g"
    assert "SONAR_SCANNER_OPTS" not in environment


@pytest.mark.parametrize("relative_path", _ALL_WORKFLOWS)
def test_full_scope_workflows_use_hardened_execution(relative_path: Path) -> None:
    """Keep workflow findings out of every full-source Sonar project.

    Given: A tracked workflow analyzed by one of the four Sonar projects,
    When: Its jobs, actions, and shell commands are inspected,
    Then: Permissions are job-local, actions are immutable, and untrusted values use env.
    """
    workflow_path = _REPO_ROOT / relative_path
    document: object = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    assert isinstance(document, dict)
    assert "permissions" not in document
    jobs = document.get("jobs")
    assert isinstance(jobs, dict)
    violations: list[str] = []
    for job_name, raw_job in jobs.items():
        assert isinstance(job_name, str)
        assert isinstance(raw_job, dict)
        permissions = raw_job.get("permissions")
        if not isinstance(permissions, dict) or not permissions:
            violations.append(f"{job_name}: missing job-level permissions")
        steps = raw_job.get("steps", [])
        assert isinstance(steps, list)
        for index, raw_step in enumerate(steps):
            assert isinstance(raw_step, dict)
            step = cast(dict[str, object], raw_step)
            action = step.get("uses")
            if isinstance(action, str) and not action.startswith("./"):
                if _PINNED_ACTION_PATTERN.fullmatch(action) is None:
                    violations.append(f"{job_name}[{index}]: mutable action {action}")
            command = step.get("run")
            if not isinstance(command, str):
                continue
            if _UNTRUSTED_SHELL_CONTEXT_PATTERN.search(command):
                violations.append(f"{job_name}[{index}]: untrusted context interpolated in shell")
            if _NPM_CI_PATTERN.search(command) and "--ignore-scripts" not in command:
                violations.append(f"{job_name}[{index}]: npm ci executes lifecycle scripts")
    assert violations == []
