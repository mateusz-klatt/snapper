"""Refresh GitHub Actions versions across all workflow files in the workspace.

Walks ``<root>/.github/workflows/*.yml`` for the parent repo plus the
``frontend``, ``ios``, and ``integrations/snapper-mcp`` submodules, and bumps
each ``uses: <owner>/<repo>@<ref>`` reference to the latest release.

Three pinning styles are recognised and preserved:

1. SHA-pinned with optional ``# vX.Y.Z`` comment - the SHA is replaced with
   the commit SHA of the latest release tag, and the trailing version comment
   is updated to the new tag.
2. Major-only tag pin (``@v6``) - bumped only when a newer major exists; minor
   and patch updates are absorbed by the floating major tag itself.
3. Full version tag pin (``@v6.0.2``) - replaced with the latest release tag.

Refs that are neither SHAs nor version-shaped tags (branch names like
``@main`` or custom refs) and ``uses:`` lines pointing at local actions or
Docker images are left untouched. Network access via the ``gh`` CLI is
required; the script aborts if ``gh`` is missing or unauthenticated.
"""

import json
import re
import subprocess
from collections.abc import Iterable
from pathlib import Path
from typing import Final

from snapper.core.json_types import JsonValue

WORKFLOW_DIR: Final = ".github/workflows"
SUBPROJECT_DIRS: Final[tuple[str, ...]] = (
    ".",
    "frontend",
    "ios",
    "integrations/snapper-mcp",
)

USES_RE: Final = re.compile(
    r"^(?P<lead>\s*-?\s*uses:\s*)"
    r"(?P<owner>[\w.-]+)/(?P<repo>[\w.-]+)"
    r"@(?P<ref>\S+)"
    r"(?P<trailer>.*)$"
)

SHA_RE: Final = re.compile(r"^[0-9a-f]{40}$")
TAG_REF_RE: Final = re.compile(r"^v\d+(\.\d+)*$")
MAJOR_TAG_RE: Final = re.compile(r"^v\d+$")
EXTRACT_MAJOR_RE: Final = re.compile(r"^v(\d+)")
SHA_VERSION_COMMENT_RE: Final = re.compile(r"^(?P<spaces>\s*)#\s*v?\S+(?P<rest>.*)$")


def _gh_api(path: str) -> JsonValue:
    """Run ``gh api <path>`` and return the parsed JSON payload.

    Args:
        path: GitHub API path (e.g. ``repos/owner/repo/releases/latest``).

    Returns:
        Parsed JSON value.

    Raises:
        subprocess.CalledProcessError: When the underlying ``gh api`` call
            exits non-zero (most commonly a 404 for missing release/tag).
    """
    result = subprocess.run(
        ["gh", "api", path],
        check=True,
        capture_output=True,
        text=True,
    )
    parsed: JsonValue = json.loads(result.stdout)
    return parsed


def ensure_gh_authenticated() -> None:
    """Verify ``gh`` is installed and authenticated, abort with a clear error otherwise.

    Raises:
        SystemExit: When the ``gh`` CLI is not available or not logged in.
    """
    try:
        subprocess.run(
            ["gh", "auth", "status"],
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise SystemExit(
            "gh CLI not found - install from https://cli.github.com/ before running."
        ) from exc
    except subprocess.CalledProcessError as exc:
        raise SystemExit(
            "gh CLI not authenticated - run 'gh auth login' before retrying."
            f"\n{exc.stderr or ''}"
        ) from exc


def latest_release_tag(owner: str, repo: str) -> str | None:
    """Return the tag name of the latest release, or None when unavailable.

    Args:
        owner: GitHub repository owner.
        repo: GitHub repository name.

    Returns:
        Release ``tag_name`` string, or None if the repo has no releases or
        the API call failed.
    """
    try:
        data = _gh_api(f"repos/{owner}/{repo}/releases/latest")
    except subprocess.CalledProcessError:
        return None
    if not isinstance(data, dict):
        return None
    tag = data.get("tag_name")
    if not isinstance(tag, str) or not tag:
        return None
    return tag


def resolve_tag_to_sha(owner: str, repo: str, tag: str) -> str | None:
    """Return the commit SHA backing a release tag, or None if not resolvable.

    Args:
        owner: GitHub repository owner.
        repo: GitHub repository name.
        tag: Tag name to resolve to its underlying commit.

    Returns:
        40-character commit SHA, or None when the tag cannot be resolved.
    """
    try:
        data = _gh_api(f"repos/{owner}/{repo}/commits/{tag}")
    except subprocess.CalledProcessError:
        return None
    if not isinstance(data, dict):
        return None
    sha = data.get("sha")
    if not isinstance(sha, str) or not sha:
        return None
    return sha


def find_workflow_files(roots: Iterable[Path]) -> list[Path]:
    """Return all workflow files under each root's ``.github/workflows`` directory.

    Args:
        roots: Repository roots whose workflow directories should be scanned.

    Returns:
        Sorted list of ``*.yml`` / ``*.yaml`` workflow files.
    """
    files: list[Path] = []
    for root in roots:
        wf_dir = root / WORKFLOW_DIR
        if not wf_dir.is_dir():
            continue
        for path in sorted(wf_dir.iterdir()):
            if path.is_file() and path.suffix in (".yml", ".yaml"):
                files.append(path)
    return files


def compute_new_tag_ref(current_ref: str, latest_tag: str) -> str | None:
    """Decide the replacement ref for tag-style pins.

    Args:
        current_ref: Existing ref string from the ``uses:`` line.
        latest_tag: Latest release tag for the action repo.

    Returns:
        Replacement ref string, or None when no change is needed.
    """
    if MAJOR_TAG_RE.match(current_ref):
        major_match = EXTRACT_MAJOR_RE.match(latest_tag)
        if major_match is None:
            return None
        new_major = f"v{major_match.group(1)}"
        if new_major == current_ref:
            return None
        return new_major
    if current_ref == latest_tag:
        return None
    return latest_tag


def update_trailer_for_sha(trailer: str, latest_tag: str) -> str:
    """Update the trailing ``# vX.Y.Z`` comment on a SHA-pinned ``uses:`` line.

    Args:
        trailer: Trailing portion of the matched line, including any comment.
        latest_tag: New release tag to record in the comment.

    Returns:
        Trailer with the version comment refreshed; original trailer if no
        recognisable ``# vX`` style comment was present.
    """
    match = SHA_VERSION_COMMENT_RE.match(trailer)
    if match is None:
        return trailer
    return f"{match.group('spaces')}# {latest_tag}{match.group('rest')}"


def process_line(line: str) -> tuple[str, bool]:
    """Process one workflow line and return ``(new_line, changed)``.

    Args:
        line: Raw workflow line, with or without trailing newline.

    Returns:
        Tuple of the (possibly rewritten) line and a flag indicating whether
        the line was modified.
    """
    stripped = line.rstrip("\n")
    match = USES_RE.match(stripped)
    if match is None:
        return line, False
    owner = match.group("owner")
    repo = match.group("repo")
    current_ref = match.group("ref")
    trailer = match.group("trailer")

    is_sha = bool(SHA_RE.match(current_ref))
    is_tag = bool(TAG_REF_RE.match(current_ref))
    if not (is_sha or is_tag):
        return line, False

    latest_tag = latest_release_tag(owner, repo)
    if latest_tag is None:
        return line, False

    if is_sha:
        new_sha = resolve_tag_to_sha(owner, repo, latest_tag)
        if new_sha is None or new_sha == current_ref:
            return line, False
        new_ref = new_sha
        new_trailer = update_trailer_for_sha(trailer, latest_tag)
    else:
        candidate = compute_new_tag_ref(current_ref, latest_tag)
        if candidate is None:
            return line, False
        new_ref = candidate
        new_trailer = trailer

    new_line = f"{match.group('lead')}{owner}/{repo}@{new_ref}{new_trailer}"
    if line.endswith("\n"):
        new_line += "\n"
    return new_line, True


def refresh_workflow_file(path: Path) -> int:
    """Refresh a single workflow file in place.

    Args:
        path: Workflow file to scan and rewrite.

    Returns:
        Number of ``uses:`` lines bumped.
    """
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)
    new_lines: list[str] = []
    changed_count = 0
    for line in lines:
        new_line, changed = process_line(line)
        if changed:
            changed_count += 1
            print(f"  {path}: {line.strip()} -> {new_line.strip()}")
        new_lines.append(new_line)
    if changed_count > 0:
        path.write_text("".join(new_lines), encoding="utf-8")
    return changed_count


def refresh_github_actions(root: Path | None = None) -> int:
    """Walk all configured subprojects and bump every action reference in place.

    Args:
        root: Project root directory. Defaults to the parent of the script
            directory.

    Returns:
        Total number of references bumped across all workflow files.
    """
    if root is None:
        root = Path(__file__).parent.parent
    ensure_gh_authenticated()

    sub_roots = [root / d for d in SUBPROJECT_DIRS]
    files = find_workflow_files(sub_roots)
    print(f"Scanning {len(files)} workflow file(s) across {len(SUBPROJECT_DIRS)} project(s)...")
    total = 0
    for path in files:
        total += refresh_workflow_file(path)
    print(f"GitHub Actions refresh complete - bumped {total} reference(s).")
    return total


def main() -> int:
    """CLI entry point.

    Returns:
        Process exit code, always 0 on success.
    """
    refresh_github_actions()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
