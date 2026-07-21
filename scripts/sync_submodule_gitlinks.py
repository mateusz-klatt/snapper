"""Point every submodule gitlink at its reviewed ``origin/master``.

Submodule changes land through pull requests, and those PRs are SQUASH-merged.
That leaves the parent repository pointing at the pre-merge commit, which exists
only on the mirror remote and is NOT an ancestor of ``origin/master``. It keeps
resolving right up until the merged branch is pruned, and then an ``origin``
clone cannot initialise the submodule at all. Bumping the gitlink by hand after
every auto-merge is exactly the step that gets forgotten.

This target makes the reviewed history authoritative:

1. Fetch each submodule's ``origin`` and read its ``origin/master`` commit.
2. Stage the gitlink at that commit when it differs.
3. Align every OTHER remote's ``master`` to the same commit — but ONLY when the
   two trees are byte-identical. Divergent content means somebody pushed work
   that never went through review, and force-aligning would destroy it, so the
   script refuses and reports instead of guessing.

Nothing is committed or pushed here; the caller reviews `git status` and commits.
"""

import subprocess
from pathlib import Path
from typing import Final

_GITMODULES: Final = ".gitmodules"
_CANONICAL_REMOTE: Final = "origin"
_BRANCH: Final = "master"


def _default_root() -> Path:
    """Return the repository root (this script lives in ``<root>/scripts``)."""
    return Path(__file__).resolve().parent.parent


def _git(root: Path, *args: str) -> tuple[int, str]:
    """Run one git command and capture its trimmed stdout.

    Args:
        root: Working directory to run in.
        *args: Git arguments, excluding the executable itself.

    Returns:
        The exit status and the captured stdout with trailing whitespace removed.
    """
    completed = subprocess.run(
        ["git", *args], cwd=root, check=False, capture_output=True, text=True
    )
    return completed.returncode, completed.stdout.strip()


def submodule_paths(gitmodules_text: str) -> list[str]:
    """Return the declared submodule paths in declaration order.

    Args:
        gitmodules_text: Full text of a ``.gitmodules`` file.

    Returns:
        Submodule paths relative to the repository root.
    """
    paths: list[str] = []
    for line in gitmodules_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("path"):
            _, _, value = stripped.partition("=")
            candidate = value.strip()
            if candidate:
                paths.append(candidate)
    return paths


def trees_match(root: Path, left: str, right: str) -> bool:
    """Report whether two commits have byte-identical trees.

    Args:
        root: Submodule working directory.
        left: First commit-ish.
        right: Second commit-ish.

    Returns:
        ``True`` when a diff between the two commits is empty.
    """
    status, output = _git(root, "diff", "--stat", left, right)
    return status == 0 and not output


def mirror_remotes(root: Path) -> list[str]:
    """Return every configured remote except the canonical one.

    Args:
        root: Submodule working directory.

    Returns:
        Sorted mirror remote names.
    """
    _, output = _git(root, "remote")
    return sorted(name for name in output.splitlines() if name and name != _CANONICAL_REMOTE)


def _align_mirrors(root: Path, path: str, target: str) -> None:
    """Fast-forward or refuse each mirror remote onto the canonical commit."""
    for remote in mirror_remotes(root):
        status, _ = _git(root, "fetch", "--quiet", remote)
        if status != 0:
            print(f"  {path}: cannot reach {remote}; skipped")
            continue
        _, head = _git(root, "rev-parse", f"{remote}/{_BRANCH}")
        if head == target:
            continue
        if not trees_match(root, f"{remote}/{_BRANCH}", target):
            print(
                f"  {path}: {remote}/{_BRANCH} has content that is NOT on "
                f"{_CANONICAL_REMOTE}/{_BRANCH}; refusing to force-align"
            )
            continue
        push_status, _ = _git(
            root, "push", "--quiet", "--force-with-lease", remote, f"{target}:{_BRANCH}"
        )
        verb = "aligned" if push_status == 0 else "FAILED to align"
        print(f"  {path}: {verb} {remote}/{_BRANCH} (identical tree)")


def _checkout_target(root: Path, target: str) -> None:
    """Move the submodule to the canonical commit, staying on its branch if safe.

    A bare ``checkout --detach`` is the conventional submodule state, but it is
    hostile in a submodule people actually commit in: the next commit lands on a
    detached HEAD and ``git push <remote> HEAD`` then fails outright, needing a
    fully-qualified refspec. So when the local branch can simply fast-forward onto
    the target it is moved there and kept checked out; only a branch carrying work
    the canonical commit does not contain falls back to detaching, which preserves
    that work rather than silently discarding it.

    Args:
        root: Submodule working directory.
        target: Canonical commit to land on.
    """
    status, branch = _git(root, "rev-parse", "--abbrev-ref", "HEAD")
    if status == 0 and branch == _BRANCH:
        ancestor, _ = _git(root, "merge-base", "--is-ancestor", "HEAD", target)
        if ancestor == 0:
            _git(root, "merge", "--quiet", "--ff-only", target)
            return
    _git(root, "checkout", "--quiet", "--detach", target)


def sync_submodule(root: Path, path: str) -> bool:
    """Sync one submodule's gitlink and mirrors to its canonical commit.

    Args:
        root: Parent repository root.
        path: Submodule path relative to the root.

    Returns:
        ``True`` when the parent's gitlink was restaged.
    """
    sub_root = root / path
    if not (sub_root / ".git").exists():
        print(f"  {path}: not initialised; skipped")
        return False
    status, _ = _git(sub_root, "fetch", "--quiet", _CANONICAL_REMOTE)
    if status != 0:
        print(f"  {path}: cannot reach {_CANONICAL_REMOTE}; skipped")
        return False
    rev_status, target = _git(sub_root, "rev-parse", f"{_CANONICAL_REMOTE}/{_BRANCH}")
    if rev_status != 0 or not target:
        print(f"  {path}: no {_CANONICAL_REMOTE}/{_BRANCH}; skipped")
        return False
    _, current = _git(sub_root, "rev-parse", "HEAD")
    _align_mirrors(sub_root, path, target)
    if current == target:
        print(f"  {path}: already at {target[:7]}")
        return False
    _checkout_target(sub_root, target)
    _git(root, "add", path)
    print(f"  {path}: gitlink {current[:7]} -> {target[:7]}")
    return True


def main() -> int:
    """Sync every submodule gitlink to its reviewed canonical commit.

    Returns:
        Zero on success; one when ``.gitmodules`` is missing.
    """
    root = _default_root()
    gitmodules = root / _GITMODULES
    if not gitmodules.is_file():
        print("sync-gitlinks: no .gitmodules; nothing to do")
        return 1
    print("sync-gitlinks: pointing submodule gitlinks at origin/master")
    changed = 0
    for path in submodule_paths(gitmodules.read_text(encoding="utf-8")):
        if sync_submodule(root, path):
            changed += 1
    if changed:
        print(f"sync-gitlinks: {changed} gitlink(s) restaged; review and commit")
    else:
        print("sync-gitlinks: all gitlinks already canonical")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
