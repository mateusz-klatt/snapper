"""Install pre-commit without moving or breaking an existing Iris guard chain."""

import subprocess
import sys
from pathlib import Path
from shutil import which


def _has_iris_guard(root: Path) -> bool:
    """Resolve Git's actual hook path, including worktrees and custom hook directories."""
    hook_path = subprocess.check_output(
        ["git", "rev-parse", "--git-path", "hooks/pre-commit"],
        cwd=root,
        encoding="utf-8",
    ).strip()
    hook = root / hook_path
    return hook.is_file() and b"# Iris cooperative guard" in hook.read_bytes()


def install_pre_commit(root: Path) -> int:
    """Refresh the quality hook inside an existing Iris wrapper when present.

    Args:
        root: Repository whose Git hook is being installed.

    Returns:
        The pre-commit installer's exit status after restoring the Iris guard.

    Raises:
        RuntimeError: An existing Iris guard cannot be maintained without its CLI.
        subprocess.CalledProcessError: Iris cannot safely remove or restore its wrapper.
    """
    iris = None
    if _has_iris_guard(root):
        iris = which("iris")
        if iris is None:
            raise RuntimeError("Iris guard is installed but the iris CLI is not on PATH")
        subprocess.run([iris, "guard", "remove"], cwd=root, check=True)
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pre_commit", "install"], cwd=root, check=False
        )
    finally:
        if iris is not None:
            subprocess.run([iris, "guard", "install"], cwd=root, check=True)
    return result.returncode


def main() -> int:
    """Install hooks for the current checkout without replacing its guard.

    Returns:
        The quality hook installer's exit status after restoring Iris.
    """
    return install_pre_commit(Path(__file__).resolve().parents[1])


if __name__ == "__main__":
    raise SystemExit(main())
