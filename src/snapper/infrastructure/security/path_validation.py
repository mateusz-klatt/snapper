"""Canonical path validation for trusted local operator scripts.

The helpers constrain faulty or untrusted path arguments against the current
filesystem state. They assume an adversary cannot concurrently rewrite the
operator's local filesystem while the script is running.
"""

from pathlib import Path


class UnsafePathError(ValueError):
    """Raised when a filesystem target cannot be proven in scope."""


def canonical_directory(path: Path) -> Path:
    """Return an existing directory's canonical path.

    Args:
        path: Directory to use as a trust boundary.

    Returns:
        The resolved directory path.

    Raises:
        UnsafePathError: If the path is missing, unreadable, or not a directory.
    """
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise UnsafePathError(f"trusted directory is not resolvable: {path}") from exc
    if not resolved.is_dir():
        raise UnsafePathError(f"trusted path is not a directory: {path}")
    return resolved


def resolve_path_within_root(
    path: Path,
    allowed_root: Path,
    *,
    must_exist: bool,
) -> Path:
    """Resolve a canonical child path confined to an allowed directory.

    Relative paths are interpreted below ``allowed_root``. Existing symlink
    components and lexical traversal are rejected, and the root itself is never
    returned.

    Args:
        path: Candidate filesystem path.
        allowed_root: Existing directory that bounds the operation.
        must_exist: Whether the complete candidate must already exist.

    Returns:
        The canonical candidate path.

    Raises:
        UnsafePathError: If the candidate is not currently confined to the root.
    """
    root = canonical_directory(allowed_root)
    candidate = path if path.is_absolute() else root / path
    lexical = candidate.absolute()
    try:
        relative = lexical.relative_to(root)
    except ValueError as exc:
        raise UnsafePathError(f"path escapes trusted directory {root}: {path}") from exc
    if not relative.parts or ".." in relative.parts:
        raise UnsafePathError(f"path is not a canonical child of {root}: {path}")

    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise UnsafePathError(f"path must not contain symlinks: {path}")

    try:
        resolved = lexical.resolve(strict=must_exist)
        resolved.relative_to(root)
    except (OSError, RuntimeError, ValueError) as exc:
        raise UnsafePathError(f"path is not safely resolvable below {root}: {path}") from exc
    try:
        if resolved.is_file() and resolved.stat().st_nlink != 1:
            raise UnsafePathError(f"path must not be a hard-linked file: {path}")
    except OSError as exc:
        raise UnsafePathError(f"path metadata is not safely readable: {path}") from exc
    return resolved


def resolve_operator_file(path: Path, *, must_exist: bool) -> Path:
    """Resolve an explicitly selected operator file without relocating it.

    Absolute paths remain supported. Relative paths are anchored at the current
    directory, while traversal, observed symlinks, missing parents, and
    directories masquerading as files are refused.

    Args:
        path: Operator-selected file path.
        must_exist: Whether the complete file path must already exist.

    Returns:
        The canonical file path.

    Raises:
        UnsafePathError: If the file is not currently canonical and in scope.
    """
    candidate = path if path.is_absolute() else Path.cwd() / path
    parent = canonical_directory(candidate.parent)
    resolved = resolve_path_within_root(
        candidate,
        parent,
        must_exist=must_exist,
    )
    if resolved.exists() and not resolved.is_file():
        raise UnsafePathError(f"operator path is not a regular file: {path}")
    return resolved
