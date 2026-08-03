"""Tests for the shared fail-closed script path validator."""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from snapper.infrastructure.security.path_validation import UnsafePathError
from snapper.infrastructure.security.path_validation import canonical_directory
from snapper.infrastructure.security.path_validation import resolve_operator_file
from snapper.infrastructure.security.path_validation import resolve_path_within_root


def test_canonical_directory_accepts_existing_directory(tmp_path: Path) -> None:
    """Accept an existing directory as the canonical trust boundary.

    Given: An existing temporary directory.
    When: The directory is canonicalized for use as a trust boundary.
    Then: Its strict resolved path is returned.
    """
    assert canonical_directory(tmp_path) == tmp_path.resolve(strict=True)


def test_canonical_directory_rejects_file(tmp_path: Path) -> None:
    """Reject a regular file as the canonical trust boundary.

    Given: An existing regular file.
    When: The file is canonicalized as though it were a trusted directory.
    Then: Validation reports that the trust boundary is not a directory.
    """
    file_path = tmp_path / "file.txt"
    file_path.write_text("value", encoding="utf-8")
    with pytest.raises(UnsafePathError, match="not a directory"):
        canonical_directory(file_path)


def test_canonical_directory_rejects_missing_path(tmp_path: Path) -> None:
    """Reject a missing canonical trust boundary.

    Given: A path to a directory that does not exist.
    When: The path is canonicalized for use as a trust boundary.
    Then: Validation fails because the boundary is not resolvable.
    """
    with pytest.raises(UnsafePathError, match="not resolvable"):
        canonical_directory(tmp_path / "missing")


def test_resolve_path_accepts_existing_child(tmp_path: Path) -> None:
    """Accept an existing canonical child inside its trust boundary.

    Given: A regular file directly below a trusted directory.
    When: The file is resolved with existence required.
    Then: The canonical file path is returned unchanged.
    """
    file_path = tmp_path / "file.txt"
    file_path.write_text("value", encoding="utf-8")
    assert resolve_path_within_root(file_path, tmp_path, must_exist=True) == file_path


def test_resolve_path_accepts_missing_nested_child(tmp_path: Path) -> None:
    """Accept a future canonical descendant when existence is optional.

    Given: A nested target whose directory and file do not exist yet.
    When: The target is resolved without requiring existence.
    Then: Its canonical path below the trusted root is returned.
    """
    target = tmp_path / "nested" / "file.txt"
    assert resolve_path_within_root(target, tmp_path, must_exist=False) == target


def test_resolve_path_rejects_missing_required_child(tmp_path: Path) -> None:
    """Reject a missing child when the caller requires existence.

    Given: A nonexistent file below a trusted directory.
    When: The file is resolved with existence required.
    Then: Validation fails because the complete target is not resolvable.
    """
    with pytest.raises(UnsafePathError, match="not safely resolvable"):
        resolve_path_within_root(tmp_path / "missing", tmp_path, must_exist=True)


def test_resolve_path_rejects_outside_child(tmp_path: Path) -> None:
    """Reject an absolute target outside the trust boundary.

    Given: An absolute target in the trusted directory's parent.
    When: The target is resolved against the trusted directory.
    Then: Validation reports that the target escapes the boundary.
    """
    with pytest.raises(UnsafePathError, match="escapes trusted directory"):
        resolve_path_within_root(tmp_path.parent / "outside", tmp_path, must_exist=False)


def test_resolve_path_rejects_lexical_traversal(tmp_path: Path) -> None:
    """Reject lexical traversal that normalizes back inside the root.

    Given: A target containing an explicit parent-directory component.
    When: The target is resolved against its trusted directory.
    Then: Validation refuses the noncanonical child path.
    """
    nested = tmp_path / "nested"
    nested.mkdir()
    target = nested / ".." / "file.txt"
    with pytest.raises(UnsafePathError, match="not a canonical child"):
        resolve_path_within_root(target, tmp_path, must_exist=False)


def test_resolve_path_rejects_root_itself(tmp_path: Path) -> None:
    """Reject the trust boundary itself as an operation target.

    Given: A trusted directory also supplied as the candidate target.
    When: The candidate is resolved with existence required.
    Then: Validation refuses it because only child paths are allowed.
    """
    with pytest.raises(UnsafePathError, match="not a canonical child"):
        resolve_path_within_root(tmp_path, tmp_path, must_exist=True)


def test_resolve_path_rejects_symlink_component(tmp_path: Path) -> None:
    """Reject a symlink component below the trust boundary.

    Given: A target whose intermediate component is reported as a symlink.
    When: The target is resolved without requiring the final file to exist.
    Then: Validation fails before the symlink can redirect the operation.
    """
    target = tmp_path / "linked" / "file.txt"

    def fake_is_symlink(path: Path) -> bool:
        """Report the intermediate component as a symlink."""
        return path == tmp_path / "linked"

    with (
        patch.object(Path, "is_symlink", fake_is_symlink),
        pytest.raises(UnsafePathError, match="must not contain symlinks"),
    ):
        resolve_path_within_root(target, tmp_path, must_exist=False)


def test_resolve_path_rejects_hard_link_to_outside_file(tmp_path: Path) -> None:
    """Reject a hard link that aliases a file outside the trust boundary.

    Given: A regular path below the trusted root hard-linked to an outside file.
    When: The path is resolved for a file operation.
    Then: Validation refuses the alias before either inode can be modified.
    """
    trusted_root = tmp_path / "trusted"
    trusted_root.mkdir()
    outside_path = tmp_path / "outside.txt"
    outside_path.write_text("unchanged", encoding="utf-8")
    linked_path = trusted_root / "linked.txt"
    linked_path.hardlink_to(outside_path)

    with pytest.raises(UnsafePathError, match="hard-linked file"):
        resolve_path_within_root(linked_path, trusted_root, must_exist=True)

    assert outside_path.read_text(encoding="utf-8") == "unchanged"


def test_resolve_path_rejects_unreadable_file_metadata(tmp_path: Path) -> None:
    """Reject a file whose link metadata cannot be inspected.

    Given: An existing child file whose final metadata read fails.
    When: The path is resolved for a file operation.
    Then: Validation reports that its metadata cannot be trusted.
    """
    target = tmp_path / "target.txt"
    target.write_text("value", encoding="utf-8")
    original_is_file = Path.is_file
    original_stat = Path.stat

    def fake_is_file(path: Path) -> bool:
        """Treat the target as a file without consuming its failing stat."""
        if path == target:
            return True
        return original_is_file(path)

    def fake_stat(path: Path, *, follow_symlinks: bool = True) -> os.stat_result:
        """Fail only the target's explicit metadata inspection."""
        if path == target:
            raise OSError("metadata unavailable")
        return original_stat(path, follow_symlinks=follow_symlinks)

    with (
        patch.object(Path, "is_file", fake_is_file),
        patch.object(Path, "stat", fake_stat),
        pytest.raises(UnsafePathError, match="metadata is not safely readable"),
    ):
        resolve_path_within_root(target, tmp_path, must_exist=True)


def test_resolve_operator_file_accepts_relative_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Anchor a relative operator file at the current directory.

    Given: A relative artifact path and a temporary current directory.
    When: The operator file is resolved without requiring existence.
    Then: The returned path is the canonical child of the current directory.
    """
    monkeypatch.chdir(tmp_path)
    assert resolve_operator_file(Path("artifact.json"), must_exist=False) == (
        tmp_path / "artifact.json"
    )


def test_resolve_operator_file_rejects_traversal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject an operator file that traverses to its parent directory.

    Given: A current directory below the requested artifact's destination.
    When: The operator supplies a path containing a parent-directory component.
    Then: Validation refuses the noncanonical file path.
    """
    child = tmp_path / "child"
    child.mkdir()
    monkeypatch.chdir(child)
    with pytest.raises(UnsafePathError, match="canonical child"):
        resolve_operator_file(Path("../artifact.json"), must_exist=False)


def test_resolve_operator_file_rejects_directory(tmp_path: Path) -> None:
    """Reject an existing directory masquerading as an operator file.

    Given: An existing directory selected as an operator file.
    When: The target is resolved with existence required.
    Then: Validation reports that the target is not a regular file.
    """
    target = tmp_path / "directory"
    target.mkdir()
    with pytest.raises(UnsafePathError, match="not a regular file"):
        resolve_operator_file(target, must_exist=True)
