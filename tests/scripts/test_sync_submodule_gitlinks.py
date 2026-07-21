"""Tests for the submodule gitlink sync.

The target exists because squash-merged PRs leave the parent pointing at a
pre-merge commit that lives only on a mirror remote. These tests pin the parsing,
the tree-equality guard that decides whether a mirror may be force-aligned, and
the entry point's reporting — the guard most of all, since force-aligning a
mirror whose content differs would destroy unreviewed work.
"""

from pathlib import Path
from typing import Final
from unittest.mock import MagicMock
from unittest.mock import patch

from scripts.sync_submodule_gitlinks import _align_mirrors
from scripts.sync_submodule_gitlinks import _checkout_target
from scripts.sync_submodule_gitlinks import _default_root
from scripts.sync_submodule_gitlinks import _git
from scripts.sync_submodule_gitlinks import main
from scripts.sync_submodule_gitlinks import mirror_remotes
from scripts.sync_submodule_gitlinks import submodule_paths
from scripts.sync_submodule_gitlinks import sync_submodule
from scripts.sync_submodule_gitlinks import trees_match

_GITMODULES: Final = """
[submodule "frontend"]
\tpath = frontend
\turl = git@github.com:acme/frontend.git
[submodule "ios"]
\tpath = ios
\turl = git@github.com:acme/ios.git
"""


class TestSubmodulePaths:
    """Cover reading declared submodule paths."""

    def test_reads_paths_in_declaration_order(self) -> None:
        """Paths come back in the order the file declares them."""
        assert submodule_paths(_GITMODULES) == ["frontend", "ios"]

    def test_empty_file_yields_nothing(self) -> None:
        """A file without submodules produces no paths."""
        assert submodule_paths("") == []

    def test_blank_path_values_are_skipped(self) -> None:
        """A malformed empty path entry is ignored rather than yielding ''."""
        assert submodule_paths('[submodule "x"]\n\tpath =\n') == []


class TestTreesMatch:
    """Cover the guard that permits a mirror force-align."""

    def test_empty_diff_means_identical(self) -> None:
        """No diff output means the two commits carry the same tree."""
        with patch("scripts.sync_submodule_gitlinks._git", return_value=(0, "")):
            assert trees_match(Path("."), "a", "b") is True

    def test_any_diff_blocks_alignment(self) -> None:
        """Differing content is never treated as safe to overwrite."""
        with patch("scripts.sync_submodule_gitlinks._git", return_value=(0, " f | 2 +-")):
            assert trees_match(Path("."), "a", "b") is False

    def test_failed_diff_blocks_alignment(self) -> None:
        """A diff that cannot run is treated as unsafe, not as identical."""
        with patch("scripts.sync_submodule_gitlinks._git", return_value=(1, "")):
            assert trees_match(Path("."), "a", "b") is False


class TestMirrorRemotes:
    """Cover mirror discovery."""

    def test_canonical_remote_is_excluded(self) -> None:
        """``origin`` is the target, never something to align."""
        with patch("scripts.sync_submodule_gitlinks._git", return_value=(0, "origin\nholzera")):
            assert mirror_remotes(Path(".")) == ["holzera"]

    def test_no_mirrors_yields_empty(self) -> None:
        """A submodule with only the canonical remote has nothing to align."""
        with patch("scripts.sync_submodule_gitlinks._git", return_value=(0, "origin")):
            assert mirror_remotes(Path(".")) == []


class TestSyncSubmodule:
    """Cover one submodule's sync decision."""

    def _prepare(self, tmp_path: Path) -> Path:
        """Create an initialised-looking submodule directory."""
        sub = tmp_path / "frontend"
        (sub / ".git").mkdir(parents=True)
        return tmp_path

    def test_uninitialised_submodule_is_skipped(self, tmp_path: Path) -> None:
        """A submodule that was never initialised is left alone."""
        assert sync_submodule(tmp_path, "frontend") is False

    def test_already_canonical_is_not_restaged(self, tmp_path: Path) -> None:
        """A gitlink already at origin/master needs no change."""
        root = self._prepare(tmp_path)
        git = MagicMock(side_effect=[(0, ""), (0, "abc123"), (0, "abc123"), (0, "origin")])
        with patch("scripts.sync_submodule_gitlinks._git", git):
            assert sync_submodule(root, "frontend") is False

    def test_stale_gitlink_is_restaged(self, tmp_path: Path) -> None:
        """A gitlink behind origin/master is moved and staged in the parent."""
        root = self._prepare(tmp_path)
        git = MagicMock(
            side_effect=[(0, ""), (0, "canonical"), (0, "stale"), (0, "origin"), (0, "")]
        )
        with (
            patch("scripts.sync_submodule_gitlinks._git", git),
            patch("scripts.sync_submodule_gitlinks._checkout_target") as checkout,
        ):
            assert sync_submodule(root, "frontend") is True
        checkout.assert_called_once()

    def test_unreachable_remote_is_skipped(self, tmp_path: Path) -> None:
        """A submodule whose origin cannot be fetched is reported, not guessed."""
        root = self._prepare(tmp_path)
        with patch("scripts.sync_submodule_gitlinks._git", MagicMock(return_value=(1, ""))):
            assert sync_submodule(root, "frontend") is False

    def test_missing_canonical_branch_is_skipped(self, tmp_path: Path) -> None:
        """Without an origin/master there is no canonical commit to adopt."""
        root = self._prepare(tmp_path)
        git = MagicMock(side_effect=[(0, ""), (1, "")])
        with patch("scripts.sync_submodule_gitlinks._git", git):
            assert sync_submodule(root, "frontend") is False


class TestMain:
    """Cover the entry point."""

    def test_missing_gitmodules_is_an_error(self, tmp_path: Path) -> None:
        """Without a .gitmodules there is nothing to sync and it is reported."""
        with patch("scripts.sync_submodule_gitlinks._default_root", return_value=tmp_path):
            assert main() == 1

    def test_reports_when_everything_is_canonical(self, tmp_path: Path) -> None:
        """A fully-synced tree completes without restaging anything."""
        (tmp_path / ".gitmodules").write_text(_GITMODULES, encoding="utf-8")
        with (
            patch("scripts.sync_submodule_gitlinks._default_root", return_value=tmp_path),
            patch("scripts.sync_submodule_gitlinks.sync_submodule", return_value=False),
        ):
            assert main() == 0

    def test_reports_restaged_gitlinks(self, tmp_path: Path) -> None:
        """Changed gitlinks are counted so the caller knows to commit."""
        (tmp_path / ".gitmodules").write_text(_GITMODULES, encoding="utf-8")
        with (
            patch("scripts.sync_submodule_gitlinks._default_root", return_value=tmp_path),
            patch("scripts.sync_submodule_gitlinks.sync_submodule", return_value=True),
        ):
            assert main() == 0


class TestAlignMirrors:
    """Cover the force-align decision — the destructive path, so pinned hardest."""

    def test_identical_tree_is_force_aligned(self) -> None:
        """A mirror whose content matches is fast-forwarded onto the canonical commit."""
        git = MagicMock(side_effect=[(0, ""), (0, "stale"), (0, ""), (0, "")])
        with (
            patch("scripts.sync_submodule_gitlinks.mirror_remotes", return_value=["holzera"]),
            patch("scripts.sync_submodule_gitlinks.trees_match", return_value=True),
            patch("scripts.sync_submodule_gitlinks._git", git),
        ):
            _align_mirrors(Path("."), "frontend", "canonical")
        pushed = [c for c in git.call_args_list if "push" in c.args]
        assert len(pushed) == 1

    def test_divergent_content_is_never_force_pushed(self) -> None:
        """A mirror holding content absent from origin is REFUSED, not overwritten.

        This is the whole safety property: force-aligning here would destroy work
        that never went through review.
        """
        git = MagicMock(side_effect=[(0, ""), (0, "stale")])
        with (
            patch("scripts.sync_submodule_gitlinks.mirror_remotes", return_value=["holzera"]),
            patch("scripts.sync_submodule_gitlinks.trees_match", return_value=False),
            patch("scripts.sync_submodule_gitlinks._git", git),
        ):
            _align_mirrors(Path("."), "frontend", "canonical")
        assert not [c for c in git.call_args_list if "push" in c.args]

    def test_already_aligned_mirror_is_left_alone(self) -> None:
        """A mirror already at the canonical commit needs no push."""
        git = MagicMock(side_effect=[(0, ""), (0, "canonical")])
        with (
            patch("scripts.sync_submodule_gitlinks.mirror_remotes", return_value=["holzera"]),
            patch("scripts.sync_submodule_gitlinks._git", git),
        ):
            _align_mirrors(Path("."), "frontend", "canonical")
        assert not [c for c in git.call_args_list if "push" in c.args]

    def test_unreachable_mirror_is_skipped(self) -> None:
        """A mirror that cannot be fetched is reported and left untouched."""
        git = MagicMock(side_effect=[(1, "")])
        with (
            patch("scripts.sync_submodule_gitlinks.mirror_remotes", return_value=["holzera"]),
            patch("scripts.sync_submodule_gitlinks._git", git),
        ):
            _align_mirrors(Path("."), "frontend", "canonical")
        assert not [c for c in git.call_args_list if "push" in c.args]

    def test_failed_push_is_reported_not_raised(self) -> None:
        """A rejected force-push is surfaced without aborting the sweep."""
        git = MagicMock(side_effect=[(0, ""), (0, "stale"), (1, "")])
        with (
            patch("scripts.sync_submodule_gitlinks.mirror_remotes", return_value=["holzera"]),
            patch("scripts.sync_submodule_gitlinks.trees_match", return_value=True),
            patch("scripts.sync_submodule_gitlinks._git", git),
        ):
            _align_mirrors(Path("."), "frontend", "canonical")


class TestPrimitives:
    """Cover the thin git wrapper and root discovery."""

    def test_default_root_is_the_repository_root(self) -> None:
        """The script locates the repo as its own parent directory."""
        assert (_default_root() / "scripts" / "sync_submodule_gitlinks.py").is_file()

    def test_git_returns_status_and_trimmed_stdout(self) -> None:
        """The wrapper surfaces the exit status and strips trailing whitespace."""
        assert _git(_default_root(), "rev-parse", "--is-inside-work-tree") == (0, "true")


class TestCheckoutTarget:
    """Cover staying on a branch versus detaching."""

    def test_fast_forwardable_branch_stays_checked_out(self) -> None:
        """A master that can fast-forward is moved, not detached.

        Detaching here is hostile in a submodule people commit in: the next
        commit lands on a detached HEAD and `git push <remote> HEAD` then fails.
        """
        git = MagicMock(side_effect=[(0, "master"), (0, ""), (0, "")])
        with patch("scripts.sync_submodule_gitlinks._git", git):
            _checkout_target(Path("."), "canonical")
        assert [c for c in git.call_args_list if "merge" in c.args]
        assert not [c for c in git.call_args_list if "--detach" in c.args]

    def test_branch_with_unmerged_work_detaches_instead(self) -> None:
        """A branch holding commits the target lacks is preserved by detaching."""
        git = MagicMock(side_effect=[(0, "master"), (1, ""), (0, "")])
        with patch("scripts.sync_submodule_gitlinks._git", git):
            _checkout_target(Path("."), "canonical")
        assert [c for c in git.call_args_list if "--detach" in c.args]

    def test_already_detached_head_detaches_to_target(self) -> None:
        """A submodule already detached simply moves to the canonical commit."""
        git = MagicMock(side_effect=[(0, "HEAD"), (0, "")])
        with patch("scripts.sync_submodule_gitlinks._git", git):
            _checkout_target(Path("."), "canonical")
        assert [c for c in git.call_args_list if "--detach" in c.args]
