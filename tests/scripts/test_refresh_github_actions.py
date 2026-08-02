"""Tests for the GitHub Actions refresh script."""

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from scripts.refresh_github_actions import _gh_api
from scripts.refresh_github_actions import compute_new_tag_ref
from scripts.refresh_github_actions import ensure_gh_authenticated
from scripts.refresh_github_actions import find_workflow_files
from scripts.refresh_github_actions import latest_release_tag
from scripts.refresh_github_actions import main
from scripts.refresh_github_actions import process_line
from scripts.refresh_github_actions import refresh_github_actions
from scripts.refresh_github_actions import refresh_workflow_file
from scripts.refresh_github_actions import resolve_tag_to_sha
from scripts.refresh_github_actions import update_trailer_for_sha


class TestEnsureGhAuthenticated:
    """Test suite for the gh CLI pre-flight check."""

    def test_passes_when_gh_returns_zero(self) -> None:
        """Verify ensure_gh_authenticated is a no-op when ``gh auth status`` succeeds.

        Given: gh auth status exits with code 0,
        When: ensure_gh_authenticated is called,
        Then: It returns silently without raising.
        """
        with patch("scripts.refresh_github_actions.subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess([], 0, stdout="", stderr="")

            ensure_gh_authenticated()

            mock_run.assert_called_once_with(
                ["gh", "auth", "status"],
                check=True,
                capture_output=True,
                text=True,
            )

    def test_aborts_with_install_hint_when_gh_missing(self) -> None:
        """Verify ensure_gh_authenticated points at the gh install page on FileNotFoundError.

        Given: gh CLI is not on PATH (subprocess raises FileNotFoundError),
        When: ensure_gh_authenticated is called,
        Then: SystemExit is raised with an install hint.
        """
        with patch(
            "scripts.refresh_github_actions.subprocess.run", side_effect=FileNotFoundError()
        ):
            with pytest.raises(SystemExit) as excinfo:
                ensure_gh_authenticated()

            assert "gh CLI not found" in str(excinfo.value)

    def test_aborts_with_login_hint_when_gh_unauthenticated(self) -> None:
        """Verify ensure_gh_authenticated points at ``gh auth login`` on auth failure.

        Given: gh auth status exits non-zero,
        When: ensure_gh_authenticated is called,
        Then: SystemExit is raised with a login hint and propagates stderr.
        """
        error = subprocess.CalledProcessError(1, ["gh", "auth", "status"])
        error.stderr = "You are not logged into any GitHub hosts.\n"
        with patch("scripts.refresh_github_actions.subprocess.run", side_effect=error):
            with pytest.raises(SystemExit) as excinfo:
                ensure_gh_authenticated()

            assert "gh auth login" in str(excinfo.value)
            assert "not logged into" in str(excinfo.value)


class TestLatestReleaseTag:
    """Test suite for latest_release_tag."""

    def test_returns_tag_on_success(self) -> None:
        """Verify latest_release_tag returns ``tag_name`` from a successful API response.

        Given: gh api returns a JSON object with tag_name,
        When: latest_release_tag is called,
        Then: The tag string is returned.
        """
        with patch(
            "scripts.refresh_github_actions._gh_api",
            return_value={"tag_name": "v6.0.3", "name": "v6.0.3"},
        ):
            assert latest_release_tag("actions", "checkout") == "v6.0.3"

    def test_returns_none_on_api_failure(self) -> None:
        """Verify latest_release_tag returns None when the API call fails.

        Given: gh api raises CalledProcessError (e.g. 404 for a missing release),
        When: latest_release_tag is called,
        Then: None is returned.
        """
        with patch(
            "scripts.refresh_github_actions._gh_api",
            side_effect=subprocess.CalledProcessError(1, ["gh"]),
        ):
            assert latest_release_tag("actions", "checkout") is None

    def test_returns_none_when_response_is_not_object(self) -> None:
        """Verify latest_release_tag returns None when API returns a non-object payload.

        Given: gh api returns a JSON array,
        When: latest_release_tag is called,
        Then: None is returned.
        """
        with patch("scripts.refresh_github_actions._gh_api", return_value=[]):
            assert latest_release_tag("actions", "checkout") is None

    def test_returns_none_when_tag_name_missing(self) -> None:
        """Verify latest_release_tag returns None when tag_name is absent.

        Given: gh api returns a JSON object without tag_name,
        When: latest_release_tag is called,
        Then: None is returned.
        """
        with patch("scripts.refresh_github_actions._gh_api", return_value={"name": "release"}):
            assert latest_release_tag("actions", "checkout") is None

    def test_returns_none_when_tag_name_is_empty(self) -> None:
        """Verify latest_release_tag returns None for an empty tag_name string.

        Given: gh api returns a JSON object with an empty tag_name,
        When: latest_release_tag is called,
        Then: None is returned.
        """
        with patch("scripts.refresh_github_actions._gh_api", return_value={"tag_name": ""}):
            assert latest_release_tag("actions", "checkout") is None

    def test_returns_none_when_tag_name_wrong_type(self) -> None:
        """Verify latest_release_tag rejects non-string tag_name values.

        Given: gh api returns tag_name as a non-string,
        When: latest_release_tag is called,
        Then: None is returned.
        """
        with patch("scripts.refresh_github_actions._gh_api", return_value={"tag_name": 6}):
            assert latest_release_tag("actions", "checkout") is None


class TestResolveTagToSha:
    """Test suite for resolve_tag_to_sha."""

    def test_returns_sha_on_success(self) -> None:
        """Verify resolve_tag_to_sha returns the commit SHA from the API.

        Given: gh api returns a commit object with sha,
        When: resolve_tag_to_sha is called,
        Then: The SHA string is returned.
        """
        sha = "de0fac2e4500dabe0009e67214ff5f5447ce83dd"
        with patch("scripts.refresh_github_actions._gh_api", return_value={"sha": sha}):
            assert resolve_tag_to_sha("actions", "checkout", "v6.0.2") == sha

    def test_returns_none_on_api_failure(self) -> None:
        """Verify resolve_tag_to_sha returns None when the API call fails."""
        with patch(
            "scripts.refresh_github_actions._gh_api",
            side_effect=subprocess.CalledProcessError(1, ["gh"]),
        ):
            assert resolve_tag_to_sha("actions", "checkout", "v6.0.2") is None

    def test_returns_none_when_response_is_not_object(self) -> None:
        """Verify resolve_tag_to_sha returns None for non-object responses."""
        with patch("scripts.refresh_github_actions._gh_api", return_value="oops"):
            assert resolve_tag_to_sha("actions", "checkout", "v6") is None

    def test_returns_none_when_sha_missing(self) -> None:
        """Verify resolve_tag_to_sha returns None when the sha key is absent."""
        with patch("scripts.refresh_github_actions._gh_api", return_value={"node_id": "x"}):
            assert resolve_tag_to_sha("actions", "checkout", "v6") is None

    def test_returns_none_when_sha_empty(self) -> None:
        """Verify resolve_tag_to_sha returns None for an empty sha string."""
        with patch("scripts.refresh_github_actions._gh_api", return_value={"sha": ""}):
            assert resolve_tag_to_sha("actions", "checkout", "v6") is None

    def test_returns_none_when_sha_wrong_type(self) -> None:
        """Verify resolve_tag_to_sha rejects non-string sha values."""
        with patch("scripts.refresh_github_actions._gh_api", return_value={"sha": 1}):
            assert resolve_tag_to_sha("actions", "checkout", "v6") is None


class TestGhApi:
    """Test suite for the thin _gh_api wrapper."""

    def test_returns_parsed_json(self) -> None:
        """Verify _gh_api parses the JSON payload returned by ``gh api``.

        Given: gh api stdout contains a JSON object,
        When: _gh_api is invoked,
        Then: The parsed dict is returned.
        """
        with patch("scripts.refresh_github_actions.subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                ["gh", "api", "x"], 0, stdout='{"tag_name": "v1"}', stderr=""
            )

            payload = _gh_api("repos/actions/checkout/releases/latest")

            assert payload == {"tag_name": "v1"}
            mock_run.assert_called_once_with(
                ["gh", "api", "repos/actions/checkout/releases/latest"],
                check=True,
                capture_output=True,
                text=True,
            )


class TestFindWorkflowFiles:
    """Test suite for find_workflow_files."""

    def test_returns_yaml_files_sorted(self, tmp_path: Path) -> None:
        """Verify find_workflow_files returns *.yml/*.yaml files in sorted order.

        Given: A workflows dir with one yml, one yaml, and one txt,
        When: find_workflow_files is called,
        Then: Only yml/yaml entries are returned, sorted by name.
        """
        wf = tmp_path / ".github" / "workflows"
        wf.mkdir(parents=True)
        (wf / "ci.yml").write_text("")
        (wf / "audit.yaml").write_text("")
        (wf / "notes.txt").write_text("")

        result = find_workflow_files([tmp_path])

        assert [p.name for p in result] == ["audit.yaml", "ci.yml"]

    def test_skips_missing_workflows_dir(self, tmp_path: Path) -> None:
        """Verify find_workflow_files silently skips roots without a workflows dir.

        Given: A root path without ``.github/workflows``,
        When: find_workflow_files is called,
        Then: An empty list is returned without raising.
        """
        assert find_workflow_files([tmp_path]) == []

    def test_skips_subdirectories_inside_workflows(self, tmp_path: Path) -> None:
        """Verify find_workflow_files only returns files (not directories).

        Given: A workflows dir containing a subdirectory named like a yaml file,
        When: find_workflow_files is called,
        Then: The subdirectory is not included.
        """
        wf = tmp_path / ".github" / "workflows"
        wf.mkdir(parents=True)
        (wf / "ci.yml").write_text("")
        (wf / "nested.yaml").mkdir()

        result = find_workflow_files([tmp_path])

        assert [p.name for p in result] == ["ci.yml"]


class TestComputeNewTagRef:
    """Test suite for compute_new_tag_ref."""

    def test_bumps_major_tag_when_newer_major_exists(self) -> None:
        """Verify compute_new_tag_ref proposes the new major when latest is in a newer major."""
        assert compute_new_tag_ref("v6", "v7.1.0") == "v7"

    def test_keeps_major_tag_when_already_at_latest_major(self) -> None:
        """Verify compute_new_tag_ref returns None when major already matches latest."""
        assert compute_new_tag_ref("v6", "v6.0.5") is None

    def test_returns_none_when_latest_tag_is_unparseable(self) -> None:
        """Verify compute_new_tag_ref aborts when latest_tag has no leading version."""
        assert compute_new_tag_ref("v6", "release-2026-01") is None

    def test_replaces_full_tag_when_different(self) -> None:
        """Verify compute_new_tag_ref returns the latest tag for full-version pins."""
        assert compute_new_tag_ref("v6.0.2", "v6.0.5") == "v6.0.5"

    def test_returns_none_when_full_tag_already_latest(self) -> None:
        """Verify compute_new_tag_ref returns None when full tag already matches latest."""
        assert compute_new_tag_ref("v6.0.5", "v6.0.5") is None


class TestUpdateTrailerForSha:
    """Test suite for update_trailer_for_sha."""

    def test_updates_version_comment_with_single_space(self) -> None:
        """Verify the trailing ``# vX.Y.Z`` comment is rewritten when a SHA bumps."""
        assert update_trailer_for_sha(" # v6.0.2", "v6.0.3") == " # v6.0.3"

    def test_preserves_extra_whitespace(self) -> None:
        """Verify update_trailer_for_sha preserves the original spacing before the comment."""
        assert update_trailer_for_sha("  # v8.0.0", "v8.1.0") == "  # v8.1.0"

    def test_preserves_text_after_version_token(self) -> None:
        """Verify text following the replaced version token remains byte-for-byte stable."""
        assert update_trailer_for_sha(" # v6.0.2  pinned", "v6.0.3") == " # v6.0.3  pinned"

    def test_returns_marker_only_comment_unchanged(self) -> None:
        """Verify a comment without a version token is not synthesized."""
        assert update_trailer_for_sha("  #   ", "v6.0.3") == "  #   "

    def test_returns_unchanged_trailer_without_comment(self) -> None:
        """Verify update_trailer_for_sha is a no-op for trailers without a version comment."""
        assert update_trailer_for_sha("", "v6.0.3") == ""


class TestProcessLine:
    """Test suite for process_line covering every supported pinning style."""

    def test_returns_unchanged_for_non_uses_line(self) -> None:
        """Verify process_line ignores lines that do not start with ``uses:``."""
        line = "      run: echo hello\n"
        assert process_line(line) == (line, False)

    def test_returns_unchanged_for_local_action(self) -> None:
        """Verify process_line skips local action references (path-style)."""
        line = "      uses: ./.github/actions/foo\n"
        assert process_line(line) == (line, False)

    def test_returns_unchanged_for_branch_ref(self) -> None:
        """Verify process_line leaves branch-name refs untouched."""
        line = "      uses: actions/checkout@main\n"
        with patch("scripts.refresh_github_actions.latest_release_tag") as mock_tag:
            assert process_line(line) == (line, False)
            mock_tag.assert_not_called()

    def test_returns_unchanged_when_release_lookup_fails(self) -> None:
        """Verify process_line skips bumping when latest_release_tag returns None."""
        line = "      uses: actions/checkout@v6\n"
        with patch("scripts.refresh_github_actions.latest_release_tag", return_value=None):
            assert process_line(line) == (line, False)

    def test_bumps_full_version_tag(self) -> None:
        """Verify process_line bumps a full-version tag and preserves the trailing newline."""
        line = "      uses: actions/checkout@v6.0.2\n"
        with patch("scripts.refresh_github_actions.latest_release_tag", return_value="v6.0.5"):
            new_line, changed = process_line(line)

        assert changed is True
        assert new_line == "      uses: actions/checkout@v6.0.5\n"

    def test_no_change_when_full_version_already_latest(self) -> None:
        """Verify process_line returns unchanged when current full version matches latest."""
        line = "      uses: actions/checkout@v6.0.5\n"
        with patch("scripts.refresh_github_actions.latest_release_tag", return_value="v6.0.5"):
            assert process_line(line) == (line, False)

    def test_bumps_major_only_tag_when_new_major_available(self) -> None:
        """Verify process_line bumps a major-only tag when a newer major is released."""
        line = "      uses: actions/checkout@v6\n"
        with patch("scripts.refresh_github_actions.latest_release_tag", return_value="v7.1.0"):
            new_line, changed = process_line(line)

        assert changed is True
        assert new_line == "      uses: actions/checkout@v7\n"

    def test_bumps_sha_pin_and_updates_version_comment(self) -> None:
        """Verify process_line replaces both SHA and version comment for SHA-pinned actions."""
        old_sha = "a" * 40
        new_sha = "b" * 40
        line = f"        uses: actions/checkout@{old_sha} # v6.0.2\n"
        with (
            patch(
                "scripts.refresh_github_actions.latest_release_tag",
                return_value="v6.0.3",
            ),
            patch(
                "scripts.refresh_github_actions.resolve_tag_to_sha",
                return_value=new_sha,
            ),
        ):
            new_line, changed = process_line(line)

        assert changed is True
        assert new_line == f"        uses: actions/checkout@{new_sha} # v6.0.3\n"

    def test_no_change_when_sha_resolution_fails(self) -> None:
        """Verify process_line skips SHA pins when the tag cannot be resolved to a commit."""
        old_sha = "a" * 40
        line = f"        uses: actions/checkout@{old_sha} # v6.0.2\n"
        with (
            patch(
                "scripts.refresh_github_actions.latest_release_tag",
                return_value="v6.0.3",
            ),
            patch("scripts.refresh_github_actions.resolve_tag_to_sha", return_value=None),
        ):
            assert process_line(line) == (line, False)

    def test_no_change_when_sha_already_matches_latest(self) -> None:
        """Verify process_line is a no-op when the latest tag already maps to the current SHA."""
        old_sha = "a" * 40
        line = f"        uses: actions/checkout@{old_sha} # v6.0.3\n"
        with (
            patch(
                "scripts.refresh_github_actions.latest_release_tag",
                return_value="v6.0.3",
            ),
            patch(
                "scripts.refresh_github_actions.resolve_tag_to_sha",
                return_value=old_sha,
            ),
        ):
            assert process_line(line) == (line, False)

    def test_handles_line_without_trailing_newline(self) -> None:
        """Verify process_line preserves the absence of a trailing newline on the input."""
        line = "      uses: actions/checkout@v6.0.2"
        with patch("scripts.refresh_github_actions.latest_release_tag", return_value="v6.0.5"):
            new_line, changed = process_line(line)

        assert changed is True
        assert new_line == "      uses: actions/checkout@v6.0.5"

    def test_no_change_when_major_tag_lookup_returns_unparseable(self) -> None:
        """Verify process_line treats a non-vN.M latest tag as 'no bump' for major pins."""
        line = "      uses: actions/checkout@v6\n"
        with patch(
            "scripts.refresh_github_actions.latest_release_tag",
            return_value="release-2026-01",
        ):
            assert process_line(line) == (line, False)


class TestRefreshWorkflowFile:
    """Test suite for refresh_workflow_file."""

    def test_rewrites_when_any_line_changes(self, tmp_path: Path) -> None:
        """Verify refresh_workflow_file writes the updated content when at least one line changes.

        Given: A workflow file with one tag-pinned ``uses:`` line and one comment,
        When: refresh_workflow_file is called and the latest tag is newer,
        Then: The file is rewritten with the bumped ref and the count is 1.
        """
        wf = tmp_path / "ci.yml"
        wf.write_text(
            "jobs:\n  build:\n    steps:\n      - uses: actions/checkout@v6.0.2\n",
            encoding="utf-8",
        )

        with patch("scripts.refresh_github_actions.latest_release_tag", return_value="v6.0.5"):
            count = refresh_workflow_file(wf)

        assert count == 1
        assert "actions/checkout@v6.0.5" in wf.read_text(encoding="utf-8")

    def test_does_not_rewrite_when_no_changes(self, tmp_path: Path) -> None:
        """Verify refresh_workflow_file leaves a file untouched when nothing bumps.

        Given: A workflow file with a uses-line already at the latest tag,
        When: refresh_workflow_file is called,
        Then: The file's mtime is unchanged and the count is 0.
        """
        wf = tmp_path / "ci.yml"
        original = "jobs:\n  build:\n    steps:\n      - uses: actions/checkout@v6.0.5\n"
        wf.write_text(original, encoding="utf-8")
        original_bytes = wf.read_bytes()

        with patch("scripts.refresh_github_actions.latest_release_tag", return_value="v6.0.5"):
            count = refresh_workflow_file(wf)

        assert count == 0
        assert wf.read_bytes() == original_bytes


class TestRefreshGithubActions:
    """Test suite for the top-level refresh_github_actions orchestrator."""

    def test_walks_files_and_returns_total(self, tmp_path: Path) -> None:
        """Verify refresh_github_actions walks all roots and aggregates the bump count.

        Given: A parent root with one workflow file containing a bumpable use,
        When: refresh_github_actions is called,
        Then: ensure_gh_authenticated runs, the file is rewritten, total is 1.
        """
        wf_dir = tmp_path / ".github" / "workflows"
        wf_dir.mkdir(parents=True)
        ci = wf_dir / "ci.yml"
        ci.write_text(
            "jobs:\n  build:\n    steps:\n      - uses: actions/checkout@v6.0.2\n",
            encoding="utf-8",
        )

        with (
            patch("scripts.refresh_github_actions.ensure_gh_authenticated") as mock_auth,
            patch(
                "scripts.refresh_github_actions.latest_release_tag",
                return_value="v6.0.5",
            ),
        ):
            total = refresh_github_actions(tmp_path)

        mock_auth.assert_called_once_with()
        assert total == 1
        assert "actions/checkout@v6.0.5" in ci.read_text(encoding="utf-8")

    def test_uses_default_root_when_not_provided(self) -> None:
        """Verify refresh_github_actions falls back to the script's parent directory.

        Given: All side-effect helpers are mocked,
        When: refresh_github_actions is called without arguments,
        Then: It executes against the default root and returns 0 bumps.
        """
        with (
            patch("scripts.refresh_github_actions.ensure_gh_authenticated"),
            patch("scripts.refresh_github_actions.find_workflow_files", return_value=[]),
        ):
            total = refresh_github_actions()

        assert total == 0


class TestMain:
    """Test suite for the main entry point."""

    def test_returns_zero(self) -> None:
        """Verify main returns exit code 0 on success.

        Given: refresh_github_actions is mocked,
        When: main is called,
        Then: Exit code 0 is returned.
        """
        with patch("scripts.refresh_github_actions.refresh_github_actions") as mock_refresh:
            mock_refresh.return_value = 0

            assert main() == 0

            mock_refresh.assert_called_once_with()
