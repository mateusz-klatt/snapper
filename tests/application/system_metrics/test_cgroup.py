"""Tests for cgroup system metrics readers."""

import os
from pathlib import Path

import pytest

from snapper.application.system_metrics import cgroup


class TestCgroup:
    """Tests for cgroup readers."""

    def _write(self, path: Path, text: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="ascii")

    def test_detect_cgroup_version_covers_v1_v2_none_and_oserror(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Covered by test body."""
        root = tmp_path / "cgroup"
        root.mkdir()

        assert cgroup.detect_cgroup_version(root) is None

        self._write(root / "memory" / "memory.limit_in_bytes", "1024")
        assert cgroup.detect_cgroup_version(root) == "v1"

        self._write(root / "cgroup.controllers", "cpu memory")
        assert cgroup.detect_cgroup_version(root) == "v2"

        def exists_raises(path: Path) -> bool:
            raise OSError(f"cannot stat {path}")

        monkeypatch.setattr(Path, "exists", exists_raises)

        assert cgroup.detect_cgroup_version(root) is None

    def test_read_cgroup_v2_parses_values_and_max_sentinels(self, tmp_path: Path) -> None:
        """Covered by test body."""
        root = tmp_path / "v2"
        self._write(root / "cgroup.controllers", "cpu memory")
        self._write(root / "memory.max", "max")
        self._write(root / "memory.current", "4096")
        self._write(root / "cpu.max", "max 100000")
        self._write(root / "cpu.stat", "usage_usec 10\nnr_throttled 7\n")

        reading = cgroup.read_cgroup_v2(root)

        assert reading == {
            "memory_max_bytes": -1,
            "memory_current_bytes": 4096,
            "cpu_quota_microseconds": -1,
            "cpu_throttled_count": 7,
        }

    def test_read_cgroup_v2_parses_finite_cpu_quota(self, tmp_path: Path) -> None:
        """Covered by test body."""
        root = tmp_path / "v2"
        self._write(root / "cgroup.controllers", "cpu memory")
        self._write(root / "memory.max", "8192")
        self._write(root / "memory.current", "4096")
        self._write(root / "cpu.max", "50000 100000")
        self._write(root / "cpu.stat", "nr_throttled 3\n")

        reading = cgroup.read_cgroup_v2(root)

        assert reading == {
            "memory_max_bytes": 8192,
            "memory_current_bytes": 4096,
            "cpu_quota_microseconds": 50000,
            "cpu_throttled_count": 3,
        }

    def test_read_cgroup_v2_returns_none_without_unified_marker(self, tmp_path: Path) -> None:
        """Covered by test body."""
        assert cgroup.read_cgroup_v2(tmp_path / "missing") is None

    def test_read_cgroup_v2_returns_none_fields_for_unparseable_and_missing_files(
        self, tmp_path: Path
    ) -> None:
        """Covered by test body."""
        root = tmp_path / "v2"
        self._write(root / "cgroup.controllers", "cpu memory")
        self._write(root / "memory.max", "not-an-int")
        self._write(root / "cpu.max", "not-a-quota 100000")
        self._write(root / "cpu.stat", "nr_throttled not-an-int\n")

        reading = cgroup.read_cgroup_v2(root)

        assert reading == {
            "memory_max_bytes": None,
            "memory_current_bytes": None,
            "cpu_quota_microseconds": None,
            "cpu_throttled_count": None,
        }

    def test_read_cgroup_v2_handles_empty_cpu_max_and_missing_throttled_key(
        self, tmp_path: Path
    ) -> None:
        """Covered by test body."""
        root = tmp_path / "v2"
        self._write(root / "cgroup.controllers", "cpu memory")
        self._write(root / "memory.max", "64")
        self._write(root / "memory.current", "32")
        self._write(root / "cpu.max", "")
        self._write(root / "cpu.stat", "usage_usec 10\nmalformed\n")

        reading = cgroup.read_cgroup_v2(root)

        assert reading == {
            "memory_max_bytes": 64,
            "memory_current_bytes": 32,
            "cpu_quota_microseconds": None,
            "cpu_throttled_count": None,
        }

    def test_read_cgroup_v2_handles_missing_cpu_files(self, tmp_path: Path) -> None:
        """Covered by test body."""
        root = tmp_path / "v2"
        self._write(root / "cgroup.controllers", "cpu memory")
        self._write(root / "memory.max", "64")
        self._write(root / "memory.current", "32")

        reading = cgroup.read_cgroup_v2(root)

        assert reading == {
            "memory_max_bytes": 64,
            "memory_current_bytes": 32,
            "cpu_quota_microseconds": None,
            "cpu_throttled_count": None,
        }

    def test_read_cgroup_v1_parses_cpuacct_controller(self, tmp_path: Path) -> None:
        """Covered by test body."""
        root = tmp_path / "v1"
        self._write(root / "memory" / "memory.limit_in_bytes", "8192")
        self._write(root / "memory" / "memory.usage_in_bytes", "4096")
        self._write(root / "cpu,cpuacct" / "cpu.cfs_quota_us", "25000")
        self._write(root / "cpu,cpuacct" / "cpu.stat", "nr_throttled 11\n")

        reading = cgroup.read_cgroup_v1(root)

        assert reading == {
            "memory_max_bytes": 8192,
            "memory_current_bytes": 4096,
            "cpu_quota_microseconds": 25000,
            "cpu_throttled_count": 11,
        }

    def test_read_cgroup_v1_falls_back_to_cpu_controller(self, tmp_path: Path) -> None:
        """Covered by test body."""
        root = tmp_path / "v1"
        self._write(root / "memory" / "memory.limit_in_bytes", "8192")
        self._write(root / "memory" / "memory.usage_in_bytes", "4096")
        self._write(root / "cpu" / "cpu.cfs_quota_us", "30000")
        self._write(root / "cpu" / "cpu.stat", "usage_usec 1\nnr_throttled 5\n")

        reading = cgroup.read_cgroup_v1(root)

        assert reading == {
            "memory_max_bytes": 8192,
            "memory_current_bytes": 4096,
            "cpu_quota_microseconds": 30000,
            "cpu_throttled_count": 5,
        }

    def test_read_cgroup_v1_returns_none_without_memory_marker(self, tmp_path: Path) -> None:
        """Covered by test body."""
        root = tmp_path / "v1"
        (root / "memory").mkdir(parents=True)

        assert cgroup.read_cgroup_v1(root) is None

    def test_read_cgroup_v1_handles_unparseable_and_missing_values(self, tmp_path: Path) -> None:
        """Covered by test body."""
        root = tmp_path / "v1"
        self._write(root / "memory" / "memory.limit_in_bytes", "bad-limit")

        reading = cgroup.read_cgroup_v1(root)

        assert reading == {
            "memory_max_bytes": None,
            "memory_current_bytes": None,
            "cpu_quota_microseconds": None,
            "cpu_throttled_count": None,
        }

    def test_read_cgroup_v1_handles_unparseable_and_missing_throttled_count(
        self, tmp_path: Path
    ) -> None:
        """Covered by test body."""
        bad_root = tmp_path / "bad"
        self._write(bad_root / "memory" / "memory.limit_in_bytes", "1")
        self._write(bad_root / "memory" / "memory.usage_in_bytes", "1")
        self._write(bad_root / "cpu,cpuacct" / "cpu.cfs_quota_us", "1")
        self._write(bad_root / "cpu,cpuacct" / "cpu.stat", "nr_throttled nope\n")

        missing_root = tmp_path / "missing"
        self._write(missing_root / "memory" / "memory.limit_in_bytes", "1")
        self._write(missing_root / "memory" / "memory.usage_in_bytes", "1")
        self._write(missing_root / "cpu,cpuacct" / "cpu.cfs_quota_us", "1")
        self._write(missing_root / "cpu,cpuacct" / "cpu.stat", "usage_usec 1\nmalformed\n")

        bad_reading = cgroup.read_cgroup_v1(bad_root)
        missing_reading = cgroup.read_cgroup_v1(missing_root)

        assert bad_reading is not None
        assert missing_reading is not None
        assert bad_reading["cpu_throttled_count"] is None
        assert missing_reading["cpu_throttled_count"] is None

    def test_read_cgroup_dispatches_by_detected_version(self, tmp_path: Path) -> None:
        """Covered by test body."""
        v2_root = tmp_path / "v2"
        self._write(v2_root / "cgroup.controllers", "cpu memory")
        self._write(v2_root / "memory.max", "1")
        self._write(v2_root / "memory.current", "1")
        self._write(v2_root / "cpu.max", "1 100000")
        self._write(v2_root / "cpu.stat", "nr_throttled 1\n")

        v1_root = tmp_path / "v1"
        self._write(v1_root / "memory" / "memory.limit_in_bytes", "2")
        self._write(v1_root / "memory" / "memory.usage_in_bytes", "1")
        self._write(v1_root / "cpu,cpuacct" / "cpu.cfs_quota_us", "2")
        self._write(v1_root / "cpu,cpuacct" / "cpu.stat", "nr_throttled 2\n")

        v2_reading, v2_version = cgroup.read_cgroup(v2_root)
        v1_reading, v1_version = cgroup.read_cgroup(v1_root)
        none_reading, none_version = cgroup.read_cgroup(tmp_path / "none")

        assert v2_version == "v2"
        assert v2_reading is not None
        assert v1_version == "v1"
        assert v1_reading is not None
        assert none_reading is None
        assert none_version is None

    def test_check_cgroup_paths_readable_uses_default_root(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Covered by test body."""

        def fake_access(path: str, mode: int) -> bool:
            return path == str(cgroup._CGROUP_ROOT) and mode == os.R_OK

        monkeypatch.setattr(cgroup.os, "access", fake_access)

        assert cgroup._check_cgroup_paths_readable() is True
