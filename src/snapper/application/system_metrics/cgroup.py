"""cgroup v1 / v2 readers for the system metrics snapshotter.

Reads memory + CPU quota counters from the kernel-exposed cgroup
hierarchy. Three reader entry points: :func:`detect_cgroup_version`
returns ``"v1"`` / ``"v2"`` / ``None``, :func:`read_cgroup_v2` parses
the unified hierarchy, :func:`read_cgroup_v1` parses the legacy
controller-per-mount layout. All three swallow ``OSError`` (path
missing on non-Linux dev hosts, container without cgroup mount, etc.)
and return ``None`` so the snapshotter degrades gracefully — the
operator sees ``cgroup_version=None`` and ``cgroup_*`` metric fields
``None`` instead of a sampler crash.

Container UID 888 reads cgroup files via the world-readable
``r--r--r--`` mode the kernel grants by default — no privileged
syscall needed.
"""

import os
from pathlib import Path
from typing import Literal
from typing import TypedDict


class CgroupReading(TypedDict):
    """Container-aware cgroup snapshot fields.

    Mirrors the ``MemoryMetrics`` + ``CpuMetrics`` cgroup-derived
    subset. ``memory_max_bytes = -1`` represents "max" / unlimited
    (treated as ``None`` upstream); a positive int is the byte limit.
    ``cpu_quota_microseconds = -1`` represents "max"; positive int is
    the per-period quota.
    """

    memory_max_bytes: int | None
    memory_current_bytes: int | None
    cpu_quota_microseconds: int | None
    cpu_throttled_count: int | None


_CGROUP_ROOT = Path("/sys/fs/cgroup")
_V1_MEMORY_LIMIT_FILE = "memory.limit_in_bytes"


def detect_cgroup_version(root: Path = _CGROUP_ROOT) -> Literal["v1", "v2"] | None:
    """Detect cgroup version present at ``root``.

    cgroup v2 unified-hierarchy hosts expose ``cgroup.controllers``
    at the root; v1 does not. Absence of both signals "no cgroup
    available" — caller treats as ``None``.

    Args:
        root: cgroup mount root. Default ``/sys/fs/cgroup``.

    Returns:
        ``"v2"`` if the unified controllers file exists, ``"v1"`` if
        the legacy memory controller dir exists, ``None`` otherwise.
    """
    try:
        if (root / "cgroup.controllers").exists():
            return "v2"
        if (root / "memory" / _V1_MEMORY_LIMIT_FILE).exists():
            return "v1"
    except OSError:
        return None
    return None


def _read_int(path: Path) -> int | None:
    """Parse an integer from a cgroup file; ``None`` on read failure.

    The kernel writes ASCII integers (or ``"max"`` for unlimited).
    ``"max"`` returns ``-1`` so callers can distinguish "unlimited"
    from "missing path" (``None``).
    """
    try:
        text = path.read_text(encoding="ascii").strip()
    except OSError:
        return None
    if text == "max":
        return -1
    try:
        return int(text)
    except ValueError:
        return None


def _read_cpu_max_v2(path: Path) -> int | None:
    """Parse cgroup v2 ``cpu.max``: ``"<quota> <period>"`` or ``"max <period>"``.

    Returns the quota microseconds, or ``-1`` for "max", or ``None``
    on parse failure / missing path.
    """
    try:
        text = path.read_text(encoding="ascii").strip()
    except OSError:
        return None
    parts = text.split()
    if not parts:
        return None
    quota = parts[0]
    if quota == "max":
        return -1
    try:
        return int(quota)
    except ValueError:
        return None


def _read_throttled_count_v2(path: Path) -> int | None:
    """Parse cgroup v2 ``cpu.stat`` for the ``nr_throttled`` counter.

    The file is line-oriented: ``"<key> <value>"``. Returns the
    cumulative count from process start (consumer derives rate); ``None``
    on read / parse failure.
    """
    try:
        text = path.read_text(encoding="ascii")
    except OSError:
        return None
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0] == "nr_throttled":
            try:
                return int(parts[1])
            except ValueError:
                return None
    return None


def read_cgroup_v2(root: Path = _CGROUP_ROOT) -> CgroupReading | None:
    """Read the cgroup v2 unified-hierarchy memory + CPU files.

    Returns ``None`` on any path-missing error so the snapshotter
    degrades gracefully on hosts without cgroup v2.

    Args:
        root: cgroup mount root. Default ``/sys/fs/cgroup``.

    Returns:
        :class:`CgroupReading` with int / ``None`` per-field results.
        ``-1`` in any limit field means the kernel reports "max".
    """
    if not (root / "cgroup.controllers").exists():
        return None
    return CgroupReading(
        memory_max_bytes=_read_int(root / "memory.max"),
        memory_current_bytes=_read_int(root / "memory.current"),
        cpu_quota_microseconds=_read_cpu_max_v2(root / "cpu.max"),
        cpu_throttled_count=_read_throttled_count_v2(root / "cpu.stat"),
    )


def read_cgroup_v1(root: Path = _CGROUP_ROOT) -> CgroupReading | None:
    """Read the cgroup v1 legacy memory + CPU files.

    Returns ``None`` on any path-missing error so the snapshotter
    degrades gracefully on hosts without cgroup v1.

    Args:
        root: cgroup mount root. Default ``/sys/fs/cgroup``.

    Returns:
        :class:`CgroupReading` with int / ``None`` per-field results.
    """
    memory_dir = root / "memory"
    if not (memory_dir / _V1_MEMORY_LIMIT_FILE).exists():
        return None
    cpu_dir = root / "cpu,cpuacct"
    if not (cpu_dir / "cpu.cfs_quota_us").exists():
        cpu_dir = root / "cpu"
    return CgroupReading(
        memory_max_bytes=_read_int(memory_dir / _V1_MEMORY_LIMIT_FILE),
        memory_current_bytes=_read_int(memory_dir / "memory.usage_in_bytes"),
        cpu_quota_microseconds=_read_int(cpu_dir / "cpu.cfs_quota_us"),
        cpu_throttled_count=_read_throttled_v1(cpu_dir / "cpu.stat"),
    )


def _read_throttled_v1(path: Path) -> int | None:
    """Parse cgroup v1 ``cpu.stat`` for ``nr_throttled``."""
    try:
        text = path.read_text(encoding="ascii")
    except OSError:
        return None
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0] == "nr_throttled":
            try:
                return int(parts[1])
            except ValueError:
                return None
    return None


def read_cgroup(
    root: Path = _CGROUP_ROOT,
) -> tuple[CgroupReading | None, Literal["v1", "v2"] | None]:
    """Detect the cgroup version present at ``root`` and read its files.

    Single entry point preferred by the snapshotter — returns the
    parsed reading + the version tag in one call.

    Args:
        root: cgroup mount root. Default ``/sys/fs/cgroup``. The default
            uses the module-level path to keep the public API stable
            even if tests want to inject a temporary directory via the
            argument.

    Returns:
        ``(CgroupReading | None, version | None)``. Both ``None`` when no
        cgroup hierarchy is detected (e.g. macOS dev host).
    """
    version = detect_cgroup_version(root)
    if version == "v2":
        return read_cgroup_v2(root), "v2"
    if version == "v1":
        return read_cgroup_v1(root), "v1"
    return None, None


def _check_cgroup_paths_readable() -> bool:
    """Sanity check used by tests to verify the default cgroup root is readable.

    Allows the test for "non-Linux dev host" to skip cleanly when the
    test machine actually has cgroup mounted (e.g. CI Linux runners).
    """
    return os.access(str(_CGROUP_ROOT), os.R_OK)
