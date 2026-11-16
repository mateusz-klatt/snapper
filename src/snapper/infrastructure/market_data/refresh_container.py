"""Pure immutable container specification and externally observed exit admission.

This module performs no filesystem, Docker or database operations. The caller
must supply independently observed private-path and local-image facts, create
the exact container, bind its returned ID and cgroup before start, and enforce
the inherited host deadline. Docker create/start latency is not covered by a
relative timeout inside the container. The timeout is PID 1 in a private PID
namespace; its expiry is an independent last resort, never proof of database
rollback or cessation. A successful admission requires an external observation
that the exact captured container cgroup is gone, not a successful kill command.

The Docker driver, immutable source staging, bootstrap source-digest check and
PostgreSQL uncertain-commit/cessation witnesses are separate unimplemented
activation prerequisites. No function here certifies a hard 45-second deadline,
financial readiness or production activation.
"""

import math
import re
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Literal
from uuid import UUID

from pydantic import BaseModel
from pydantic import ConfigDict

from snapper.core.market_snapshot_refresh_budget import RefreshBudget
from snapper.core.market_snapshot_refresh_scope import normalize_refresh_fx_exchanges
from snapper.core.market_snapshot_refresh_scope import normalize_refresh_instrument_ids

CLEANUP_RESERVE_SECONDS = 5.0
_DOCKER_EXECUTABLE = "/usr/bin/docker"
_TIMEOUT_EXECUTABLE = "/usr/bin/timeout"
_SOURCE_DESTINATION = "/snapshot-source"
_OUTPUT_DESTINATION = "/snapshot-output"
_TMPFS_OPTIONS = "rw,nosuid,nodev,noexec,size=67108864,mode=1777"
_MEMORY_BYTES = 1073741824
_NANO_CPUS = 2000000000
_PIDS_LIMIT = 128


class RefreshContainerError(ValueError):
    """A fixed admission refusal without external commands, credentials or payloads."""


class _FrozenModel(BaseModel):
    """Reject coercion, extra fields and mutation in externally supplied evidence."""

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True, allow_inf_nan=False)


class RefreshContainerScope(_FrozenModel):
    """Explicit selected identities, FX venues and the reviewed metadata digest."""

    instrument_public_ids: tuple[str, ...]
    fx_exchanges: tuple[str, ...]
    expected_scope_digest: str


class RefreshContainerPaths(_FrozenModel):
    """Explicit private host paths and an independently verified in-image executable."""

    env_file: PurePosixPath
    output_directory: PurePosixPath
    source_directory: PurePosixPath
    bootstrap_executable: PurePosixPath


class RefreshContainerConfig(_FrozenModel):
    """A pinned local image and invocation without a mutable tag or implicit scope."""

    image_id: str
    host_invocation_id: str
    expected_source_digest: str
    scope: RefreshContainerScope
    paths: RefreshContainerPaths
    network: str
    user_id: int
    group_id: int


class RefreshPrivatePathEvidence(_FrozenModel):
    """Driver-observed path facts, including symlink-free ancestry and exact mode."""

    path: PurePosixPath
    kind: Literal["file", "directory"]
    owner_uid: int
    mode: int
    symlink_free: bool


class RefreshContainerPreflight(_FrozenModel):
    """External preflight observations that this pure specification cannot establish."""

    image_id: str
    host_invocation_id: str
    source_digest: str
    env_file: RefreshPrivatePathEvidence
    output_directory: RefreshPrivatePathEvidence
    source_directory: RefreshPrivatePathEvidence
    local_daemon: bool
    same_monotonic_clock: bool
    immutable_source_binding_verified: bool
    image_has_no_declared_volumes: bool
    bootstrap_executable_verified: bool
    timeout_executable_verified: bool
    mountpoints_verified: bool
    container_user_access_verified: bool
    invocation_name_available: bool
    output_empty: bool


class RefreshContainerBind(_FrozenModel):
    """One exact observed host bind without application-volume inheritance."""

    source: str
    destination: str
    read_only: bool
    propagation: str


class RefreshContainerIsolation(_FrozenModel):
    """Lossless inspected isolation fields compared with the fixed launch policy."""

    pid_mode: str
    cgroup_namespace_mode: str
    ipc_mode: str
    network_mode: str
    user: str
    read_only_root: bool
    healthcheck_disabled: bool
    init_enabled: bool
    privileged: bool
    auto_remove: bool
    restart_policy: str
    cap_drop: tuple[str, ...]
    security_options: tuple[str, ...]
    memory_bytes: int
    memory_swap_bytes: int
    nano_cpus: int
    pids_limit: int
    binds: tuple[RefreshContainerBind, ...]
    tmpfs: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class RefreshContainerLaunch:
    """Immutable create argv plus the exact invocation, command and inherited deadlines."""

    config: RefreshContainerConfig
    name: str
    create_argv: tuple[str, ...]
    entrypoint: tuple[str, ...]
    command: tuple[str, ...]
    isolation: RefreshContainerIsolation
    prepared_at_monotonic: float
    work_deadline_monotonic: float
    deadline_monotonic: float


class RefreshContainerIdentity(_FrozenModel):
    """Exact owned daemon identity and host cgroup captured before container execution."""

    host_invocation_id: str
    container_id: str
    cgroup_path: PurePosixPath
    private_pid_namespace_verified: bool


class RefreshContainerInspection(_FrozenModel):
    """Sanitized exact-container inspection after its worker has exited."""

    container_id: str
    image_id: str
    name: str
    entrypoint: tuple[str, ...]
    command: tuple[str, ...]
    isolation: RefreshContainerIsolation
    status: str
    pid: int
    exit_code: int
    oom_killed: bool
    started: bool
    finished: bool


class RefreshContainerCessation(_FrozenModel):
    """External observation of the exact previously captured cgroup after exit."""

    container_id: str
    cgroup_path: PurePosixPath
    cgroup_gone: bool
    observed_at_monotonic: float


@dataclass(frozen=True, slots=True)
class RefreshContainerExitReceipt:
    """Price-free identity receipt for admitted container exit, not database settlement."""

    host_invocation_id: str
    container_id: str
    image_id: str
    source_digest: str
    scope_digest: str
    cgroup_gone_observed_at_monotonic: float


def _digest(value: str) -> bool:
    """Recognize one exact lowercase SHA256 digest without normalization.

    Args:
        value: Claimed immutable digest text.

    Returns:
        Whether the value has exactly the expected hexadecimal representation.
    """
    return re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _absolute_path(path: PurePosixPath) -> bool:
    """Recognize an unambiguous non-root Linux path safe for Docker mount arguments.

    Args:
        path: Explicit host or in-image path.

    Returns:
        Whether no traversal, CSV delimiter or control character is present.
    """
    text = str(path)
    return (
        path.is_absolute()
        and text != "/"
        and not text.startswith("//")
        and ".." not in path.parts
        and not any(character in text for character in (",", "\x00", "\n", "\r"))
    )


def _validated_config(config: RefreshContainerConfig) -> RefreshContainerConfig:
    """Normalize only explicit scope and reject mutable or ambiguous launch configuration.

    Args:
        config: Proposed selected container configuration.

    Returns:
        An equivalent configuration with canonical UUIDs and deduplicated scope.

    Raises:
        RefreshContainerError: If identity, scope, paths or isolation inputs are unusable.
    """
    if not config.image_id.startswith("sha256:") or not _digest(config.image_id[7:]):
        raise RefreshContainerError("immutable_local_image_required")
    if not _digest(config.expected_source_digest) or not _digest(
        config.scope.expected_scope_digest
    ):
        raise RefreshContainerError("invalid_source_or_scope_digest")
    try:
        invocation = str(UUID(config.host_invocation_id))
        instruments = normalize_refresh_instrument_ids(config.scope.instrument_public_ids)
        exchanges = normalize_refresh_fx_exchanges(config.scope.fx_exchanges)
    except ValueError:
        raise RefreshContainerError("invalid_explicit_container_scope") from None
    if invocation != config.host_invocation_id:
        raise RefreshContainerError("canonical_invocation_uuid_required")
    if (
        re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", config.network) is None
        or config.network in {"host", "none", "bridge", "default"}
        or config.user_id <= 0
        or config.group_id <= 0
    ):
        raise RefreshContainerError("explicit_private_container_identity_required")
    _validate_paths(config.paths)
    scope = RefreshContainerScope(
        instrument_public_ids=instruments,
        fx_exchanges=exchanges,
        expected_scope_digest=config.scope.expected_scope_digest,
    )
    return config.model_copy(update={"scope": scope})


def _validate_paths(paths: RefreshContainerPaths) -> None:
    """Reject ambiguous or overlapping private bind paths before building any argv.

    Args:
        paths: Explicit source, output, credential-file and executable locations.

    Raises:
        RefreshContainerError: If path spelling or host-path overlap is unsafe.
    """
    all_paths = (
        paths.env_file,
        paths.output_directory,
        paths.source_directory,
        paths.bootstrap_executable,
    )
    if not all(_absolute_path(path) for path in all_paths):
        raise RefreshContainerError("absolute_unambiguous_paths_required")
    host_paths = all_paths[:3]
    if any(
        left.is_relative_to(right)
        for index, left in enumerate(host_paths)
        for other, right in enumerate(host_paths)
        if index != other
    ):
        raise RefreshContainerError("private_bind_paths_must_not_overlap")
    if paths.bootstrap_executable.is_relative_to(
        _OUTPUT_DESTINATION
    ) or paths.bootstrap_executable.is_relative_to("/tmp"):
        raise RefreshContainerError("bootstrap_must_not_be_in_writable_mount")


def _admit_path(
    observed: RefreshPrivatePathEvidence, expected: PurePosixPath, kind: str, owner_uid: int
) -> None:
    """Compare driver-observed private path facts without inspecting the filesystem.

    Args:
        observed: No-follow observation supplied by the external owner.
        expected: Exact configured path.
        kind: Required file or directory kind.
        owner_uid: Non-root host and container owner of the private path.

    Raises:
        RefreshContainerError: If any privacy or identity fact is unproven.
    """
    mode = 0o600 if kind == "file" else 0o700
    if (
        observed.path != expected
        or observed.kind != kind
        or observed.owner_uid != owner_uid
        or observed.mode != mode
        or not observed.symlink_free
    ):
        raise RefreshContainerError("private_path_admission_failed")


def _admit_preflight(config: RefreshContainerConfig, evidence: RefreshContainerPreflight) -> None:
    """Require verified provenance and fresh private bindings before constructing a launch.

    Args:
        config: Validated immutable selected configuration.
        evidence: External local-daemon, source, image and filesystem observations.

    Raises:
        RefreshContainerError: If any required observation is false or mismatched.
    """
    verified = (
        evidence.local_daemon,
        evidence.same_monotonic_clock,
        evidence.immutable_source_binding_verified,
        evidence.image_has_no_declared_volumes,
        evidence.bootstrap_executable_verified,
        evidence.timeout_executable_verified,
        evidence.mountpoints_verified,
        evidence.container_user_access_verified,
        evidence.invocation_name_available,
        evidence.output_empty,
    )
    if (
        not all(verified)
        or evidence.image_id != config.image_id
        or evidence.host_invocation_id != config.host_invocation_id
        or evidence.source_digest != config.expected_source_digest
    ):
        raise RefreshContainerError("unverified_container_provenance")
    _admit_path(evidence.env_file, config.paths.env_file, "file", config.user_id)
    _admit_path(
        evidence.output_directory, config.paths.output_directory, "directory", config.user_id
    )
    _admit_path(
        evidence.source_directory, config.paths.source_directory, "directory", config.user_id
    )


def _isolation(config: RefreshContainerConfig) -> RefreshContainerIsolation:
    """Describe the exact inspectable policy applied by the create command.

    Args:
        config: Validated selected configuration.

    Returns:
        Fixed resource bounds and exactly two permitted host bind mounts.
    """
    return RefreshContainerIsolation(
        pid_mode="",
        cgroup_namespace_mode="private",
        ipc_mode="private",
        network_mode=config.network,
        user=f"{config.user_id}:{config.group_id}",
        read_only_root=True,
        healthcheck_disabled=True,
        init_enabled=False,
        privileged=False,
        auto_remove=False,
        restart_policy="no",
        cap_drop=("ALL",),
        security_options=("no-new-privileges",),
        memory_bytes=_MEMORY_BYTES,
        memory_swap_bytes=_MEMORY_BYTES,
        nano_cpus=_NANO_CPUS,
        pids_limit=_PIDS_LIMIT,
        binds=(
            RefreshContainerBind(
                source=str(config.paths.source_directory),
                destination=_SOURCE_DESTINATION,
                read_only=True,
                propagation="rprivate",
            ),
            RefreshContainerBind(
                source=str(config.paths.output_directory),
                destination=_OUTPUT_DESTINATION,
                read_only=False,
                propagation="rprivate",
            ),
        ),
        tmpfs=(("/tmp", _TMPFS_OPTIONS),),
    )


def _worker_command(
    config: RefreshContainerConfig, work_deadline: float, duration_ms: int
) -> tuple[str, ...]:
    """Construct the timeout PID-1 command without credentials or arbitrary extra options.

    Args:
        config: Validated explicit source and selected scope.
        work_deadline: Absolute inherited work expiry before the cleanup reserve.
        duration_ms: Rounded-down remaining independent timeout duration.

    Returns:
        Explicit arguments for the overridden timeout entrypoint and bootstrap.
    """
    command = [
        "--signal=KILL",
        "--",
        f"{duration_ms / 1000:.3f}s",
        str(config.paths.bootstrap_executable),
        "--deadline-monotonic",
        repr(work_deadline),
        "--host-invocation-id",
        config.host_invocation_id,
        "--scope-digest",
        config.scope.expected_scope_digest,
        "--source-digest",
        config.expected_source_digest,
        "--output",
        _OUTPUT_DESTINATION,
    ]
    for identity in config.scope.instrument_public_ids:
        command.extend(("--instrument-public-id", identity))
    for exchange in config.scope.fx_exchanges:
        command.extend(("--fx-exchange", exchange))
    return tuple(command)


def _create_argv(
    config: RefreshContainerConfig, name: str, command: tuple[str, ...]
) -> tuple[str, ...]:
    """Build only a create command, preserving the container for exact-ID inspection.

    Args:
        config: Validated pinned image and private bindings.
        name: Invocation-specific unused container name.
        command: Exact timeout and bootstrap arguments.

    Returns:
        Immutable Docker argv with no shell, tag resolution or application volumes.
    """
    return (
        _DOCKER_EXECUTABLE,
        "container",
        "create",
        "--pull=never",
        "--name",
        name,
        "--pid=",
        "--cgroupns=private",
        "--ipc=private",
        "--init=false",
        "--no-healthcheck",
        "--read-only",
        "--cap-drop=ALL",
        "--security-opt=no-new-privileges",
        "--restart=no",
        "--cpus=2",
        f"--memory={_MEMORY_BYTES}",
        f"--memory-swap={_MEMORY_BYTES}",
        f"--pids-limit={_PIDS_LIMIT}",
        "--network",
        config.network,
        "--user",
        f"{config.user_id}:{config.group_id}",
        "--env-file",
        str(config.paths.env_file),
        "--tmpfs",
        f"/tmp:{_TMPFS_OPTIONS}",
        "--mount",
        f"type=bind,src={config.paths.source_directory},dst={_SOURCE_DESTINATION},readonly,bind-propagation=rprivate",
        "--mount",
        f"type=bind,src={config.paths.output_directory},dst={_OUTPUT_DESTINATION},bind-propagation=rprivate",
        "--workdir",
        _SOURCE_DESTINATION,
        "--entrypoint",
        _TIMEOUT_EXECUTABLE,
        config.image_id,
        *command,
    )


def build_refresh_container_launch(
    config: RefreshContainerConfig, budget: RefreshBudget, preflight: RefreshContainerPreflight
) -> RefreshContainerLaunch:
    """Admit a pure exact launch specification under one existing host budget.

    Args:
        config: Explicit immutable container and scope configuration.
        budget: Host-owned absolute monotonic deadline, never renewed here.
        preflight: Independently obtained provenance and private-path observations.

    Returns:
        Create argv, exact command and isolation expectations for an external driver.

    Raises:
        RefreshContainerError: If configuration or external preflight is unproven.
        TimeoutError: If no usable work time remains before cleanup.
    """
    admitted = _validated_config(config)
    _admit_preflight(admitted, preflight)
    remaining = budget.remaining(reserve=CLEANUP_RESERVE_SECONDS)
    duration_ms = math.floor(remaining * 1000)
    if duration_ms < 1:
        raise TimeoutError("Refresh container work budget exhausted")
    prepared_at = budget.deadline - CLEANUP_RESERVE_SECONDS - remaining
    work_deadline = budget.deadline - CLEANUP_RESERVE_SECONDS
    name = f"snapper-refresh-{UUID(admitted.host_invocation_id).hex}"
    command = _worker_command(admitted, work_deadline, duration_ms)
    return RefreshContainerLaunch(
        admitted,
        name,
        _create_argv(admitted, name, command),
        (_TIMEOUT_EXECUTABLE,),
        command,
        _isolation(admitted),
        prepared_at,
        work_deadline,
        budget.deadline,
    )


def _admit_identity(
    launch: RefreshContainerLaunch,
    owned: RefreshContainerIdentity,
    observed: RefreshContainerInspection,
) -> None:
    """Bind inspected exit to the independently captured container identity and policy.

    Args:
        launch: Admitted original create specification.
        owned: Exact creation and namespace ownership evidence.
        observed: Sanitized post-exit Docker inspection.

    Raises:
        RefreshContainerError: If identity, command or isolation differs from the plan.
    """
    if (
        not _digest(owned.container_id)
        or owned.host_invocation_id != launch.config.host_invocation_id
        or observed.container_id != owned.container_id
        or observed.image_id != launch.config.image_id
        or observed.name != f"/{launch.name}"
        or not owned.private_pid_namespace_verified
        or not _absolute_path(owned.cgroup_path)
    ):
        raise RefreshContainerError("container_identity_mismatch")
    if (
        observed.entrypoint != launch.entrypoint
        or observed.command != launch.command
        or observed.isolation != launch.isolation
    ):
        raise RefreshContainerError("container_launch_policy_mismatch")


def admit_refresh_container_exit(
    launch: RefreshContainerLaunch,
    owned: RefreshContainerIdentity,
    observed: RefreshContainerInspection,
    cessation: RefreshContainerCessation,
    budget: RefreshBudget,
) -> RefreshContainerExitReceipt:
    """Admit exact clean exit only with separate captured-cgroup disappearance evidence.

    Args:
        launch: Original admitted immutable create specification.
        owned: Exact container and cgroup identity captured before execution.
        observed: Post-exit exact-container inspection without environment or prices.
        cessation: Independent host observation that the captured cgroup is gone.
        budget: The same absolute host budget used to construct the launch.

    Returns:
        A price-free container exit receipt, never a database settlement claim.

    Raises:
        RefreshContainerError: If any identity, isolation, exit or cessation fact fails.
        TimeoutError: If admission reaches the inherited host deadline.
    """
    remaining = budget.remaining()
    now = budget.deadline - remaining
    if budget.deadline != launch.deadline_monotonic:
        raise RefreshContainerError("container_deadline_changed")
    _admit_identity(launch, owned, observed)
    if (
        observed.status != "exited"
        or observed.pid != 0
        or observed.exit_code != 0
        or observed.oom_killed
        or not observed.started
        or not observed.finished
    ):
        raise RefreshContainerError("container_exit_not_successful")
    if (
        cessation.container_id != owned.container_id
        or cessation.cgroup_path != owned.cgroup_path
        or not cessation.cgroup_gone
        or not launch.prepared_at_monotonic <= cessation.observed_at_monotonic <= now
    ):
        raise RefreshContainerError("container_cessation_unproven")
    return RefreshContainerExitReceipt(
        launch.config.host_invocation_id,
        owned.container_id,
        launch.config.image_id,
        launch.config.expected_source_digest,
        launch.config.scope.expected_scope_digest,
        cessation.observed_at_monotonic,
    )
