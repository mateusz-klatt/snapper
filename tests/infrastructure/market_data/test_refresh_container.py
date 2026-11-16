"""Pure witnesses for immutable container launch and exact post-exit admission."""

from dataclasses import dataclass
from pathlib import PurePosixPath

import pytest
from pydantic import ValidationError

from snapper.core.market_snapshot_refresh_budget import RefreshBudget
from snapper.infrastructure.market_data.refresh_container import RefreshContainerCessation
from snapper.infrastructure.market_data.refresh_container import RefreshContainerConfig
from snapper.infrastructure.market_data.refresh_container import RefreshContainerError
from snapper.infrastructure.market_data.refresh_container import RefreshContainerIdentity
from snapper.infrastructure.market_data.refresh_container import RefreshContainerInspection
from snapper.infrastructure.market_data.refresh_container import RefreshContainerLaunch
from snapper.infrastructure.market_data.refresh_container import RefreshContainerPaths
from snapper.infrastructure.market_data.refresh_container import RefreshContainerPreflight
from snapper.infrastructure.market_data.refresh_container import RefreshContainerScope
from snapper.infrastructure.market_data.refresh_container import RefreshPrivatePathEvidence
from snapper.infrastructure.market_data.refresh_container import admit_refresh_container_exit
from snapper.infrastructure.market_data.refresh_container import build_refresh_container_launch

_INVOCATION = "11111111-2222-4333-8444-555555555555"
_INSTRUMENT = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
_CONTAINER_ID = "c" * 64


@dataclass
class _Clock:
    """Controllable monotonic clock with no sleeping or process operations."""

    now: float = 100.0

    def __call__(self) -> float:
        """Read the synthetic current instant.

        Returns:
            The configured monotonic instant.
        """
        return self.now


def _config() -> RefreshContainerConfig:
    """Build explicit synthetic immutable configuration without reading host paths.

    Returns:
        One selected Kraken invocation and private independent host paths.
    """
    return RefreshContainerConfig(
        image_id="sha256:" + "a" * 64,
        host_invocation_id=_INVOCATION,
        expected_source_digest="b" * 64,
        scope=RefreshContainerScope(
            instrument_public_ids=(_INSTRUMENT,),
            fx_exchanges=("kraken",),
            expected_scope_digest="d" * 64,
        ),
        paths=RefreshContainerPaths(
            env_file=PurePosixPath("/private/config/runtime.env"),
            output_directory=PurePosixPath("/private/output"),
            source_directory=PurePosixPath("/private/source"),
            bootstrap_executable=PurePosixPath("/usr/local/bin/snapshot-bootstrap"),
        ),
        network="snapper-test-internal",
        user_id=1000,
        group_id=1000,
    )


def _preflight(config: RefreshContainerConfig) -> RefreshContainerPreflight:
    """Supply explicitly synthetic already-verified external facts for pure tests.

    Args:
        config: Expected image, source and private-path identities.

    Returns:
        Complete evidence without performing Docker, filesystem or database operations.
    """
    return RefreshContainerPreflight(
        image_id=config.image_id,
        host_invocation_id=config.host_invocation_id,
        source_digest=config.expected_source_digest,
        env_file=RefreshPrivatePathEvidence(
            path=config.paths.env_file, kind="file", owner_uid=1000, mode=0o600, symlink_free=True
        ),
        output_directory=RefreshPrivatePathEvidence(
            path=config.paths.output_directory,
            kind="directory",
            owner_uid=1000,
            mode=0o700,
            symlink_free=True,
        ),
        source_directory=RefreshPrivatePathEvidence(
            path=config.paths.source_directory,
            kind="directory",
            owner_uid=1000,
            mode=0o700,
            symlink_free=True,
        ),
        local_daemon=True,
        same_monotonic_clock=True,
        immutable_source_binding_verified=True,
        image_has_no_declared_volumes=True,
        bootstrap_executable_verified=True,
        timeout_executable_verified=True,
        mountpoints_verified=True,
        container_user_access_verified=True,
        invocation_name_available=True,
        output_empty=True,
    )


@dataclass(frozen=True)
class _Context:
    """Synthetic owned launch, observations and unchanged absolute budget."""

    launch: RefreshContainerLaunch
    owned: RefreshContainerIdentity
    observed: RefreshContainerInspection
    cessation: RefreshContainerCessation
    budget: RefreshBudget
    clock: _Clock


@pytest.fixture
def context() -> _Context:
    """Build complete successful pure evidence without executing a container.

    Returns:
        One invocation whose exact cgroup disappearance was observed before expiry.
    """
    config = _config()
    clock = _Clock()
    budget = RefreshBudget(145.0, clock)
    launch = build_refresh_container_launch(config, budget, _preflight(config))
    cgroup = PurePosixPath(f"/sys/fs/cgroup/system.slice/docker-{_CONTAINER_ID}.scope")
    owned = RefreshContainerIdentity(
        host_invocation_id=_INVOCATION,
        container_id=_CONTAINER_ID,
        cgroup_path=cgroup,
        private_pid_namespace_verified=True,
    )
    observed = RefreshContainerInspection(
        container_id=_CONTAINER_ID,
        image_id=config.image_id,
        name=f"/{launch.name}",
        entrypoint=launch.entrypoint,
        command=launch.command,
        isolation=launch.isolation,
        status="exited",
        pid=0,
        exit_code=0,
        oom_killed=False,
        started=True,
        finished=True,
    )
    cessation = RefreshContainerCessation(
        container_id=_CONTAINER_ID,
        cgroup_path=cgroup,
        cgroup_gone=True,
        observed_at_monotonic=119.0,
    )
    clock.now = 120.0
    return _Context(launch, owned, observed, cessation, budget, clock)


def test_launch_pins_exact_image_command_scope_and_private_resource_policy(
    context: _Context,
) -> None:
    """Produce an isolated create specification without invoking any runtime.

    Given: Exact local image and source provenance and private-path facts are verified.
    When: The pure builder receives one existing forty-five-second absolute budget.
    Then: Its immutable create argv admits only the selected scope and fixed isolation.
    """
    launch = context.launch
    argv = launch.create_argv

    assert argv[:3] == ("/usr/bin/docker", "container", "create")
    assert "--pull=never" in argv
    assert argv[argv.index("--name") + 1] == "snapper-refresh-11111111222243338444555555555555"
    assert argv[argv.index("--entrypoint") + 1] == "/usr/bin/timeout"
    assert launch.entrypoint == ("/usr/bin/timeout",)
    assert launch.command[:4] == (
        "--signal=KILL",
        "--",
        "40.000s",
        "/usr/local/bin/snapshot-bootstrap",
    )
    assert launch.command[launch.command.index("--deadline-monotonic") + 1] == "140.0"
    assert launch.command[launch.command.index("--instrument-public-id") + 1] == _INSTRUMENT
    assert launch.command[launch.command.index("--fx-exchange") + 1] == "kraken"
    assert launch.command[launch.command.index("--source-digest") + 1] == "b" * 64
    assert launch.command[launch.command.index("--scope-digest") + 1] == "d" * 64
    assert argv[argv.index("--entrypoint") + 2] == launch.config.image_id
    assert argv.count("--mount") == 2
    assert not {"--rm", "--volumes-from", "--privileged", "--pid=host"}.intersection(argv)
    assert {
        "--pid=",
        "--cgroupns=private",
        "--no-healthcheck",
        "--init=false",
        "--read-only",
        "--cap-drop=ALL",
        "--security-opt=no-new-privileges",
        "--restart=no",
    }.issubset(argv)
    assert {
        "--cpus=2",
        "--memory=1073741824",
        "--memory-swap=1073741824",
        "--pids-limit=128",
    }.issubset(argv)
    assert launch.isolation.pid_mode == ""
    assert launch.isolation.memory_bytes == launch.isolation.memory_swap_bytes
    assert launch.isolation.binds[0].read_only
    assert not launch.isolation.binds[1].read_only
    assert (
        launch.prepared_at_monotonic,
        launch.work_deadline_monotonic,
        launch.deadline_monotonic,
    ) == (100.0, 140.0, 145.0)


def test_rebuilding_spec_uses_remaining_original_budget_without_reset(context: _Context) -> None:
    """Preserve the absolute deadline when prelaunch work has already consumed time.

    Given: Twenty seconds of the original budget elapsed before rebuilding argv.
    When: The builder receives the same budget and unchanged explicit configuration.
    Then: The independent timeout shrinks while absolute work and final deadlines remain fixed.
    """
    config = context.launch.config
    later = build_refresh_container_launch(config, context.budget, _preflight(config))

    assert later.command[2] == "20.000s"
    assert later.work_deadline_monotonic == context.launch.work_deadline_monotonic
    assert later.deadline_monotonic == context.launch.deadline_monotonic


@pytest.mark.parametrize("now", [140.0, 140.1, 139.9995])
def test_launch_refuses_exhausted_or_submillisecond_work_budget(now: float) -> None:
    """Retain the cleanup reserve rather than granting a replacement runtime budget.

    Given: Less than one millisecond of usable work remains before cleanup.
    When: The builder derives the timeout from the existing absolute deadline.
    Then: Launch specification fails with timeout.
    """
    config = _config()
    with pytest.raises(TimeoutError):
        build_refresh_container_launch(
            config, RefreshBudget(145.0, _Clock(now)), _preflight(config)
        )


@pytest.mark.parametrize(
    "image",
    ["latest", "snapper:release", "sha256:short", "sha256:" + "A" * 64, "sha256:" + "g" * 64],
)
def test_mutable_or_malformed_image_references_are_rejected(image: str) -> None:
    """Require the full local content identity rather than resolving a tag later.

    Given: A requested image is a tag or malformed SHA256 reference.
    When: The pure builder validates configuration.
    Then: It refuses before constructing a launch command.
    """
    config = _config().model_copy(update={"image_id": image})
    with pytest.raises(RefreshContainerError, match="immutable_local_image_required"):
        build_refresh_container_launch(config, RefreshBudget(145.0, _Clock()), _preflight(config))


@pytest.mark.parametrize("field", ["source", "scope"])
def test_source_and_scope_digests_are_mandatory(field: str) -> None:
    """Bind both installed source and reviewed selected metadata before launch.

    Given: Either required provenance digest is malformed.
    When: Configuration is admitted.
    Then: The builder rejects the incomplete source or scope binding.
    """
    config = _config()
    if field == "source":
        config = config.model_copy(update={"expected_source_digest": "not-a-digest"})
    else:
        scope = config.scope.model_copy(update={"expected_scope_digest": "not-a-digest"})
        config = config.model_copy(update={"scope": scope})

    with pytest.raises(RefreshContainerError, match="invalid_source_or_scope_digest"):
        build_refresh_container_launch(config, RefreshBudget(145.0, _Clock()), _preflight(config))


@pytest.mark.parametrize(
    "scope",
    [
        RefreshContainerScope(
            instrument_public_ids=(), fx_exchanges=("kraken",), expected_scope_digest="d" * 64
        ),
        RefreshContainerScope(
            instrument_public_ids=("invalid",),
            fx_exchanges=("kraken",),
            expected_scope_digest="d" * 64,
        ),
        RefreshContainerScope(
            instrument_public_ids=(_INSTRUMENT,), fx_exchanges=(), expected_scope_digest="d" * 64
        ),
    ],
)
def test_missing_explicit_scope_never_means_all(scope: RefreshContainerScope) -> None:
    """Prevent implicit instrument or FX-venue expansion.

    Given: Main identities or FX selection are empty or malformed.
    When: The launch builder normalizes explicit scope.
    Then: It rejects configuration rather than choosing defaults.
    """
    config = _config().model_copy(update={"scope": scope})
    with pytest.raises(RefreshContainerError, match="invalid_explicit_container_scope"):
        build_refresh_container_launch(config, RefreshBudget(145.0, _Clock()), _preflight(config))


@pytest.mark.parametrize("invocation", ["invalid", _INVOCATION.replace("-", "")])
def test_invocation_requires_an_exact_canonical_uuid(invocation: str) -> None:
    """Bind container naming to one explicit canonical invocation identity.

    Given: The invocation is malformed or uses a noncanonical UUID spelling.
    When: The builder validates its unique name input.
    Then: It refuses instead of guessing or silently changing the invocation.
    """
    config = _config().model_copy(update={"host_invocation_id": invocation})
    with pytest.raises(RefreshContainerError):
        build_refresh_container_launch(config, RefreshBudget(145.0, _Clock()), _preflight(config))


@pytest.mark.parametrize(
    "updates",
    [
        {"network": "host"},
        {"network": "none"},
        {"network": "bridge"},
        {"network": ""},
        {"network": "container:other"},
        {"user_id": 0},
        {"group_id": 0},
    ],
)
def test_private_network_and_nonroot_identity_are_required(updates: dict[str, object]) -> None:
    """Prevent implicit networking or an unintended root container identity.

    Given: The proposed network or numeric process identity violates fixed isolation.
    When: Configuration is admitted.
    Then: The builder rejects the configuration before producing argv.
    """
    config = _config().model_copy(update=updates)
    with pytest.raises(RefreshContainerError, match="explicit_private_container_identity_required"):
        build_refresh_container_launch(config, RefreshBudget(145.0, _Clock()), _preflight(config))


@pytest.mark.parametrize(
    "path",
    [
        "relative",
        "/",
        "//private/source",
        "/private/../source",
        "/private/a,b",
        "/private/a\x00b",
        "/private/a\nb",
        "/private/a\rb",
    ],
)
def test_ambiguous_host_paths_are_rejected(path: str) -> None:
    """Keep bind arguments absolute and structurally unambiguous without shell escaping.

    Given: A host source path is relative, traversing or contains a mount delimiter.
    When: The builder validates private path spelling.
    Then: It refuses the path before any runtime operation.
    """
    config = _config()
    paths = config.paths.model_copy(update={"source_directory": PurePosixPath(path)})
    config = config.model_copy(update={"paths": paths})
    with pytest.raises(RefreshContainerError, match="absolute_unambiguous_paths_required"):
        build_refresh_container_launch(config, RefreshBudget(145.0, _Clock()), _preflight(config))


@pytest.mark.parametrize(
    "updates",
    [
        {"env_file": PurePosixPath("/private/source/runtime.env")},
        {"output_directory": PurePosixPath("/private/source")},
        {"bootstrap_executable": PurePosixPath("/tmp/bootstrap")},
        {"bootstrap_executable": PurePosixPath("/snapshot-output/bootstrap")},
    ],
)
def test_bind_overlap_and_writable_bootstrap_are_rejected(updates: dict[str, object]) -> None:
    """Keep credentials out of source mounts and executable code out of writable output.

    Given: Host bindings overlap or the bootstrap resides in a writable mount.
    When: The builder checks path relationships.
    Then: It refuses the inconsistent mount or executable plan.
    """
    config = _config()
    config = config.model_copy(update={"paths": config.paths.model_copy(update=updates)})
    with pytest.raises(RefreshContainerError):
        build_refresh_container_launch(config, RefreshBudget(145.0, _Clock()), _preflight(config))


@pytest.mark.parametrize(
    "field",
    [
        "local_daemon",
        "same_monotonic_clock",
        "immutable_source_binding_verified",
        "image_has_no_declared_volumes",
        "bootstrap_executable_verified",
        "timeout_executable_verified",
        "mountpoints_verified",
        "container_user_access_verified",
        "invocation_name_available",
        "output_empty",
    ],
)
def test_unverified_preflight_facts_refuse_the_launch(field: str) -> None:
    """Require observed admission facts rather than optimistic defaults.

    Given: One required provenance, namespace, executable or output fact is false.
    When: The builder checks the external preflight receipt.
    Then: It refuses to issue an admitted launch specification.
    """
    config = _config()
    evidence = _preflight(config).model_copy(update={field: False})
    with pytest.raises(RefreshContainerError, match="unverified_container_provenance"):
        build_refresh_container_launch(config, RefreshBudget(145.0, _Clock()), evidence)


@pytest.mark.parametrize("field", ["image_id", "source_digest", "host_invocation_id"])
def test_preflight_identity_must_match_this_invocation(field: str) -> None:
    """Reject evidence obtained for another image, source tree or invocation.

    Given: A preflight receipt disagrees with one configured immutable identity.
    When: The builder compares that receipt with this launch.
    Then: It rejects the mismatched provenance.
    """
    config = _config()
    evidence = _preflight(config).model_copy(update={field: "different"})
    with pytest.raises(RefreshContainerError, match="unverified_container_provenance"):
        build_refresh_container_launch(config, RefreshBudget(145.0, _Clock()), evidence)


@pytest.mark.parametrize(
    "updates",
    [
        {"path": PurePosixPath("/wrong")},
        {"kind": "directory"},
        {"owner_uid": 2000},
        {"mode": 0o644},
        {"symlink_free": False},
    ],
)
def test_env_path_requires_exact_private_nofollow_evidence(updates: dict[str, object]) -> None:
    """Keep credential-file privacy an explicit externally verified prerequisite.

    Given: The environment-file path, type, owner, mode or ancestry is unproven.
    When: The builder compares the observed path facts.
    Then: It rejects the path without reading or logging credential contents.
    """
    config = _config()
    evidence = _preflight(config)
    evidence = evidence.model_copy(
        update={"env_file": evidence.env_file.model_copy(update=updates)}
    )
    with pytest.raises(RefreshContainerError, match="private_path_admission_failed"):
        build_refresh_container_launch(config, RefreshBudget(145.0, _Clock()), evidence)


def test_successful_exit_requires_exact_external_cgroup_receipt(context: _Context) -> None:
    """Admit clean exact-container exit without claiming database settlement.

    Given: Exact identity, command and isolation match and the captured cgroup is gone.
    When: Admission runs within the original host deadline.
    Then: It returns only the bound invocation, image, source, scope and cessation evidence.
    """
    receipt = admit_refresh_container_exit(
        context.launch, context.owned, context.observed, context.cessation, context.budget
    )

    assert receipt.host_invocation_id == _INVOCATION
    assert receipt.container_id == _CONTAINER_ID
    assert receipt.image_id == context.launch.config.image_id
    assert receipt.source_digest == context.launch.config.expected_source_digest
    assert receipt.scope_digest == context.launch.config.scope.expected_scope_digest
    assert receipt.cgroup_gone_observed_at_monotonic == 119.0


@pytest.mark.parametrize(
    "updates",
    [
        {"container_id": "short"},
        {"host_invocation_id": "wrong"},
        {"private_pid_namespace_verified": False},
        {"cgroup_path": PurePosixPath("relative")},
    ],
)
def test_post_exit_requires_original_owned_container_identity(
    context: _Context, updates: dict[str, object]
) -> None:
    """Do not adopt a container identity merely because inspection reports success.

    Given: The independently captured creation or namespace identity is invalid.
    When: Exit admission binds the inspection to that owned identity.
    Then: It refuses the mismatched or unproven ownership.
    """
    owned = context.owned.model_copy(update=updates)
    with pytest.raises(RefreshContainerError, match="container_identity_mismatch"):
        admit_refresh_container_exit(
            context.launch, owned, context.observed, context.cessation, context.budget
        )


@pytest.mark.parametrize(
    "updates",
    [
        {"container_id": "f" * 64},
        {"image_id": "sha256:" + "f" * 64},
        {"name": "/other"},
        {"entrypoint": ("/app/default-entrypoint",)},
        {"command": ("different",)},
    ],
)
def test_inspected_image_name_and_command_must_match(
    context: _Context, updates: dict[str, object]
) -> None:
    """Bind success to the exact immutable invocation rather than a similar service.

    Given: Inspection reports another identity or an altered executable command.
    When: Exit admission compares the original launch.
    Then: It refuses the mismatch even if the reported exit code is zero.
    """
    observed = context.observed.model_copy(update=updates)
    with pytest.raises(RefreshContainerError):
        admit_refresh_container_exit(
            context.launch, context.owned, observed, context.cessation, context.budget
        )


@pytest.mark.parametrize(
    "updates",
    [
        {"pid_mode": "host"},
        {"cgroup_namespace_mode": "host"},
        {"healthcheck_disabled": False},
        {"read_only_root": False},
        {"init_enabled": True},
        {"privileged": True},
        {"binds": ()},
        {"memory_swap_bytes": 2147483648},
        {"network_mode": "host"},
    ],
)
def test_inspected_isolation_must_match_fixed_policy(
    context: _Context, updates: dict[str, object]
) -> None:
    """Reject a clean exit from a container that did not follow the admitted isolation.

    Given: One namespace, mount, resource or process-lifecycle setting differs.
    When: Exit admission compares the inspected isolation record.
    Then: It refuses the container despite a nominal successful exit.
    """
    isolation = context.observed.isolation.model_copy(update=updates)
    observed = context.observed.model_copy(update={"isolation": isolation})
    with pytest.raises(RefreshContainerError, match="container_launch_policy_mismatch"):
        admit_refresh_container_exit(
            context.launch, context.owned, observed, context.cessation, context.budget
        )


@pytest.mark.parametrize(
    "updates",
    [
        {"status": "running"},
        {"status": "dead"},
        {"pid": 123},
        {"exit_code": 137},
        {"oom_killed": True},
        {"started": False},
        {"finished": False},
    ],
)
def test_exit_zero_alone_is_not_success(context: _Context, updates: dict[str, object]) -> None:
    """Require actual clean completion rather than a default or partial exit record.

    Given: The container is not stopped cleanly or lacks completed-lifecycle evidence.
    When: Exit admission evaluates its final state.
    Then: It refuses the state regardless of other matching identities.
    """
    observed = context.observed.model_copy(update=updates)
    with pytest.raises(RefreshContainerError, match="container_exit_not_successful"):
        admit_refresh_container_exit(
            context.launch, context.owned, observed, context.cessation, context.budget
        )


@pytest.mark.parametrize(
    "updates",
    [
        {"container_id": "f" * 64},
        {"cgroup_path": PurePosixPath("/sys/fs/cgroup/other")},
        {"cgroup_gone": False},
        {"observed_at_monotonic": 99.0},
        {"observed_at_monotonic": 121.0},
    ],
)
def test_cessation_requires_exact_captured_cgroup_and_current_observation(
    context: _Context, updates: dict[str, object]
) -> None:
    """Keep a Docker kill result separate from proof that the owned container disappeared.

    Given: Cgroup disappearance is false, stale or bound to another identity.
    When: Exit admission checks independent cessation evidence.
    Then: It refuses to certify container completion.
    """
    cessation = context.cessation.model_copy(update=updates)
    with pytest.raises(RefreshContainerError, match="container_cessation_unproven"):
        admit_refresh_container_exit(
            context.launch, context.owned, context.observed, cessation, context.budget
        )


def test_exit_admission_cannot_replace_the_original_deadline(context: _Context) -> None:
    """Keep post-exit verification inside the same inherited execution budget.

    Given: A caller tries to supply a later deadline after launch.
    When: Exit admission compares budget identity.
    Then: It rejects the replacement even if every container observation is otherwise valid.
    """
    changed = RefreshBudget(160.0, context.clock)
    with pytest.raises(RefreshContainerError, match="container_deadline_changed"):
        admit_refresh_container_exit(
            context.launch, context.owned, context.observed, context.cessation, changed
        )


def test_exit_observation_after_original_deadline_is_not_admitted(context: _Context) -> None:
    """Do not convert late cleanup evidence into an on-time completion claim.

    Given: The original final deadline has elapsed before admission.
    When: The caller presents otherwise successful exact-container observations.
    Then: Admission reports timeout.
    """
    context.clock.now = 145.0
    with pytest.raises(TimeoutError):
        admit_refresh_container_exit(
            context.launch, context.owned, context.observed, context.cessation, context.budget
        )


def test_external_evidence_rejects_truthy_coercion_and_nonfinite_clock() -> None:
    """Keep external proof fields typed rather than treating arbitrary values as facts.

    Given: A caller supplies a textual Boolean or an infinite cessation timestamp.
    When: The frozen strict evidence model parses that receipt.
    Then: It rejects the malformed evidence before any admission decision.
    """
    common = {"container_id": _CONTAINER_ID, "cgroup_path": PurePosixPath("/sys/fs/cgroup/test")}
    with pytest.raises(ValidationError):
        RefreshContainerCessation.model_validate(
            {**common, "cgroup_gone": "true", "observed_at_monotonic": 100.0}
        )
    with pytest.raises(ValidationError):
        RefreshContainerCessation.model_validate(
            {**common, "cgroup_gone": True, "observed_at_monotonic": float("inf")}
        )
