"""Contract tests for the profile-gated no-egress delegate container."""

from pathlib import Path
from typing import Final
from typing import cast

import yaml

from snapper.config.delegate_profile import ENV_VARS as DELEGATE_PROFILE_ENV_VARS

_REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent.parent
_COMPOSE_FILE: Final[Path] = _REPO_ROOT / "docker-compose.yml"
_SERVICE_NAME: Final[str] = "snapper-delegate"


def _delegate_service() -> dict[str, object]:
    """Load the real delegate service from the Compose document."""
    document: object = yaml.safe_load(_COMPOSE_FILE.read_text(encoding="utf-8"))
    assert isinstance(document, dict)
    services = document.get("services")
    assert isinstance(services, dict)
    service = services.get(_SERVICE_NAME)
    assert isinstance(service, dict)
    return cast(dict[str, object], service)


def test_delegate_service_is_explicitly_profile_gated() -> None:
    """The disconnected canary does not join the default stack.

    Given: The real Compose delegate service,
    When: Its activation metadata is inspected,
    Then: It requires the dedicated delegate profile.
    """
    service = _delegate_service()
    assert service["profiles"] == ["delegate"]
    assert service["container_name"] == "snapper-delegate"
    assert service["restart"] == "unless-stopped"


def test_delegate_service_builds_the_baked_runtime_image() -> None:
    """A delegate-only build uses the unified image containing its PID1.

    Given: An operator building only the optional delegate service,
    When: Compose resolves that service's build definition,
    Then: It selects the repository context and unified runtime target.
    """
    service = _delegate_service()
    assert service["image"] == "klattm/snapper:latest"
    assert service["build"] == {"context": ".", "target": "runtime"}


def test_delegate_service_runs_new_runner_only_pid1() -> None:
    """The container bypasses the full Snapper coordinator command.

    Given: The inherited image entrypoint and HTTP healthcheck,
    When: The delegate service overrides its lifecycle,
    Then: Python runs the integration PID1 directly with no inherited command.
    """
    service = _delegate_service()
    assert service["entrypoint"] == ["python", "-m", "snapper_delegate.pid1"]
    assert service["command"] == []
    assert service["healthcheck"] == {"disable": True}
    serialized = repr(service)
    assert "delegate-engine" not in serialized
    assert "feed-engine" not in serialized
    assert "strategies-engine" not in serialized


def test_delegate_service_has_provably_no_network_namespace_egress() -> None:
    """No network, dependency, or published socket reaches the delegate.

    Given: The no-egress Step 5 service,
    When: Network-affecting Compose keys are inspected,
    Then: It uses Docker's none network and declares no alternate path.
    """
    service = _delegate_service()
    assert service["network_mode"] == "none"
    for forbidden_key in (
        "networks",
        "network_mode_ipv4_address",
        "depends_on",
        "ports",
        "expose",
        "links",
        "external_links",
        "dns",
        "dns_search",
        "extra_hosts",
    ):
        assert forbidden_key not in service


def test_delegate_service_environment_is_minimal_and_reference_only() -> None:
    """The PID1 receives no DB, broker, proxy, or inline credential.

    Given: The service's explicit environment without an env-file,
    When: Its names and reference values are inspected,
    Then: Only runtime controls, one route, and fixed secret paths remain.
    """
    service = _delegate_service()
    assert "env_file" not in service
    environment = service["environment"]
    assert isinstance(environment, dict)
    interpolated_names = {
        str(name)
        for name, value in environment.items()
        if isinstance(value, str) and value.startswith("${")
    }
    assert interpolated_names == DELEGATE_PROFILE_ENV_VARS
    assert set(environment) == {
        "PYTHONPATH",
        "PYTHONDONTWRITEBYTECODE",
        "PYTHONNOUSERSITE",
        "PYTHONSAFEPATH",
        "PYTHONUNBUFFERED",
        "SNAPPER_DELEGATE_MODEL_ALIAS",
        "SNAPPER_DELEGATE_BASE_URL",
        "SNAPPER_DELEGATE_ENDPOINT_PATH",
        "SNAPPER_DELEGATE_SNAPPER_URL",
        "SNAPPER_DELEGATE_API_KEY_FILE",
        "SNAPPER_DELEGATE_TOKEN_FILE",
        "SNAPPER_DELEGATE_MAX_TOOL_ROUNDS",
        "SNAPPER_LOG_FILE",
    }
    assert environment["SNAPPER_DELEGATE_API_KEY_FILE"] == "/run/secrets/delegate/api_key"
    assert environment["SNAPPER_DELEGATE_TOKEN_FILE"] == "/run/secrets/delegate/delegate_token"
    assert environment["SNAPPER_LOG_FILE"] == "/app/data/log/delegate/model/delegate.log"
    dangerous_fragments = (
        "DB_URL",
        "MASTER_PASSWORD",
        "ZMQ",
        "BROKER",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
    )
    assert all(
        all(fragment not in str(name).upper() for fragment in dangerous_fragments)
        for name in environment
    )


def test_delegate_service_mounts_only_one_model_log_directory() -> None:
    """The container cannot read the full data tree or any secret mount.

    Given: The service's volume declaration,
    When: Bind sources and targets are inspected,
    Then: Only a pre-created no-egress model log directory is writable.
    """
    service = _delegate_service()
    volumes = service["volumes"]
    assert isinstance(volumes, list)
    assert len(volumes) == 1
    volume = volumes[0]
    assert isinstance(volume, dict)
    assert volume == {
        "type": "bind",
        "source": "./data/log/delegate/no-egress",
        "target": "/app/data/log/delegate/model",
        "bind": {"create_host_path": False},
    }
    serialized = repr(service)
    assert "/var/run/docker.sock" not in serialized
    assert "/run/secrets" not in repr(volumes)
    assert "'source': './data'" not in serialized
    assert "'target': '/app/data'" not in serialized


def test_delegate_service_has_only_noexec_bounded_temporary_storage() -> None:
    """The read-only root has one tightly bounded no-exec temporary mount.

    Given: The service filesystem settings,
    When: Root and tmpfs options are inspected,
    Then: Temporary writes cannot execute, escalate, or persist.
    """
    service = _delegate_service()
    assert service["read_only"] is True
    assert service["tmpfs"] == [
        "/tmp:rw,noexec,nosuid,nodev,size=16777216,uid=888,gid=888,mode=1770"
    ]


def test_delegate_service_drops_privilege_and_caps_resources() -> None:
    """The canary runs as the image UID with no Linux capabilities.

    Given: The hardened runner-only service,
    When: Privilege and resource controls are inspected,
    Then: Every Step 5 limit is explicit and no elevation key exists.
    """
    service = _delegate_service()
    assert service["user"] == "888:888"
    assert service["cap_drop"] == ["ALL"]
    assert service["security_opt"] == ["no-new-privileges:true"]
    assert service["mem_limit"] == "512m"
    assert service["cpus"] == 1.0
    assert service["pids_limit"] == 64
    for forbidden_key in (
        "cap_add",
        "privileged",
        "devices",
        "device_cgroup_rules",
        "pid",
        "ipc",
        "uts",
        "sysctls",
    ):
        assert forbidden_key not in service
