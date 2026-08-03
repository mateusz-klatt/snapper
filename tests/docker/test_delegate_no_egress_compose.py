"""Contract tests for the profile-gated no-egress delegate container."""

from dataclasses import dataclass
from pathlib import Path
from typing import Final
from typing import cast

import pytest
import yaml

from snapper.config.delegate_profile import RUNNER_ENV_VARS as DELEGATE_RUNNER_ENV_VARS

_REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent.parent
_COMPOSE_FILE: Final[Path] = _REPO_ROOT / "docker-compose.yml"
_CADDY_FILE: Final[Path] = _REPO_ROOT / "docker" / "web" / "Caddyfile"
_SERVICE_NAME: Final[str] = "snapper-delegate"
_CONTROL_SUBNETS: Final[tuple[str, str]] = ("172.30.10.0/29", "172.30.11.0/29")


@dataclass(frozen=True, slots=True)
class _ModelProfile:
    """Describe one pinned per-model Compose service contract."""

    name: str
    model_alias: str
    base_url: str
    endpoint_path: str
    control_network: str
    vendor_network: str
    host_directory: str
    subnet: str


_MODEL_PROFILES: Final[tuple[_ModelProfile, ...]] = (
    _ModelProfile(
        name="kimi",
        model_alias="${SNAPPER_DELEGATE_KIMI_MODEL:-kimi-k3}",
        base_url="${SNAPPER_DELEGATE_KIMI_BASE_URL:-https://api.moonshot.ai}",
        endpoint_path="/v1/chat/completions",
        control_network="delegate-kimi-control",
        vendor_network="delegate-kimi-vendor",
        host_directory="kimi",
        subnet="172.30.10.0/29",
    ),
    _ModelProfile(
        name="gemini",
        model_alias="${SNAPPER_DELEGATE_GEMINI_MODEL:-gemini-2.5-pro}",
        base_url="${SNAPPER_DELEGATE_GEMINI_BASE_URL:-https://generativelanguage.googleapis.com}",
        endpoint_path="/v1beta/openai/chat/completions",
        control_network="delegate-gemini-control",
        vendor_network="delegate-gemini-vendor",
        host_directory="gemini",
        subnet="172.30.11.0/29",
    ),
)


def _compose_document() -> dict[str, object]:
    """Load the real Compose document as an object mapping."""
    document: object = yaml.safe_load(_COMPOSE_FILE.read_text(encoding="utf-8"))
    assert isinstance(document, dict)
    return cast(dict[str, object], document)


def _service(service_name: str) -> dict[str, object]:
    """Load one named service from the real Compose document."""
    services = _compose_document().get("services")
    assert isinstance(services, dict)
    service = services.get(service_name)
    assert isinstance(service, dict)
    return cast(dict[str, object], service)


def _network(network_name: str) -> dict[str, object]:
    """Load one named network from the real Compose document."""
    networks = _compose_document().get("networks")
    assert isinstance(networks, dict)
    network = networks.get(network_name)
    assert isinstance(network, dict)
    return cast(dict[str, object], network)


def _delegate_service() -> dict[str, object]:
    """Load the real delegate service from the Compose document."""
    return _service(_SERVICE_NAME)


def _model_service(profile: _ModelProfile) -> dict[str, object]:
    """Load one pinned per-model delegate service."""
    return _service(f"snapper-delegate-{profile.name}")


def _brace_block(source: str, header: str) -> str:
    """Extract one exact Caddy brace block, including nested blocks."""
    lines = source.splitlines()
    start = next(index for index, line in enumerate(lines) if line.strip() == f"{header} {{")
    selected: list[str] = []
    depth = 0
    for line in lines[start:]:
        selected.append(line)
        depth += line.count("{") - line.count("}")
        if depth == 0:
            return "\n".join(selected)
    raise AssertionError(f"Unclosed Caddy block: {header}")


def _site_block(address: str) -> str:
    """Extract one site block from the real Caddyfile."""
    return _brace_block(_CADDY_FILE.read_text(encoding="utf-8"), address)


def _matcher_contract(site_block: str, matcher: str) -> tuple[str, str]:
    """Return the sole method and path declared by one named matcher."""
    matcher_block = _brace_block(site_block, matcher)
    declared_methods = [
        line.strip().removeprefix("method ")
        for line in matcher_block.splitlines()
        if line.strip().startswith("method ")
    ]
    declared_paths = [
        line.strip().removeprefix("path ")
        for line in matcher_block.splitlines()
        if line.strip().startswith("path ")
    ]
    assert len(declared_methods) == 1
    assert len(declared_paths) == 1
    return declared_methods[0], declared_paths[0]


def _services_on_network(network_name: str) -> set[str]:
    """Return every service attached to one list-form Compose network."""
    services = _compose_document().get("services")
    assert isinstance(services, dict)
    attached: set[str] = set()
    for service_name, service in services.items():
        if not isinstance(service_name, str) or not isinstance(service, dict):
            continue
        declared_networks = service.get("networks", [])
        if isinstance(declared_networks, list) and network_name in declared_networks:
            attached.add(service_name)
    return attached


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
    assert interpolated_names == DELEGATE_RUNNER_ENV_VARS
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


@pytest.mark.parametrize("profile", _MODEL_PROFILES, ids=("kimi", "gemini"))
def test_model_delegate_uses_blackbox_image(profile: _ModelProfile) -> None:
    """Each model runs PID1 from the minimal blackbox image, not the unified one.

    Given: A model-specific delegate service in the Compose contract,
    When: Its image, build context, and process configuration are inspected,
    Then: It uses only the minimal blackbox image and delegate build context.
    """
    service = _model_service(profile)
    assert service["profiles"] == ["delegate"]
    assert service["container_name"] == f"snapper-delegate-{profile.name}"
    assert service["image"] == "snapper-delegate:blackbox"
    assert service["build"] == {"context": "./integrations/snapper-delegate"}
    assert service["entrypoint"] == ["python", "-m", "snapper_delegate.pid1"]
    assert service["command"] == []
    assert service["restart"] == "unless-stopped"
    assert service["healthcheck"] == {"disable": True}
    serialized = repr(service)
    assert "coordinator" not in serialized.lower()
    assert "DB_URL" not in serialized
    assert "ZMQ" not in serialized


@pytest.mark.parametrize("profile", _MODEL_PROFILES, ids=("kimi", "gemini"))
def test_model_delegate_environment_pins_only_reference_configuration(
    profile: _ModelProfile,
) -> None:
    """Each profile carries fixed paths and references rather than credentials.

    Given: A model-specific delegate service environment,
    When: Its configuration keys and values are inspected,
    Then: It contains only fixed settings and secret-file references.
    """
    service = _model_service(profile)
    assert "env_file" not in service
    environment = service["environment"]
    assert isinstance(environment, dict)
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
    assert environment["SNAPPER_DELEGATE_MODEL_ALIAS"] == profile.model_alias
    assert environment["SNAPPER_DELEGATE_BASE_URL"] == profile.base_url
    assert environment["SNAPPER_DELEGATE_ENDPOINT_PATH"] == profile.endpoint_path
    assert environment["SNAPPER_DELEGATE_SNAPPER_URL"] == (
        "${SNAPPER_DELEGATE_SNAPPER_URL:-http://snapper-web:8300}"
    )
    assert environment["SNAPPER_DELEGATE_API_KEY_FILE"] == "/run/secrets/delegate/api_key"
    assert environment["SNAPPER_DELEGATE_TOKEN_FILE"] == ("/run/secrets/delegate/delegate_token")
    assert environment["SNAPPER_LOG_FILE"] == "/app/data/log/delegate/model/delegate.log"
    assert environment["PYTHONPATH"] == "/app/src"
    serialized = repr(environment).upper()
    assert "MASTER_PASSWORD" not in serialized
    assert "HTTP_PROXY" not in serialized
    assert "HTTPS_PROXY" not in serialized
    assert "ALL_PROXY" not in serialized


@pytest.mark.parametrize("profile", _MODEL_PROFILES, ids=("kimi", "gemini"))
def test_model_delegate_attaches_only_its_internal_control_network(
    profile: _ModelProfile,
) -> None:
    """Vendor and application-plane networks remain unreachable.

    Given: A model-specific delegate service and its network declarations,
    When: Its attached services and network properties are inspected,
    Then: Only the dedicated internal control network is reachable.
    """
    service = _model_service(profile)
    assert service["networks"] == [profile.control_network]
    assert "network_mode" not in service
    assert profile.vendor_network not in repr(service["networks"])
    assert "snapper-internal" not in repr(service["networks"])
    control_network = _network(profile.control_network)
    vendor_network = _network(profile.vendor_network)
    assert control_network["driver"] == "bridge"
    assert control_network["internal"] is True
    assert control_network["ipam"] == {"config": [{"subnet": profile.subnet}]}
    assert vendor_network == {"driver": "bridge", "internal": True}
    assert _services_on_network(profile.control_network) == {
        f"snapper-delegate-{profile.name}",
        "snapper-web",
    }
    assert _services_on_network(profile.vendor_network) == set()


@pytest.mark.parametrize("profile", _MODEL_PROFILES, ids=("kimi", "gemini"))
def test_model_delegate_mounts_only_model_log_and_secret_references(
    profile: _ModelProfile,
) -> None:
    """Each model sees only its writable log and read-only secret directory.

    Given: A model-specific delegate service volume contract,
    When: Its bind mounts are inspected,
    Then: Only the model log and read-only secret directory are visible.
    """
    service = _model_service(profile)
    assert service["volumes"] == [
        {
            "type": "bind",
            "source": f"./data/log/delegate/{profile.host_directory}",
            "target": "/app/data/log/delegate/model",
            "bind": {"create_host_path": False},
        },
        {
            "type": "bind",
            "source": f"./data/secrets/delegate/{profile.host_directory}",
            "target": "/run/secrets/delegate",
            "read_only": True,
            "bind": {"create_host_path": False},
        },
    ]
    serialized = repr(service)
    assert "/var/run/docker.sock" not in serialized
    assert "'source': './data'" not in serialized
    assert "'target': '/app/data'" not in serialized


@pytest.mark.parametrize("profile", _MODEL_PROFILES, ids=("kimi", "gemini"))
def test_model_delegate_preserves_all_runner_hardening(profile: _ModelProfile) -> None:
    """Each model keeps the canary's privilege and resource limits.

    Given: A model-specific delegate service derived from the hardened runner,
    When: Its privilege, filesystem, and resource controls are inspected,
    Then: Every required hardening control remains active.
    """
    service = _model_service(profile)
    assert service["user"] == "888:888"
    assert service["read_only"] is True
    assert service["cap_drop"] == ["ALL"]
    assert service["security_opt"] == ["no-new-privileges:true"]
    assert service["tmpfs"] == [
        "/tmp:rw,noexec,nosuid,nodev,size=16777216,uid=888,gid=888,mode=1770"
    ]
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
        "ports",
        "expose",
        "dns",
        "extra_hosts",
    ):
        assert forbidden_key not in service


def test_snapper_web_exposes_bridge_only_inside_control_networks() -> None:
    """The web service joins both controls without publishing the bridge.

    Given: The dashboard service and both delegate control networks,
    When: Its network membership and published ports are inspected,
    Then: The control bridge is exposed internally but never published to the host.
    """
    service = _service("snapper-web")
    assert service["image"] == "klattm/snapper:latest"
    assert service["expose"] == ["8300"]
    assert service["ports"] == ["127.0.0.1:8000:8000"]
    assert service["networks"] == [
        "snapper-internal",
        "delegate-kimi-control",
        "delegate-gemini-control",
    ]
    assert all(":8300" not in str(port) for port in service["ports"])


def test_caddy_control_bridge_has_exact_source_route_and_method_allowlist() -> None:
    """The bridge denies every source and route outside four control contracts.

    Given: The public and internal Caddy site blocks,
    When: Their source, route, and method matchers are inspected,
    Then: Only the four explicit delegate control contracts are admitted.
    """
    public_block = _site_block(":8000")
    control_block = _site_block(":8300")
    for subnet in _CONTROL_SUBNETS:
        assert subnet in public_block
        assert subnet in control_block
    public_route = _brace_block(public_block, "route")
    public_route_lines = [line.strip() for line in public_route.splitlines()[1:-1] if line.strip()]
    assert public_route_lines[0] == "respond @delegate_control_on_public 403"
    assert public_route.index("respond @delegate_control_on_public 403") < public_route.index(
        "encode gzip zstd"
    )
    assert public_route.index("encode gzip zstd") < public_route.index("handle /api/*")
    assert public_route.count("respond @delegate_control_on_public 403") == 1
    assert public_route.count("\n\t\thandle") == 6
    assert "\n\trespond @delegate_control_on_public 403" not in public_block
    public_log = _brace_block(public_block, "log")
    assert public_log not in public_route
    assert "output stdout" in public_log
    assert "format console" in public_log
    assert "@delegate_control remote_ip 172.30.10.0/29 172.30.11.0/29" in control_block
    assert {
        _matcher_contract(control_block, "@delegate_ws_token"),
        _matcher_contract(control_block, "@delegate_pending_reviews"),
        _matcher_contract(control_block, "@delegate_websocket"),
        _matcher_contract(control_block, "@delegate_mcp"),
    } == {
        ("POST", "/api/auth/ws_token"),
        ("GET", "/api/ai-reviews/pending"),
        ("GET", "/api/ws"),
        ("POST", "/api/mcp"),
    }
    websocket_matcher = _brace_block(control_block, "@delegate_websocket")
    assert "header Connection *Upgrade*" in websocket_matcher
    assert "header Upgrade websocket" in websocket_matcher
    route_block = _brace_block(control_block, "route @delegate_control")
    proxy_lines = {line.strip() for line in route_block.splitlines() if "reverse_proxy" in line}
    assert proxy_lines == {
        "reverse_proxy @delegate_ws_token snapper:8000",
        "reverse_proxy @delegate_pending_reviews snapper:8000",
        "reverse_proxy @delegate_websocket snapper:8000",
        "reverse_proxy @delegate_mcp snapper:8000",
    }
    assert control_block.count("respond 403") == 2
