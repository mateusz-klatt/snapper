"""Contracts for the blackbox delegate's reproducible Python dependency install."""

import re
from pathlib import Path
from typing import Final

_REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
_DELEGATE_ROOT: Final[Path] = _REPO_ROOT / "integrations" / "snapper-delegate"
_DOCKERFILE: Final[Path] = _DELEGATE_ROOT / "Dockerfile"
_REQUIREMENTS: Final[Path] = _DELEGATE_ROOT / "requirements.txt"
_LOCK: Final[Path] = _DELEGATE_ROOT / "requirements.lock"
_LOCK_ENTRY: Final[re.Pattern[str]] = re.compile(
    r"^(?P<name>[A-Za-z0-9_.-]+)==(?P<version>[^ ;\\]+)(?: ; .+)? \\$"
)
_EXTRA_WITNESSES: Final[dict[tuple[str, str], frozenset[str]]] = {
    ("httpx", "http2"): frozenset({"h2"}),
    ("httpx", "socks"): frozenset({"socksio"}),
}


def _normalized_name(name: str) -> str:
    """Return the canonical comparison spelling for one package name."""
    return re.sub(r"[-_.]+", "-", name).lower()


def _direct_pins() -> dict[str, str]:
    """Read every exact direct dependency pin from the input requirements."""
    pins: dict[str, str] = {}
    for raw_line in _REQUIREMENTS.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        name_with_extras, separator, version = line.partition("==")
        assert separator == "==", f"direct delegate dependency is not exact: {line}"
        name = name_with_extras.partition("[")[0]
        pins[_normalized_name(name)] = version
    return pins


def _declared_extras() -> set[tuple[str, str]]:
    """Return every normalized direct package-extra declaration."""
    declared: set[tuple[str, str]] = set()
    for raw_line in _REQUIREMENTS.read_text(encoding="utf-8").splitlines():
        name_with_extras = raw_line.strip().partition("==")[0]
        name, separator, extras_text = name_with_extras.partition("[")
        if not separator:
            continue
        assert extras_text.endswith("]"), f"malformed delegate extras: {raw_line}"
        normalized_name = _normalized_name(name)
        declared.update(
            (normalized_name, extra.strip().lower())
            for extra in extras_text.removesuffix("]").split(",")
            if extra.strip()
        )
    return declared


def _locked_pins_and_hashes() -> tuple[dict[str, str], dict[str, int]]:
    """Read resolved pins and count their verified distribution hashes."""
    pins: dict[str, str] = {}
    hashes: dict[str, int] = {}
    current_name: str | None = None
    for raw_line in _LOCK.read_text(encoding="utf-8").splitlines():
        match = _LOCK_ENTRY.fullmatch(raw_line)
        if match is not None:
            current_name = _normalized_name(match.group("name"))
            pins[current_name] = match.group("version")
            hashes[current_name] = 0
        elif raw_line.lstrip().startswith("--hash="):
            assert current_name is not None, "lock hash appeared before a package pin"
            hashes[current_name] += 1
    return pins, hashes


def test_delegate_dockerfile_installs_only_verified_binary_lock() -> None:
    """The image build consumes only the fully verified binary lock.

    Given: The blackbox delegate Dockerfile.
    When: Its Python installation instruction is inspected.
    Then: It requires hashes, forbids source distributions, and consumes the lock.
    """
    dockerfile = _DOCKERFILE.read_text(encoding="utf-8")
    assert "COPY requirements.lock /tmp/requirements.lock" in dockerfile
    assert "python -m pip install" in dockerfile
    assert "--require-hashes" in dockerfile
    assert "--only-binary=:all:" in dockerfile
    assert "-r /tmp/requirements.lock" in dockerfile
    assert "-r /tmp/requirements.txt" not in dockerfile


def test_delegate_lock_pins_and_hashes_complete_resolution() -> None:
    """Every resolved dependency is exact, hashed, and covers direct inputs.

    Given: The direct requirements and generated delegate lock.
    When: Package pins and artifact hashes are parsed.
    Then: Every direct version is retained and every resolved package is hashed.
    """
    direct = _direct_pins()
    locked, hashes = _locked_pins_and_hashes()
    assert locked
    assert direct.items() <= locked.items()
    assert locked.keys() == hashes.keys()
    assert all(count > 0 for count in hashes.values())


def test_delegate_lock_is_universal_binary_resolution() -> None:
    """The lock resolves binary artifacts across supported Docker platforms.

    Given: The generated delegate dependency lock header.
    When: Its reproducibility command is inspected.
    Then: Resolution is universal and excludes source-distribution builds.
    """
    header = "\n".join(_LOCK.read_text(encoding="utf-8").splitlines()[:3])
    assert "--universal" in header
    assert "--only-binary=:all:" in header


def test_delegate_lock_preserves_declared_extra_capabilities() -> None:
    """Each direct extra retains a hashed runtime capability witness.

    Given: The delegate declares HTTP/2 and SOCKS support through HTTPX extras.
    When: The flattened lock is inspected independently of pip's resolver.
    Then: Every declared extra has its expected pinned and hashed runtime package.
    """
    declared = _declared_extras()
    locked, hashes = _locked_pins_and_hashes()
    assert declared == _EXTRA_WITNESSES.keys()
    for extra in declared:
        witnesses = _EXTRA_WITNESSES[extra]
        assert witnesses <= locked.keys()
        assert all(hashes[witness] > 0 for witness in witnesses)
