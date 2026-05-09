"""snapper dev-mint-pat — mint a long-lived AI delegate via production REST.

Drives the production endpoints (POST /api/auth/login + POST /api/ai-delegates)
the AI Integration UI uses, then writes the minted JWT to a JSON file consumed
by snapper-mcp's --config=PATH flag. Lets the dev-iteration cycle
``rm data/snapper.db && make dev-backend && make mcp-pat`` regenerate
working credentials without browser clicks; ``~/.claude.json`` mcpServers
entry references the file via --config=PATH and picks up the freshly
minted token on the next bridge spawn.

**Credentials resolution** — admin username/password are resolved from the
seed TOML using the same three-tier lookup as ``snapper db-seed``:

    1. ``data/seed/{profile}.toml`` (deployment / volume override)
    2. ``proprietary/data/seed/{profile}.toml`` (private dev workspace)
    3. Package-bundled ``snapper/data/seed/{profile}.toml`` (OSS fallback)

Profile resolution: ``mcp`` is tried first (lets operators drop a
custom override outside open source); falls back to ``dev`` when no
``mcp`` profile exists. Within the chosen profile, the first user with
``role == "admin"`` provides the credentials. CLI / env-var overrides
short-circuit the seed lookup when explicitly supplied.
"""

import base64
import contextlib
import json
import os
import re
import secrets
import stat
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Final
from typing import NoReturn
from uuid import uuid7

import httpx
import typer
from loguru import logger

from snapper.data.seed.loader import load_seed_profile

DEFAULT_BASE_URL: Final = "http://localhost:8000"
DEFAULT_OUTPUT: Final = Path("data/dev-pat.json")
DEFAULT_LABEL: Final = "Local Dev MCP"
HTTP_TIMEOUT_SECONDS: Final = 10.0
ERROR_BODY_MAX_CHARS: Final = 200
SEED_PROFILE_LOOKUP_ORDER: Final = ("mcp", "dev")

_JWT_SHAPE_RE: Final = re.compile(r"[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}")


def _fatal(message: str) -> NoReturn:
    """Write a one-line stderr message and exit 1.

    Bypasses Typer's rich-formatted error UI so test assertions can match
    substrings of the message without box-drawing/line-wrapping noise.

    Args:
        message: Single-line stderr message to echo before exiting.

    Raises:
        typer.Exit: Always — exit code 1.
    """
    typer.echo(message, err=True)
    raise typer.Exit(code=1)


def resolve_admin_credentials_from_seed() -> tuple[str, str]:
    """Resolve (username, password) for the admin role from a seed TOML.

    Tries each profile in :data:`SEED_PROFILE_LOOKUP_ORDER` in turn,
    using :func:`snapper.data.seed.loader.load_seed_profile` for the
    three-tier file lookup (data/ → proprietary/ → bundled OSS). The
    first profile that exists AND contains a user with ``role == "admin"``
    wins. ``mcp`` is checked before ``dev`` so operators can drop a
    private override outside open source without touching dev.toml.

    Returns:
        Tuple of (username, password) for the seed admin user.

    Raises:
        typer.Exit: When no profile in the lookup order yields an admin
            user; exits the CLI with a stderr message naming the
            profiles that were attempted.
    """
    profiles_tried: list[str] = []
    for profile in SEED_PROFILE_LOOKUP_ORDER:
        profiles_tried.append(profile)
        try:
            seed = load_seed_profile(profile)
        except FileNotFoundError:
            continue
        for user in seed.users:
            if user.role == "admin":
                logger.info(
                    f"Resolved admin credentials from seed profile '{profile}' "
                    f"(user: {user.username})"
                )
                return user.username, user.password
    _fatal(
        "Could not resolve admin credentials from seed TOML. Tried profiles "
        f"{profiles_tried} via three-tier lookup (data/ -> proprietary/ -> "
        "bundled OSS). Add an admin user to data/seed/mcp.toml or "
        "data/seed/dev.toml, or pass --admin-username and --admin-password "
        "explicitly."
    )


def redact_token(value: str) -> str:
    """Replace JWT-shaped substrings with <jwt-NN-chars-ending-XXXX> placeholders.

    Mirrors the bridge's redactToken helper so tokens never reach stderr
    when HTTP error bodies are echoed to the operator.

    Args:
        value: Free-form text that may embed one or more JWT-shaped tokens.

    Returns:
        Text with every JWT-shape substring replaced by a placeholder.
    """

    def _replace(match: re.Match[str]) -> str:
        token = match.group(0)
        suffix = token[-8:]
        return f"<jwt-{len(token)}-chars-ending-{suffix}>"

    return _JWT_SHAPE_RE.sub(_replace, value)


def _stamp_provenance(
    payload_type: str,
    payload: dict[str, object],
    *,
    sequence_id: int,
    session_id: str,
) -> dict[str, object]:
    """Wrap payload in a PayloadRequest envelope with stamped provenance."""
    return {
        "type": payload_type,
        "payload": payload,
        "sequence_id": sequence_id,
        "public_id": str(uuid7()),
        "timestamp": datetime.now(UTC).isoformat(timespec="milliseconds"),
        "session_id": session_id,
    }


def _format_http_error(label: str, status_code: int, body_text: str) -> str:
    redacted = redact_token(body_text)[:ERROR_BODY_MAX_CHARS]
    return f"{label} returned HTTP {status_code}: {redacted}"


def _decode_jwt_exp(jwt: str) -> str | None:
    """Best-effort decode of a JWT's ``exp`` claim into an ISO 8601 string.

    Decodes the second (payload) segment without signature verification —
    purely informational, surfaced only in the success-path stdout banner.
    Returns ``None`` on any decoding failure so the banner falls back to
    a no-expires variant; never raises.
    """
    try:
        parts = jwt.split(".")
        if len(parts) != 3:
            return None
        padded = parts[1] + "=" * ((4 - len(parts[1]) % 4) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode("ascii"))
        claims = json.loads(decoded)
    except ValueError:
        return None
    exp = claims.get("exp") if isinstance(claims, dict) else None
    if not isinstance(exp, (int, float)):
        return None
    try:
        return datetime.fromtimestamp(exp, tz=UTC).isoformat()
    except (OverflowError, OSError, ValueError):
        return None


def _login(
    client: httpx.Client,
    *,
    base_url: str,
    username: str,
    password: str,
    sequence_id: int,
    session_id: str,
) -> str:
    """Drive POST /api/auth/login?return_tokens=true and return the access JWT."""
    body = _stamp_provenance(
        "login_request",
        {"username": username, "password": password, "remember_me": False},
        sequence_id=sequence_id,
        session_id=session_id,
    )
    try:
        response = client.post(
            f"{base_url}/api/auth/login",
            params={"return_tokens": "true"},
            json=body,
        )
    except httpx.ConnectError as exc:
        _fatal(
            f"Connection refused at {base_url}/api/auth/login. Is `make dev-backend` "
            f"running? ({exc})",
        )

    if response.status_code == 401:
        _fatal(
            f"Login failed for user '{username}'. "
            f"Check --admin-password or SNAPPER_DEV_ADMIN_PASSWORD env.",
        )
    if response.status_code != 200:
        _fatal(
            _format_http_error("/api/auth/login", response.status_code, response.text),
        )

    document = response.json()
    payload = document.get("payload") if isinstance(document, dict) else None
    access_token = payload.get("access_token") if isinstance(payload, dict) else None
    if not isinstance(access_token, str) or not access_token:
        _fatal(
            "/api/auth/login response did not include payload.access_token. "
            "Backend may be running an incompatible version.",
        )
    return access_token


def _create_delegate(
    client: httpx.Client,
    *,
    base_url: str,
    admin_access_token: str,
    label: str,
    sequence_id: int,
    session_id: str,
) -> str:
    """Drive POST /api/ai-delegates and return the minted long-lived JWT."""
    body = _stamp_provenance(
        "delegate_create_request",
        {"label": label},
        sequence_id=sequence_id,
        session_id=session_id,
    )
    try:
        response = client.post(
            f"{base_url}/api/ai-delegates",
            json=body,
            headers={"Authorization": f"Bearer {admin_access_token}"},
        )
    except httpx.ConnectError as exc:
        _fatal(
            f"Connection refused at {base_url}/api/ai-delegates between login and "
            f"delegate creation. Backend dropped mid-flight? ({exc})",
        )

    if response.status_code == 401:
        _fatal(
            "Admin token rejected by /api/ai-delegates. Token expired during script "
            "run? Re-run.",
        )
    if response.status_code == 409:
        detail = redact_token(response.text)[:ERROR_BODY_MAX_CHARS]
        _fatal(
            f"Delegate proliferation cap reached: {detail}. Deactivate an existing "
            f"delegate via POST /api/ai-delegates/{{id}}/deactivate and re-run.",
        )
    if response.status_code != 200:
        _fatal(
            _format_http_error("/api/ai-delegates", response.status_code, response.text),
        )

    document = response.json()
    payload = document.get("payload") if isinstance(document, dict) else None
    access_token = payload.get("access_token") if isinstance(payload, dict) else None
    if not isinstance(access_token, str) or not access_token:
        _fatal(
            "/api/ai-delegates response did not include payload.access_token. "
            "Backend may be running an incompatible version.",
        )
    return access_token


def _write_pat_file(output: Path, base_url: str, jwt: str) -> None:
    """Atomically write the bridge --config=PATH JSON envelope at mode 0600.

    Uses ``os.open(O_CREAT | O_EXCL | O_WRONLY, 0o600)`` to create the temp
    file with mode 0600 from the start — closes the brief umask window
    where ``Path.write_text`` would create the file at process default
    permissions before the chmod. All filesystem failures (mkdir, open,
    write, rename) route through the unified ``_fatal`` stderr path.

    If the parent directory already exists with looser-than-0700
    permissions, emits an info-level warning to stderr (does not enforce
    a chmod, which would surprise operators with shared dev directories
    like ``data/`` at 0755). The 0600 file mode itself is the real
    security boundary — directory mode controls listing, not file
    content reads on POSIX.
    """
    payload = {
        "SNAPPER_BASE_URL": f"{base_url}/api/mcp",
        "SNAPPER_ACCESS_TOKEN": jwt,
    }
    body = json.dumps(payload, indent=2) + "\n"
    parent = output.parent
    tmp: Path | None = None
    try:
        parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        parent_mode = stat.S_IMODE(parent.stat().st_mode)
        if parent_mode & 0o077:
            typer.echo(
                f"warn: parent dir {parent} has mode {oct(parent_mode)} "
                f"(file itself is 0600; consider chmod 0700 for stricter dir listing).",
                err=True,
            )
        tmp = output.with_name(f"{output.name}.tmp.{os.getpid()}.{secrets.token_hex(4)}")
        fd = os.open(tmp, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            os.write(fd, body.encode("utf-8"))
        finally:
            os.close(fd)
        os.rename(tmp, output)
    except OSError as exc:
        if tmp is not None and tmp.exists():
            with contextlib.suppress(OSError):
                tmp.unlink()
        _fatal(f"Failed to write {output}: {exc}")


def dev_mint_pat(
    base_url: str = typer.Option(
        DEFAULT_BASE_URL,
        envvar="SNAPPER_DEV_BASE_URL",
        help="Snapper backend base URL (without /api/mcp suffix).",
    ),
    admin_username: str | None = typer.Option(
        None,
        envvar="SNAPPER_DEV_ADMIN_USERNAME",
        help=(
            "Admin username override. When omitted, resolved from the seed "
            "TOML (mcp profile, falls back to dev) via the standard three-tier "
            "lookup (data/ -> proprietary/ -> bundled OSS)."
        ),
    ),
    admin_password: str | None = typer.Option(
        None,
        envvar="SNAPPER_DEV_ADMIN_PASSWORD",
        help=("Admin password override. When omitted, resolved from seed TOML. NEVER logged."),
    ),
    output: Path = typer.Option(
        DEFAULT_OUTPUT,
        envvar="SNAPPER_DEV_PAT_OUTPUT",
        help="Where to write the dev PAT JSON envelope (mode 0600).",
    ),
    label: str = typer.Option(
        DEFAULT_LABEL,
        help="Human-readable label for the minted delegate.",
    ),
) -> None:
    """Mint a long-lived dev AI delegate via production REST and write to JSON.

    Flow: POST /api/auth/login?return_tokens=true (admin creds) -> Bearer
    token -> POST /api/ai-delegates -> long-lived JWT -> atomic 0600
    write to --output. Bridge picks up the file via --config=PATH on
    next spawn; operator's ~/.claude.json mcpServers entry only needs
    the file path, never the token bytes.

    Admin credentials are resolved per-field from the seed TOML by
    default (:func:`resolve_admin_credentials_from_seed`); pass
    --admin-username and/or --admin-password (or the matching env vars)
    to override one or both fields. Partial overrides are honoured —
    e.g. setting only ``SNAPPER_DEV_ADMIN_PASSWORD`` keeps the seed
    username while taking the password from env, which is the natural
    pattern for CI environments that share the seed admin user but
    rotate the password through CI secrets.

    All error paths exit 1 with a stderr message; tokens are redacted via
    JWT-shape regex before any HTTP error body reaches stderr.

    Args:
        base_url: Snapper backend base URL (without /api/mcp suffix).
        admin_username: Optional admin username override. Empty -> seed lookup.
        admin_password: Optional admin password override. NEVER logged.
        output: Filesystem path to write the dev PAT JSON envelope (mode 0600).
        label: Human-readable label for the minted delegate.

    Raises:
        typer.Exit: On any validation, network, or filesystem error
            (exit code 1).
    """
    seed_username: str | None = None
    seed_password: str | None = None
    if admin_username is None or admin_password is None:
        seed_username, seed_password = resolve_admin_credentials_from_seed()
    resolved_username = admin_username if admin_username is not None else seed_username
    resolved_password = admin_password if admin_password is not None else seed_password
    assert resolved_username is not None
    assert resolved_password is not None
    base = base_url.rstrip("/")
    out = output.expanduser().resolve()
    session_id = str(uuid7())

    with httpx.Client(timeout=HTTP_TIMEOUT_SECONDS) as client:
        admin_token = _login(
            client,
            base_url=base,
            username=resolved_username,
            password=resolved_password,
            sequence_id=1,
            session_id=session_id,
        )
        delegate_jwt = _create_delegate(
            client,
            base_url=base,
            admin_access_token=admin_token,
            label=label,
            sequence_id=2,
            session_id=session_id,
        )

    _write_pat_file(out, base, delegate_jwt)
    expires_at = _decode_jwt_exp(delegate_jwt)
    expires_suffix = f" Token expires {expires_at}." if expires_at is not None else ""
    typer.echo(
        f"Wrote {out} (mode 0600).{expires_suffix} "
        f"Operator ~/.claude.json picks up via --config=PATH.",
    )


__all__ = [
    "dev_mint_pat",
    "redact_token",
    "resolve_admin_credentials_from_seed",
    "_stamp_provenance",
    "_login",
    "_create_delegate",
    "_write_pat_file",
    "DEFAULT_BASE_URL",
    "DEFAULT_OUTPUT",
    "DEFAULT_LABEL",
    "SEED_PROFILE_LOOKUP_ORDER",
]
