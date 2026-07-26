"""``snapper token preflight`` — prove a live credential still authenticates.

One read-only command, run BEFORE a deploy that tightens token
acceptance. It answers the only question that matters for a long-lived
delegate credential that nobody can re-mint without breaking the
integration it drives:

    *Does this exact token's hash resolve to an inventory row that is
    unrevoked, unexpired, owned by an active user, typed ``access``,
    and naming the same ``jti`` the token was signed with?*

The verdict is NOT recomputed here. The command calls
:meth:`snapper.auth.tokens.TokenManager.verify_token_with_db` — the
same entry point the REST, MCP and WebSocket transports call — so a
preflight that says ACCEPTED cannot disagree with what the running
server will do with the same credential. The extra lines the command
prints are diagnostics read from the inventory row; they explain the
verdict, they do not produce it.

Nothing that could be replayed is ever printed. The raw JWT never
reaches stdout, stderr or the logs, and neither does its SHA-256 hash —
publishing the hash would hand an attacker the exact inventory lookup
key. The ``jti``, the owner's public id and the row's lifecycle
timestamps ARE printed: they are public correlation ids that the
inventory, the blacklist and the audit log already key by, and without
them the output could not be acted on.

Because the signing algorithm is itself a database-backed setting, the
command binds a real :class:`SettingsService` to the token manager
before it verifies anything — exactly as ``snapper.server.app`` does at
boot. Skipping that bind does not make the command read-only-cheaper;
it makes it die on ``auth_algorithm`` and report the crash as exit 1,
which this command's own contract defines as "the credential would be
rejected". A preflight that cries lockout because it forgot to load its
own configuration is worse than no preflight at all.

Exit codes are part of the contract:

    0 — the credential is accepted as an access bearer, today.
    1 — refusal: unreadable input, an unverifiable JWT, or a
        credential the running server would reject. Never deploy past
        a 1 assuming the integration will survive.
"""

import asyncio
import json
from pathlib import Path
from typing import Annotated
from typing import Final
from typing import NoReturn

import typer

from snapper.application.services.settings import get_settings_service
from snapper.auth.schemas.tokens import TokenClaims
from snapper.auth.tokens import TOKEN_TYPE_ACCESS
from snapper.auth.tokens import get_token_manager
from snapper.auth.tokens import hash_token
from snapper.config.settings import get_bootstrap_settings
from snapper.data.repository import Repository
from snapper.data.repository import dispose_repositories
from snapper.data.repository import get_repository
from snapper.data.repository_types import UserActiveTokenVerificationRow

token_app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Read-only checks on credentials the deployment has already issued.",
)

EXIT_REFUSED: Final[int] = 1
"""Exit code for unreadable input or a credential the server would reject."""

BRIDGE_CONFIG_TOKEN_KEY: Final[str] = "SNAPPER_ACCESS_TOKEN"
"""Key holding the JWT in the MCP bridge ``--config=PATH`` envelope."""


def _fatal(message: str) -> NoReturn:
    """Write a one-line stderr message and exit 1.

    Bypasses Typer's rich-formatted error UI so runbook greps and test
    assertions can match substrings without box-drawing noise.

    Args:
        message: Single-line stderr message to echo before exiting.

    Raises:
        typer.Exit: Always — exit code :data:`EXIT_REFUSED`.
    """
    typer.echo(message, err=True)
    raise typer.Exit(code=EXIT_REFUSED)


def _read_token(token_file: Path) -> str:
    """Load the credential from a bridge config envelope or a raw JWT file.

    Accepts the JSON envelope ``snapper dev-mint-pat`` writes (the
    shape an MCP bridge is configured with, so the operator can point
    this command at the file the integration actually uses) and a file
    containing nothing but the JWT.

    Args:
        token_file: Path to the credential file.

    Returns:
        The JWT string, stripped of surrounding whitespace.

    Raises:
        typer.Exit: When the file cannot be read or holds no credential.
    """
    try:
        raw = token_file.read_text(encoding="utf-8").strip()
    except OSError as error:
        _fatal(f"refused: cannot read {token_file}: {error}")
    if not raw:
        _fatal(f"refused: {token_file} is empty")
    try:
        envelope = json.loads(raw)
    except json.JSONDecodeError:
        return raw
    if not isinstance(envelope, dict):
        _fatal(f"refused: {token_file} holds JSON that is not an object")
    token = envelope.get(BRIDGE_CONFIG_TOKEN_KEY)
    if not isinstance(token, str) or not token.strip():
        _fatal(f"refused: {token_file} has no non-empty {BRIDGE_CONFIG_TOKEN_KEY}")
    return token.strip()


def _echo_row_diagnostics(row: UserActiveTokenVerificationRow, claims: TokenClaims) -> None:
    """Print the inventory facts that explain the verdict.

    Args:
        row: The verification projection for the presented hash.
        claims: Claims decoded from the presented JWT.

    Returns:
        None.
    """
    typer.echo("  inventory row: found")
    typer.echo(f"  owner: {row['user_public_id']}")
    typer.echo(f"  user active: {'yes' if row['user_is_active'] else 'NO'}")
    revoked_at = row["revoked_at"]
    typer.echo(f"  revoked: {'NO' if revoked_at is None else revoked_at.isoformat()}")
    typer.echo(f"  row expires_at: {row['expires_at'].isoformat()}")
    typer.echo(f"  row token_type: {row['token_type']} (expected {TOKEN_TYPE_ACCESS})")
    typer.echo(f"  row jti: {row['jti']}")
    typer.echo(f"  signed jti: {claims.jti}")
    typer.echo(f"  jti agreement: {'yes' if row['jti'] == claims.jti else 'NO'}")


async def _check_credential(token: str, token_file: Path, db_url: str) -> bool:
    """Verify one credential once the token manager is configured.

    Split from :func:`_run_preflight` so the settings/repository
    lifecycle owns a single ``try/finally`` and this function can stay
    a straight line of checks.

    Args:
        token: The JWT read from the credential file.
        token_file: Path the credential came from, echoed for the operator.
        db_url: Database URL the inventory row is read from.

    Returns:
        ``True`` when the running server would accept the credential as
        an access bearer.

    Raises:
        typer.Exit: When the JWT itself does not verify — there is
            nothing to look up in that case.
    """
    manager = get_token_manager()
    claims = manager.verify_token(token)
    if claims is None:
        _fatal(
            "refused: the JWT failed signature/expiry verification — nothing to look up. "
            "Run this on the deployment's own host: the signing key is derived from "
            "MASTER_PASSWORD, so a different .env refuses a credential that works."
        )
    typer.echo(f"preflight: token-file={token_file}")
    typer.echo("  jwt signature + expiry: ok")
    repository: Repository = get_repository(db_url)
    row = await repository.get_active_token_by_hash(hash_token(token))
    accepted = await manager.verify_token_with_db(
        token,
        repository,
        expected_token_type=TOKEN_TYPE_ACCESS,
    )
    if row is None:
        typer.echo("  inventory row: NOT FOUND")
    else:
        _echo_row_diagnostics(row, claims)
    return accepted is not None


async def _run_preflight(token_file: Path) -> bool:
    """Configure the token manager, then verify one credential.

    The settings bind is not optional and not deferrable: the JWT
    signing algorithm is a database-backed setting, so an unbound
    :class:`~snapper.config.app.AppSettings` raises ``RuntimeError`` on
    the very first verification step. Binding here mirrors
    ``snapper.server.app`` so the command resolves configuration the
    same way the process whose verdict it is predicting does.

    The credential file is read BEFORE any service starts, so a typo in
    ``--token-file`` costs no database connection and no ZMQ socket.

    Args:
        token_file: Path to the credential file.

    Returns:
        ``True`` when the running server would accept the credential as
        an access bearer.

    Raises:
        typer.Exit: When the file is unreadable or the JWT itself does
            not verify.
    """
    token = _read_token(token_file)
    bootstrap = get_bootstrap_settings()
    settings_service = await get_settings_service(bootstrap.db_url, bootstrap.zmq_broker_xsub)
    get_token_manager().set_settings_service(settings_service)
    try:
        return await _check_credential(token, token_file, bootstrap.db_url)
    finally:
        await settings_service.shutdown()
        await dispose_repositories()


@token_app.command(name="preflight")
def preflight(
    token_file: Annotated[
        Path,
        typer.Option("--token-file", help="Bridge config JSON or a file holding the raw JWT"),
    ],
) -> None:
    """Confirm a live credential still authenticates as an access bearer.

    Read-only; writes nothing and prints neither the token nor its hash.
    Run this BEFORE deploying a change to token acceptance.

    Args:
        token_file: Path to the bridge config envelope or raw JWT file.

    Raises:
        typer.Exit: Exit code :data:`EXIT_REFUSED` when the credential
            would be rejected by the running server.
    """
    accepted = asyncio.run(_run_preflight(token_file))
    if not accepted:
        typer.echo(
            "verdict: REJECTED — this credential would NOT authenticate as an access bearer",
            err=True,
        )
        raise typer.Exit(code=EXIT_REFUSED)
    typer.echo("verdict: ACCEPTED — this credential authenticates as an access bearer")
