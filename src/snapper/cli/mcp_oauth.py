"""Operator CLI for pre-registering confidential MCP OAuth clients."""

import asyncio
import json
from typing import Annotated
from typing import NoReturn

import typer

from snapper.config.settings import get_bootstrap_settings
from snapper.data.repository import dispose_repositories
from snapper.data.repository import get_repository
from snapper.mcp.oauth.client_provisioning import OAuthClientProvisioningError
from snapper.mcp.oauth.client_provisioning import OAuthClientProvisioningRequest
from snapper.mcp.oauth.client_provisioning import ProvisionedOAuthClient
from snapper.mcp.oauth.client_provisioning import build_oauth_client_provisioning_request
from snapper.mcp.oauth.client_provisioning import provision_oauth_client_request
from snapper.mcp.oauth.store import MCPOAuthStore

mcp_oauth_app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Provision confidential clients for the disabled-by-default MCP OAuth server.",
)


def _fatal(error: OAuthClientProvisioningError) -> NoReturn:
    """Render one safe provisioning refusal and exit nonzero."""
    typer.echo(f"refused: {error}", err=True)
    raise typer.Exit(code=1)


async def _provision(request: OAuthClientProvisioningRequest) -> ProvisionedOAuthClient:
    """Persist one validated request through the configured repository."""
    bootstrap = get_bootstrap_settings()
    repository = get_repository(bootstrap.db_url)
    try:
        return await provision_oauth_client_request(MCPOAuthStore(repository), request)
    finally:
        await dispose_repositories()


def _credential_document(result: ProvisionedOAuthClient) -> dict[str, str | list[str]]:
    """Build the one-time machine-readable credential document."""
    return {
        "client_id": result.client_id,
        "client_secret": result.client_secret,
        "client_name": result.client_name,
        "redirect_uris": list(result.redirect_uris),
        "token_endpoint_auth_method": result.token_endpoint_auth_method,
        "allowed_scopes": list(result.allowed_scopes),
    }


@mcp_oauth_app.command(name="provision-client")
def provision_client(
    client_name: Annotated[
        str,
        typer.Option("--name", help="Exact client name shown during consent."),
    ],
    redirect_uris: Annotated[
        list[str],
        typer.Option(
            "--redirect-uri",
            help="Exact registered callback URI. Repeat for additional callbacks.",
        ),
    ],
    auth_method: Annotated[
        str,
        typer.Option(
            "--auth-method",
            help="client_secret_basic or client_secret_post.",
        ),
    ] = "client_secret_basic",
    scopes: Annotated[
        list[str] | None,
        typer.Option(
            "--scope",
            help=(
                "Initial-release scope: snapper.read or offline_access. "
                "Repeat as needed; defaults to both."
            ),
        ),
    ] = None,
) -> None:
    """Create one client and print its nonrecoverable secret exactly once.

    Args:
        client_name: Exact display name shown during future consent.
        redirect_uris: Exact callback URIs accepted for this client.
        auth_method: Confidential token endpoint authentication method.
        scopes: Ordered scope ceiling, or the safe read-only default.

    Raises:
        typer.Exit: When metadata or unique persistence is refused.
    """
    try:
        request = build_oauth_client_provisioning_request(
            client_name=client_name,
            redirect_uris=redirect_uris,
            token_endpoint_auth_method=auth_method,
            allowed_scopes=scopes,
        )
        result = asyncio.run(_provision(request))
    except OAuthClientProvisioningError as error:
        _fatal(error)
    typer.echo("Save this credential now; client_secret cannot be recovered.", err=True)
    typer.echo(json.dumps(_credential_document(result), sort_keys=True))
