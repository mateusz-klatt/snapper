"""Shared pieces of operator commands that change durable state on purpose.

Such a command refuses with one line and a non-zero status, names its target
database without credentials, and reaches it through the configured repository.
"""

from typing import NoReturn
from typing import cast

import typer
from sqlalchemy.engine import make_url

from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import get_repository


def refuse(message: str) -> NoReturn:
    """Print a refusal and terminate the command.

    Args:
        message: Refusal shown to the operator.

    Returns:
        Never; the call always raises.

    Raises:
        typer.Exit: Always, with a non-zero status.
    """
    typer.echo(message, err=True)
    raise typer.Exit(code=1)


def database_identity(db_url: str) -> str:
    """Render a credential-free database identity.

    Args:
        db_url: Configured SQLAlchemy database URL.

    Returns:
        Dialect, host and database without user, password, port or query.
    """
    url = make_url(db_url)
    return (
        f"dialect={url.get_backend_name()} "
        f"host={url.host or '(local)'} database={url.database or '(none)'}"
    )


def configured_repository(
    db_url: str, *, application_name: str | None = None
) -> SQLAlchemyRepository:
    """Resolve the configured repository.

    Args:
        db_url: Configured SQLAlchemy database URL.
        application_name: Optional PostgreSQL connection name for this invocation.

    Returns:
        Repository for the target database, dedicated to this invocation when named.
    """
    if application_name is not None:
        return SQLAlchemyRepository(db_url, application_name=application_name)
    return cast(SQLAlchemyRepository, get_repository(db_url))
