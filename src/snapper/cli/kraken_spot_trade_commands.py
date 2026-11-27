"""Register the Kraken Spot recovery commands without loading server settings."""

import typer
from typer._click import Context as ClickContext
from typer.core import TyperCommand

from snapper.cli.kraken_spot_trade_import import IMPORT_KRAKEN_SPOT_TRADES_CONTEXT
from snapper.cli.kraken_spot_trade_import import import_kraken_spot_trades
from snapper.cli.kraken_spot_trade_recovery import PLAN_KRAKEN_SPOT_TRADE_RECOVERY_CONTEXT
from snapper.cli.kraken_spot_trade_recovery import plan_kraken_spot_trade_recovery

_PLANNER = "plan-kraken-spot-trade-recovery"
_IMPORTER = "import-kraken-spot-trades"
KRAKEN_SPOT_TRADE_COMMANDS = frozenset({_PLANNER, _IMPORTER})
"""Recovery commands that handle their own arguments, setup, and output."""


class _ArgparseCommand(TyperCommand):
    """Leave the entire argument tail for the command's argparse parser."""

    def parse_args(self, ctx: ClickContext, args: list[str]) -> list[str]:
        """Forward arguments verbatim so argparse retains its ``--`` boundary.

        Args:
            ctx: Context for the registered command.
            args: Arguments following the command name.

        Returns:
            The original argument tail.
        """
        ctx.args = args
        return args


def register_kraken_spot_trade_commands(app: typer.Typer) -> None:
    """Register both argparse-backed recovery commands on a Typer application.

    Args:
        app: The CLI application receiving the two commands.
    """
    app.command(
        name=_PLANNER,
        cls=_ArgparseCommand,
        context_settings=PLAN_KRAKEN_SPOT_TRADE_RECOVERY_CONTEXT,
    )(plan_kraken_spot_trade_recovery)
    app.command(
        name=_IMPORTER, cls=_ArgparseCommand, context_settings=IMPORT_KRAKEN_SPOT_TRADES_CONTEXT
    )(import_kraken_spot_trades)
