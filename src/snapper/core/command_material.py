"""Refuse replay of a durable intent whose historical request material changed."""

COMMAND_MATERIAL_CONFLICT_EVENT = "command_material_conflict"

TRADE_COMMAND_LIFECYCLE_FIELDS: frozenset[str] = frozenset(
    {
        "acked_at",
        "attempt_count",
        "dispatched_at",
        "exchange_order_id",
        "id",
        "known_to",
        "last_error",
        "public_id",
        "sequence_id",
        "session_id",
        "status",
        "terminal_at",
        "timestamp",
    }
)


class TradeCommandMaterialUnavailableError(RuntimeError):
    """Refuse CREATE when its durable request material cannot be proven consistent."""

    error_code = "trade_command_material_unavailable"

    def __init__(self, command_public_id: str | None, reason: str) -> None:
        """Retain known logical command identity without inventing one on missing lineage.

        Args:
            command_public_id: Actual durable logical UUID, or None when unresolved.
            reason: Explicit local proof failure, independent of venue order status.
        """
        self.command_public_id = command_public_id
        self._proof_reason = reason
        super().__init__(f"{self.error_code}: command {command_public_id}: {reason}")

    @property
    def reason(self) -> str:
        """Return the local proof failure independently of temporal record fields.

        Returns:
            The original local proof failure retained by this exception.
        """
        return self._proof_reason


class TradeCommandMaterialChangedError(TradeCommandMaterialUnavailableError):
    """Report a material conflict after its durable local dispatch barrier committed."""

    error_code = "trade_command_material_conflict"

    def __init__(self, command_public_id: str, changed_fields: tuple[str, ...]) -> None:
        """Identify the logical intent and exact conflicting material fields.

        Args:
            command_public_id: Actual durable command UUID shared by all its SCD2 versions.
            changed_fields: Immutable material fields that differ anywhere in its history.
        """
        self.changed_fields = changed_fields
        reason = (
            f"request material changed: {', '.join(changed_fields)}"
            if changed_fields
            else "durable dispatch barrier retained"
        )
        super().__init__(command_public_id, reason)
