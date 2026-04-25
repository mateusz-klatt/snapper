"""``AlertRule`` ABC + ``RuleRegistry`` for the notify sidecar (BE-3b §D6).

Each rule owns a fire condition on one or more ZMQ topic prefixes
plus an evaluator that turns a received event into zero or more
``AlertEventInsertRow`` rows. The sidecar consults a ``RuleRegistry``
at dispatch time to look up which rules apply to a given topic via
``get_longest_match`` (stable registration-order tiebreak).

The registry DOES NOT live in ``__init__.py`` per the project
invariant on empty init files (Plan 2 R10 INV-3 closure) — this module
owns the class plus a ``load_default_registry()`` factory that wires
in all four P0 rules.
"""

from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from datetime import datetime

from snapper.data.repository import Repository
from snapper.data.repository_types import AlertEventInsertRow


class AlertRule(ABC):
    """Observes upstream ZMQ event topic(s) and produces alert rows.

    Attributes:
        alert_type: One of the 5 enumerated P0 alert types —
            ``"order_fill_full"`` / ``"order_rejected"`` /
            ``"position_stop_loss_fired"`` / ``"critical_system_error"``
            / ``"margin_warning"``.
        subscribe_topic_prefixes: Tuple of ZMQ topic prefix strings
            the sidecar subscribes on behalf of this rule. Tuple
            (rather than a single string) is required by Rule 4's
            historical design which allowed multi-prefix
            subscriptions; the current v1.11 rules each use a single
            prefix but the contract stays multi-prefix-capable.
        priority: ``AlertPriority`` Literal — ``"low"`` / ``"medium"``
            / ``"high"``. Rendered into ``AlertEventInsertRow.priority``.
        is_safety_critical: When True the routing layer bypasses
            quiet-hours suppression and always delivers the alert.
        thread_key_prefix: Rendered into the alert's ``thread_key``
            so iOS groups related alerts under one notification
            thread.
        suppression_window_seconds: Dedup window checked against
            ``alert_events.dedup_key`` before emitting. ``0`` opts out
            of pre-check; the non-unique index on ``alert_events``
            provides defence in depth.
    """

    alert_type: str
    subscribe_topic_prefixes: tuple[str, ...]
    priority: str
    is_safety_critical: bool
    thread_key_prefix: str
    suppression_window_seconds: int

    @abstractmethod
    async def evaluate(
        self,
        topic: str,
        payload: bytes,
        repo: Repository,
        now: datetime,
    ) -> list[AlertEventInsertRow]:
        """Return zero or more alert rows for a single received event.

        Args:
            topic: ZMQ topic string the frame arrived on.
            payload: Raw JSON bytes of the ``StrictDataSchema`` payload.
            repo: Repository for enrichment reads (instruments,
                orders, users, dedup-window scan).
            now: Entry-boundary timestamp threaded by the sidecar per
                ``feedback_timestamp_discipline.md``.

        Returns:
            List of zero or more ``AlertEventInsertRow`` TypedDicts —
            each must have ``dedup_key`` populated so the caller can
            run the suppression-window check before insert. An empty
            list suppresses the alert (fire condition not met, or
            dedup-window hit, or enrichment failed).
        """
        ...


@dataclass
class RuleRegistry:
    """In-memory index of ``AlertRule`` instances keyed by prefix."""

    def __init__(self) -> None:
        """Initialise an empty registry — call ``register`` to populate."""
        self._rules: list[AlertRule] = []

    def register(self, rule: AlertRule) -> None:
        """Append a rule to the registry.

        Later registrations tiebreak on ``get_longest_match`` by
        registration order.

        Args:
            rule: Concrete ``AlertRule`` instance to index.
        """
        self._rules.append(rule)

    def all_subscribe_prefixes(self) -> tuple[str, ...]:
        """Union of every registered rule's ``subscribe_topic_prefixes``.

        Returns:
            Tuple of distinct prefix strings in the order they first
            appear across registered rules. Used by the sidecar to
            drive a single ``ValidatedSubscriber.subscribe(prefix)``
            call per distinct prefix at ``start()`` time.
        """
        seen: list[str] = []
        for rule in self._rules:
            for prefix in rule.subscribe_topic_prefixes:
                if prefix not in seen:
                    seen.append(prefix)
        return tuple(seen)

    def get_longest_match(self, topic: str) -> list[AlertRule]:
        """Return rules whose longest-matching prefix covers ``topic``.

        Rules are compared by the longest prefix each one subscribes
        to that is itself a proper prefix of ``topic``. Rules sharing
        the same longest prefix are returned in registration order
        (stable tiebreak).

        Args:
            topic: Fully-qualified ZMQ topic string (e.g.
                ``orders.events.kraken.BTC-USD.executed``).

        Returns:
            Rules that match the longest prefix. Empty when no rule's
            ``subscribe_topic_prefixes`` contains a prefix of ``topic``.
        """
        best_len = -1
        matches: list[AlertRule] = []
        for rule in self._rules:
            best_for_rule = -1
            for prefix in rule.subscribe_topic_prefixes:
                if topic.startswith(prefix) and len(prefix) > best_for_rule:
                    best_for_rule = len(prefix)
            if best_for_rule < 0:
                continue
            if best_for_rule > best_len:
                best_len = best_for_rule
                matches = [rule]
            elif best_for_rule == best_len:
                matches.append(rule)
        return matches
