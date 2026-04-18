"""Static-hash shard partitioning primitives for the trade runtime.

This module provides :class:`ShardOwnership` which decides — given a
process-level ``(instance_id, instance_count)`` tuple — whether this
coordinator owns a given ``shard_key``. Ownership is deterministic
across Python restarts, machines, and versions because the hash is
SHA-256 over the UTF-8 encoding of the shard key (the first 8 bytes
interpreted as a big-endian unsigned 64-bit integer, modulo
``instance_count``).

Design notes:
    - Python's builtin ``hash()`` is randomised per process by
      ``PYTHONHASHSEED``. SHA-256 is deterministic, uniform, and uses
      well-understood library code. The uint64 prefix is plenty of
      entropy for a modulo into a small instance count.
    - ``instance_count=1`` is the default / pre-partitioning mode —
      every shard key is owned, so :meth:`ShardOwnership.owns` returns
      True unconditionally.
    - Validation is fail-fast at construction time via ``__post_init__``.
      ``instance_count < 1`` or ``instance_id`` outside
      ``[0, instance_count)`` raises :class:`ValueError`.

Example:
    >>> ownership = ShardOwnership(instance_id=0, instance_count=2)
    >>> ownership.owns("kraken.BTC-USD.live")  # deterministic bool
    True

Raises:
    ShardOwnershipError: Raised by the repository insert guard when a
        TradeCommand row's shard_key is not owned by the caller's
        :class:`ShardOwnership`. Callers that legitimately insert
        foreign-shard rows (HTTP handlers, plan services) pass
        ``ownership=None`` to bypass the check.
"""

import hashlib
from dataclasses import dataclass

__all__ = ["ShardOwnership", "ShardOwnershipError"]


@dataclass(frozen=True)
class ShardOwnership:
    """Deterministic static-hash shard ownership decision.

    Attributes:
        instance_id: Zero-based identifier for this coordinator
            instance. Must satisfy ``0 <= instance_id < instance_count``.
        instance_count: Total number of coordinator instances sharing
            the same DB/broker. Must be ``>= 1``.
    """

    instance_id: int
    instance_count: int

    def __post_init__(self) -> None:
        """Validate the ``(instance_id, instance_count)`` tuple.

        Raises:
            ValueError: If ``instance_count < 1`` or ``instance_id`` is
                outside the half-open range ``[0, instance_count)``.
        """
        if self.instance_count < 1:
            raise ValueError(f"instance_count must be >= 1, got {self.instance_count}")
        if not 0 <= self.instance_id < self.instance_count:
            raise ValueError(
                f"instance_id {self.instance_id} out of range [0, {self.instance_count})"
            )

    @staticmethod
    def _hash(shard_key: str) -> int:
        """Return a stable uint64 hash of ``shard_key``.

        SHA-256 is used instead of Python's builtin ``hash()`` to ensure
        the result is identical across Python restarts (not affected by
        ``PYTHONHASHSEED``), Python versions, and machines. The first 8
        bytes of the digest are interpreted as a big-endian unsigned
        64-bit integer — plenty of entropy for modulo into a small
        instance count and cheap to compute.

        Args:
            shard_key: The shard key (e.g., ``"kraken.BTC-USD.live"``).

        Returns:
            A deterministic unsigned 64-bit integer.
        """
        digest = hashlib.sha256(shard_key.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], byteorder="big")

    def owns(self, shard_key: str) -> bool:
        """Decide whether this coordinator owns ``shard_key``.

        At ``instance_count == 1`` every shard is owned, so the result
        is always True — Phase 4 is a pure no-op in single-instance
        deployments.

        Args:
            shard_key: The shard key to check.

        Returns:
            True iff ``_hash(shard_key) % instance_count == instance_id``.
        """
        if self.instance_count == 1:
            return True
        return self._hash(shard_key) % self.instance_count == self.instance_id


@dataclass
class ShardOwnershipError(Exception):
    """Raised when a repository insert targets a foreign-shard row.

    The repository's :meth:`insert_trade_command` method uses this
    exception as a defense-in-depth guard against operational
    misconfiguration (e.g., overlapping ``instance_id`` across
    coordinators) and future split-brain (if HA is ever added). The
    guard is opt-in — callers pass ``ownership=None`` to skip it.

    Attributes:
        shard_key: The shard key that was rejected.
        instance_id: The coordinator instance attempting the insert.
        instance_count: The total coordinator count at rejection time.
    """

    shard_key: str
    instance_id: int
    instance_count: int

    def __str__(self) -> str:
        """Return a human-readable error message.

        Returns:
            A string of the form ``shard 'X' is not owned by instance
            I/N``.
        """
        return (
            f"shard {self.shard_key!r} is not owned by instance "
            f"{self.instance_id}/{self.instance_count}"
        )
