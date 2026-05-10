"""Wallet UUID7 → 12-hex routing suffix helper.

The per-wallet executor / shard / engine system uses a 12-hex prefix
of the wallet ``public_id`` UUID7 as a routing suffix
(``executor_<exchange>_w<wallet_short>``,
``<exchange>.<instrument>.<mode>.w<wallet_short>``, etc.).

The canonical algorithm uses the LAST 12 hex characters (the random
portion of UUID7) which keeps collisions limited to ~1 in 2^48 per
same-exchange wallet pair. The legacy algorithm used the FIRST 12 hex
characters — the timestamp prefix — and collided deterministically
for any two wallets created in the same millisecond. Recovery code
that reads persisted ``shard_key`` columns written under the legacy
algorithm uses :func:`compute_legacy_wallet_short` to populate
backward-compat lookup entries alongside the canonical ones.
"""


def compute_wallet_short(wallet_public_id: str) -> str:
    """Return the canonical 12-hex routing suffix for a wallet UUID7.

    Uses the last 12 hex characters of the public_id (random portion)
    so collisions on a given exchange are limited to ~1 in 2^48.

    Args:
        wallet_public_id: Wallet UUID7 with or without dashes.

    Returns:
        Lowercase 12-hex suffix used in process names, shard keys,
        engine keys, and heartbeat components.
    """
    return wallet_public_id.replace("-", "")[-12:].lower()


def compute_legacy_wallet_short(wallet_public_id: str) -> str:
    """Return the legacy first-12-hex routing suffix.

    Used solely by recovery code that needs to resolve persisted
    ``shard_key`` strings written before the algorithm was changed
    from first-12 to last-12. New code MUST NOT use this function.

    Args:
        wallet_public_id: Wallet UUID7 with or without dashes.

    Returns:
        Lowercase first-12-hex suffix as the legacy code wrote it.
    """
    return wallet_public_id.replace("-", "")[:12].lower()
