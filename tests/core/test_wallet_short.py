"""Coverage for the wallet UUID7 → 12-hex routing suffix helpers."""

from snapper.core.wallet_short import compute_legacy_wallet_short
from snapper.core.wallet_short import compute_wallet_short


class TestComputeWalletShort:
    """``compute_wallet_short`` returns the canonical last-12-hex suffix."""

    def test_returns_last_12_hex_lowercase(self) -> None:
        """Last 12 hex chars (random portion) are returned lowercase.

        Given: A UUID7 with mixed-case hex characters,
        When: ``compute_wallet_short`` is called,
        Then: The returned string is the lowercase last 12 hex chars
            with dashes stripped.
        """
        assert compute_wallet_short("ABCDEF12-3456-7890-ABCD-EF0123456789") == "ef0123456789"

    def test_handles_dashless_input(self) -> None:
        """Dashless UUIDs round-trip through dash-strip cleanly.

        Given: A UUID7 already without dashes,
        When: ``compute_wallet_short`` is called,
        Then: The function still returns the lowercase last 12 chars.
        """
        assert compute_wallet_short("abcdef1234567890abcdef0123456789") == "ef0123456789"

    def test_same_millisecond_wallets_have_distinct_suffixes(self) -> None:
        """UUID7 wallets sharing the timestamp prefix still produce distinct suffixes.

        Given: Two UUID7 values whose first 12 hex chars are identical
            (same millisecond, same version+random_a) but random_b
            differs,
        When: ``compute_wallet_short`` is called on each,
        Then: The two returned suffixes are distinct — preventing the
            same-millisecond collision the legacy first-12 algorithm
            had.
        """
        same_prefix_a = "01975a8b-3c7d-7000-8000-aaaaaaaaaaaa"
        same_prefix_b = "01975a8b-3c7d-cccc-8000-bbbbbbbbbbbb"
        assert compute_wallet_short(same_prefix_a) == "aaaaaaaaaaaa"
        assert compute_wallet_short(same_prefix_b) == "bbbbbbbbbbbb"


class TestComputeLegacyWalletShort:
    """``compute_legacy_wallet_short`` preserves the first-12-hex algorithm.

    Recovery code populates a backward-compat lookup with both keys so
    persisted shard_keys written under the legacy algorithm still
    resolve their wallet.
    """

    def test_returns_first_12_hex_lowercase(self) -> None:
        """First 12 hex chars (timestamp prefix) are returned lowercase."""
        assert compute_legacy_wallet_short("ABCDEF12-3456-7890-ABCD-EF0123456789") == (
            "abcdef123456"
        )

    def test_same_millisecond_wallets_collide(self) -> None:
        """Documented collision: same-ms UUID7s share the legacy suffix.

        Given: Two UUID7 values from the same millisecond,
        When: ``compute_legacy_wallet_short`` is called on each,
        Then: They return IDENTICAL suffixes — the bug the canonical
            ``compute_wallet_short`` was introduced to fix.
        """
        same_prefix_a = "01975a8b-3c7d-7000-8000-aaaaaaaaaaaa"
        same_prefix_b = "01975a8b-3c7d-cccc-8000-bbbbbbbbbbbb"
        assert compute_legacy_wallet_short(same_prefix_a) == compute_legacy_wallet_short(
            same_prefix_b
        )
