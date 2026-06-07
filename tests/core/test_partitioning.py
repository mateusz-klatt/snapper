"""Tests for :mod:`snapper.core.partitioning`.

Covers:
    - Validation of ``instance_id`` and ``instance_count``.
    - Hash stability within a process.
    - Hash stability across Python subprocesses with randomised
      ``PYTHONHASHSEED`` (proves SHA-256 ignores the builtin hash
      randomisation).
    - Uniform distribution of ownership over a synthetic key space.
    - Single-instance short-circuit (``instance_count=1`` always owns).
    - :class:`ShardOwnershipError` string + attribute contract +
      pickle / :func:`copy.copy` round-trip.
"""

import copy
import pickle
import subprocess
import sys
from collections import Counter

import pytest

from snapper.core.partitioning import ShardOwnership
from snapper.core.partitioning import ShardOwnershipError


class TestShardOwnershipValidation:
    """Constructor argument validation."""

    def test_defaults_require_explicit_values(self) -> None:
        """``ShardOwnership`` is a dataclass without defaults — id and count required.

        Call is routed through ``**{}`` so the type checker sees
        potentially-valid kwargs while Python raises ``TypeError`` at
        runtime for the missing required args.
        """
        empty_kwargs: dict[str, int] = {}
        with pytest.raises(TypeError):
            ShardOwnership(**empty_kwargs)

    def test_valid_single_instance(self) -> None:
        """``(0, 1)`` is the single-instance default and must validate."""
        ownership = ShardOwnership(instance_id=0, instance_count=1)
        assert ownership.instance_id == 0
        assert ownership.instance_count == 1

    def test_valid_multi_instance(self) -> None:
        """Any ``(i, N)`` with ``0 <= i < N`` is valid."""
        for count in (2, 3, 8, 100):
            for instance_id in range(count):
                ShardOwnership(instance_id=instance_id, instance_count=count)

    def test_instance_count_zero_rejected(self) -> None:
        """``instance_count == 0`` is outside the supported range."""
        with pytest.raises(ValueError, match=r"instance_count must be >= 1"):
            ShardOwnership(instance_id=0, instance_count=0)

    def test_instance_count_negative_rejected(self) -> None:
        """Negative ``instance_count`` is rejected."""
        with pytest.raises(ValueError, match=r"instance_count must be >= 1"):
            ShardOwnership(instance_id=0, instance_count=-1)

    def test_instance_id_negative_rejected(self) -> None:
        """Negative ``instance_id`` is outside the range ``[0, count)``."""
        with pytest.raises(ValueError, match=r"instance_id -1 out of range"):
            ShardOwnership(instance_id=-1, instance_count=2)

    def test_instance_id_equal_to_count_rejected(self) -> None:
        """``instance_id == instance_count`` is outside the half-open range."""
        with pytest.raises(ValueError, match=r"instance_id 2 out of range"):
            ShardOwnership(instance_id=2, instance_count=2)

    def test_instance_id_above_count_rejected(self) -> None:
        """``instance_id > instance_count`` is rejected."""
        with pytest.raises(ValueError, match=r"instance_id 10 out of range"):
            ShardOwnership(instance_id=10, instance_count=3)


class TestShardOwnershipHash:
    """``_hash`` is a deterministic, stable SHA-256 prefix."""

    def test_hash_is_stable_within_process(self) -> None:
        """Repeated calls on the same key yield the same int."""
        key = "kraken.BTC-USD.live"
        hash_a = ShardOwnership._hash(key)
        hash_b = ShardOwnership._hash(key)
        assert hash_a == hash_b

    def test_hash_is_uint64(self) -> None:
        """Result fits in a 64-bit unsigned integer."""
        key = "paper.ETH-USD.paper"
        result = ShardOwnership._hash(key)
        assert 0 <= result < 2**64

    def test_hash_differs_for_different_keys(self) -> None:
        """Different keys produce different hashes (with overwhelming probability)."""
        samples = {ShardOwnership._hash(f"k-{i}") for i in range(1000)}
        assert len(samples) == 1000

    def test_hash_stable_across_processes_with_random_pythonhashseed(self) -> None:
        """Hash is not affected by ``PYTHONHASHSEED`` (unlike builtin ``hash``).

        Runs a subprocess with ``PYTHONHASHSEED=random`` and compares
        the hash it produces for a fixed key against the in-process
        value. If SHA-256 ever got replaced with ``hash()`` (or tainted
        by any randomised source), this test fails.
        """
        key = "kraken.BTC-USD.live"
        in_process = ShardOwnership._hash(key)
        script = (
            "from snapper.core.partitioning import ShardOwnership; "
            f"print(ShardOwnership._hash({key!r}))"
        )
        result = subprocess.run(
            [sys.executable, "-c", script],
            env={"PYTHONHASHSEED": "random", "PATH": "/usr/bin:/bin"},
            capture_output=True,
            check=True,
            text=True,
        )
        subprocess_value = int(result.stdout.strip())
        assert in_process == subprocess_value


class TestShardOwnershipOwns:
    """``owns`` semantics under single- and multi-instance deployments."""

    def test_single_instance_always_owns(self) -> None:
        """At ``instance_count=1`` every key is owned — the no-op partitioning case."""
        ownership = ShardOwnership(instance_id=0, instance_count=1)
        for key in ("", "a", "kraken.BTC-USD.live", "paper.ETH-USD.paper.tag"):
            assert ownership.owns(key) is True

    def test_two_instance_partitions_disjoint_and_complete(self) -> None:
        """With N=2, a key is owned by exactly one instance; union covers all."""
        ownership_a = ShardOwnership(instance_id=0, instance_count=2)
        ownership_b = ShardOwnership(instance_id=1, instance_count=2)
        for i in range(500):
            key = f"shard-{i}"
            assert ownership_a.owns(key) != ownership_b.owns(key)

    def test_uniform_distribution_across_eight_instances(self) -> None:
        """Over 10k synthetic keys, ownership is roughly uniform across N=8.

        Expected per-bucket count: 1250. A loose tolerance (half the
        expected count) is enough to distinguish uniform SHA-256 from a
        pathological distribution, while not flaking on normal
        statistical variation.
        """
        count = 8
        samples = 10_000
        ownerships = [ShardOwnership(instance_id=i, instance_count=count) for i in range(count)]
        bucket_sizes: Counter[int] = Counter()
        for i in range(samples):
            key = f"instrument-{i}.mode-{i % 3}.live"
            for instance_id, ownership in enumerate(ownerships):
                if ownership.owns(key):
                    bucket_sizes[instance_id] += 1
                    break
        expected = samples / count
        for instance_id, size in bucket_sizes.items():
            assert (
                expected / 2 < size < expected * 2
            ), f"bucket {instance_id} has {size} keys, far from expected {expected}"
        assert sum(bucket_sizes.values()) == samples

    def test_deterministic_per_key_across_constructors(self) -> None:
        """Two ``ShardOwnership`` instances with same id/count agree on every key."""
        a = ShardOwnership(instance_id=1, instance_count=4)
        b = ShardOwnership(instance_id=1, instance_count=4)
        for i in range(200):
            key = f"k-{i}"
            assert a.owns(key) == b.owns(key)

    def test_different_instance_ids_disagree(self) -> None:
        """Different instance_ids see disjoint ownership slices."""
        a = ShardOwnership(instance_id=0, instance_count=3)
        b = ShardOwnership(instance_id=1, instance_count=3)
        c = ShardOwnership(instance_id=2, instance_count=3)
        for i in range(200):
            key = f"k-{i}"
            owns_count = sum(1 for o in (a, b, c) if o.owns(key))
            assert owns_count == 1


class TestShardOwnershipError:
    """Error carries the shard_key + ownership context."""

    def test_str_contains_shard_and_instance(self) -> None:
        """Message names the shard and the ``I/N`` identifier."""
        exc = ShardOwnershipError(
            shard_key="kraken.BTC-USD.live",
            instance_id=1,
            instance_count=2,
        )
        message = str(exc)
        assert "kraken.BTC-USD.live" in message
        assert "1/2" in message

    def test_is_exception_subclass(self) -> None:
        """Raised normally via ``raise`` — must subclass :class:`Exception`."""
        with pytest.raises(ShardOwnershipError) as info:
            raise ShardOwnershipError(shard_key="x", instance_id=0, instance_count=2)
        assert info.value.shard_key == "x"
        assert info.value.instance_id == 0
        assert info.value.instance_count == 2

    def test_attributes_exposed_on_instance(self) -> None:
        """Dataclass exposes the three fields as attributes."""
        exc = ShardOwnershipError(shard_key="s", instance_id=3, instance_count=5)
        assert exc.shard_key == "s"
        assert exc.instance_id == 3
        assert exc.instance_count == 5

    def test_pickle_round_trip_preserves_fields(self) -> None:
        """Round-trip through :mod:`pickle` reconstructs the same exception.

        Regression guard against a subtle ``@dataclass``-on-``Exception``
        pitfall: the generated ``__init__`` does not call
        ``super().__init__()``, leaving ``BaseException.args`` empty. The
        default ``BaseException.__reduce__`` would then serialise as
        ``(ShardOwnershipError, ())`` and unpickling would call the
        no-arg form → :class:`TypeError` for missing required fields.
        :meth:`ShardOwnershipError.__post_init__` populates ``args`` to
        fix this.
        """
        original = ShardOwnershipError(
            shard_key="kraken.BTC-USD.live",
            instance_id=1,
            instance_count=2,
        )
        restored = pickle.loads(pickle.dumps(original))
        assert isinstance(restored, ShardOwnershipError)
        assert restored.shard_key == original.shard_key
        assert restored.instance_id == original.instance_id
        assert restored.instance_count == original.instance_count
        assert str(restored) == str(original)

    def test_copy_round_trip_preserves_fields(self) -> None:
        """Round-trip through :func:`copy.copy` also works (uses ``__reduce__``)."""
        original = ShardOwnershipError(
            shard_key="paper.ETH-USD.paper.scalp",
            instance_id=0,
            instance_count=4,
        )
        restored = copy.copy(original)
        assert restored.shard_key == original.shard_key
        assert restored.instance_id == original.instance_id
        assert restored.instance_count == original.instance_count

    def test_args_exposes_constructor_values(self) -> None:
        """``BaseException.args`` should carry the three fields verbatim."""
        exc = ShardOwnershipError(shard_key="x.y.z", instance_id=2, instance_count=3)
        assert exc.args == ("x.y.z", 2, 3)
