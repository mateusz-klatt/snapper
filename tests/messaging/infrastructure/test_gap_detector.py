"""Tests for GapDetector sequence gap detection."""

from loguru import logger

from snapper.messaging.infrastructure.gap_detector import GapDetector


class TestGapDetector:
    """Tests for GapDetector sequence tracking and gap reporting."""

    def test_reject_when_session_id_empty(self) -> None:
        """Unstamped message with empty session_id is rejected with warning.

        Given: A GapDetector,
        When: Checking with empty session_id,
        Then: Returns False, increments rejected_unstamped, logs warning.
        """
        messages: list[str] = []
        sink_id = logger.add(lambda msg: messages.append(str(msg)), level="WARNING")
        try:
            gd = GapDetector()
            result = gd.check("topic.a", "", 1)
        finally:
            logger.remove(sink_id)
        assert result is False
        assert gd.stats.rejected_unstamped == 1
        assert gd.stats.gaps_detected == 0
        assert gd.stats.session_resets == 0
        assert any("Rejected unstamped" in m for m in messages)

    def test_reject_when_sequence_id_zero(self) -> None:
        """Unstamped message with sequence_id 0 is rejected with warning.

        Given: A GapDetector,
        When: Checking with sequence_id of 0,
        Then: Returns False, increments rejected_unstamped, logs warning.
        """
        messages: list[str] = []
        sink_id = logger.add(lambda msg: messages.append(str(msg)), level="WARNING")
        try:
            gd = GapDetector()
            result = gd.check("topic.a", "session-abc", 0)
        finally:
            logger.remove(sink_id)
        assert result is False
        assert gd.stats.rejected_unstamped == 1
        assert gd.stats.gaps_detected == 0
        assert any("Rejected unstamped" in m for m in messages)

    def test_first_message_initializes_state(self) -> None:
        """First message on a new topic initializes tracking state.

        Given: A GapDetector with no prior state,
        When: Receiving sequence 1 on a topic,
        Then: No gaps, no mid-stream joins.
        """
        gd = GapDetector()
        gd.check("topic.a", "session-1", 1)
        assert gd.stats.mid_stream_joins == 0
        assert gd.stats.gaps_detected == 0

    def test_first_message_mid_stream_join(self) -> None:
        """First message with sequence > 1 counts as mid-stream join.

        Given: A GapDetector with no prior state,
        When: Receiving sequence 5 on a topic (joined after producer start),
        Then: mid_stream_joins is incremented.
        """
        gd = GapDetector()
        gd.check("topic.a", "session-1", 5)
        assert gd.stats.mid_stream_joins == 1

    def test_sequential_messages_no_gaps(self) -> None:
        """Sequential messages (1, 2, 3) produce no gaps.

        Given: A GapDetector,
        When: Receiving sequence 1, 2, 3 on the same topic and session,
        Then: No gaps detected, no duplicates.
        """
        gd = GapDetector()
        gd.check("topic.a", "s1", 1)
        gd.check("topic.a", "s1", 2)
        gd.check("topic.a", "s1", 3)
        assert gd.stats.gaps_detected == 0
        assert gd.stats.duplicates == 0

    def test_gap_detection(self) -> None:
        """Gap from sequence 2 to 5 detects 2 missing messages.

        Given: A GapDetector tracking a topic,
        When: Receiving sequence 1, 2, then 5 (skipping 3 and 4),
        Then: gaps_detected is 2.
        """
        gd = GapDetector()
        gd.check("topic.a", "s1", 1)
        gd.check("topic.a", "s1", 2)
        gd.check("topic.a", "s1", 5)
        assert gd.stats.gaps_detected == 2

    def test_session_reset(self) -> None:
        """New session_id on same topic triggers session reset.

        Given: A GapDetector tracking topic with session s1,
        When: Receiving a message with different session s2,
        Then: session_resets is incremented.
        """
        gd = GapDetector()
        gd.check("topic.a", "s1-aabb-ccdd", 1)
        gd.check("topic.a", "s2-eeff-0011", 1)
        assert gd.stats.session_resets == 1

    def test_duplicate_reorder_detection(self) -> None:
        """Receiving sequence below expected counts as duplicate.

        Given: A GapDetector that has seen sequences 1 and 2,
        When: Receiving sequence 1 again,
        Then: duplicates counter is incremented.
        """
        gd = GapDetector()
        gd.check("topic.a", "s1", 1)
        gd.check("topic.a", "s1", 2)
        gd.check("topic.a", "s1", 1)
        assert gd.stats.duplicates == 1

    def test_multiple_topics_independent(self) -> None:
        """Different topics are tracked independently.

        Given: A GapDetector,
        When: Receiving sequence 1 on topic.a and sequence 1 on topic.b,
        Then: No gaps or session resets.
        """
        gd = GapDetector()
        gd.check("topic.a", "s1", 1)
        gd.check("topic.b", "s1", 1)
        gd.check("topic.a", "s1", 2)
        gd.check("topic.b", "s1", 2)
        assert gd.stats.gaps_detected == 0
        assert gd.stats.session_resets == 0

    def test_stats_accumulate(self) -> None:
        """Stats counters accumulate across multiple events.

        Given: A GapDetector,
        When: Multiple gaps and duplicates occur,
        Then: All counters reflect total counts.
        """
        gd = GapDetector()
        gd.check("topic.a", "s1", 1)
        gd.check("topic.a", "s1", 4)
        gd.check("topic.a", "s1", 7)
        assert gd.stats.gaps_detected == 4

        gd.check("topic.a", "s1", 1)
        assert gd.stats.duplicates == 1

    def test_named_detector_includes_name_in_log(self) -> None:
        """GapDetector with name includes name in log output.

        Given: A GapDetector with name 'ws-client',
        When: A mid-stream join occurs,
        Then: Log message contains the detector name.
        """
        messages: list[str] = []
        sink_id = logger.add(lambda msg: messages.append(str(msg)), level="INFO")
        try:
            gd = GapDetector(name="ws-client")
            gd.check("topic.a", "s1-aabb-ccdd", 5)
        finally:
            logger.remove(sink_id)
        assert any("[GapDetector:ws-client]" in m for m in messages)

    def test_reset_topic_clears_state(self) -> None:
        """reset_topic removes tracking state so resubscribe starts fresh.

        Given: A GapDetector tracking topic.a at seq=3,
        When: reset_topic("topic.a") is called and new seq=1 arrives,
        Then: No gap is detected (state was cleared).
        """
        gd = GapDetector(name="bridge")
        gd.check("topic.a", "s1-aabb-ccdd", 1)
        gd.check("topic.a", "s1-aabb-ccdd", 2)
        gd.check("topic.a", "s1-aabb-ccdd", 3)
        gd.reset_topic("topic.a")
        result = gd.check("topic.a", "s1-aabb-ccdd", 1)
        assert result is True
        assert gd.stats.gaps_detected == 0

    def test_reset_topic_no_op_for_unknown(self) -> None:
        """reset_topic on unknown topic does not raise.

        Given: A GapDetector with no tracked topics,
        When: reset_topic is called for an untracked topic,
        Then: No error is raised.
        """
        gd = GapDetector()
        gd.reset_topic("nonexistent.topic")


class TestGapDetectorPhase0c7WalletPartitioning:
    """Phase 0c.7: ``(topic, wallet_public_id)`` stream partitioning."""

    def test_two_wallets_on_same_topic_do_not_collide(self) -> None:
        """Messages from two wallets on the same topic are tracked independently.

        Given: A GapDetector receiving messages on the same topic from
            two different per-wallet producers, each with its own
            monotonic sequence starting at 1,
        When: The interleaved sequences arrive,
        Then: No gaps are detected — the detector partitions state by
            ``(topic, wallet_public_id)``.
        """
        gd = GapDetector()
        wallet_a = "01975a8b-3c7d-7000-8000-0000000000a1"
        wallet_b = "01975a8b-aaaa-7000-8000-0000000000a2"
        gd.check("orders.events.kraken.BTC-USD.filled", "s1", 1, wallet_public_id=wallet_a)
        gd.check("orders.events.kraken.BTC-USD.filled", "s1", 1, wallet_public_id=wallet_b)
        gd.check("orders.events.kraken.BTC-USD.filled", "s1", 2, wallet_public_id=wallet_a)
        gd.check("orders.events.kraken.BTC-USD.filled", "s1", 2, wallet_public_id=wallet_b)
        assert gd.stats.gaps_detected == 0
        assert gd.stats.mid_stream_joins == 0
        assert gd.stats.session_resets == 0

    def test_legacy_empty_wallet_degrades_to_topic_only(self) -> None:
        """Omitting wallet_public_id keeps the legacy topic-only behavior.

        Given: A GapDetector called without ``wallet_public_id``,
        When: Two producers publish on the same topic (legacy single
            stream assumption),
        Then: The second producer's sequence=1 triggers a session_reset
            (expected topic-only semantics).
        """
        gd = GapDetector()
        gd.check("orders.events.kraken.BTC-USD.filled", "s1", 1)
        gd.check("orders.events.kraken.BTC-USD.filled", "s2", 1)
        assert gd.stats.session_resets == 1

    def test_wallet_partition_gap_detection(self) -> None:
        """Gap detection still runs per-wallet partition.

        Given: Wallet A publishes sequence 1, 2, 5 on a shared topic
            (gap of 2), wallet B publishes sequence 1, 2 on the same
            topic (no gap),
        When: The interleaved stream arrives,
        Then: Only the wallet-A gap is counted; wallet B is clean.
        """
        gd = GapDetector()
        wallet_a = "01975a8b-3c7d-7000-8000-0000000000a1"
        wallet_b = "01975a8b-aaaa-7000-8000-0000000000a2"
        gd.check("shared.topic", "s1", 1, wallet_public_id=wallet_a)
        gd.check("shared.topic", "s1", 1, wallet_public_id=wallet_b)
        gd.check("shared.topic", "s1", 2, wallet_public_id=wallet_a)
        gd.check("shared.topic", "s1", 2, wallet_public_id=wallet_b)
        gd.check("shared.topic", "s1", 5, wallet_public_id=wallet_a)
        assert gd.stats.gaps_detected == 2

    def test_reset_topic_clears_all_wallet_partitions(self) -> None:
        """reset_topic drops every wallet partition for the given topic.

        Given: A GapDetector tracking two wallet partitions on the
            same topic,
        When: ``reset_topic`` is called for the topic,
        Then: Both partitions are removed and a follow-up sequence=1
            from either wallet does not register as mid-stream join.
        """
        gd = GapDetector(name="bridge")
        wallet_a = "01975a8b-3c7d-7000-8000-0000000000a1"
        wallet_b = "01975a8b-aaaa-7000-8000-0000000000a2"
        gd.check("shared.topic", "s1", 1, wallet_public_id=wallet_a)
        gd.check("shared.topic", "s1", 1, wallet_public_id=wallet_b)
        gd.check("shared.topic", "s1", 2, wallet_public_id=wallet_a)
        gd.reset_topic("shared.topic")
        gd.check("shared.topic", "s1", 1, wallet_public_id=wallet_a)
        gd.check("shared.topic", "s1", 1, wallet_public_id=wallet_b)
        assert gd.stats.gaps_detected == 0
        assert gd.stats.mid_stream_joins == 0

    def test_log_label_includes_wallet_short_when_populated(self) -> None:
        """Gap log messages include a wallet short prefix when wallet is set."""
        messages: list[str] = []
        sink_id = logger.add(lambda msg: messages.append(str(msg)), level="WARNING")
        try:
            gd = GapDetector(name="trader")
            wallet = "01975a8b-3c7d-7000-8000-0000000000a1"
            gd.check("shared.topic", "s1", 1, wallet_public_id=wallet)
            gd.check("shared.topic", "s1", 5, wallet_public_id=wallet)
        finally:
            logger.remove(sink_id)
        assert any("wallet=" in m for m in messages)
