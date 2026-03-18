"""Tests for GapDetector sequence gap detection."""

from loguru import logger

from snapper.messaging.infrastructure.gap_detector import GapDetector


class TestGapDetector:
    """Tests for GapDetector sequence tracking and gap reporting."""

    def test_skip_when_session_id_empty(self) -> None:
        """Check is skipped when session_id is empty.

        Given: A GapDetector,
        When: Checking with empty session_id,
        Then: No state is initialized and stats remain zero.
        """
        gd = GapDetector()
        gd.check("topic.a", "", 1)
        assert gd.stats.gaps_detected == 0
        assert gd.stats.session_resets == 0

    def test_skip_when_sequence_id_zero(self) -> None:
        """Check is skipped when sequence_id is 0.

        Given: A GapDetector,
        When: Checking with sequence_id of 0,
        Then: No state is initialized and stats remain zero.
        """
        gd = GapDetector()
        gd.check("topic.a", "session-abc", 0)
        assert gd.stats.gaps_detected == 0

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
