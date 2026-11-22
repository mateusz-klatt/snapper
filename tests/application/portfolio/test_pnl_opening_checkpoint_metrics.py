"""Process-local metric tests for durable opening checkpoints."""

from snapper.application.portfolio.pnl_opening_checkpoint_metrics import checkpoint_metrics
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    record_checkpoint_capture_committed,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    record_checkpoint_capture_refusal,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import record_checkpoint_fallback
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    record_checkpoint_invalidation_deleted,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import record_checkpoint_recapture
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    record_checkpoint_resume_attempted,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    record_checkpoint_resume_succeeded,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    record_checkpoint_shadow_comparison,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    record_checkpoint_shadow_mismatch,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import reset_checkpoint_metrics


def test_checkpoint_metrics_are_detached_and_grouped_by_reason() -> None:
    """Count every transition and isolate snapshots from later mutations.

    Given: Reset process-local counters.
    When: Capture, resume, fallback, deletion, and shadow events are recorded.
    Then: Scalar counters and reason maps agree and earlier snapshots stay detached.
    """
    reset_checkpoint_metrics()
    record_checkpoint_capture_committed()
    record_checkpoint_capture_refusal("capture_value_error")
    record_checkpoint_capture_refusal("capture_value_error")
    record_checkpoint_resume_attempted()
    record_checkpoint_resume_succeeded()
    record_checkpoint_fallback("checkpoint_absent")
    record_checkpoint_invalidation_deleted()
    record_checkpoint_shadow_comparison()
    record_checkpoint_shadow_mismatch()
    record_checkpoint_recapture("age")
    record_checkpoint_recapture("age")

    snapshot = checkpoint_metrics()
    assert snapshot.captures_committed == 1
    assert snapshot.capture_refusals == {"capture_value_error": 2}
    assert snapshot.resumes_attempted == 1
    assert snapshot.resumes_succeeded == 1
    assert snapshot.fallbacks == {"checkpoint_absent": 1}
    assert snapshot.invalidations_deleted == 1
    assert snapshot.shadow_comparisons == 1
    assert snapshot.shadow_mismatches == 1
    assert snapshot.recaptures == {"age": 2}

    record_checkpoint_fallback("checkpoint_absent")
    assert snapshot.fallbacks == {"checkpoint_absent": 1}
    assert checkpoint_metrics().fallbacks == {"checkpoint_absent": 2}
    reset_checkpoint_metrics()
    assert checkpoint_metrics().captures_committed == 0
    assert checkpoint_metrics().capture_refusals == {}
