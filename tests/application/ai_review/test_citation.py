"""Tests for the manual-order ``ai_review_public_id`` citation validator.

Plan D Phase 2 #10 R1 — closes the unauthorized citation gap on the
manual-order entry points (MCP ``submit_manual_order`` + REST
``POST /api/orders``). Without this validator, any authenticated
caller could supply an arbitrary ``ai_review_public_id`` to trigger
``bus.caps_violation_after_ai_approve`` fanout to other delegates'
UIs (info leak + fanout spam).
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import uuid7

import pytest

from snapper.application.ai_review.citation import AiReviewCitationError
from snapper.application.ai_review.citation import validate_ai_review_citation
from snapper.data.repository import SQLAlchemyRepository

TEST_TIMEOUT = 15


async def _build_repo(tmp_path: Path) -> SQLAlchemyRepository:
    """Construct a fresh SQLite repository with the full schema."""
    db_path = tmp_path / "citation_test.db"
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path.as_posix()}")
    await repo.create_all()
    return repo


async def _seed_review(
    repo: SQLAlchemyRepository,
    *,
    user_public_id: str,
    wallet_public_id: str,
    status: str = "resolved_approved",
) -> str:
    """Insert a single ``ai_reviews`` row in the requested status; return public_id."""
    review_pid = str(uuid7())
    now = datetime.now(UTC)
    payload: dict[str, Any] = {
        "public_id": review_pid,
        "session_id": str(uuid7()),
        "sequence_id": 1,
        "user_public_id": user_public_id,
        "operator_public_id": str(uuid7()),
        "wallet_public_id": wallet_public_id,
        "instrument_public_id": str(uuid7()),
        "strategy_public_id": str(uuid7()),
        "selected_delegate_public_id": str(uuid7()),
        "status": status,
        "signal_envelope": {"side": "buy"},
        "signal_snapshot_hash": "h",
        "instrument_metadata": {},
        "deadline": now + timedelta(seconds=60),
        "fanout_after": now + timedelta(seconds=30),
        "dispatch_version": 0,
        "created_at": now,
        "updated_at": now,
    }
    if status.startswith("resolved_"):
        payload["decision"] = "approve" if status == "resolved_approved" else "reject"
        payload["responding_delegate_public_id"] = payload["selected_delegate_public_id"]
        payload["resolution_mode"] = "pick_one_primary"
        payload["resolved_at"] = now
    elif status == "timeout":
        payload["resolution_mode"] = "timeout_no_response"
        payload["resolved_at"] = now
    elif status == "superseded":
        payload["resolution_mode"] = "superseded_by_strategy"
        payload["resolved_at"] = now
    await repo.insert_ai_review(payload)
    return review_pid


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_happy_path_passes_for_owner_wallet_match_and_resolved_approved(
    tmp_path: Path,
) -> None:
    """Caller owns the row + wallet matches + status is resolved_approved -> no raise.

    Given a resolved_approved ai_review whose user + wallet match the caller's,
    When validate_ai_review_citation runs,
    Then it returns silently (no exception raised).
    """
    repo = await _build_repo(tmp_path)
    user_pid = str(uuid7())
    wallet_pid = str(uuid7())
    review_pid = await _seed_review(repo, user_public_id=user_pid, wallet_public_id=wallet_pid)
    await validate_ai_review_citation(
        repo,
        ai_review_public_id=review_pid,
        expected_user_public_id=user_pid,
        expected_wallet_public_id=wallet_pid,
    )


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_unknown_review_id_raises_citation_error(tmp_path: Path) -> None:
    """Citing a non-existent ai_review_public_id raises AiReviewCitationError.

    Given a fresh repo with no ai_reviews,
    When validate_ai_review_citation is called with a fabricated id,
    Then AiReviewCitationError fires with a "not found" message.
    """
    repo = await _build_repo(tmp_path)
    with pytest.raises(AiReviewCitationError, match="not found"):
        await validate_ai_review_citation(
            repo,
            ai_review_public_id="ghost-review-id",
            expected_user_public_id=str(uuid7()),
            expected_wallet_public_id=str(uuid7()),
        )


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_owner_mismatch_raises_citation_error(tmp_path: Path) -> None:
    """A caller citing another user's ai_review_public_id is rejected.

    Given an ai_review owned by user A,
    When user B tries to cite it,
    Then AiReviewCitationError fires with an "owner mismatch" message
    so the caller cannot trigger fanout to user A's delegate UI.
    """
    repo = await _build_repo(tmp_path)
    owner_pid = str(uuid7())
    attacker_pid = str(uuid7())
    wallet_pid = str(uuid7())
    review_pid = await _seed_review(repo, user_public_id=owner_pid, wallet_public_id=wallet_pid)
    with pytest.raises(AiReviewCitationError, match="owner mismatch"):
        await validate_ai_review_citation(
            repo,
            ai_review_public_id=review_pid,
            expected_user_public_id=attacker_pid,
            expected_wallet_public_id=wallet_pid,
        )


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_wallet_mismatch_raises_citation_error(tmp_path: Path) -> None:
    """Citation must match the submission's wallet, not just the user's.

    Given a resolved_approved review for wallet A,
    When the caller cites it on a submission for wallet B (still owned
    by the same user),
    Then AiReviewCitationError fires with a "wallet mismatch" message.
    The cross-link prevents the caller from cross-applying an approval
    issued for one wallet onto a manual order against a different
    wallet (which would silently bypass the AI delegate's intent).
    """
    repo = await _build_repo(tmp_path)
    user_pid = str(uuid7())
    wallet_a = str(uuid7())
    wallet_b = str(uuid7())
    review_pid = await _seed_review(repo, user_public_id=user_pid, wallet_public_id=wallet_a)
    with pytest.raises(AiReviewCitationError, match="wallet mismatch"):
        await validate_ai_review_citation(
            repo,
            ai_review_public_id=review_pid,
            expected_user_public_id=user_pid,
            expected_wallet_public_id=wallet_b,
        )


@pytest.mark.parametrize(
    "non_approved_status",
    ["pending", "fanout_dispatched", "resolved_rejected", "timeout", "superseded"],
)
@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_non_approved_status_raises_citation_error(
    tmp_path: Path, non_approved_status: str
) -> None:
    """Only ``resolved_approved`` rows may legitimately authorize a manual order.

    Given an ai_review row in any non-approved status (pending /
    fanout_dispatched / resolved_rejected / timeout / superseded),
    When the caller cites it,
    Then AiReviewCitationError fires referencing the wrong status so
    the caller cannot cite a pending / rejected / timed-out / abandoned
    row to trigger caps_violation fanout for a trade the AI either
    didn't approve or hadn't yet decided on.
    """
    repo = await _build_repo(tmp_path)
    user_pid = str(uuid7())
    wallet_pid = str(uuid7())
    review_pid = await _seed_review(
        repo,
        user_public_id=user_pid,
        wallet_public_id=wallet_pid,
        status=non_approved_status,
    )
    with pytest.raises(AiReviewCitationError, match="cannot authorize a manual order"):
        await validate_ai_review_citation(
            repo,
            ai_review_public_id=review_pid,
            expected_user_public_id=user_pid,
            expected_wallet_public_id=wallet_pid,
        )
