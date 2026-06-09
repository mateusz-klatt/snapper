"""Pydantic schemas for the paired-execution operator surface.

Carries the read projections the paired-execution runbook works from — a
per-scope INCIDENT view joining the scope's durable halt (when present) with
every currently-exposed group and its per-leg signed exposure — plus the
terminalize attestation response. The incident is modelled per
``(wallet, strategy, group_key)`` SCOPE rather than per halt row: the durable
halt is active-unique per scope and its ``group_public_id`` records only the
group that first created it, so a halt-row-centric view could name a stale
group while a sibling keeps the halt alive, and an exposed scope whose halt is
momentarily missing (crash/clear windows) must still be discoverable — the
``halt_missing`` anomaly flag marks exactly that state.
"""

from datetime import datetime
from typing import Literal

from snapper.api.schemas.base import PayloadListResponse
from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictDataSchema


class PairedLegExposure(StrictDataSchema[Literal["paired_leg_exposure"]]):
    """Read projection of one paired-execution leg's current signed exposure.

    ``open_qty = filled_signed_qty - compensated_signed_qty`` is the residual
    the guard still considers un-flattened (buy +, sell −); the runbook's
    manual-flatten step trades exactly this quantity in the opposite
    direction. ``compensation_seq`` counts the flatten rounds already
    attempted.
    """

    type: Literal["paired_leg_exposure"] = "paired_leg_exposure"
    leg_public_id: str
    leg_index: int
    exchange: str
    instrument: str
    mode: str
    shard_key: str
    side: str
    status: str
    filled_signed_qty: float
    compensated_signed_qty: float
    open_qty: float
    compensation_seq: int


class PairedGroupIncident(StrictDataSchema[Literal["paired_group_incident"]]):
    """Read projection of one exposed paired-execution group with its legs."""

    type: Literal["paired_group_incident"] = "paired_group_incident"
    group_public_id: str
    status: str
    policy: str
    failure_reason: str | None
    halted_at: datetime | None
    created_at: datetime
    legs: list[PairedLegExposure]


class PairedHaltInfo(StrictDataSchema[Literal["paired_halt_info"]]):
    """Read projection of a scope's active durable halt row."""

    type: Literal["paired_halt_info"] = "paired_halt_info"
    halt_public_id: str
    reason: str
    group_public_id: str
    created_at: datetime


class PairedExecutionIncident(StrictDataSchema[Literal["paired_execution_incident"]]):
    """One halted / exposed paired-execution SCOPE with everything attached.

    ``halt_missing`` is True for the anomalous window where exposed groups
    exist but no durable halt row does (a clear racing a reopen, a crash
    between escalation and the halts sweep, startup before the first scan) —
    the scanner re-halts such a scope within a cycle, but the operator view
    must surface it rather than hide it behind a halt join.
    """

    type: Literal["paired_execution_incident"] = "paired_execution_incident"
    wallet_public_id: str
    strategy_id: str
    group_key: str
    halt: PairedHaltInfo | None
    halt_missing: bool
    groups: list[PairedGroupIncident]


class PairedExecutionIncidentListResponse(
    PayloadListResponse[Literal["paired_execution_incident_list_response"], PairedExecutionIncident]
):
    """List wrapper for ``GET /api/paired-execution/incidents``."""

    type: Literal["paired_execution_incident_list_response"] = (
        "paired_execution_incident_list_response"
    )


class PairedGroupTerminalizeResponse(
    PayloadResponse[Literal["paired_group_terminalize_response"], PairedGroupIncident]
):
    """Response for ``POST /api/paired-execution/groups/{id}/terminalize``.

    Carries the post-attestation group projection (now ``completed``) with
    its legs' true, unmodified accounting.
    """

    type: Literal["paired_group_terminalize_response"] = "paired_group_terminalize_response"
