"""Independent, bounded HTTP observation of an explicitly frozen refresh scope.

This monitor never refreshes prices or provisions credentials. A successful
observation proves freshness only, not conversion consistency or order admission.
The caller supplies an existing private bearer file and a reviewed scope digest.
"""

import asyncio
import math
import os
import re
import stat
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path
from uuid import UUID

import httpx
from pydantic import ValidationError

from snapper.api.schemas.market_snapshot_freshness import ExcludedMarketSnapshotFxInstrument
from snapper.api.schemas.market_snapshot_freshness import MarketSnapshotFreshnessData
from snapper.api.schemas.market_snapshot_freshness import MarketSnapshotFreshnessItem
from snapper.api.schemas.market_snapshot_freshness import MarketSnapshotFreshnessResponse
from snapper.application.market_snapshot_refresh.freshness import MAX_REQUESTED_INSTRUMENTS
from snapper.application.market_snapshot_refresh.freshness import SNAPSHOT_REFRESH_EXCHANGES
from snapper.application.market_snapshot_refresh.freshness import (
    market_snapshot_refresh_scope_digest,
)
from snapper.application.pricing.usd_converter import MAX_FUTURE_SKEW_SECONDS
from snapper.application.pricing.usd_converter import STALENESS_THRESHOLD_SECONDS
from snapper.core.market_snapshot_freshness import MAX_REFRESH_TARGETS
from snapper.data.repository_types import MarketSnapshotRefreshTargetRow

MAX_MONITOR_SECONDS = 10.0
MAX_RESPONSE_BYTES = 1_048_576
MAX_TOKEN_BYTES = 16_384
FRESHNESS_PATH = "/api/metrics/market-snapshots/freshness"


class FreshnessMonitorError(ValueError):
    """A fixed diagnostic code safe to log without response or credential data."""


def _canonical_ids(values: tuple[str, ...]) -> tuple[str, ...]:
    """Validate the raw scope bound before normalizing UUID aliases.

    Args:
        values: Explicit caller-supplied instrument identifiers.

    Returns:
        Canonical identities in first-occurrence order.

    Raises:
        FreshnessMonitorError: If any identifier or the raw scope is invalid.
    """
    if not values or len(values) > MAX_REQUESTED_INSTRUMENTS:
        raise FreshnessMonitorError("invalid_instrument_scope")
    if any(not value or value.strip() != value for value in values):
        raise FreshnessMonitorError("invalid_instrument_scope")
    try:
        return tuple(dict.fromkeys(str(UUID(value)) for value in values))
    except ValueError:
        raise FreshnessMonitorError("invalid_instrument_scope") from None


def _validate_endpoint(endpoint: str) -> None:
    """Refuse insecure origins, credentials, redirects and query-bearing endpoints.

    Args:
        endpoint: Exact HTTPS freshness route URL.

    Raises:
        FreshnessMonitorError: If the endpoint can send a bearer outside the route.
    """
    try:
        url = httpx.URL(endpoint)
    except httpx.InvalidURL:
        raise FreshnessMonitorError("invalid_endpoint") from None
    valid = (
        endpoint == endpoint.strip()
        and url.scheme == "https"
        and bool(url.host)
        and not url.userinfo
        and not url.query
        and not url.fragment
        and url.path == FRESHNESS_PATH
    )
    if not valid:
        raise FreshnessMonitorError("invalid_endpoint")


@dataclass(frozen=True, slots=True)
class FreshnessMonitorConfig:
    """Public configuration shared with the independently supervised refresh run.

    Attributes:
        endpoint: Exact HTTPS freshness route without credentials or query text.
        token_file: Existing private file holding only a bearer access token.
        instrument_public_ids: Explicit main instrument UUIDs, normalized on construction.
        fx_exchanges: Explicit supported FX venues, deduplicated on construction.
        expected_scope_digest: Reviewed SHA256 binding required and excluded metadata.
    """

    endpoint: str
    token_file: Path
    instrument_public_ids: tuple[str, ...]
    fx_exchanges: tuple[str, ...]
    expected_scope_digest: str

    def __post_init__(self) -> None:
        """Validate all public configuration before reading any credential."""
        _validate_endpoint(self.endpoint)
        object.__setattr__(
            self, "instrument_public_ids", _canonical_ids(self.instrument_public_ids)
        )
        if not self.fx_exchanges or len(self.fx_exchanges) > MAX_REQUESTED_INSTRUMENTS:
            raise FreshnessMonitorError("invalid_fx_scope")
        if any(value not in SNAPSHOT_REFRESH_EXCHANGES for value in self.fx_exchanges):
            raise FreshnessMonitorError("invalid_fx_scope")
        object.__setattr__(self, "fx_exchanges", tuple(dict.fromkeys(self.fx_exchanges)))
        if re.fullmatch(r"[0-9a-f]{64}", self.expected_scope_digest) is None:
            raise FreshnessMonitorError("invalid_scope_digest")


def _read_bearer(path: Path) -> str:
    """Read a bounded regular private file without following a final symlink.

    Args:
        path: Existing token file on the POSIX monitoring host.

    Returns:
        A single nonempty printable ASCII bearer value.

    Raises:
        FreshnessMonitorError: If the file is inaccessible, public or malformed.
    """
    if os.name != "posix":
        raise FreshnessMonitorError("unsupported_credential_platform")
    flags = os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
        with os.fdopen(descriptor, "rb") as handle:
            metadata = os.fstat(handle.fileno())
            if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) & 0o077:
                raise FreshnessMonitorError("invalid_token_file")
            raw = handle.read(MAX_TOKEN_BYTES + 1)
        token = raw.decode("ascii").strip()
    except (OSError, UnicodeError):
        raise FreshnessMonitorError("invalid_token_file") from None
    if len(raw) > MAX_TOKEN_BYTES or not token or any(not 33 <= ord(char) <= 126 for char in token):
        raise FreshnessMonitorError("invalid_token_file")
    return token


async def _read_response(response: httpx.Response) -> bytes:
    """Read an uncompressed JSON response with bounded decoded size.

    Args:
        response: Stream whose context is owned by the caller.

    Returns:
        At most one mebibyte of JSON bytes.

    Raises:
        FreshnessMonitorError: For HTTP errors, unsupported encodings or oversized bodies.
    """
    if response.status_code != 200:
        raise FreshnessMonitorError("http_status_refused")
    if response.headers.get("content-type", "").split(";", 1)[0].strip() != "application/json":
        raise FreshnessMonitorError("invalid_response_type")
    if response.headers.get("content-encoding", "identity").lower() != "identity":
        raise FreshnessMonitorError("invalid_response_encoding")
    content_length = response.headers.get("content-length")
    if content_length is not None:
        if (
            not content_length.isdecimal()
            or len(content_length) > 7
            or int(content_length) > MAX_RESPONSE_BYTES
        ):
            raise FreshnessMonitorError("response_too_large")
    body = bytearray()
    async for chunk in response.aiter_bytes(chunk_size=8192):
        if len(body) + len(chunk) > MAX_RESPONSE_BYTES:
            raise FreshnessMonitorError("response_too_large")
        body.extend(chunk)
    return bytes(body)


def _metadata(
    item: MarketSnapshotFreshnessItem | ExcludedMarketSnapshotFxInstrument,
) -> MarketSnapshotRefreshTargetRow:
    """Project response metadata into the shared canonical scope-digest shape.

    Args:
        item: One required or excluded instrument from a validated wire envelope.

    Returns:
        Identity metadata without prices, timestamps or diagnostic fields.

    Raises:
        FreshnessMonitorError: If returned identity metadata is not explicit.
    """
    values = (
        item.instrument_public_id,
        item.native_symbol,
        item.exchange,
        item.base,
        item.quote,
        item.asset_type,
    )
    if any(not value or value.strip() != value for value in values) or "*" in item.native_symbol:
        raise FreshnessMonitorError("inconsistent_scope")
    try:
        if str(UUID(item.instrument_public_id)) != item.instrument_public_id:
            raise FreshnessMonitorError("inconsistent_scope")
    except ValueError:
        raise FreshnessMonitorError("inconsistent_scope") from None
    return MarketSnapshotRefreshTargetRow(
        instrument_public_id=item.instrument_public_id,
        native_symbol=item.native_symbol,
        exchange=item.exchange,
        base=item.base,
        quote=item.quote,
        asset_type=item.asset_type,
    )


def _validate_scope(payload: MarketSnapshotFreshnessData, config: FreshnessMonitorConfig) -> None:
    """Require a complete disjoint scope whose metadata matches the frozen digest.

    Args:
        payload: Structurally validated freshness data.
        config: Explicit requested identities, FX venues and reviewed digest.

    Raises:
        FreshnessMonitorError: If scope echoes, classification or digests differ.
    """
    if (
        tuple(payload.requested_instrument_public_ids) != config.instrument_public_ids
        or tuple(payload.configured_fx_exchanges) != config.fx_exchanges
        or len(payload.instruments) + len(payload.excluded_fx_instruments) > MAX_REFRESH_TARGETS
    ):
        raise FreshnessMonitorError("inconsistent_scope")
    required = {item.instrument_public_id: _metadata(item) for item in payload.instruments}
    excluded = {
        item.instrument_public_id: _metadata(item) for item in payload.excluded_fx_instruments
    }
    if (
        len(required) != len(payload.instruments)
        or len(excluded) != len(payload.excluded_fx_instruments)
        or required.keys() & excluded.keys()
        or not set(config.instrument_public_ids).issubset(required)
    ):
        raise FreshnessMonitorError("inconsistent_scope")
    _validate_fx_coverage(config, required, excluded)
    digest = market_snapshot_refresh_scope_digest(
        list(config.instrument_public_ids), list(config.fx_exchanges), required, excluded
    )
    if digest != payload.scope_digest or digest != config.expected_scope_digest:
        raise FreshnessMonitorError("scope_digest_mismatch")


def _validate_fx_coverage(
    config: FreshnessMonitorConfig,
    required: dict[str, MarketSnapshotRefreshTargetRow],
    excluded: dict[str, MarketSnapshotRefreshTargetRow],
) -> None:
    """Independently refuse inconsistent venue classification or missing USD FX legs.

    Args:
        config: Explicit requested main instruments and selected FX venues.
        required: Required rows keyed by their canonical identity.
        excluded: Rows explicitly excluded from this refresh selection.

    Raises:
        FreshnessMonitorError: If a claimed ready scope omits a conversion dependency.
    """
    if any(row["exchange"] in config.fx_exchanges for row in excluded.values()):
        raise FreshnessMonitorError("inconsistent_scope")
    for identity, row in required.items():
        if row["exchange"] not in SNAPSHOT_REFRESH_EXCHANGES:
            raise FreshnessMonitorError("inconsistent_scope")
        if (
            identity not in config.instrument_public_ids
            and row["exchange"] not in config.fx_exchanges
        ):
            raise FreshnessMonitorError("inconsistent_scope")
    quotes = {required[identity]["quote"] for identity in config.instrument_public_ids} - {"USD"}
    for quote in quotes:
        if not any(
            row["exchange"] in config.fx_exchanges
            and row["asset_type"] == "forex"
            and (row["base"], row["quote"]) in {(quote, "USD"), ("USD", quote)}
            for row in required.values()
        ):
            raise FreshnessMonitorError("inconsistent_fx_coverage")


def _validate_times(
    payload: MarketSnapshotFreshnessData, started_at: datetime, now: datetime
) -> None:
    """Recheck every required original timestamp after the HTTP stream has closed.

    Args:
        payload: Ready response whose snapshot evidence must remain fresh.
        started_at: Local UTC observation just before the request.
        now: Fresh local clock after response cleanup.

    Raises:
        FreshnessMonitorError: If clocks, reported ages or original evidence are invalid.
    """
    checked = payload.checked_at
    if any(value.utcoffset() is None for value in (started_at, now, checked)):
        raise FreshnessMonitorError("invalid_observation_time")
    if now < started_at:
        raise FreshnessMonitorError("invalid_observation_time")
    if (checked - now).total_seconds() > MAX_FUTURE_SKEW_SECONDS or (
        started_at - checked
    ).total_seconds() > MAX_FUTURE_SKEW_SECONDS:
        raise FreshnessMonitorError("invalid_observation_time")
    for item in payload.instruments:
        _validate_item_time(item, checked, now)


def _validate_item_time(
    item: MarketSnapshotFreshnessItem, checked: datetime, now: datetime
) -> None:
    """Validate one required mark and its reported age against both clocks.

    Args:
        item: Required mark and the server's diagnostic fields.
        checked: Server observation clock.
        now: Final local observation clock.

    Raises:
        FreshnessMonitorError: If missing, invalid, stale or future evidence claims readiness.
    """
    timestamp = item.snapshot_timestamp
    age = item.age_seconds
    if timestamp is None or timestamp.utcoffset() is None or age is None or item.reason is not None:
        raise FreshnessMonitorError("inconsistent_readiness")
    actual_age = (checked - timestamp).total_seconds()
    if not math.isfinite(age) or not math.isclose(age, actual_age, rel_tol=0.0, abs_tol=0.000001):
        raise FreshnessMonitorError("inconsistent_readiness")
    final_age = (now - timestamp).total_seconds()
    if not -MAX_FUTURE_SKEW_SECONDS <= final_age <= STALENESS_THRESHOLD_SECONDS:
        raise FreshnessMonitorError("snapshot_not_fresh")
    if not -MAX_FUTURE_SKEW_SECONDS <= age <= STALENESS_THRESHOLD_SECONDS:
        raise FreshnessMonitorError("inconsistent_readiness")


async def check_market_snapshot_freshness(
    config: FreshnessMonitorConfig,
    *,
    timeout_seconds: float = MAX_MONITOR_SECONDS,
    transport: httpx.AsyncBaseTransport | None = None,
    now: Callable[[], datetime] | None = None,
) -> MarketSnapshotFreshnessData:
    """Read and validate one independent freshness observation within a caller budget.

    Args:
        config: Validated explicit configuration and private credential path.
        timeout_seconds: Positive remaining budget, at most ten seconds.
        transport: Optional injected HTTP transport for isolated tests.
        now: Optional UTC clock for deterministic boundary witnesses.

    Returns:
        A strictly validated fresh payload matching the reviewed scope digest.

    Raises:
        FreshnessMonitorError: With a fixed safe code on any failed observation.
    """
    if not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= MAX_MONITOR_SECONDS:
        raise FreshnessMonitorError("invalid_timeout")
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    clock = now if now is not None else lambda: datetime.now(UTC)
    bearer = _read_bearer(config.token_file)
    started_at = clock()
    parameters = [("instrument_public_ids", value) for value in config.instrument_public_ids]
    parameters.extend(("fx_exchanges", value) for value in config.fx_exchanges)
    headers = {
        "Authorization": f"Bearer {bearer}",
        "Accept": "application/json",
        "Accept-Encoding": "identity",
        "Cache-Control": "no-cache",
    }
    try:
        async with asyncio.timeout_at(deadline):
            async with (
                httpx.AsyncClient(
                    timeout=timeout_seconds,
                    follow_redirects=False,
                    trust_env=False,
                    transport=transport,
                ) as client,
                client.stream(
                    "GET", config.endpoint, params=parameters, headers=headers
                ) as response,
            ):
                body = await _read_response(response)
            payload = MarketSnapshotFreshnessResponse.model_validate_json(body).payload
            if not payload.freshness_ready or payload.unresolved_fx_currencies:
                raise FreshnessMonitorError("not_ready")
            _validate_scope(payload, config)
            _validate_times(payload, started_at, clock())
            if loop.time() >= deadline:
                raise FreshnessMonitorError("request_timeout")
    except TimeoutError:
        raise FreshnessMonitorError("request_timeout") from None
    except httpx.HTTPError:
        raise FreshnessMonitorError("request_failed") from None
    except ValidationError:
        raise FreshnessMonitorError("invalid_response") from None
    return payload
