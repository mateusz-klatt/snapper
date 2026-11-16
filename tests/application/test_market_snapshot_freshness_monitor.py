"""Independent monitor witnesses using private temporary files and mocked HTTP."""

import asyncio
import time
from collections.abc import AsyncIterator
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import httpx
import pytest

from snapper.api.schemas.market_snapshot_freshness import ExcludedMarketSnapshotFxInstrument
from snapper.api.schemas.market_snapshot_freshness import MarketSnapshotFreshnessData
from snapper.api.schemas.market_snapshot_freshness import MarketSnapshotFreshnessItem
from snapper.api.schemas.market_snapshot_freshness import MarketSnapshotFreshnessResponse
from snapper.application.market_snapshot_refresh import monitor
from snapper.application.market_snapshot_refresh.freshness import (
    market_snapshot_refresh_scope_digest,
)
from snapper.application.market_snapshot_refresh.monitor import FreshnessMonitorConfig
from snapper.application.market_snapshot_refresh.monitor import FreshnessMonitorError
from snapper.application.market_snapshot_refresh.monitor import check_market_snapshot_freshness
from snapper.data.repository_types import MarketSnapshotRefreshTargetRow

_NOW = datetime(2026, 11, 16, 20, tzinfo=UTC)
_MAIN = "feed0000-1234-4567-89ab-0123456789ab"
_FX = "feed0000-1234-4567-89ab-0123456789ac"
_EXCLUDED = "feed0000-1234-4567-89ab-0123456789ad"
_TOKEN = "temporary-test-bearer"


def test_unsupported_credential_platform_refuses_safely(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Return a sanitized failure before using POSIX-only file-opening flags.

    Given: A credential path on a platform without the required POSIX guarantees.
    When: The monitor attempts to read a credential.
    Then: It refuses before accessing platform-specific file-opening flags.
    """
    token = tmp_path / "token"
    monkeypatch.setattr(monitor.os, "name", "nt")
    with pytest.raises(FreshnessMonitorError, match="unsupported_credential_platform"):
        monitor._read_bearer(token)


def _target(
    identity: str, base: str, quote: str, exchange: str = "kraken"
) -> MarketSnapshotRefreshTargetRow:
    """Build explicit metadata for a synthetic test identity.

    Args:
        identity: Canonical UUID.
        base: Base asset code.
        quote: Quote currency code.
        exchange: Source venue.

    Returns:
        Required digest fields without evidence prices.
    """
    return MarketSnapshotRefreshTargetRow(
        instrument_public_id=identity,
        native_symbol=f"{base}/{quote}",
        exchange=exchange,
        base=base,
        quote=quote,
        asset_type="forex" if base == "EUR" else "crypto",
    )


def _payload(now: datetime = _NOW) -> MarketSnapshotFreshnessData:
    """Build a coherent ready response with both selected and excluded FX evidence.

    Args:
        now: Shared server and snapshot observation time.

    Returns:
        A ready payload whose digest includes all classified metadata.
    """
    required = {_MAIN: _target(_MAIN, "BTC", "EUR"), _FX: _target(_FX, "EUR", "USD")}
    excluded = {_EXCLUDED: _target(_EXCLUDED, "EUR", "USD", "polygon")}
    return MarketSnapshotFreshnessData(
        public_id="payload",
        session_id="session",
        sequence_id=1,
        timestamp=now,
        requested_instrument_public_ids=[_MAIN],
        configured_fx_exchanges=["kraken"],
        scope_digest=market_snapshot_refresh_scope_digest([_MAIN], ["kraken"], required, excluded),
        checked_at=now,
        freshness_ready=True,
        unresolved_fx_currencies=[],
        instruments=[
            MarketSnapshotFreshnessItem(**row, snapshot_timestamp=now, age_seconds=0.0, reason=None)
            for row in required.values()
        ],
        excluded_fx_instruments=[
            ExcludedMarketSnapshotFxInstrument(**row) for row in excluded.values()
        ],
    )


def _body(payload: MarketSnapshotFreshnessData) -> bytes:
    """Serialize an actual strict response envelope.

    Args:
        payload: Deliberately controlled evidence payload.

    Returns:
        UTF-8 JSON bytes.
    """
    return (
        MarketSnapshotFreshnessResponse(
            public_id="envelope",
            session_id="session",
            sequence_id=2,
            timestamp=_NOW,
            payload=payload,
        )
        .model_dump_json()
        .encode()
    )


def _config(tmp_path: Path) -> FreshnessMonitorConfig:
    """Create explicit scope configuration and a private synthetic bearer file.

    Args:
        tmp_path: Isolated test directory.

    Returns:
        Valid configuration without any production credential.
    """
    token = tmp_path / "token"
    token.write_text(_TOKEN, encoding="ascii")
    token.chmod(0o600)
    return FreshnessMonitorConfig(
        endpoint="https://monitor.invalid/api/metrics/market-snapshots/freshness",
        token_file=token,
        instrument_public_ids=(_MAIN,),
        fx_exchanges=("kraken",),
        expected_scope_digest=_payload().scope_digest,
    )


class _Stream(httpx.AsyncByteStream):
    """Track cleanup while supplying fixed chunks or an interrupted stream."""

    def __init__(self, chunks: list[bytes], delay: float = 0.0) -> None:
        """Retain controlled bytes and an optional await delay.

        Args:
            chunks: Response body pieces.
            delay: Per-stream asynchronous delay.
        """
        self.chunks = chunks
        self.delay = delay
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        """Yield supplied chunks after an optional cancellation point.

        Yields:
            The next controlled response bytes.
        """
        if self.delay:
            await asyncio.sleep(self.delay)
        for chunk in self.chunks:
            yield chunk

    async def aclose(self) -> None:
        """Record disposal independently from successful body consumption."""
        self.closed = True


async def _observe(
    config: FreshnessMonitorConfig,
    payload: MarketSnapshotFreshnessData,
    now: Callable[[], datetime] = lambda: _NOW,
) -> MarketSnapshotFreshnessData:
    """Call the real monitor against a controlled streaming transport.

    Args:
        config: Explicit local monitor configuration.
        payload: Server evidence to serialize.
        now: Local observation clock.

    Returns:
        The accepted payload, if all validations pass.
    """
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200, headers={"content-type": "application/json"}, stream=_Stream([_body(payload)])
        )
    )
    return await check_market_snapshot_freshness(config, transport=transport, now=now)


async def test_ready_request_is_explicit_authenticated_and_stream_is_closed(tmp_path: Path) -> None:
    """Observe the complete frozen scope without following implicit defaults.

    Given: A private test bearer and one explicit main with selected FX evidence.
    When: The independent monitor reads a streamed valid response.
    Then: Query scope and auth are exact, response cleanup finishes, and readiness passes.
    """
    config = _config(tmp_path)
    stream = _Stream([_body(_payload())])
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        """Capture the sole request and return isolated evidence."""
        requests.append(request)
        return httpx.Response(
            200, headers={"content-type": "application/json; charset=utf-8"}, stream=stream
        )

    result = await check_market_snapshot_freshness(
        config, transport=httpx.MockTransport(handle), now=lambda: _NOW
    )
    assert result.freshness_ready
    assert stream.closed
    assert len(requests) == 1
    assert requests[0].url.params.multi_items() == [
        ("instrument_public_ids", _MAIN),
        ("fx_exchanges", "kraken"),
    ]
    assert requests[0].headers["authorization"] == f"Bearer {_TOKEN}"
    assert requests[0].headers["accept-encoding"] == "identity"


async def test_default_clock_accepts_current_evidence(tmp_path: Path) -> None:
    """Exercise the real UTC clock without touching a live endpoint.

    Given: Evidence stamped immediately before a mocked HTTP exchange.
    When: No injected clock is supplied.
    Then: The independent observation is accepted.
    """
    config = _config(tmp_path)
    body = _body(_payload(datetime.now(UTC)))
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200, content=body, headers={"content-type": "application/json"}
        )
    )
    assert (await check_market_snapshot_freshness(config, transport=transport)).freshness_ready


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://monitor.invalid/api/metrics/market-snapshots/freshness",
        "https://name:secret@monitor.invalid/api/metrics/market-snapshots/freshness",
        "https://monitor.invalid/other",
        "https://monitor.invalid/api/metrics/market-snapshots/freshness?x=1",
        "https://monitor.invalid/api/metrics/market-snapshots/freshness#x",
        " https://monitor.invalid/api/metrics/market-snapshots/freshness",
        "https://monitor.invalid:bad/api/metrics/market-snapshots/freshness",
        "https:///api/metrics/market-snapshots/freshness",
    ],
)
def test_invalid_endpoint_refuses_before_credentials(tmp_path: Path, endpoint: str) -> None:
    """Reject insecure or redirected credential destinations.

    Given: An endpoint with an invalid scheme, origin, path or embedded data.
    When: Monitor configuration is constructed.
    Then: A fixed diagnostic refuses it without a network operation.
    """
    with pytest.raises(FreshnessMonitorError, match="^invalid_endpoint$"):
        replace(_config(tmp_path), endpoint=endpoint)


@pytest.mark.parametrize("ids", [(), ("",), ("not-uuid",), (f" {_MAIN}",), (_MAIN,) * 101])
def test_invalid_scope_is_rejected(tmp_path: Path, ids: tuple[str, ...]) -> None:
    """Keep raw scope bounds and UUID validation independent of deduplication.

    Given: Empty, padded, malformed or excessive instrument input.
    When: Configuration is built before any credential read.
    Then: The monitor refuses the invalid scope.
    """
    with pytest.raises(FreshnessMonitorError, match="^invalid_instrument_scope$"):
        replace(_config(tmp_path), instrument_public_ids=ids)


@pytest.mark.parametrize("venues", [(), ("polygon",), ("kraken ",), ("kraken",) * 101])
def test_invalid_fx_scope_is_rejected(tmp_path: Path, venues: tuple[str, ...]) -> None:
    """Keep selected FX venues explicit and bounded.

    Given: Missing, unsupported, padded or excessive FX configuration.
    When: A monitor config is constructed.
    Then: It refuses without treating missing configuration as all venues.
    """
    with pytest.raises(FreshnessMonitorError, match="^invalid_fx_scope$"):
        replace(_config(tmp_path), fx_exchanges=venues)


def test_scope_aliases_deduplicate_but_invalid_digest_refuses(tmp_path: Path) -> None:
    """Normalize equivalent identifiers without weakening the reviewed digest.

    Given: Uppercase and compact spellings of one UUID and repeated FX venues.
    When: A monitor config is constructed and an invalid digest is attempted.
    Then: The valid config is canonical and the malformed digest is refused.
    """
    config = replace(
        _config(tmp_path),
        instrument_public_ids=(_MAIN.upper(), _MAIN.replace("-", "")),
        fx_exchanges=("kraken", "kraken"),
    )
    assert config.instrument_public_ids == (_MAIN,)
    assert config.fx_exchanges == ("kraken",)
    with pytest.raises(FreshnessMonitorError, match="^invalid_scope_digest$"):
        replace(config, expected_scope_digest="not-sha256")


@pytest.mark.parametrize(
    "contents",
    [b"", b" ", b"secret\nsecond", b"secret\x00", b"\xff", b"x" * (monitor.MAX_TOKEN_BYTES + 1)],
)
async def test_malformed_token_never_reaches_transport(tmp_path: Path, contents: bytes) -> None:
    """Reject malformed credentials without including their contents in diagnostics.

    Given: A private file containing an empty, oversized or malformed test token.
    When: The monitor loads its existing credential.
    Then: It refuses before HTTP and emits only a fixed code.
    """
    config = _config(tmp_path)
    config.token_file.write_bytes(contents)
    with pytest.raises(FreshnessMonitorError, match="^invalid_token_file$"):
        await check_market_snapshot_freshness(config)


@pytest.mark.parametrize("kind", ["missing", "public", "directory", "symlink", "fifo"])
async def test_nonprivate_or_nonregular_token_file_refuses(tmp_path: Path, kind: str) -> None:
    """Refuse unsafe file shapes without blocking on a FIFO or following a symlink.

    Given: A token path that is not a private readable regular file.
    When: The monitor tries to load it.
    Then: It fails before HTTP with no credential or path in the diagnostic.
    """
    config = _config(tmp_path)
    config.token_file.unlink()
    if kind == "public":
        config.token_file.write_text(_TOKEN)
        config.token_file.chmod(0o644)
    elif kind == "directory":
        config.token_file.mkdir(mode=0o700)
    elif kind == "symlink":
        config.token_file.symlink_to(tmp_path / "missing")
    elif kind == "fifo":
        monitor.os.mkfifo(config.token_file, mode=0o600)
    with pytest.raises(FreshnessMonitorError, match="^invalid_token_file$"):
        await check_market_snapshot_freshness(config)


@pytest.mark.parametrize("timeout", [0.0, -1.0, 10.001, float("nan"), float("inf")])
async def test_invalid_timeout_is_rejected(tmp_path: Path, timeout: float) -> None:
    """Prevent a caller from resetting or enlarging the bounded request budget.

    Given: A nonpositive, nonfinite or over-ten-second timeout.
    When: The monitor is invoked.
    Then: It refuses before reading a credential or making a request.
    """
    with pytest.raises(FreshnessMonitorError, match="^invalid_timeout$"):
        await check_market_snapshot_freshness(_config(tmp_path), timeout_seconds=timeout)


@pytest.mark.parametrize(
    "status,headers,expected",
    [
        (302, {"location": "https://other.invalid/secret"}, "http_status_refused"),
        (401, {}, "http_status_refused"),
        (503, {}, "http_status_refused"),
        (200, {}, "invalid_response_type"),
        (200, {"content-type": "text/html"}, "invalid_response_type"),
        (
            200,
            {"content-type": "application/json", "content-encoding": "gzip"},
            "invalid_response_encoding",
        ),
        (
            200,
            {"content-type": "application/json", "content-length": "invalid"},
            "response_too_large",
        ),
        (
            200,
            {"content-type": "application/json", "content-length": "1048577"},
            "response_too_large",
        ),
        (
            200,
            {"content-type": "application/json", "content-length": "99999999"},
            "response_too_large",
        ),
    ],
)
async def test_http_failures_close_stream_without_redirect(
    tmp_path: Path, status: int, headers: dict[str, str], expected: str
) -> None:
    """Refuse status and framing failures before parsing or forwarding credentials.

    Given: A failed, redirected, compressed or incorrectly framed response.
    When: The bounded monitor reads its headers.
    Then: The stream is closed and only one request is made.
    """
    stream = _Stream([])
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        """Return the controlled response and record every attempted request."""
        requests.append(request)
        return httpx.Response(status, headers=headers, stream=stream)

    with pytest.raises(FreshnessMonitorError, match=f"^{expected}$"):
        await check_market_snapshot_freshness(
            _config(tmp_path), transport=httpx.MockTransport(handle), now=lambda: _NOW
        )
    assert stream.closed
    assert len(requests) == 1


async def test_streamed_body_limit_is_enforced_without_content_length(tmp_path: Path) -> None:
    """Stop an unbounded chunked body independently of a Content-Length header.

    Given: A response stream larger than the one-mebibyte limit.
    When: The monitor receives successive chunks.
    Then: It refuses and closes the response before accepting a payload.
    """
    stream = _Stream([b"x" * 8192] * 129)
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200, headers={"content-type": "application/json"}, stream=stream
        )
    )
    with pytest.raises(FreshnessMonitorError, match="^response_too_large$"):
        await check_market_snapshot_freshness(_config(tmp_path), transport=transport)
    assert stream.closed


@pytest.mark.parametrize(
    "body",
    [
        b"{",
        b"{}",
        _body(_payload()).replace(b'"freshness_ready":true', b'"freshness_ready":"true"'),
        _body(_payload()).replace(b'"freshness_ready":true', b'"freshness_ready":1'),
    ],
)
async def test_malformed_or_coercible_ready_response_refuses(tmp_path: Path, body: bytes) -> None:
    """Require a complete strict wire envelope and an actual JSON boolean.

    Given: Invalid JSON, an incomplete envelope or coercible readiness values.
    When: The monitor parses the streamed response.
    Then: It refuses without logging any remote body contents.
    """
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200, headers={"content-type": "application/json"}, content=body
        )
    )
    with pytest.raises(FreshnessMonitorError, match="^invalid_response$"):
        await check_market_snapshot_freshness(_config(tmp_path), transport=transport)


@pytest.mark.parametrize(
    "mutate,expected",
    [
        (lambda p: setattr(p, "freshness_ready", False), "not_ready"),
        (lambda p: setattr(p, "unresolved_fx_currencies", ["EUR"]), "not_ready"),
        (lambda p: setattr(p, "requested_instrument_public_ids", [_FX]), "inconsistent_scope"),
        (lambda p: setattr(p, "configured_fx_exchanges", ["walutomat"]), "inconsistent_scope"),
        (lambda p: setattr(p, "instruments", p.instruments * 501), "inconsistent_scope"),
        (lambda p: p.instruments.append(p.instruments[0]), "inconsistent_scope"),
        (
            lambda p: p.excluded_fx_instruments.append(p.excluded_fx_instruments[0]),
            "inconsistent_scope",
        ),
        (
            lambda p: setattr(p.excluded_fx_instruments[0], "instrument_public_id", _MAIN),
            "inconsistent_scope",
        ),
        (lambda p: setattr(p, "instruments", p.instruments[1:]), "inconsistent_scope"),
        (lambda p: setattr(p.instruments[0], "native_symbol", "*"), "inconsistent_scope"),
        (lambda p: setattr(p.instruments[0], "base", " "), "inconsistent_scope"),
        (
            lambda p: setattr(p.instruments[0], "instrument_public_id", _MAIN.upper()),
            "inconsistent_scope",
        ),
        (
            lambda p: setattr(p.instruments[0], "instrument_public_id", "bad-uuid"),
            "inconsistent_scope",
        ),
        (
            lambda p: setattr(p.excluded_fx_instruments[0], "exchange", "kraken"),
            "inconsistent_scope",
        ),
        (lambda p: setattr(p.instruments[0], "exchange", "polygon"), "inconsistent_scope"),
        (lambda p: setattr(p.instruments[1], "exchange", "walutomat"), "inconsistent_scope"),
        (lambda p: setattr(p.instruments[1], "asset_type", "crypto"), "inconsistent_fx_coverage"),
        (lambda p: setattr(p, "instruments", p.instruments[:1]), "inconsistent_fx_coverage"),
        (lambda p: setattr(p, "scope_digest", "0" * 64), "scope_digest_mismatch"),
        (lambda p: setattr(p.instruments[0], "native_symbol", "OTHER"), "scope_digest_mismatch"),
        (lambda p: setattr(p, "checked_at", _NOW.replace(tzinfo=None)), "invalid_observation_time"),
        (
            lambda p: setattr(p, "checked_at", _NOW + timedelta(seconds=6)),
            "invalid_observation_time",
        ),
        (
            lambda p: setattr(p, "checked_at", _NOW - timedelta(seconds=6)),
            "invalid_observation_time",
        ),
        (lambda p: setattr(p.instruments[0], "snapshot_timestamp", None), "inconsistent_readiness"),
        (
            lambda p: setattr(p.instruments[0], "snapshot_timestamp", _NOW.replace(tzinfo=None)),
            "inconsistent_readiness",
        ),
        (lambda p: setattr(p.instruments[0], "age_seconds", None), "inconsistent_readiness"),
        (
            lambda p: setattr(p.instruments[0], "reason", "missing_snapshot"),
            "inconsistent_readiness",
        ),
        (lambda p: setattr(p.instruments[0], "age_seconds", 1.0), "inconsistent_readiness"),
    ],
)
async def test_inconsistent_ready_payload_refuses(
    tmp_path: Path, mutate: Callable[[MarketSnapshotFreshnessData], object], expected: str
) -> None:
    """Do not trust readiness when evidence, identity or scope contradicts it.

    Given: A ready response with one inconsistent field or missing required dependency.
    When: The independent monitor validates the complete evidence.
    Then: It refuses with the relevant fixed diagnostic code.
    """
    payload = _payload()
    mutate(payload)
    with pytest.raises(FreshnessMonitorError, match=f"^{expected}$"):
        await _observe(_config(tmp_path), payload)


@pytest.mark.parametrize(
    "age,accepted", [(-5.0, True), (-5.000001, False), (300.0, True), (300.000001, False)]
)
async def test_original_timestamp_boundaries_are_inclusive(
    tmp_path: Path, age: float, accepted: bool
) -> None:
    """Preserve the shared exact five-second future and 300-second age boundaries.

    Given: A response whose original required timestamps lie at or just past a boundary.
    When: The monitor rechecks its final UTC clock.
    Then: Equality passes and either exceeded boundary fails.
    """
    payload = _payload()
    for item in payload.instruments:
        item.snapshot_timestamp = _NOW - timedelta(seconds=age)
        item.age_seconds = age
    if accepted:
        assert (await _observe(_config(tmp_path), payload)).freshness_ready
    else:
        with pytest.raises(FreshnessMonitorError, match="^snapshot_not_fresh$"):
            await _observe(_config(tmp_path), payload)


async def test_final_clock_rejects_mark_that_ages_during_request(tmp_path: Path) -> None:
    """Recheck original mark age after response cleanup instead of trusting server readiness.

    Given: Evidence is 299 seconds old when the request begins.
    When: Two seconds elapse before response cleanup finishes.
    Then: The monitor refuses the now-301-second-old mark.
    """
    payload = _payload()
    payload.instruments[0].snapshot_timestamp = _NOW - timedelta(seconds=299)
    payload.instruments[0].age_seconds = 299.0
    clocks = iter([_NOW, _NOW + timedelta(seconds=2)])
    with pytest.raises(FreshnessMonitorError, match="^snapshot_not_fresh$"):
        await _observe(_config(tmp_path), payload, now=lambda: next(clocks))


async def test_backward_local_clock_refuses(tmp_path: Path) -> None:
    """Refuse a local clock regression instead of turning older evidence younger.

    Given: The local UTC clock moves backward while the response is in flight.
    When: The monitor compares its start and final observations.
    Then: It reports an invalid observation time.
    """
    clocks = iter([_NOW, _NOW - timedelta(seconds=1)])
    with pytest.raises(FreshnessMonitorError, match="^invalid_observation_time$"):
        await _observe(_config(tmp_path), _payload(), now=lambda: next(clocks))


async def test_elapsed_deadline_cannot_be_hidden_by_synchronous_cleanup(tmp_path: Path) -> None:
    """Refuse a deadline overrun even before the event loop delivers cancellation.

    Given: Synchronous final-clock work consumes the remaining request budget.
    When: Validation would otherwise accept fresh evidence.
    Then: The monotonic final check still refuses success.
    """
    observations: list[datetime] = []

    def clock() -> datetime:
        """Consume the budget during the second clock read.

        Returns:
            An unchanged UTC evidence clock.
        """
        if observations:
            time.sleep(0.02)
        observations.append(_NOW)
        return _NOW

    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200, content=_body(_payload()), headers={"content-type": "application/json"}
        )
    )
    with pytest.raises(FreshnessMonitorError, match="^request_timeout$"):
        await check_market_snapshot_freshness(
            _config(tmp_path), transport=transport, now=clock, timeout_seconds=0.01
        )


async def test_reported_nonfinite_age_is_refused(tmp_path: Path) -> None:
    """Refuse nonfinite ages even when a permissive JSON decoder accepts NaN.

    Given: A valid envelope with NaN in a required age field.
    When: The monitor compares original timestamp evidence with that age.
    Then: It reports inconsistent readiness.
    """
    body = _body(_payload()).replace(b'"age_seconds":0.0', b'"age_seconds":NaN', 1)
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200, content=body, headers={"content-type": "application/json"}
        )
    )
    with pytest.raises(FreshnessMonitorError, match="^inconsistent_readiness$"):
        await check_market_snapshot_freshness(
            _config(tmp_path), transport=transport, now=lambda: _NOW
        )


async def test_false_age_at_server_clock_refuses_even_if_fresh_locally(tmp_path: Path) -> None:
    """Reject a server's impossible ready claim at its own observation time.

    Given: A mark is 301 seconds old at a server clock two seconds ahead.
    When: The local final clock would still consider it 299 seconds old.
    Then: The monitor refuses the inconsistent server claim.
    """
    payload = _payload()
    payload.checked_at = _NOW + timedelta(seconds=2)
    for item in payload.instruments:
        item.snapshot_timestamp = _NOW - timedelta(seconds=299)
        item.age_seconds = 301.0
    with pytest.raises(FreshnessMonitorError, match="^inconsistent_readiness$"):
        await _observe(_config(tmp_path), payload)


async def test_expected_digest_is_not_replaced_by_server_digest(tmp_path: Path) -> None:
    """Keep the operator's frozen metadata scope independent of a coherent server response.

    Given: A valid response whose digest differs from the configured reviewed digest.
    When: The monitor validates the observation.
    Then: It refuses even though the response is internally consistent.
    """
    with pytest.raises(FreshnessMonitorError, match="^scope_digest_mismatch$"):
        await _observe(replace(_config(tmp_path), expected_scope_digest="0" * 64), _payload())


async def test_timeout_closes_stream_and_respects_smaller_remaining_budget(tmp_path: Path) -> None:
    """Apply a caller's remaining budget to the entire HTTP stream lifecycle.

    Given: A response stream stalls beyond a tiny caller budget.
    When: The monitor's outer timeout expires.
    Then: It closes the stream and returns a sanitized failure.
    """
    stream = _Stream([], delay=1.0)
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200, headers={"content-type": "application/json"}, stream=stream
        )
    )
    with pytest.raises(FreshnessMonitorError, match="^request_timeout$"):
        await check_market_snapshot_freshness(
            _config(tmp_path), timeout_seconds=0.001, transport=transport
        )
    assert stream.closed


async def test_transport_failure_never_exposes_exception_text(tmp_path: Path) -> None:
    """Sanitize transport exceptions instead of printing their request context.

    Given: A mocked transport failure embeds credential-like text.
    When: The independent monitor handles the error.
    Then: Only a fixed request_failed diagnostic escapes.
    """

    def fail(request: httpx.Request) -> httpx.Response:
        """Raise a synthetic transport error containing data that must not be logged."""
        raise httpx.ConnectError(f"do-not-print-{_TOKEN}", request=request)

    with pytest.raises(FreshnessMonitorError, match="^request_failed$"):
        await check_market_snapshot_freshness(
            _config(tmp_path), transport=httpx.MockTransport(fail)
        )


async def test_external_cancellation_propagates_after_stream_cleanup(tmp_path: Path) -> None:
    """Preserve the supervisor's cancellation instead of converting it to readiness.

    Given: A stream is waiting when its owning task is cancelled.
    When: The caller cancels the monitor task.
    Then: CancelledError propagates and the response closes.
    """
    stream = _Stream([], delay=1.0)
    entered = asyncio.Event()

    def handle(request: httpx.Request) -> httpx.Response:
        """Signal that a cancellable stream is about to be consumed."""
        entered.set()
        return httpx.Response(200, headers={"content-type": "application/json"}, stream=stream)

    task = asyncio.create_task(
        check_market_snapshot_freshness(_config(tmp_path), transport=httpx.MockTransport(handle))
    )
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert stream.closed
