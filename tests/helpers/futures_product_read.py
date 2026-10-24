"""Actual signed local HTTP and independent SQLite visibility probes for product-read integration."""

import json
import sqlite3
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextlib import closing
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

import pytest
from requests import ConnectionError as RequestsConnectionError
from requests import PreparedRequest
from requests import Response
from urllib3.response import HTTPResponse

import snapper.data.futures_product_read_store as product_store
import snapper.infrastructure.exchanges.futures_trading_instruments_wire as product_wire
from tests.helpers.native_account_authority import NativeAuthorityClock
from tests.helpers.native_account_authority import NativeAuthorityHttpAdapter
from tests.helpers.native_account_authority import NativeAuthorityScenario
from tests.helpers.native_account_authority import native_authority_scenario

PRODUCT_PATH = "/derivatives/api/v3/trading/instruments"


class FuturesProductHttpAdapter(NativeAuthorityHttpAdapter):
    """Use actual final PreparedRequest objects and externally visible committed read/proof rows."""

    def __init__(self, path: Path, clock: NativeAuthorityClock) -> None:
        """Extend the real key/accounts adapter only at the fixed independent product endpoint."""
        super().__init__(path, clock)
        self.product_status = 200
        self.product_body: bytes | None = None
        self.fail_product = False
        self.product_calls: list[PreparedRequest] = []
        self.visible_product_sources: list[tuple[int, int, int]] = []

    def _original_body(self) -> bytes:
        """Provide exact successful source bytes without mocking any product parser or binder."""
        if self.product_body is not None:
            return self.product_body
        return json.dumps(
            {
                "result": "success",
                "serverTime": self.clock.current.isoformat().replace("+00:00", "Z"),
                "instruments": [
                    {
                        "symbol": "PF_XBTUSD",
                        "type": "flexible_futures",
                        "isExpired": False,
                        "suspended": False,
                        "postOnly": False,
                        "restricted": False,
                    }
                ],
                "uninterpreted": {"safe_source_field": "original-product-marker"},
            },
            separators=(",", ":"),
        ).encode()

    def _probe_committed_sources(self) -> None:
        """Observe real proof, accounts success and separate product attempt before HTTP dispatch."""
        with closing(sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)) as connection:
            counts = connection.execute(
                "SELECT "
                "(SELECT count(*) FROM native_account_identity_proofs), "
                "(SELECT count(*) FROM native_account_read_outcomes WHERE result_kind='captured'), "
                "(SELECT count(*) FROM futures_product_read_attempts)"
            ).fetchone()
            matching = connection.execute(
                "SELECT count(*) FROM futures_product_read_attempts AS product "
                "JOIN native_account_identity_proofs AS proof "
                "ON product.key_proof_public_id=proof.public_id "
                "AND product.key_proof_content_digest=proof.content_digest"
            ).fetchone()
        observed = (int(counts[0]), int(counts[1]), int(counts[2]))
        assert all(value >= 1 for value in observed)
        assert int(matching[0]) == observed[2]
        self.visible_product_sources.append(observed)

    def send(self, request: PreparedRequest, *args: object, **kwargs: object) -> Response:
        """Return actual response entities at the final Requests transport boundary.

        Args:
            request: Actual prepared HTTP request with synthetic credentials only.
            args: Original Requests adapter positional arguments.
            kwargs: Original routed stream, timeout and proxy arguments.

        Returns:
            Real streaming Response from a closed local source, without any network call.
        """
        if not (request.url or "").endswith(PRODUCT_PATH):
            return super().send(request, *args, **kwargs)
        self.calls.append(request)
        self.product_calls.append(request)
        self._probe_committed_sources()
        if self.fail_product:
            raise RequestsConnectionError("synthetic-product-transport-secret-never-retained")
        response = Response()
        response.status_code = self.product_status
        response.url = request.url or ""
        response.request = request
        response.raw = HTTPResponse(
            body=BytesIO(self._original_body()),
            status=self.product_status,
            preload_content=False,
        )
        self.responses.append(response)
        return response


@dataclass
class FuturesProductScenario:
    """One real native SDK/source scenario plus its distinct product endpoint observations."""

    native: NativeAuthorityScenario
    adapter: FuturesProductHttpAdapter


@asynccontextmanager
async def futures_product_scenario(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[FuturesProductScenario]:
    """Own actual isolated database and SDK sessions while extending only the closed HTTP fixture.

    Args:
        tmp_path: Caller-owned test directory, never an external or production database URL.
        monkeypatch: Caller-owned clock and routing patch lifetime.

    Yields:
        Actual product producer dependencies with real immutable storage and signed local HTTP.
    """
    async with native_authority_scenario(tmp_path, monkeypatch) as native:
        adapter = FuturesProductHttpAdapter(native.adapter.path, native.clock)
        native.adapter = adapter
        for session in native.sessions:
            session.mount("https://", adapter)
        monkeypatch.setattr(product_store, "datetime", native.clock)
        monkeypatch.setattr(product_wire, "datetime", native.clock)
        try:
            yield FuturesProductScenario(native, adapter)
        finally:
            adapter.accounts_release.set()
            adapter.close()
