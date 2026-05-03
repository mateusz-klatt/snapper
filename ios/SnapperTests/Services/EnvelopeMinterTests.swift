import XCTest
@testable import Snapper

@MainActor
final class EnvelopeMinterTests: XCTestCase {

    /// Distinct minter instance per test isolates counter state — the
    /// shared singleton is reserved for the live app where session
    /// continuity matters.
    private func makeMinter(sessionId: String = "session-test") -> EnvelopeMinter {
        return EnvelopeMinter(sessionId: sessionId)
    }

    /// A fresh minter mints session-scoped provenance with a
    /// monotonic ``sequence_id`` starting at 1 (matching bridge
    /// semantics in `integrations/snapper-mcp/src/envelope.ts`).
    func testNextControlReturnsMonotonicSequenceStartingAtOne() {
        let minter = makeMinter()

        let first = minter.next(.control)
        let second = minter.next(.control)
        let third = minter.next(.control)

        XCTAssertEqual(first.sequenceId, 1)
        XCTAssertEqual(second.sequenceId, 2)
        XCTAssertEqual(third.sequenceId, 3)
    }

    /// Telemetry counter advances independently of control so a burst
    /// of pings cannot leave gaps in the control stream that backend
    /// gap detection would otherwise flag.
    func testControlAndTelemetryCountersAreIndependent() {
        let minter = makeMinter()

        let c1 = minter.next(.control)
        let t1 = minter.next(.telemetry)
        let c2 = minter.next(.control)
        let t2 = minter.next(.telemetry)
        let t3 = minter.next(.telemetry)

        XCTAssertEqual(c1.sequenceId, 1)
        XCTAssertEqual(c2.sequenceId, 2)
        XCTAssertEqual(t1.sequenceId, 1)
        XCTAssertEqual(t2.sequenceId, 2)
        XCTAssertEqual(t3.sequenceId, 3)
    }

    /// Every minted envelope carries the same in-memory session UUID
    /// — a fresh ID per request would defeat backend session-scoped
    /// dedup.
    func testSessionIdIsStableAcrossNextCalls() {
        let minter = makeMinter(sessionId: "session-stable")

        let a = minter.next(.control)
        let b = minter.next(.telemetry)
        let c = minter.next(.control)

        XCTAssertEqual(a.sessionId, "session-stable")
        XCTAssertEqual(b.sessionId, "session-stable")
        XCTAssertEqual(c.sessionId, "session-stable")
        XCTAssertEqual(minter.sessionId, "session-stable")
    }

    /// ``public_id`` is fresh per call so a transport retry produces
    /// a distinct envelope identity even when the payload is byte-
    /// identical.
    func testPublicIdIsFreshPerCall() {
        let minter = makeMinter()

        let ids = (0..<25).map { _ in minter.next(.control).publicId }

        XCTAssertEqual(Set(ids).count, ids.count)
        for id in ids {
            XCTAssertEqual(UUID(uuidString: id)?.uuidString, id)
        }
    }

    /// The ISO 8601 string carries millisecond precision so the
    /// shape matches frontend's ``new Date().toISOString()`` output.
    func testTimestampStringIsMillisecondPrecisionISO8601() {
        let minter = makeMinter()
        let fixed = Date(timeIntervalSince1970: 1_700_000_000.123)

        let provenance = minter.next(.control, now: fixed)

        XCTAssertEqual(provenance.timestamp, fixed)
        XCTAssertTrue(provenance.timestampString.contains("."))
        XCTAssertTrue(provenance.timestampString.hasSuffix("Z"))

        let parser = ISO8601DateFormatter()
        parser.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        let parsed = parser.date(from: provenance.timestampString)
        XCTAssertNotNil(parsed)
        if let parsed {
            XCTAssertEqual(parsed.timeIntervalSince1970, fixed.timeIntervalSince1970, accuracy: 0.001)
        }
    }

    /// ``EnvelopeMinter.formatTimestamp`` is the same formatter used
    /// by ``next()`` — the WS path can format an externally supplied
    /// timestamp without minting a new envelope.
    func testFormatTimestampMatchesNextOutput() {
        let minter = makeMinter()
        let fixed = Date(timeIntervalSince1970: 1_700_000_000.5)

        let viaNext = minter.next(.control, now: fixed).timestampString
        let viaStatic = EnvelopeMinter.formatTimestamp(fixed)

        XCTAssertEqual(viaNext, viaStatic)
    }
}
