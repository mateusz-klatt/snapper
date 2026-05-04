import XCTest
@testable import Snapper

@MainActor
final class AttachBracketSheetTests: XCTestCase {

    private static let baseTimestamp = Date(timeIntervalSince1970: 1_700_000_000)

    private static let fixedProvenance = EnvelopeMinter.Provenance(
        publicId: "test-public-id",
        sessionId: "session-test",
        sequenceId: 21,
        timestamp: baseTimestamp,
        timestampString: "2023-11-14T22:13:20.000Z"
    )

    /// Free-text price with comma decimal (Polish locale habit) and
    /// surrounding whitespace must round-trip to a positive Double.
    /// Empty / non-numeric / non-positive collapses to nil so the
    /// caller treats the leg as "not supplied".
    func testParsePriceHandlesLocalisedAndPathologicalInput() {
        XCTAssertEqual(AttachBracketSheet.parsePrice("100"), 100.0)
        XCTAssertEqual(AttachBracketSheet.parsePrice("100.5"), 100.5)
        XCTAssertEqual(AttachBracketSheet.parsePrice("100,5"), 100.5)
        XCTAssertEqual(AttachBracketSheet.parsePrice("  42 "), 42.0)
        XCTAssertNil(AttachBracketSheet.parsePrice(""))
        XCTAssertNil(AttachBracketSheet.parsePrice("   "))
        XCTAssertNil(AttachBracketSheet.parsePrice("0"))
        XCTAssertNil(AttachBracketSheet.parsePrice("-5"))
        XCTAssertNil(AttachBracketSheet.parsePrice("abc"))
    }

    /// Backend `BracketCreateBody` validator requires at least one of
    /// `sl_price` / `tp_price`. The submit gate must mirror that
    /// constraint so the user cannot fire a request the server will
    /// reject with HTTP 400.
    func testCanSubmitRequiresAtLeastOneLeg() {
        XCTAssertFalse(AttachBracketSheet.canSubmit(slPrice: nil, tpPrice: nil, isSubmitting: false))
        XCTAssertTrue(AttachBracketSheet.canSubmit(slPrice: 95, tpPrice: nil, isSubmitting: false))
        XCTAssertTrue(AttachBracketSheet.canSubmit(slPrice: nil, tpPrice: 110, isSubmitting: false))
        XCTAssertTrue(AttachBracketSheet.canSubmit(slPrice: 95, tpPrice: 110, isSubmitting: false))
    }

    /// In-flight submission disables the submit button so a double-tap
    /// cannot duplicate the bracket.
    func testCanSubmitBlocksWhileInFlight() {
        XCTAssertFalse(
            AttachBracketSheet.canSubmit(slPrice: 95, tpPrice: 110, isSubmitting: true)
        )
    }

    /// Builder propagates the cycle id + both legs verbatim into the
    /// envelope payload, and stamps provenance from the injected
    /// minter so backend gap detection sees a coherent session
    /// across iOS-originated commands.
    func testMakeCommandEmbedsCycleIdAndProvenance() {
        let command = AttachBracketSheet.makeCommand(
            positionCyclePublicId: "cycle-1",
            slPrice: 95.5,
            tpPrice: 110.0,
            idempotencyKey: "bracket-idem-1",
            provenance: Self.fixedProvenance
        )
        XCTAssertEqual(command.type, "create_bracket_command")
        XCTAssertEqual(command.publicId, "test-public-id")
        XCTAssertEqual(command.sessionId, "session-test")
        XCTAssertEqual(command.sequenceId, 21)
        XCTAssertEqual(command.timestamp, Self.baseTimestamp)
        XCTAssertEqual(command.payload.positionCyclePublicId, "cycle-1")
        XCTAssertEqual(command.payload.slPrice, 95.5)
        XCTAssertEqual(command.payload.tpPrice, 110.0)
        XCTAssertEqual(command.payload.idempotencyKey, "bracket-idem-1")
    }
}
