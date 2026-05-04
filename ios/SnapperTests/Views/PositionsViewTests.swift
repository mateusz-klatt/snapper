import XCTest
@testable import Snapper

@MainActor
final class PositionsViewTests: XCTestCase {

    private static let baseTimestamp = Date(timeIntervalSince1970: 1_700_000_000)

    /// Deterministic provenance for builder tests — the live minter
    /// would advance counters across tests; here we want every
    /// command tagged with the same fixture so assertions on
    /// ``sessionId`` / ``sequenceId`` are stable.
    private static let fixedProvenance = EnvelopeMinter.Provenance(
        publicId: "test-public-id",
        sessionId: "session-test",
        sequenceId: 7,
        timestamp: baseTimestamp,
        timestampString: "2023-11-14T22:13:20.000Z"
    )

    private func makePosition(
        publicId: String,
        walletPublicId: String?,
        quantity: Double = 1.0
    ) -> PositionSnapshot {
        return PositionData(
            type: "position",
            sequenceId: 1,
            publicId: publicId,
            timestamp: Self.baseTimestamp,
            sessionId: "session-test",
            topic: nil,
            instrument: "BTC-USD",
            instrumentPublicId: "inst-1",
            exchange: "kraken",
            mode: nil,
            quantity: quantity,
            averagePrice: 50000.0,
            unrealizedPnl: 0.0,
            realizedPnl: 0.0,
            positionCyclePublicId: nil,
            walletPublicId: walletPublicId
        )
    }

    func testDirectionForPositiveQuantityIsLong() {
        XCTAssertEqual(PositionCard.direction(for: 1.5), "Long")
    }

    func testDirectionForNegativeQuantityIsShort() {
        XCTAssertEqual(PositionCard.direction(for: -2.0), "Short")
    }

    /// A zero quantity surfaces "Flat" rather than misleading the user
    /// into thinking the position has direction. The backend stops
    /// emitting positions once a cycle closes, but during a tight
    /// reduce/replace window the WS feed can momentarily report 0.
    func testDirectionForZeroQuantityIsFlat() {
        XCTAssertEqual(PositionCard.direction(for: 0.0), "Flat")
    }

    func testFilterRespectsWalletScope() {
        let walletA = makePosition(publicId: "p-a", walletPublicId: "wallet-a")
        let walletB = makePosition(publicId: "p-b", walletPublicId: "wallet-b")
        let orphan = makePosition(publicId: "p-orphan", walletPublicId: nil)

        let scoped = PositionsView.filter(
            positions: [walletA, walletB, orphan],
            selectedWalletPublicId: "wallet-a"
        )

        XCTAssertEqual(Set(scoped.map(\.publicId)), Set(["p-a", "p-orphan"]))
    }

    func testFilterPassesThroughWhenNoWalletSelected() {
        let walletA = makePosition(publicId: "p-a", walletPublicId: "wallet-a")
        let walletB = makePosition(publicId: "p-b", walletPublicId: "wallet-b")

        let unscoped = PositionsView.filter(
            positions: [walletA, walletB],
            selectedWalletPublicId: nil
        )

        XCTAssertEqual(unscoped.count, 2)
    }

    func testWalletMatchesPolicyMirrorsOrdersView() {
        XCTAssertTrue(PositionsView.walletMatches(rowWalletId: nil, selected: "wallet-a"))
        XCTAssertTrue(PositionsView.walletMatches(rowWalletId: "wallet-a", selected: nil))
        XCTAssertTrue(PositionsView.walletMatches(rowWalletId: "wallet-a", selected: "wallet-a"))
        XCTAssertFalse(PositionsView.walletMatches(rowWalletId: "wallet-b", selected: "wallet-a"))
    }

    func testMakeReduceCommandLongPositionFiresSell() {
        let position = makePosition(publicId: "p-1", walletPublicId: "wallet-a", quantity: 0.5)
        let command = PositionsView.makeReduceCommand(
            position: position,
            quantity: 0.25,
            provenance: Self.fixedProvenance
        )
        XCTAssertNotNil(command)
        guard let command else { return }
        XCTAssertEqual(command.payload.side, "sell")
        XCTAssertEqual(command.payload.orderType, "market")
        XCTAssertEqual(command.payload.quantity, 0.25)
        XCTAssertEqual(command.payload.reduceOnly, true)
        XCTAssertEqual(command.payload.instrument, "BTC-USD")
        XCTAssertEqual(command.payload.instrumentPublicId, "inst-1")
        XCTAssertEqual(command.payload.walletPublicId, "wallet-a")
        XCTAssertEqual(command.type, "create_order_command")
    }

    func testMakeReduceCommandShortPositionFiresBuy() {
        let position = makePosition(publicId: "p-2", walletPublicId: "wallet-a", quantity: -1.0)
        let command = PositionsView.makeReduceCommand(
            position: position,
            quantity: 1.0,
            provenance: Self.fixedProvenance
        )
        XCTAssertNotNil(command)
        guard let command else { return }
        XCTAssertEqual(
            command.payload.side,
            "buy",
            "Short positions liquidate via the buy side."
        )
        XCTAssertEqual(command.payload.reduceOnly, true)
    }

    func testMakeReduceCommandFullCloseHandlesNegativeQuantityAbsolutely() {
        let position = makePosition(publicId: "p-3", walletPublicId: "wallet-a", quantity: -2.5)
        let command = PositionsView.makeReduceCommand(
            position: position,
            quantity: 2.5,
            provenance: Self.fixedProvenance
        )
        XCTAssertNotNil(command)
        guard let command else { return }
        XCTAssertEqual(command.payload.quantity, 2.5)
        XCTAssertEqual(command.payload.side, "buy")
    }

    /// Provenance plumbing — the builder must propagate the injected
    /// envelope onto the outbound command so the backend gap
    /// detector sees the iOS-minted ``session_id`` and
    /// monotonic ``sequence_id``.
    func testMakeReduceCommandStampsInjectedProvenance() {
        let position = makePosition(publicId: "p-4", walletPublicId: "wallet-a", quantity: 1.0)
        let command = PositionsView.makeReduceCommand(
            position: position,
            quantity: 1.0,
            provenance: Self.fixedProvenance
        )
        XCTAssertNotNil(command)
        guard let command else { return }
        XCTAssertEqual(command.publicId, "test-public-id")
        XCTAssertEqual(command.sessionId, "session-test")
        XCTAssertEqual(command.sequenceId, 7)
        XCTAssertEqual(command.timestamp, Self.baseTimestamp)
    }

    /// Backend rejects orders with empty ``walletPublicId`` /
    /// ``instrumentPublicId``; the builder must refuse to mint the
    /// command (returns nil) when the source position arrives without
    /// either id, and ``submitMarketReduce`` must surface the error to
    /// the user instead of firing a malformed request.
    func testMakeReduceCommandReturnsNilWhenWalletIdMissing() {
        let position = makePosition(publicId: "p-5", walletPublicId: nil, quantity: 1.0)
        let command = PositionsView.makeReduceCommand(
            position: position,
            quantity: 1.0,
            provenance: Self.fixedProvenance
        )
        XCTAssertNil(command)
    }

    func testCanSubmitReduceRequiresBothIds() {
        let ok = makePosition(publicId: "p-ok", walletPublicId: "wallet-a", quantity: 1.0)
        let missingWallet = makePosition(publicId: "p-mw", walletPublicId: nil, quantity: 1.0)
        XCTAssertTrue(PositionsView.canSubmitReduce(position: ok))
        XCTAssertFalse(PositionsView.canSubmitReduce(position: missingWallet))
    }
}
