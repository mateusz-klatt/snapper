import XCTest
@testable import Snapper

@MainActor
final class PositionsViewTests: XCTestCase {

    private static let baseTimestamp = Date(timeIntervalSince1970: 1_700_000_000)

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
            instrument: "BTC-USD",
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
}
