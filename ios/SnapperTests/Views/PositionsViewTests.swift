import XCTest
@testable import Snapper

@MainActor
final class PositionsViewTests: XCTestCase {

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
}
