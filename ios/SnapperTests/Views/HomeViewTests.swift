import XCTest
@testable import Snapper

@MainActor
final class HomeViewTests: XCTestCase {

    private static let baseTimestamp = Date(timeIntervalSince1970: 1_700_000_000)

    private func makeAlert(publicId: String, title: String) -> AlertEventInfo {
        return AlertEventInfo(
            type: "alert_event_info",
            sequenceId: 1,
            publicId: publicId,
            timestamp: Self.baseTimestamp,
            sessionId: "session-home-test",
            topic: nil,
            userPublicId: "user-1",
            operatorPublicId: nil,
            walletPublicId: nil,
            alertType: "order_filled",
            priority: "low",
            isSafetyCritical: false,
            title: title,
            body: "fixture body",
            payload: nil,
            dedupKey: nil,
            threadKey: nil,
            sourceTopic: nil
        )
    }

    private func makeHistory(payload: [AlertEventInfo]) -> AlertHistoryResponse {
        return AlertHistoryResponse(
            type: "alert_history_response",
            sequenceId: 1,
            publicId: "envelope-1",
            timestamp: Self.baseTimestamp,
            sessionId: "session-home-test",
            topic: nil,
            payload: payload,
            count: payload.count,
            nextCursor: nil
        )
    }

    /// Empty history pages collapse to ``nil`` so ``LatestAlertCard``
    /// renders ``EmptyView`` and the Home layout stays compact.
    func testFirstAlertFromEmptyHistoryReturnsNil() {
        let history = makeHistory(payload: [])
        XCTAssertNil(HomeView.firstAlert(from: history))
    }

    /// Non-empty history surfaces the page-leading row — the backend
    /// orders alerts newest-first so this is the most recent alert.
    func testFirstAlertFromHistoryReturnsLeadingRow() {
        let head = makeAlert(publicId: "newest", title: "BTC fill")
        let tail = makeAlert(publicId: "older", title: "BTC stop")
        let history = makeHistory(payload: [head, tail])

        let result = HomeView.firstAlert(from: history)

        XCTAssertEqual(result?.publicId, "newest")
        XCTAssertEqual(result?.title, "BTC fill")
    }
}
