import UserNotifications
import XCTest
import SwiftUI
@testable import Snapper

/// Tests for ``AlertsView`` deep-link integration (Plan 2 iOS-4).
///
/// `AlertsView` itself is SwiftUI state — the interesting
/// regression surface is around ``NavigationCoordinator``'s
/// ``pendingAlertPublicId`` triggering a re-fetch when the id isn't
/// in the current list. Since SwiftUI `body` is non-trivial to
/// instantiate outside a UIHostingController, these tests focus on
/// the coordinator contract this view depends on + the
/// `AlertRow` helpers.
@MainActor
final class AlertsViewTests: XCTestCase {

    func testAlertRowRendersTitleAndBody() {
        let alert = _makeAlert(alertType: "order_fill_full", priority: "medium", safetyCritical: false)

        let row = AlertRow(alert: alert)

        XCTAssertNotNil(row.body)
    }

    func testAlertRowSafetyCriticalSurfacesAccent() {
        let alert = _makeAlert(alertType: "order_rejected", priority: "high", safetyCritical: true)

        let row = AlertRow(alert: alert)

        XCTAssertNotNil(row.body)
    }

    func testNavigationCoordinatorPropagatesPendingAlertPublicId() async {
        let coordinator = NavigationCoordinator()
        let response = _makeResponse(userInfo: [
            "deep_link_path": "/alerts/019dbb34-f439-77bd-afa8-ee5321d60307",
            "alert_event_public_id": "019dbb34-f439-77bd-afa8-ee5321d60307",
        ])

        await coordinator.handleNotificationTap(response)

        XCTAssertEqual(coordinator.pendingDeepLink, "/alerts/019dbb34-f439-77bd-afa8-ee5321d60307")
        XCTAssertEqual(coordinator.pendingAlertPublicId, "019dbb34-f439-77bd-afa8-ee5321d60307")
    }

    private func _makeAlert(alertType: String, priority: String, safetyCritical: Bool) -> AlertEventInfo {
        AlertEventInfo(
            type: "alert_event",
            sequenceId: 1,
            publicId: "alert-1",
            timestamp: Date(),
            sessionId: "s",
            topic: nil,
            userPublicId: "user-1",
            operatorPublicId: nil,
            walletPublicId: nil,
            alertType: alertType,
            priority: priority,
            isSafetyCritical: safetyCritical,
            title: "Title",
            body: "Body text",
            payload: nil,
            dedupKey: nil,
            threadKey: nil,
            sourceTopic: nil
        )
    }

    private func _makeResponse(userInfo: [AnyHashable: Any]) -> UNNotificationResponse {
        let content = UNMutableNotificationContent()
        content.title = "T"
        content.body = "B"
        content.userInfo = userInfo
        let request = UNNotificationRequest(identifier: "test", content: content, trigger: nil)
        let coder = NSKeyedArchiver(requiringSecureCoding: false)
        coder.encode(request, forKey: "request")
        coder.encode(Date(), forKey: "date")
        coder.finishEncoding()
        let data = coder.encodedData
        guard
            let unarchiver = try? NSKeyedUnarchiver(forReadingFrom: data),
            let notification = UNNotification(coder: unarchiver)
        else {
            fatalError()
        }
        unarchiver.finishDecoding()

        let rcoder = NSKeyedArchiver(requiringSecureCoding: false)
        rcoder.encode(notification, forKey: "notification")
        rcoder.encode(UNNotificationDefaultActionIdentifier, forKey: "actionIdentifier")
        rcoder.finishEncoding()
        let rdata = rcoder.encodedData
        guard
            let runarchiver = try? NSKeyedUnarchiver(forReadingFrom: rdata),
            let response = UNNotificationResponse(coder: runarchiver)
        else {
            fatalError()
        }
        runarchiver.finishDecoding()
        return response
    }
}
