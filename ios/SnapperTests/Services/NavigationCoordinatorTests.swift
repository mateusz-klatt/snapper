import XCTest
import UserNotifications
@testable import Snapper

/// Tests for ``NavigationCoordinator`` deep-link state holder.
///
/// Coordinator only exposes two state fields (pendingDeepLink +
/// pendingAlertPublicId) + two mutators
/// (`handleNotificationTap`, `clearPendingDeepLink`). Tests cover
/// the happy path, the missing-payload-path, and the clear path.
@MainActor
final class NavigationCoordinatorTests: XCTestCase {

    private var coordinator: NavigationCoordinator!

    override func setUp() {
        super.setUp()
        coordinator = NavigationCoordinator()
    }

    override func tearDown() {
        coordinator = nil
        super.tearDown()
    }

    func testHandleTapWithFullPayloadPrimesBothFields() async {
        let response = _makeResponse(withUserInfo: [
            "deep_link_path": "/orders/coid-1",
            "alert_event_public_id": "alert-1",
        ])

        await coordinator.handleNotificationTap(response)

        XCTAssertEqual(coordinator.pendingDeepLink, "/orders/coid-1")
        XCTAssertEqual(coordinator.pendingAlertPublicId, "alert-1")
    }

    func testHandleTapWithDeepLinkOnlyLeavesAnchorNil() async {
        let response = _makeResponse(withUserInfo: ["deep_link_path": "/system"])

        await coordinator.handleNotificationTap(response)

        XCTAssertEqual(coordinator.pendingDeepLink, "/system")
        XCTAssertNil(coordinator.pendingAlertPublicId)
    }

    func testHandleTapWithoutDeepLinkIsNoOp() async {
        let response = _makeResponse(withUserInfo: ["unrelated": "key"])

        await coordinator.handleNotificationTap(response)

        XCTAssertNil(coordinator.pendingDeepLink)
        XCTAssertNil(coordinator.pendingAlertPublicId)
    }

    func testClearResetsBothFields() async {
        let response = _makeResponse(withUserInfo: [
            "deep_link_path": "/orders/x",
            "alert_event_public_id": "alert-x",
        ])
        await coordinator.handleNotificationTap(response)

        coordinator.clearPendingDeepLink()

        XCTAssertNil(coordinator.pendingDeepLink)
        XCTAssertNil(coordinator.pendingAlertPublicId)
    }

    private func _makeResponse(withUserInfo userInfo: [AnyHashable: Any]) -> UNNotificationResponse {
        let content = UNMutableNotificationContent()
        content.title = "Test"
        content.body = "Body"
        content.userInfo = userInfo
        let request = UNNotificationRequest(identifier: "navtest", content: content, trigger: nil)
        let coder = NSKeyedArchiver(requiringSecureCoding: false)
        coder.encode(request, forKey: "request")
        coder.encode(Date(), forKey: "date")
        coder.finishEncoding()
        let data = coder.encodedData
        guard
            let unarchiver = try? NSKeyedUnarchiver(forReadingFrom: data),
            let notification = UNNotification(coder: unarchiver)
        else {
            fatalError("UNNotification synth failed")
        }
        unarchiver.finishDecoding()

        let responseCoder = NSKeyedArchiver(requiringSecureCoding: false)
        responseCoder.encode(notification, forKey: "notification")
        responseCoder.encode(UNNotificationDefaultActionIdentifier, forKey: "actionIdentifier")
        responseCoder.finishEncoding()
        let responseData = responseCoder.encodedData
        guard
            let responseUnarchiver = try? NSKeyedUnarchiver(forReadingFrom: responseData),
            let response = UNNotificationResponse(coder: responseUnarchiver)
        else {
            fatalError("UNNotificationResponse synth failed")
        }
        responseUnarchiver.finishDecoding()
        return response
    }
}
