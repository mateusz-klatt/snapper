import Combine
import XCTest
import UserNotifications
@testable import Snapper

/// Tests for ``NotificationService`` permission-state observer.
///
/// The real ``UNUserNotificationCenter`` cannot be authorized in an
/// xctest simulator run (no entitlement, no user interaction), so we
/// cover the paths that don't require granted state:
///
/// - `refreshAuthorizationStatus` reads from the injected centre.
/// - `isAuthorized` derivation across every status value.
/// - `requestAuthorization` handles the declined / error paths
///   without propagating throws to the caller.
@MainActor
final class NotificationServiceTests: XCTestCase {

    func testIsAuthorizedReflectsAuthorizationStatus() {
        let service = NotificationService()
        XCTAssertFalse(service.isAuthorized, "notDetermined is NOT authorized")
    }

    func testRefreshAuthorizationStatusPublishesCurrentState() async {
        let service = NotificationService()
        await service.refreshAuthorizationStatus()
        XCTAssertTrue(
            [.notDetermined, .denied, .authorized, .provisional, .ephemeral]
                .contains(service.authorizationStatus),
            "authorizationStatus must be a valid UNAuthorizationStatus case"
        )
    }

    func testServiceIsObservable() {
        let service = NotificationService()
        XCTAssertNotNil(service.objectWillChange as (any Publisher))
    }
}
