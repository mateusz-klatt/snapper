import XCTest
import UserNotifications
@testable import Snapper

/// Tests for ``AppDelegate`` push-related surface (Plan 2 iOS-1 §D9).
///
/// Covers the three behaviour classes that matter for regressions:
/// - ``willPresent`` WS-suppression when the socket is connected.
/// - ``willPresent`` shows banner when WS is disconnected.
/// - ``didReceive`` routes the tap into ``NavigationCoordinator``.
///
/// The APNs-token callback is exercised indirectly through
/// ``DeviceRegistrationServiceTests`` (the delegate just forwards).
@MainActor
final class AppDelegateTests: XCTestCase {

    private var delegate: AppDelegate!
    private var coordinator: NavigationCoordinator!

    override func setUp() {
        super.setUp()
        delegate = AppDelegate()
        coordinator = NavigationCoordinator.shared
        coordinator.clearPendingDeepLink()
    }

    override func tearDown() {
        delegate = nil
        coordinator.clearPendingDeepLink()
        coordinator = nil
        super.tearDown()
    }

    /// Foreground WS-connected → willPresent returns empty options.
    func testWillPresentSuppressesBannerWhenWebSocketConnected() async {
        WebSocketManager.shared._setConnectionStateForTests(.connected)
        defer { WebSocketManager.shared._setConnectionStateForTests(.disconnected) }
        let notification = _makeFakeNotification(withUserInfo: ["deep_link_path": "/orders"])

        let options = await _capturePresentOptions(for: notification)

        XCTAssertEqual(options, [])
    }

    /// Foreground WS-disconnected → willPresent returns full banner options.
    func testWillPresentShowsBannerWhenWebSocketDisconnected() async {
        WebSocketManager.shared._setConnectionStateForTests(.disconnected)
        let notification = _makeFakeNotification(withUserInfo: ["deep_link_path": "/orders"])

        let options = await _capturePresentOptions(for: notification)

        XCTAssertTrue(options.contains(.banner))
        XCTAssertTrue(options.contains(.sound))
        XCTAssertTrue(options.contains(.badge))
    }

    /// Notification tap with `deep_link_path` primes NavigationCoordinator.
    func testDidReceivePopulatesPendingDeepLink() async {
        let response = _makeFakeResponse(withUserInfo: [
            "deep_link_path": "/positions/abc",
            "alert_event_public_id": "alert-1",
        ])

        await _invokeDidReceive(delegate: delegate, response: response)
        try? await Task.sleep(nanoseconds: 100_000_000)

        XCTAssertEqual(coordinator.pendingDeepLink, "/positions/abc")
        XCTAssertEqual(coordinator.pendingAlertPublicId, "alert-1")
    }

    /// Notification tap missing `deep_link_path` is a no-op.
    func testDidReceiveIgnoresTapWithoutDeepLinkPath() async {
        let response = _makeFakeResponse(withUserInfo: ["unrelated": "value"])

        await _invokeDidReceive(delegate: delegate, response: response)
        try? await Task.sleep(nanoseconds: 100_000_000)

        XCTAssertNil(coordinator.pendingDeepLink)
        XCTAssertNil(coordinator.pendingAlertPublicId)
    }

    // MARK: - Helpers

    private func _capturePresentOptions(for notification: UNNotification) async -> UNNotificationPresentationOptions {
        await withCheckedContinuation { (continuation: CheckedContinuation<UNNotificationPresentationOptions, Never>) in
            let center = UNUserNotificationCenter.current()
            delegate.userNotificationCenter(center, willPresent: notification) { options in
                continuation.resume(returning: options)
            }
        }
    }

    private func _invokeDidReceive(delegate: AppDelegate, response: UNNotificationResponse) async {
        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            let center = UNUserNotificationCenter.current()
            delegate.userNotificationCenter(center, didReceive: response) {
                continuation.resume()
            }
        }
    }

    private func _makeFakeNotification(withUserInfo userInfo: [AnyHashable: Any]) -> UNNotification {
        let content = UNMutableNotificationContent()
        content.title = "Test"
        content.body = "Test body"
        content.userInfo = userInfo
        let request = UNNotificationRequest(identifier: "test", content: content, trigger: nil)
        return _UnsafeNotificationFactory.make(request: request)
    }

    private func _makeFakeResponse(withUserInfo userInfo: [AnyHashable: Any]) -> UNNotificationResponse {
        let notification = _makeFakeNotification(withUserInfo: userInfo)
        return _UnsafeNotificationFactory.makeResponse(notification: notification)
    }
}

/// UNNotification / UNNotificationResponse have no public initialiser.
/// The production surface constructs them via the runtime; tests must
/// rely on NSKeyedArchiver to produce synthetic instances. Isolated in
/// this file-local helper so production code is not polluted.
private enum _UnsafeNotificationFactory {
    static func make(request: UNNotificationRequest) -> UNNotification {
        let coder = NSKeyedArchiver(requiringSecureCoding: false)
        coder.encode(request, forKey: "request")
        coder.encode(Date(), forKey: "date")
        coder.finishEncoding()
        let data = coder.encodedData
        guard
            let unarchiver = try? NSKeyedUnarchiver(forReadingFrom: data),
            let notification = UNNotification(coder: unarchiver)
        else {
            fatalError("Failed to synthesise UNNotification for tests")
        }
        unarchiver.finishDecoding()
        return notification
    }

    static func makeResponse(notification: UNNotification) -> UNNotificationResponse {
        let coder = NSKeyedArchiver(requiringSecureCoding: false)
        coder.encode(notification, forKey: "notification")
        coder.encode(UNNotificationDefaultActionIdentifier, forKey: "actionIdentifier")
        coder.finishEncoding()
        let data = coder.encodedData
        guard
            let unarchiver = try? NSKeyedUnarchiver(forReadingFrom: data),
            let response = UNNotificationResponse(coder: unarchiver)
        else {
            fatalError("Failed to synthesise UNNotificationResponse for tests")
        }
        unarchiver.finishDecoding()
        return response
    }
}

// MARK: - WebSocketManager test hook

extension WebSocketManager {
    /// Test-only setter for `connectionState` so AppDelegate tests can
    /// pin the state without spinning up a real socket. Not declared
    /// on the main interface to avoid accidental production use.
    @MainActor
    func _setConnectionStateForTests(_ state: ConnectionState) {
        self.connectionState = state
    }
}
