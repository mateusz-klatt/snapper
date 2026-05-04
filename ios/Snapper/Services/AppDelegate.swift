import Foundation
import UIKit
import UserNotifications
import os

/// iOS application delegate wired for APNs push + foreground WS-suppression.
///
/// Single source of truth for UNUserNotificationCenter delegate
/// callbacks:
///
/// - `didRegisterForRemoteNotificationsWithDeviceToken` → hands the
///   APNs token to `DeviceRegistrationService` (actor-isolated).
/// - `didFailToRegisterForRemoteNotifications` → logs; no retry (APNs
///   is expected to re-deliver on its own or next launch).
/// - `userNotificationCenter(_:willPresent:)` → suppresses banner /
///   sound / badge when the WebSocket is connected (the app already
///   renders live alerts inline — §D10 foreground-suppression).
/// - `userNotificationCenter(_:didReceive:)` → routes the tap into
///   `NavigationCoordinator` so `MainTabView` can deep-link to the
///   target row.
///
/// All callbacks hop to `@MainActor` before touching observable state
/// — the delegate methods themselves are not `@MainActor` in
/// `UIApplicationDelegate`, so we use `Task { @MainActor in ... }`
/// to mark the boundary explicitly.
@MainActor
final class AppDelegate: NSObject, UIApplicationDelegate, @preconcurrency UNUserNotificationCenterDelegate {
    private let logger = Logger(
        subsystem: Bundle.main.bundleIdentifier ?? "Snapper",
        category: "APNs"
    )

    nonisolated func application(
        _ _: UIApplication,
        didFinishLaunchingWithOptions _: [UIApplication.LaunchOptionsKey: Any]?
    ) -> Bool {
        Task { @MainActor in
            UNUserNotificationCenter.current().delegate = self
        }
        return true
    }

    nonisolated func application(
        _ _: UIApplication,
        didRegisterForRemoteNotificationsWithDeviceToken deviceToken: Data
    ) {
        Task { @MainActor in
            await DeviceRegistrationService.shared().onTokenReceived(deviceToken)
        }
    }

    nonisolated func application(
        _ _: UIApplication,
        didFailToRegisterForRemoteNotificationsWithError error: Error
    ) {
        Task { @MainActor in
            self.logger.error("APNs registration failed: \(error.localizedDescription)")
        }
    }

    // MARK: - UNUserNotificationCenterDelegate

    func userNotificationCenter(
        _ _: UNUserNotificationCenter,
        willPresent _: UNNotification,
        withCompletionHandler completionHandler: @escaping @Sendable (UNNotificationPresentationOptions) -> Void
    ) {
        if case .connected = WebSocketManager.shared.connectionState {
            completionHandler([])
        } else {
            completionHandler([.banner, .sound, .badge])
        }
    }

    func userNotificationCenter(
        _ _: UNUserNotificationCenter,
        didReceive response: UNNotificationResponse,
        withCompletionHandler completionHandler: @escaping @Sendable () -> Void
    ) {
        Task { @MainActor in
            await NavigationCoordinator.shared.handleNotificationTap(response)
            completionHandler()
        }
    }
}
