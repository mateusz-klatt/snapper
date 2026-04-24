import Foundation
import UserNotifications
import UIKit
import os

/// Observable facade for UNUserNotificationCenter state.
///
/// Owns the permission prompt lifecycle + exposes `authorizationStatus` as
/// a `@Published` field so SwiftUI can render "Notifications off" banners
/// or settings-link hints. The actual APNs token plumbing lives in
/// `AppDelegate` (application delegate is required for `didRegisterForRemoteNotificationsWithDeviceToken`);
/// this service is the user-facing layer.
///
/// Lazy shared instance — safe to touch from any `@MainActor` context
/// (SnapperApp `@StateObject`, settings views, auth hooks). Not an actor;
/// UserNotifications APIs are main-actor-friendly on iOS 17+.
@MainActor
final class NotificationService: ObservableObject {
    static let shared = NotificationService()

    private let logger = Logger(
        subsystem: Bundle.main.bundleIdentifier ?? "Snapper",
        category: "Notifications"
    )
    private let notificationCenter: UNUserNotificationCenter

    /// Current UNUserNotificationCenter authorization status.
    ///
    /// `.notDetermined` until the first permission probe. Updated via
    /// `refreshAuthorizationStatus()` on app launch + after every
    /// `requestAuthorization()` / `Settings.app` round-trip.
    @Published private(set) var authorizationStatus: UNAuthorizationStatus = .notDetermined

    init(notificationCenter: UNUserNotificationCenter = .current()) {
        self.notificationCenter = notificationCenter
    }

    /// Read the current authorization status and publish it.
    ///
    /// Called from app launch + from `SettingsView` every time the
    /// screen appears so a permission flipped via Settings.app shows
    /// up without a restart.
    func refreshAuthorizationStatus() async {
        let settings = await notificationCenter.notificationSettings()
        self.authorizationStatus = settings.authorizationStatus
    }

    /// Prompt the user for notification permission + register for APNs.
    ///
    /// When the user grants, triggers
    /// `UIApplication.registerForRemoteNotifications()` on the main
    /// thread so the AppDelegate's
    /// `didRegisterForRemoteNotificationsWithDeviceToken` fires and
    /// the token flows to `DeviceRegistrationService`.
    ///
    /// When the user denies, the status is published and the caller
    /// can surface an inline hint to Settings.app. The method never
    /// throws — denied / error statuses are represented via the
    /// `@Published authorizationStatus` field.
    func requestAuthorization() async {
        do {
            let granted = try await notificationCenter.requestAuthorization(
                options: [.alert, .sound, .badge]
            )
            await refreshAuthorizationStatus()
            if granted {
                UIApplication.shared.registerForRemoteNotifications()
            } else {
                logger.info("User declined notification permission")
            }
        } catch {
            logger.error("Notification permission request failed: \(error)")
            await refreshAuthorizationStatus()
        }
    }

    /// True iff permission granted AND APNs token present in device list.
    ///
    /// Derived from `authorizationStatus` only — a token's presence is
    /// a `DeviceRegistrationService` concern. Kept here as a
    /// convenience boolean for UI.
    var isAuthorized: Bool {
        authorizationStatus == .authorized || authorizationStatus == .provisional
    }
}
