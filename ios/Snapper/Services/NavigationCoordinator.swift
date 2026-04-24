import Foundation
import UserNotifications
import os

/// Observable holder for notification-tap deep-link state.
///
/// When a user taps a notification (foreground or background),
/// `AppDelegate.userNotificationCenter(_:didReceive:)` calls
/// `handleNotificationTap(_:)` with the `UNNotificationResponse`. We
/// extract the `deep_link_path` from the custom APNs payload (minted
/// by the backend's alert rules in `payload["deep_link_path"]`) and
/// publish it as `pendingDeepLink`. `MainTabView` observes that field
/// and routes to the target tab + scrolls to the referenced alert /
/// order / position when the value changes.
///
/// Once the view has consumed the deep-link, it calls
/// `clearPendingDeepLink()` so a repeated tap of the same
/// notification doesn't re-fire the same navigation.
@MainActor
final class NavigationCoordinator: ObservableObject {
    static let shared = NavigationCoordinator()

    private let logger = Logger(
        subsystem: Bundle.main.bundleIdentifier ?? "Snapper",
        category: "Navigation"
    )

    /// Deep-link path pulled from the tapped notification's custom
    /// payload, or `nil` when no navigation is pending.
    @Published private(set) var pendingDeepLink: String?

    /// Alert public_id the deep-link references, when present.
    ///
    /// Separate from `pendingDeepLink` so views can decide how
    /// granularly to react — e.g. the Alerts tab scrolls to the row
    /// keyed by this id while the Positions tab just navigates to the
    /// path and ignores the id.
    @Published private(set) var pendingAlertPublicId: String?

    /// Extract deep-link metadata from the tapped notification response.
    ///
    /// The APNs payload minted server-side carries:
    /// - ``deep_link_path: str`` — target route (e.g.
    ///   ``/orders/{client_order_id}``).
    /// - ``alert_event_public_id: str | None`` — optional anchor
    ///   (currently not minted by BE-3b rules; reserved for BE-3c /
    ///   future rules that want row-level navigation).
    ///
    /// A missing / non-string `deep_link_path` logs a warning and
    /// leaves the coordinator's state untouched.
    ///
    /// Args:
    ///   response: Foundation's tap event.
    func handleNotificationTap(_ response: UNNotificationResponse) async {
        let info = response.notification.request.content.userInfo
        guard let path = info["deep_link_path"] as? String else {
            logger.warning("Notification tap carried no deep_link_path; ignoring")
            return
        }
        pendingDeepLink = path
        if let anchor = info["alert_event_public_id"] as? String {
            pendingAlertPublicId = anchor
        } else {
            pendingAlertPublicId = nil
        }
        logger.info("Deep-link pending: \(path)")
    }

    /// Clear the pending deep-link after the view has consumed it.
    ///
    /// Called by `MainTabView` after routing completes so that a
    /// subsequent tap of the same notification re-fires through the
    /// full observer pipeline (otherwise SwiftUI's ``.onChange``
    /// sees no new value).
    func clearPendingDeepLink() {
        pendingDeepLink = nil
        pendingAlertPublicId = nil
    }
}
