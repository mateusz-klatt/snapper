import Foundation
import UIKit
import os

/// Actor that owns the APNs token → backend registration flow.
///
/// Two asynchronous inputs feed this actor:
/// 1. `onTokenReceived(_:)` — from `AppDelegate.didRegisterForRemoteNotificationsWithDeviceToken`.
/// 2. `onLogin()` / `onLogout()` — from `AuthService.login` / `.logout`.
///
/// The actor holds both the most recent APNs token and the current
/// authenticated state; registration to `POST /api/devices` fires
/// only when BOTH are present. A token that arrives pre-login waits
/// inside `pendingToken` until login flips the guard; a login that
/// happens before the token arrives waits for the token.
///
/// ``APIClient`` is `@MainActor`-annotated, so `register()` uses an
/// explicit `await` hop when it invokes `apiClient.registerDevice` —
/// this is Swift 6 strict-concurrency clean because the actor and
/// `@MainActor` are distinct isolation domains.
///
/// Lazy `@MainActor`-isolated shared factory: callers reach the
/// service from `AppDelegate` callbacks (already running inside
/// `Task { @MainActor in }`) and from `AuthService` `@MainActor`
/// methods, so the factory hop is a no-op on the hot path and
/// sidesteps module-load-time `APIClient.shared` touches per Plan
/// v1.2 fix.
actor DeviceRegistrationService {
    @MainActor
    static func shared() -> DeviceRegistrationService {
        if let existing = _shared {
            return existing
        }
        let instance = DeviceRegistrationService(apiClient: APIClient.shared)
        _shared = instance
        return instance
    }

    @MainActor private static var _shared: DeviceRegistrationService?

    private let apiClient: APIClient
    private let logger = Logger(
        subsystem: Bundle.main.bundleIdentifier ?? "Snapper",
        category: "DeviceRegistration"
    )
    private var pendingToken: Data?
    private var isLoggedIn: Bool = false
    private var lastRegisteredDevicePublicId: String?

    init(apiClient: APIClient) {
        self.apiClient = apiClient
    }

    /// Store an incoming APNs token; register immediately if logged-in.
    ///
    /// Called from `AppDelegate.didRegisterForRemoteNotificationsWithDeviceToken`.
    /// The token arrives as raw `Data`; `register()` hex-encodes it
    /// before sending so the backend gets the canonical device-token
    /// string APNs itself expects as a URL segment on provider API.
    func onTokenReceived(_ token: Data) async {
        pendingToken = token
        if isLoggedIn {
            await register()
        }
    }

    /// Mark the user as logged-in; register pending token if present.
    ///
    /// Called from `AuthService.login` after successful authentication
    /// flips `isAuthenticated = true`. If the token arrived earlier
    /// (cold-start permission prompt on a previously-authorized device)
    /// it is registered now.
    func onLogin() async {
        isLoggedIn = true
        if pendingToken != nil {
            await register()
        }
    }

    /// Clear login state + forget the pending token.
    ///
    /// Called from `AuthService.logout` before the session is torn
    /// down. The backend-side device row is NOT deleted on logout
    /// (the user may log back in on the same device within minutes);
    /// deletion happens on explicit user-initiated unregister via
    /// `DELETE /api/devices/{public_id}` (covered by iOS-5 Settings).
    func onLogout() {
        isLoggedIn = false
        pendingToken = nil
    }

    /// Public_id of the last-registered device (nil until first register).
    ///
    /// Exposed for UI (e.g. Settings screen showing the currently
    /// active device) + for tests. Mutation is strictly internal.
    func currentDevicePublicId() -> String? {
        lastRegisteredDevicePublicId
    }

    private func register() async {
        guard let token = pendingToken else {
            return
        }
        let hex = token.map { String(format: "%02x", $0) }.joined()
        let appVersion = Bundle.main.object(forInfoDictionaryKey: "CFBundleShortVersionString") as? String
        let deviceId = await MainActor.run {
            UIDevice.current.identifierForVendor?.uuidString ?? UUID().uuidString
        }
        let body = RegisterDeviceBody(
            deviceToken: hex,
            deviceId: deviceId,
            env: currentEnv(),
            appVersion: appVersion ?? "unknown",
            previewsMode: nil
        )
        let command = RegisterDeviceCommand(
            type: "register_device_command",
            sequenceId: 1,
            publicId: UUID().uuidString,
            timestamp: Date(),
            sessionId: UUID().uuidString,
            payload: body
        )
        do {
            let response = try await apiClient.registerDevice(command: command)
            lastRegisteredDevicePublicId = response.payload.publicId
            logger.info("Device registered: \(response.payload.publicId)")
        } catch {
            logger.error("Device registration failed: \(error)")
        }
    }

    private func currentEnv() -> String {
        #if DEBUG
        return "sandbox"
        #else
        return "prod"
        #endif
    }
}
