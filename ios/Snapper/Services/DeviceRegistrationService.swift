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
/// sidesteps module-load-time `APIClient.shared` touches that
/// produced Sendable-isolation crashes during launch.
/// Externally observable lifecycle state for the registration flow.
///
/// ``SettingsView`` reads this to show a meaningful state instead of
/// the previous binary "registered / not registered" — a transient
/// failure plus pending retry should not look identical to a fresh
/// install before the APNs token has even arrived.
enum DeviceRegistrationStatus: Equatable, Sendable {
    case idle
    case awaitingLogin
    case awaitingToken
    case inFlight
    case succeeded
    case failed(attempt: Int, message: String)
}

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

    /// Exponential-ish backoff capped to four retries (~80s wall-clock
    /// upper bound). Beyond four the user is expected to take action
    /// from Settings (toggle airplane mode, re-grant push permission,
    /// etc.) rather than the device looping forever in the background.
    private static let retryDelaysSeconds: [TimeInterval] = [1, 4, 16, 60]

    private let apiClient: APIClient
    private let sleeper: Sleeper
    private let logger = Logger(
        subsystem: Bundle.main.bundleIdentifier ?? "Snapper",
        category: "DeviceRegistration"
    )
    private var pendingToken: Data?
    private var isLoggedIn: Bool = false
    private var lastRegisteredDevicePublicId: String?
    private var status: DeviceRegistrationStatus = .idle
    private var retryTask: Task<Void, Never>?

    init(apiClient: APIClient, sleeper: Sleeper = TaskSleeper()) {
        self.apiClient = apiClient
        self.sleeper = sleeper
    }

    /// Store an incoming APNs token; register immediately if logged-in.
    ///
    /// Called from `AppDelegate.didRegisterForRemoteNotificationsWithDeviceToken`.
    /// The token arrives as raw `Data`; `register()` hex-encodes it
    /// before sending so the backend gets the canonical device-token
    /// string APNs itself expects as a URL segment on provider API.
    func onTokenReceived(_ token: Data) async {
        cancelPendingRetry()
        pendingToken = token
        if isLoggedIn {
            await register(attempt: 1)
        } else {
            status = .awaitingLogin
        }
    }

    /// Mark the user as logged-in; register pending token if present.
    ///
    /// Called from `AuthService.login` after successful authentication
    /// flips `isAuthenticated = true`. If the token arrived earlier
    /// (cold-start permission prompt on a previously-authorized device)
    /// it is registered now.
    func onLogin() async {
        cancelPendingRetry()
        isLoggedIn = true
        if pendingToken != nil {
            await register(attempt: 1)
        } else {
            status = .awaitingToken
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
        cancelPendingRetry()
        isLoggedIn = false
        pendingToken = nil
        status = .idle
    }

    /// Public_id of the last-registered device (nil until first register).
    ///
    /// Exposed for UI (e.g. Settings screen showing the currently
    /// active device) + for tests. Mutation is strictly internal.
    func currentDevicePublicId() -> String? {
        lastRegisteredDevicePublicId
    }

    /// Externally observable status (idle / inFlight / succeeded /
    /// failed). Settings UI reads this to surface a transient
    /// registration failure instead of pretending the device simply
    /// has not registered yet.
    func currentStatus() -> DeviceRegistrationStatus {
        return status
    }

    /// Manual retry trigger fired from Settings UI when a user wants
    /// to bypass the backoff window after seeing the failed status.
    /// Resets the attempt counter so the user-initiated try gets the
    /// full retry envelope rather than tail-end backoff.
    func retryNow() async {
        cancelPendingRetry()
        guard isLoggedIn, pendingToken != nil else {
            return
        }
        await register(attempt: 1)
    }

    private func register(attempt: Int) async {
        guard let token = pendingToken else {
            return
        }
        status = .inFlight
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
        let provenance = await MainActor.run {
            EnvelopeMinter.shared.next(.control)
        }
        let command = RegisterDeviceCommand(
            type: "register_device_command",
            sequenceId: provenance.sequenceId,
            publicId: provenance.publicId,
            timestamp: provenance.timestamp,
            sessionId: provenance.sessionId,
            topic: nil,
            payload: body
        )
        do {
            let response = try await apiClient.registerDevice(command: command)
            lastRegisteredDevicePublicId = response.payload.publicId
            status = .succeeded
            logger.info("Device registered: \(response.payload.publicId)")
        } catch {
            let message = error.localizedDescription
            status = .failed(attempt: attempt, message: message)
            logger.error("Device registration failed (attempt \(attempt)): \(error)")
            if attempt < Self.retryDelaysSeconds.count {
                let delay = Self.retryDelaysSeconds[attempt - 1]
                scheduleRetry(after: delay, nextAttempt: attempt + 1)
            } else {
                logger.error("Device registration giving up after \(attempt) attempts; user can retry from Settings")
            }
        }
    }

    private func scheduleRetry(after delay: TimeInterval, nextAttempt: Int) {
        retryTask = Task { [weak self, sleeper] in
            try? await sleeper.sleep(seconds: delay)
            if Task.isCancelled { return }
            await self?.attemptRetryIfPending(attempt: nextAttempt)
        }
    }

    private func attemptRetryIfPending(attempt: Int) async {
        if Task.isCancelled { return }
        guard isLoggedIn, pendingToken != nil else {
            return
        }
        await register(attempt: attempt)
    }

    private func cancelPendingRetry() {
        retryTask?.cancel()
        retryTask = nil
    }

    private func currentEnv() -> String {
        #if DEBUG
        return "sandbox"
        #else
        return "prod"
        #endif
    }
}
