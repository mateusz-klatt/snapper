import Foundation
import Combine
import os

@MainActor
class WebSocketManager: ObservableObject {
    static let shared = WebSocketManager(
        authService: AuthService.shared,
        taskFactory: URLSessionWebSocketTaskFactory(session: .shared),
        sleeper: TaskSleeper()
    )

    @Published var connectionState: ConnectionState = .disconnected
    @Published var availableTopics: [String] = []
    @Published private(set) var state = WSState()
    @Published private(set) var reconnectAttempts = 0

    private let logger = Logger(subsystem: Bundle.main.bundleIdentifier ?? "Snapper", category: "WebSocket")

    private let authService: AuthRefreshing
    private let taskFactory: WebSocketTaskFactory
    let sleeper: Sleeper

    private var webSocketTask: WebSocketTaskProtocol?
    private var pingTimer: Timer?
    private var shouldReconnect = false
    private var intentionalDisconnect = false
    private let baseReconnectInterval: TimeInterval = 3
    /// Per-attempt ceiling for backoff (5 minutes). No hard cap on the
    /// attempt counter — the client keeps trying forever so that a phone
    /// returning online after an extended offline period recovers without
    /// user intervention.
    private let maxReconnectDelay: TimeInterval = 300
    /// Cancelled on disconnect + re-scheduled after every `auth_complete`
    /// so the proactive refresh fires before the server-issued ws_token
    /// actually expires.
    private var proactiveRefreshTask: Task<Void, Never>?
    /// Cancellable handle for the backoff-delayed reconnect attempt.
    /// Cleared whenever the manager reaches a terminal/paused state so
    /// `disconnect()` or `enterAuthFailedAndLogout()` actually stop the
    /// client rather than just flipping `shouldReconnect = false` and
    /// hoping the already-queued closure respects it.
    private var reconnectWorkItem: DispatchWorkItem?

    /// Confirmed subscriptions — survive reconnects so the dispatcher can
    /// replay them on the next `auth_complete`.
    private var subscribedTopics: Set<String> = []
    /// Queued subscriptions made while not `.connected`; cleared after the
    /// first replay (then their topics live in `subscribedTopics`).
    private var pendingSubscriptions: Set<String> = []

    enum ConnectionState: Equatable {
        case disconnected
        case connecting
        case authenticating
        case connected
        case error(String)
        /// Terminal state reached when token refresh fails or auth is
        /// rejected — the manager calls `AuthService.logout()` so the UI
        /// routes back to `LoginView`. Distinct from `.error` so views
        /// can render a logout-specific message.
        case authFailed(String)
    }

    init(authService: AuthRefreshing, taskFactory: WebSocketTaskFactory, sleeper: Sleeper) {
        self.authService = authService
        self.taskFactory = taskFactory
        self.sleeper = sleeper
    }

    func connect() {
        if case .connected = connectionState { return }
        if case .connecting = connectionState { return }
        if case .authenticating = connectionState { return }
        // `.authFailed` is terminal — the user must explicitly re-authenticate
        // via `LoginView` (which resets AuthService.isAuthenticated and drives
        // `SnapperApp`'s auth-change observer). Silently reconnecting from
        // here would defeat the kill-switch the logout path just armed.
        if case .authFailed = connectionState { return }

        guard let url = URL(string: AppConfig.wsBaseURL) else {
            connectionState = .error("Invalid WebSocket URL")
            return
        }

        connectionState = .connecting
        shouldReconnect = true
        intentionalDisconnect = false

        let request = URLRequest(url: url)
        webSocketTask = taskFactory.makeTask(request: request)
        webSocketTask?.resume()

        listenForMessages()
    }

    func disconnect() {
        shouldReconnect = false
        intentionalDisconnect = true
        reconnectAttempts = 0
        pingTimer?.invalidate()
        pingTimer = nil
        proactiveRefreshTask?.cancel()
        proactiveRefreshTask = nil
        reconnectWorkItem?.cancel()
        reconnectWorkItem = nil
        webSocketTask?.cancel(with: .goingAway, reason: nil)
        webSocketTask = nil
        connectionState = .disconnected
    }

    func sendJSON(_ dict: [String: Any]) {
        guard let data = try? JSONSerialization.data(withJSONObject: dict),
              let text = String(data: data, encoding: .utf8),
              let task = webSocketTask else {
            return
        }
        Task { [weak self] in
            do {
                try await task.send(.string(text))
            } catch {
                self?.logger.error("WebSocket send error: \(error)")
            }
        }
    }

    /// Stamp top-level provenance fields onto a WS frame and send it.
    /// Mirrors the frontend's ``stampProvenance`` in
    /// `frontend/src/lib/websocket/client.ts`: provenance lives at
    /// the top level alongside ``type`` and type-specific fields,
    /// NOT wrapped in a ``payload`` envelope. Confirmed against
    /// ``snapper.interface.websocket.schemas`` (e.g. ``WSAuthenticateRequest``
    /// inherits ``StrictDataSchema`` so ``ws_token`` is a sibling of
    /// the provenance fields).
    func sendEnvelope(_ message: [String: Any], counter: EnvelopeMinter.Counter) {
        let provenance = EnvelopeMinter.shared.next(counter)
        var frame = message
        frame["public_id"] = provenance.publicId
        frame["session_id"] = provenance.sessionId
        frame["sequence_id"] = provenance.sequenceId
        frame["timestamp"] = provenance.timestampString
        sendJSON(frame)
    }

    func subscribe(topics: [String]) {
        if case .connected = connectionState {
            sendEnvelope(["type": "subscribe", "topics": topics], counter: .control)
            topics.forEach { subscribedTopics.insert($0) }
        } else {
            topics.forEach { pendingSubscriptions.insert($0) }
        }
    }

    func unsubscribe(topics: [String]) {
        topics.forEach {
            pendingSubscriptions.remove($0)
            subscribedTopics.remove($0)
        }
        if case .connected = connectionState {
            sendEnvelope(["type": "unsubscribe", "topics": topics], counter: .control)
        }
    }

    private func replayPendingSubscriptions() {
        let toSend = subscribedTopics.union(pendingSubscriptions)
        guard !toSend.isEmpty else { return }
        sendEnvelope(["type": "subscribe", "topics": Array(toSend)], counter: .control)
        subscribedTopics.formUnion(pendingSubscriptions)
        pendingSubscriptions.removeAll()
    }

    private func listenForMessages() {
        guard let task = webSocketTask else { return }
        Task { @MainActor [weak self] in
            do {
                let message = try await task.receive()
                guard let self = self else { return }
                // Continue the read loop only while the in-flight task still
                // matches the one we kicked off — guards against double-loops
                // if the socket was swapped while `receive()` was suspended.
                guard let current = self.webSocketTask, current === task else {
                    return
                }
                self.handleRawMessage(message)
                // handleRawMessage may have mutated webSocketTask (e.g.
                // auth_failed cancels the task). Re-check before recursing
                // so we never loop on a stale reference.
                if let now = self.webSocketTask, now === task {
                    self.listenForMessages()
                }
            } catch {
                guard let self = self else { return }
                // Stale errors from a task we've already replaced or nilled
                // must not touch shared state — otherwise an old receive()
                // resuming after `disconnect()`, `enterAuthFailedAndLogout`,
                // or a full reconnect swap would wipe the new socket via
                // `handleDisconnection()`. The only case where this catch
                // path has work to do is when the current task is STILL
                // the one we suspended on.
                guard let current = self.webSocketTask, current === task else {
                    return
                }
                if !self.intentionalDisconnect {
                    self.logger.error("WebSocket receive error: \(error)")
                }
                self.handleDisconnection()
            }
        }
    }

    /// Dispatch a raw inbound frame. Internal for `@testable` access so the
    /// dispatcher tests can pump decoded frames without a live socket.
    func handleRawMessage(_ message: URLSessionWebSocketTask.Message) {
        let data: Data
        switch message {
        case .string(let text):
            guard let d = text.data(using: .utf8) else { return }
            data = d
        case .data(let d):
            data = d
        @unknown default:
            return
        }

        guard let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let type = json["type"] as? String else {
            return
        }

        switch type {
        case "auth_required":
            connectionState = .authenticating
            Task { await self.performAuthentication() }

        case "auth_ok":
            break

        case "auth_complete":
            handleAuthComplete(data: data, rawJSON: json)

        case "auth_failed":
            let reason = json["reason"] as? String ?? "unknown"
            logger.error("WebSocket auth rejected by server: \(reason)")
            // Server rejection is also terminal — route through the same
            // logout flow as client-side refresh failure so the UI
            // consistently falls back to LoginView.
            Task { await self.enterAuthFailedAndLogout(reason: "Server rejected auth: \(reason)") }

        case "reauth_required":
            Task { await self.performReauthentication() }

        case "reauth_ok":
            break

        case "auth_expired":
            logger.warning("WebSocket auth expired")
            handleDisconnection()

        case "pong":
            break

        default:
            dispatchTypedFrame(type: type, data: data)
        }
    }

    /// Two-pass decode: parse the full `WSAuthCompleteResponse` shape to
    /// surface `availableTopics`, `userRole`, and `wsTokenExp` (the last
    /// drives proactive refresh).
    private func handleAuthComplete(data: Data, rawJSON: [String: Any]) {
        reconnectAttempts = 0
        connectionState = .connected

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        if let parsed = try? decoder.decode(WSAuthCompleteResponse.self, from: data) {
            availableTopics = parsed.availableTopics
            state.wsTokenExp = parsed.wsTokenExp
        } else if let topics = rawJSON["available_topics"] as? [String] {
            // Fallback: keep the pre-envelope behaviour so partial frames
            // from older backends still expose topic lists to the UI.
            availableTopics = topics
        }

        replayPendingSubscriptions()
        startPingTimer()
        scheduleProactiveTokenRefresh()
    }

    /// Schedule proactive reauth at 80% of the ws_token TTL.
    ///
    /// Uses the injected `Sleeper` so tests can assert the scheduled
    /// interval without wall-clock dependency. If the token is already
    /// past the 80% mark on arrival (short TTL, clock skew), refresh
    /// immediately so the server doesn't time us out mid-session.
    private func scheduleProactiveTokenRefresh() {
        proactiveRefreshTask?.cancel()
        guard let exp = state.wsTokenExp else { return }
        let ttl = exp.timeIntervalSinceNow
        let fireAt = ttl * 0.8
        if fireAt <= 0 {
            logger.warning("ws_token already >= 80% expired on arrival; refreshing immediately")
            // Track the immediate-refresh path too so a disconnect
            // arriving before the refresh fires cancels it cleanly.
            proactiveRefreshTask = Task { [weak self] in
                guard let self = self, !Task.isCancelled else { return }
                await self.performReauthentication()
            }
            return
        }
        let sleeper = self.sleeper
        proactiveRefreshTask = Task { [weak self] in
            do {
                try await sleeper.sleep(seconds: fireAt)
                try Task.checkCancellation()
                guard let self = self else { return }
                await self.performReauthentication()
            } catch {
                // Task cancelled — normal on disconnect.
            }
        }
    }

    /// Decode typed payloads into `WSState` `@Published` properties.
    private func dispatchTypedFrame(type: String, data: Data) {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        switch type {
        case "trade":
            if let decoded = try? decoder.decode(TradeData.self, from: data) {
                state.lastTrade = decoded
            } else {
                logger.debug("WS trade frame failed to decode")
            }
        case "order_event":
            if let decoded = try? decoder.decode(OrderEventData.self, from: data) {
                state.lastOrderEvent = decoded
            } else {
                logger.debug("WS order_event frame failed to decode")
            }
        case "order_cancel":
            if let decoded = try? decoder.decode(OrderCancelData.self, from: data) {
                state.lastOrderCancel = decoded
            } else {
                logger.debug("WS order_cancel frame failed to decode")
            }
        case "heartbeat":
            if let decoded = try? decoder.decode(HeartbeatData.self, from: data) {
                state.lastHeartbeat = decoded
                state.lastHeartbeatAt = Date()
            } else {
                logger.debug("WS heartbeat frame failed to decode")
            }
        case "user_deactivated":
            if let decoded = try? decoder.decode(UserDeactivatedData.self, from: data) {
                state.lastUserDeactivated = decoded
            } else {
                logger.debug("WS user_deactivated frame failed to decode")
            }
        default:
            logger.debug("WS frame type=\(type, privacy: .public) not bound in Plan 1")
        }
    }

    /// True when the manager is still in a live session. Used by the
    /// refresh-failure paths to distinguish a genuine auth failure from
    /// a cancelled refresh (e.g. disconnect or background fired while
    /// `fetchFreshWsToken()` was in flight). Without this check, a
    /// racing disconnect would be converted into an `.authFailed` +
    /// forced logout, which is wrong — disconnect is not auth failure.
    private var isLiveSession: Bool {
        switch connectionState {
        case .connecting, .authenticating, .connected:
            return true
        case .disconnected, .error, .authFailed:
            return false
        }
    }

    private func performAuthentication() async {
        let token = await authService.fetchFreshWsToken()
        guard isLiveSession else { return }
        guard let token = token else {
            logger.error("Failed to get ws_token for WebSocket auth — forcing logout")
            await enterAuthFailedAndLogout(reason: "Token refresh failed")
            return
        }
        sendEnvelope(["type": "authenticate", "ws_token": token], counter: .control)
    }

    private func performReauthentication() async {
        let token = await authService.fetchFreshWsToken()
        guard isLiveSession else { return }
        guard let token = token else {
            logger.error("Failed to get ws_token for reauth — forcing logout")
            await enterAuthFailedAndLogout(reason: "Token refresh failed")
            return
        }
        sendEnvelope(["type": "reauth", "ws_token": token], counter: .control)
    }

    /// Terminal auth-failure path. Stops reconnect loop, cancels the
    /// socket, enters `.authFailed`, and logs the user out so the app
    /// flips to `LoginView` via `SnapperApp`'s auth binding (Commit 3).
    private func enterAuthFailedAndLogout(reason: String) async {
        connectionState = .authFailed(reason)
        shouldReconnect = false
        proactiveRefreshTask?.cancel()
        proactiveRefreshTask = nil
        reconnectWorkItem?.cancel()
        reconnectWorkItem = nil
        pingTimer?.invalidate()
        pingTimer = nil
        webSocketTask?.cancel(with: .normalClosure, reason: nil)
        webSocketTask = nil
        await authService.logout()
    }

    private func startPingTimer() {
        pingTimer?.invalidate()
        pingTimer = Timer.scheduledTimer(withTimeInterval: 30, repeats: true) { [weak self] _ in
            Task { @MainActor in
                self?.sendPing()
            }
        }
    }

    private func sendPing() {
        guard case .connected = connectionState else { return }
        sendEnvelope(["type": "ping"], counter: .telemetry)
    }

    private func handleDisconnection() {
        pingTimer?.invalidate()
        pingTimer = nil
        proactiveRefreshTask?.cancel()
        proactiveRefreshTask = nil
        webSocketTask = nil
        connectionState = .disconnected

        guard shouldReconnect else { return }

        reconnectAttempts += 1
        let delay = nextReconnectDelay()
        // Cancel any previously-armed reconnect so we never double-fire.
        reconnectWorkItem?.cancel()
        let workItem = DispatchWorkItem { [weak self] in
            Task { @MainActor in
                guard let self = self else { return }
                // Re-check the intent right before reconnecting — a
                // disconnect/background/logout that arrived while we
                // were queued must win over the stale reconnect.
                guard self.shouldReconnect else { return }
                self.connect()
            }
        }
        reconnectWorkItem = workItem
        DispatchQueue.main.asyncAfter(deadline: .now() + delay, execute: workItem)
    }

    /// Exponential backoff capped at `maxReconnectDelay` (300s) with up to
    /// 30% jitter added. Exposed `internal` so unit tests can assert the
    /// return range without having to mock `DispatchQueue.asyncAfter`.
    func nextReconnectDelay() -> TimeInterval {
        let exponent = Double(max(reconnectAttempts - 1, 0))
        let base = baseReconnectInterval * pow(2.0, exponent)
        let capped = min(base, maxReconnectDelay)
        let jitter = Double.random(in: 0..<(capped * 0.3))
        return capped + jitter
    }

    // MARK: - Test hooks (internal, @testable visibility only)
    //
    // Swift gives test bundles using `@testable import` visibility into
    // `internal` symbols. These helpers expose just enough state to test
    // the infinite-retry invariant + backoff range without bouncing off
    // `DispatchQueue.asyncAfter` or `URLSession.webSocketTask`.

    func setReconnectAttemptsForTesting(_ n: Int) {
        reconnectAttempts = n
    }

    func forceHandleDisconnectionForTesting() {
        shouldReconnect = true
        handleDisconnection()
    }
}
