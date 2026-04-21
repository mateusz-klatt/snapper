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

    private let logger = Logger(subsystem: Bundle.main.bundleIdentifier ?? "Snapper", category: "WebSocket")

    private let authService: AuthRefreshing
    private let taskFactory: WebSocketTaskFactory
    // Exposed so Commit 2 (proactive refresh) and tests can inject a fake.
    let sleeper: Sleeper

    private var webSocketTask: WebSocketTaskProtocol?
    private var pingTimer: Timer?
    private var shouldReconnect = false
    private var intentionalDisconnect = false
    private var reconnectAttempts = 0
    private let maxReconnectAttempts = 10
    private let baseReconnectInterval: TimeInterval = 3

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

    func subscribe(topics: [String]) {
        if case .connected = connectionState {
            sendJSON(["type": "subscribe", "topics": topics])
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
            sendJSON(["type": "unsubscribe", "topics": topics])
        }
    }

    private func replayPendingSubscriptions() {
        let toSend = subscribedTopics.union(pendingSubscriptions)
        guard !toSend.isEmpty else { return }
        sendJSON(["type": "subscribe", "topics": Array(toSend)])
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
                // Stale errors from a task we've already replaced/cancelled
                // must not touch shared state — otherwise an old receive()
                // resuming after reconnect would wipe the new socket via
                // `handleDisconnection()`. Compare identity against the
                // currently-owned task; if it no longer matches, the stale
                // task was already taken offline by its replacement.
                if let current = self.webSocketTask, current !== task {
                    return
                }
                if self.webSocketTask == nil && self.intentionalDisconnect {
                    // Expected close from `disconnect()`; the cancel path
                    // has already cleared state. Nothing more to do.
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
            logger.error("WebSocket auth failed: \(reason)")
            connectionState = .error("Auth failed: \(reason)")
            webSocketTask?.cancel(with: .normalClosure, reason: nil)
            webSocketTask = nil

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
    /// of which Commit 2's proactive refresh consumes).
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

    private func performAuthentication() async {
        guard let token = await authService.fetchFreshWsToken() else {
            logger.error("Failed to get ws_token for WebSocket auth")
            connectionState = .error("No ws_token")
            webSocketTask?.cancel(with: .normalClosure, reason: nil)
            webSocketTask = nil
            return
        }
        sendJSON(["type": "authenticate", "ws_token": token])
    }

    private func performReauthentication() async {
        guard let token = await authService.fetchFreshWsToken() else {
            logger.error("Failed to get ws_token for reauth")
            return
        }
        sendJSON(["type": "reauth", "ws_token": token])
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
        sendJSON(["type": "ping"])
    }

    private func handleDisconnection() {
        pingTimer?.invalidate()
        pingTimer = nil
        webSocketTask = nil
        connectionState = .disconnected

        guard shouldReconnect, reconnectAttempts < maxReconnectAttempts else {
            return
        }

        reconnectAttempts += 1
        let delay = min(baseReconnectInterval * pow(1.5, Double(reconnectAttempts - 1)), 30)
        DispatchQueue.main.asyncAfter(deadline: .now() + delay) { [weak self] in
            Task { @MainActor in
                self?.connect()
            }
        }
    }
}
