import Foundation
import Combine

@MainActor
class WebSocketManager: ObservableObject {
    static let shared = WebSocketManager()

    @Published var connectionState: ConnectionState = .disconnected
    @Published var lastMessage: ServerMessage?
    @Published var availableTopics: [String] = []

    private var webSocketTask: URLSessionWebSocketTask?
    private var pingTimer: Timer?
    private var shouldReconnect = false
    private var intentionalDisconnect = false
    private var reconnectAttempts = 0
    private let maxReconnectAttempts = 10
    private let baseReconnectInterval: TimeInterval = 3

    enum ConnectionState: Equatable {
        case disconnected
        case connecting
        case authenticating
        case connected
        case error(String)
    }

    private init() {
        // Singleton: prevent external instantiation
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
        webSocketTask = URLSession.shared.webSocketTask(with: request)
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
              let text = String(data: data, encoding: .utf8) else {
            return
        }
        let message = URLSessionWebSocketTask.Message.string(text)
        webSocketTask?.send(message) { error in
            if let error = error {
                print("WebSocket send error: \(error)")
            }
        }
    }

    func subscribe(topics: [String]) {
        guard case .connected = connectionState else { return }
        sendJSON(["type": "subscribe", "topics": topics])
    }

    func unsubscribe(topics: [String]) {
        guard case .connected = connectionState else { return }
        sendJSON(["type": "unsubscribe", "topics": topics])
    }

    private func listenForMessages() {
        webSocketTask?.receive { [weak self] result in
            Task { @MainActor in
                guard let self = self else { return }
                switch result {
                case .success(let message):
                    self.handleRawMessage(message)
                    self.listenForMessages()
                case .failure(let error):
                    if !self.intentionalDisconnect {
                        print("WebSocket receive error: \(error)")
                    }
                    self.handleDisconnection()
                }
            }
        }
    }

    private func handleRawMessage(_ message: URLSessionWebSocketTask.Message) {
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
            reconnectAttempts = 0
            connectionState = .connected
            if let topics = json["available_topics"] as? [String] {
                availableTopics = topics
            }
            startPingTimer()

        case "auth_failed":
            let reason = json["reason"] as? String ?? "unknown"
            print("WebSocket auth failed: \(reason)")
            connectionState = .error("Auth failed: \(reason)")
            webSocketTask?.cancel(with: .normalClosure, reason: nil)
            webSocketTask = nil

        case "reauth_required":
            Task { await self.performReauthentication() }

        case "reauth_ok":
            break

        case "auth_expired":
            print("WebSocket auth expired")
            handleDisconnection()

        case "pong":
            break

        default:
            let serverMsg = ServerMessage(type: type, data: data)
            lastMessage = serverMsg
        }
    }

    private func performAuthentication() async {
        guard let token = await AuthService.shared.fetchFreshWsToken() else {
            print("Failed to get ws_token for WebSocket auth")
            connectionState = .error("No ws_token")
            webSocketTask?.cancel(with: .normalClosure, reason: nil)
            webSocketTask = nil
            return
        }
        sendJSON(["type": "authenticate", "ws_token": token])
    }

    private func performReauthentication() async {
        guard let token = await AuthService.shared.fetchFreshWsToken() else {
            print("Failed to get ws_token for reauth")
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

struct ServerMessage {
    let type: String
    let data: Data
}
