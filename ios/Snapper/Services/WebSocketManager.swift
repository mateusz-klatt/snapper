import Foundation
import Combine

class WebSocketManager: ObservableObject {
    static let shared = WebSocketManager()

    @Published var connectionState: ConnectionState = .disconnected
    @Published var lastMessage: WebSocketMessage?
    @Published var marketData: [String: MarketDataUpdate] = [:]
    @Published var orderUpdates: [OrderUpdate] = []

    private var webSocketTask: URLSessionWebSocketTask?
    private var reconnectTimer: Timer?
    private var shouldReconnect = false

    enum ConnectionState {
        case disconnected
        case connecting
        case connected
        case error(String)
    }

    private init() { /* Singleton: use WebSocketManager.shared */ }

    func connect() {
        guard case .connected = connectionState else { return }

        guard let token = AuthService.shared.getWsToken() else {
            print("No WebSocket token available")
            return
        }

        guard var urlComponents = URLComponents(string: AppConfig.wsBaseURL) else {
            connectionState = .error("Invalid WebSocket URL")
            return
        }

        urlComponents.queryItems = [URLQueryItem(name: "token", value: token)]

        guard let url = urlComponents.url else {
            connectionState = .error("Failed to build WebSocket URL")
            return
        }

        connectionState = .connecting
        shouldReconnect = true

        var request = URLRequest(url: url)
        request.timeoutInterval = 10

        webSocketTask = URLSession.shared.webSocketTask(with: request)
        webSocketTask?.resume()

        connectionState = .connected

        receiveMessage()

        startPingTimer()
    }

    func disconnect() {
        shouldReconnect = false
        reconnectTimer?.invalidate()
        webSocketTask?.cancel(with: .goingAway, reason: nil)
        webSocketTask = nil
        connectionState = .disconnected
    }

    func send(message: WebSocketMessage) {
        guard case .connected = connectionState else {
            print("Cannot send message - not connected")
            return
        }

        do {
            let encoder = JSONEncoder()
            encoder.keyEncodingStrategy = .convertToSnakeCase
            let data = try encoder.encode(message)

            let message = URLSessionWebSocketTask.Message.data(data)
            webSocketTask?.send(message) { error in
                if let error = error {
                    print("WebSocket send error: \(error)")
                }
            }
        } catch {
            print("Failed to encode message: \(error)")
        }
    }

    private func receiveMessage() {
        webSocketTask?.receive { [weak self] result in
            switch result {
            case .success(let message):
                self?.handleMessage(message)
                self?.receiveMessage()

            case .failure(let error):
                print("WebSocket receive error: \(error)")
                self?.handleDisconnection()
            }
        }
    }

    private func handleMessage(_ message: URLSessionWebSocketTask.Message) {
        switch message {
        case .data(let data):
            parseMessage(data)
        case .string(let text):
            if let data = text.data(using: .utf8) {
                parseMessage(data)
            }
        @unknown default:
            break
        }
    }

    private func parseMessage(_ data: Data) {
        let decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .convertFromSnakeCase

        do {
            let message = try decoder.decode(WebSocketMessage.self, from: data)

            DispatchQueue.main.async {
                self.lastMessage = message

                switch message.type {
                case "market_data":
                    if let update = try? decoder.decode(MarketDataUpdate.self, from: data) {
                        self.marketData[update.symbol] = update
                    }
                case "order_update":
                    if let update = try? decoder.decode(OrderUpdate.self, from: data) {
                        self.orderUpdates.append(update)
                    }
                default:
                    break
                }
            }
        } catch {
            print("Failed to parse WebSocket message: \(error)")
        }
    }

    private func startPingTimer() {
        reconnectTimer = Timer.scheduledTimer(withTimeInterval: 30, repeats: true) { [weak self] _ in
            self?.sendPing()
        }
    }

    private func sendPing() {
        webSocketTask?.sendPing { error in
            if let error = error {
                print("WebSocket ping failed: \(error)")
                self.handleDisconnection()
            }
        }
    }

    private func handleDisconnection() {
        connectionState = .disconnected

        if shouldReconnect {
            DispatchQueue.main.asyncAfter(deadline: .now() + 3) { [weak self] in
                self?.connect()
            }
        }
    }

    func subscribeToMarketData(symbols: [String]) {
        let message = WebSocketMessage(
            type: "subscribe",
            action: "market_data",
            payload: ["symbols": symbols]
        )
        send(message: message)
    }

    func subscribeToOrders() {
        let message = WebSocketMessage(
            type: "subscribe",
            action: "orders",
            payload: nil
        )
        send(message: message)
    }
}

struct WebSocketMessage: Codable {
    let type: String
    let action: String?
    let payload: [String: Any]?

    enum CodingKeys: String, CodingKey {
        case type
        case action
        case payload
    }

    init(type: String, action: String?, payload: [String: Any]?) {
        self.type = type
        self.action = action
        self.payload = payload
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        type = try container.decode(String.self, forKey: .type)
        action = try container.decodeIfPresent(String.self, forKey: .action)
        payload = nil
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(type, forKey: .type)
        try container.encodeIfPresent(action, forKey: .action)

    }
}

struct MarketDataUpdate: Codable {
    let symbol: String
    let price: Double
    let volume: Double?
    let timestamp: Date
}

struct OrderUpdate: Codable, Identifiable {
    let id: String
    let orderId: String
    let status: String
    let symbol: String
    let side: String
    let quantity: Double
    let price: Double?
    let timestamp: Date

    enum CodingKeys: String, CodingKey {
        case id
        case orderId = "order_id"
        case status
        case symbol
        case side
        case quantity
        case price
        case timestamp
    }
}
