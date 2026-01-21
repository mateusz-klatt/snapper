import Foundation

class APIClient {
    static let shared = APIClient()

    private let session: URLSession
    private let authService: AuthService

    init(session: URLSession = .shared, authService: AuthService = .shared) {
        self.session = session
        self.authService = authService
    }

    private convenience init() {
        self.init(session: .shared, authService: .shared)
    }

    private func request<T: Decodable>(
        endpoint: String,
        method: String = "GET",
        body: Encodable? = nil,
        requiresAuth: Bool = true
    ) async throws -> T {
        guard let url = URL(string: "\(AppConfig.apiBaseURL)\(endpoint)") else {
            throw APIError.invalidURL
        }

        var request = URLRequest(url: url)
        request.httpMethod = method
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")

        if requiresAuth, let token = authService.getAccessToken() {
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }

        if let body = body {
            let encoder = JSONEncoder()
            encoder.keyEncodingStrategy = .convertToSnakeCase
            request.httpBody = try encoder.encode(body)
        }

        let (data, response) = try await session.data(for: request)

        guard let httpResponse = response as? HTTPURLResponse else {
            throw APIError.invalidResponse
        }

        guard (200...299).contains(httpResponse.statusCode) else {
            if let errorResponse = try? JSONDecoder().decode(ErrorResponse.self, from: data) {
                throw APIError.serverError(errorResponse.detail)
            }
            throw APIError.httpError(httpResponse.statusCode)
        }

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601

        return try decoder.decode(T.self, from: data)
    }

    func fetchOrders() async throws -> [Order] {
        return try await request(endpoint: AppConfig.Endpoints.orders)
    }

    func createOrder(_ order: CreateOrderRequest) async throws -> Order {
        return try await request(
            endpoint: AppConfig.Endpoints.orders,
            method: "POST",
            body: order
        )
    }

    func cancelOrder(orderId: String) async throws {
        let _: EmptyResponse = try await request(
            endpoint: "\(AppConfig.Endpoints.orders)/\(orderId)",
            method: "DELETE"
        )
    }

    func fetchPortfolio() async throws -> Portfolio {
        return try await request(endpoint: AppConfig.Endpoints.portfolio)
    }

    func fetchPositions() async throws -> [Position] {
        return try await request(endpoint: AppConfig.Endpoints.positions)
    }

    func fetchMarketData(symbol: String) async throws -> MarketData {
        return try await request(endpoint: "\(AppConfig.Endpoints.marketData)/\(symbol)")
    }

    func fetchStrategies() async throws -> [Strategy] {
        return try await request(endpoint: AppConfig.Endpoints.strategies)
    }

    func startStrategy(strategyId: String) async throws {
        let _: EmptyResponse = try await request(
            endpoint: "\(AppConfig.Endpoints.strategies)/\(strategyId)/start",
            method: "POST"
        )
    }

    func stopStrategy(strategyId: String) async throws {
        let _: EmptyResponse = try await request(
            endpoint: "\(AppConfig.Endpoints.strategies)/\(strategyId)/stop",
            method: "POST"
        )
    }
}

enum APIError: LocalizedError {
    case invalidURL
    case invalidResponse
    case httpError(Int)
    case serverError(String)
    case decodingError

    var errorDescription: String? {
        switch self {
        case .invalidURL:
            return "Invalid URL"
        case .invalidResponse:
            return "Invalid response from server"
        case .httpError(let code):
            return "HTTP error: \(code)"
        case .serverError(let message):
            return message
        case .decodingError:
            return "Failed to decode response"
        }
    }
}

struct CreateOrderRequest: Codable {
    let symbol: String
    let side: String
    let quantity: Double
    let orderType: String
    let price: Double?

    enum CodingKeys: String, CodingKey {
        case symbol
        case side
        case quantity
        case orderType = "order_type"
        case price
    }
}

struct EmptyResponse: Codable {}
