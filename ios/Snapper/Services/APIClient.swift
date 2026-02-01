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
        body: Encodable? = nil
    ) async throws -> T {
        guard let url = URL(string: "\(AppConfig.apiBaseURL)\(endpoint)") else {
            throw APIError.invalidURL
        }

        var request = URLRequest(url: url)
        request.httpMethod = method
        request.setValue(AppConfig.ContentType.json, forHTTPHeaderField: AppConfig.HTTPHeader.contentType)

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

    func fetchOrders() async throws -> [OrderStatus] {
        return try await request(endpoint: AppConfig.Endpoints.orders)
    }

    func fetchPositions() async throws -> [PositionSnapshot] {
        return try await request(endpoint: AppConfig.Endpoints.positions)
    }

    func fetchSignals() async throws -> [TradingSignal] {
        return try await request(endpoint: AppConfig.Endpoints.signals)
    }

    func fetchExecutions() async throws -> [ExecutionRecord] {
        return try await request(endpoint: AppConfig.Endpoints.executions)
    }

    func fetchSystemStatus() async throws -> SystemStatus {
        return try await request(endpoint: AppConfig.Endpoints.status)
    }

    func fetchHealth() async throws -> HealthCheckResponse {
        return try await request(endpoint: AppConfig.Endpoints.health)
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
