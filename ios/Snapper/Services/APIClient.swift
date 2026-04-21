import Foundation

final class APIClient: Sendable {
    @MainActor static let shared = APIClient(session: .shared)

    private let session: URLSession

    init(session: URLSession) {
        self.session = session
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
        let envelope: OrderListResponse = try await request(endpoint: AppConfig.Endpoints.orders)
        return envelope.payload
    }

    func fetchPositions() async throws -> [PositionSnapshot] {
        let envelope: PositionListResponse = try await request(endpoint: AppConfig.Endpoints.positions)
        return envelope.payload
    }

    func fetchSignals() async throws -> [TradingSignal] {
        let envelope: SignalListResponse = try await request(endpoint: AppConfig.Endpoints.signals)
        return envelope.payload
    }

    func fetchExecutions() async throws -> [ExecutionRecord] {
        let envelope: ExecutionListResponse = try await request(endpoint: AppConfig.Endpoints.executions)
        return envelope.payload
    }

    func fetchSystemStatus() async throws -> SystemStatus {
        let envelope: SystemStatusResponse = try await request(endpoint: AppConfig.Endpoints.status)
        return envelope.payload
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
