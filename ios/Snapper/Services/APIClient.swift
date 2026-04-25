import Foundation

final class APIClient: Sendable {
    @MainActor static let shared = APIClient(session: .shared, authService: AuthService.shared)

    private let session: URLSession
    private let authService: AuthRefreshing

    init(session: URLSession, authService: AuthRefreshing) {
        self.session = session
        self.authService = authService
    }

    private func request<T: Decodable>(
        endpoint: String,
        method: String = "GET",
        body: Encodable? = nil,
        isRetry: Bool = false
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

        // 401 retry path: refresh ws_token exactly once, then replay.
        // Second 401 → force logout so the UI routes to LoginView via
        // SnapperApp's isAuthenticated observer (see plan §D7).
        if httpResponse.statusCode == 401 {
            if isRetry {
                await authService.logout()
                throw APIError.httpError(401)
            }
            guard await authService.fetchFreshWsToken() != nil else {
                await authService.logout()
                throw APIError.httpError(401)
            }
            return try await self.request(endpoint: endpoint, method: method, body: body, isRetry: true)
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

    /// Fetch wallets visible to the current user (iOS-2 / Phase A
    /// backend ``GET /api/wallets``).
    ///
    /// Powers ``WalletPicker``. Returns the full list; the picker
    /// caches it on ``AppState.availableWallets`` and surfaces every
    /// row in the menu — backend already filters to the wallets the
    /// user has access to via SCD2 grants.
    func fetchWallets() async throws -> [WalletInfo] {
        let envelope: WalletListResponse = try await request(endpoint: AppConfig.Endpoints.wallets)
        return envelope.payload
    }

    func fetchSystemStatus() async throws -> SystemStatus {
        let envelope: SystemStatusResponse = try await request(endpoint: AppConfig.Endpoints.status)
        return envelope.payload
    }

    func fetchHealth() async throws -> HealthCheckResponse {
        return try await request(endpoint: AppConfig.Endpoints.health)
    }

    /// Register an APNs device token with the backend (BE-1c / iOS-1).
    ///
    /// Sends ``POST /api/devices`` with a full ``RegisterDeviceCommand``
    /// envelope (the handler strips everything but the ``payload`` and
    /// mints its own provenance; the iOS-side envelope fields are
    /// placeholder values). The 401 retry path in ``request`` handles
    /// an expired ws_token transparently.
    ///
    /// Args:
    ///   command: Envelope-wrapped ``RegisterDeviceBody`` minted by
    ///     ``DeviceRegistrationService``.
    ///
    /// Returns:
    ///   ``NotificationDeviceResponse`` whose ``payload`` carries the
    ///   server-assigned ``public_id`` that identifies this device
    ///   row on subsequent `DELETE` / `PATCH` calls.
    func registerDevice(command: RegisterDeviceCommand) async throws -> NotificationDeviceResponse {
        return try await request(
            endpoint: AppConfig.Endpoints.devices,
            method: "POST",
            body: command
        )
    }

    /// Fetch the authenticated user's recent alert history (BE-1c).
    ///
    /// Powers the Alerts tab (iOS-4). ``limit`` caps page size; the
    /// backend enforces its own upper bound. ``before`` is the opaque
    /// cursor from the previous page's ``next_cursor`` — pass ``nil``
    /// for the first page.
    func fetchAlertHistory(limit: Int? = nil, before: String? = nil) async throws -> AlertHistoryResponse {
        var query: [String] = []
        if let limit {
            query.append("limit=\(limit)")
        }
        if let before, !before.isEmpty {
            query.append("before=\(before)")
        }
        let suffix = query.isEmpty ? "" : "?\(query.joined(separator: "&"))"
        return try await request(endpoint: "\(AppConfig.Endpoints.alerts)\(suffix)")
    }

    /// Fetch a single alert by ``public_id`` (BE-1c — used for deep-linking).
    ///
    /// Args:
    ///   publicId: UUID7 of the alert event to load.
    ///
    /// Returns:
    ///   ``AlertEventResponse`` wrapping the one matching row.
    func fetchAlert(publicId: String) async throws -> AlertEventResponse {
        return try await request(endpoint: "\(AppConfig.Endpoints.alerts)/\(publicId)")
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
