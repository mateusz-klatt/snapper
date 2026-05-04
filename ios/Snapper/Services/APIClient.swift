import Foundation

final class APIClient: Sendable {
    @MainActor static let shared = APIClient(session: .shared, authService: AuthService.shared)

    private let session: URLSession
    private let authService: AuthRefreshing

    init(session: URLSession, authService: AuthRefreshing) {
        self.session = session
        self.authService = authService
    }

    /// Percent-encode a value for safe inclusion as a URL path segment.
    ///
    /// Used at every path interpolation site (publicIds, opaque ids,
    /// venue names) so reserved characters do not silently produce
    /// malformed URLs. Falls back to the raw segment only on the
    /// (theoretical) case where the encoder fails.
    private static func encodePathSegment(_ value: String) -> String {
        return value.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? value
    }

    /// Build the trailing query suffix from name/value pairs.
    ///
    /// Uses ``URLComponents`` so reserved characters in values (e.g.
    /// ``+``, ``=``, ``/`` in opaque server-emitted cursors) get the
    /// proper percent-encoding that ``URLQueryItem`` performs. Returns
    /// the empty string when no items are supplied so callers can
    /// concatenate unconditionally.
    private static func querySuffix(_ items: [URLQueryItem]) -> String {
        guard !items.isEmpty else {
            return ""
        }
        var components = URLComponents()
        components.queryItems = items
        return components.percentEncodedQuery.map { "?\($0)" } ?? ""
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
            // ISO-8601 mirrors the response decoder set below at line ~63;
            // Codable Date fields (e.g. DeviceAlertPrefBody.mute_until)
            // would otherwise serialize as numeric Unix timestamps and
            // decode to a wrong instant on the backend.
            encoder.dateEncodingStrategy = .iso8601
            request.httpBody = try encoder.encode(body)
        }

        let (data, response) = try await session.data(for: request)

        guard let httpResponse = response as? HTTPURLResponse else {
            throw APIError.invalidResponse
        }

        // 401 retry path: refresh ws_token exactly once, then replay.
        // Second 401 → force logout so the UI routes to LoginView via
        // SnapperApp's isAuthenticated observer.
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

    /// Fetch operators the caller may act AS (multi-tenant).
    ///
    /// Powers ``EditDevicePrefView``'s scope picker — operator scope
    /// AND wallet scope (the latter requires an accompanying
    /// operator per the backend ``ck_device_alert_valid_scope``
    /// CHECK constraint). Backend filters by
    /// ``principal.operator_public_ids`` for non-admin roles; admins
    /// get every active operator.
    func fetchOperators() async throws -> [OperatorInfo] {
        let envelope: OperatorListResponse = try await request(
            endpoint: AppConfig.Endpoints.operators
        )
        return envelope.payload
    }

    /// Submit a manual order via the existing ``POST /api/orders``
    /// route (iOS-Position-Mutations).
    ///
    /// Used by ``PositionsView`` to fire reduce / close actions and
    /// by ``NewOrderSheet`` to enter fresh orders — the caller
    /// builds a ``CreateOrderCommand`` from ``EnvelopeMinter``.
    /// Backend routes into the existing trade-command machinery; the
    /// response carries the spawned ``ExecutionPlanData`` so the UI
    /// can confirm the submission succeeded.
    func createOrder(command: CreateOrderCommand) async throws -> ExecutionPlanResponse {
        return try await request(
            endpoint: AppConfig.Endpoints.orders,
            method: "POST",
            body: command
        )
    }

    /// Cancel a live execution plan via
    /// ``POST /api/orders/{plan_public_id}/cancel``. Backend
    /// transitions the plan to ``cancel_requested`` and emits a
    /// venue-facing cancel command for the active child order.
    func cancelOrder(planPublicId: String, reason: String? = nil) async throws -> ExecutionPlanResponse {
        let provenance = await MainActor.run {
            EnvelopeMinter.shared.next(.control)
        }
        let command = CancelOrderCommand(
            type: "cancel_order_command",
            sequenceId: provenance.sequenceId,
            publicId: provenance.publicId,
            timestamp: provenance.timestamp,
            sessionId: provenance.sessionId,
            topic: nil,
            payload: CancelOrderBody(reason: reason)
        )
        return try await request(
            endpoint: "\(AppConfig.Endpoints.orders)/\(Self.encodePathSegment(planPublicId))/cancel",
            method: "POST",
            body: command
        )
    }

    /// Attach a stop-loss / take-profit bracket to a live position
    /// cycle via ``POST /api/execution-plans`` (the bracket creation
    /// route lives on the execution-plans router per
    /// ``snapper.server.execution_plan_routes``).
    func createBracket(command: BracketCreateCommand) async throws -> ExecutionPlanResponse {
        return try await request(
            endpoint: AppConfig.Endpoints.executionPlans,
            method: "POST",
            body: command
        )
    }

    /// Attach a trailing-stop plan to a live position cycle via
    /// ``POST /api/trailing-stops``. Backend validates the cycle is
    /// open, the venue supports ``reduce_only``, and ``trailing_pct``
    /// falls within configured bounds before spawning the plan.
    func createTrailingStop(command: TrailingStopCreateCommand) async throws -> ExecutionPlanResponse {
        return try await request(
            endpoint: AppConfig.Endpoints.trailingStops,
            method: "POST",
            body: command
        )
    }

    /// Fetch capability-aware instrument rows for a venue via
    /// ``GET /api/exchanges/{exchange}/instruments/detail``. Each
    /// row carries ``can_trade``, ``can_market_data``, and
    /// ``instrument_kind`` so order-entry can disable rows the
    /// venue treats as market-data-only (TradFi mirrors).
    func fetchInstruments(exchange: String) async throws -> [InstrumentDetailData] {
        let envelope: InstrumentDetailListResponse = try await request(
            endpoint: "/exchanges/\(Self.encodePathSegment(exchange))/instruments/detail"
        )
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
        var queryItems: [URLQueryItem] = []
        if let limit {
            queryItems.append(URLQueryItem(name: "limit", value: "\(limit)"))
        }
        if let before, !before.isEmpty {
            queryItems.append(URLQueryItem(name: "before", value: before))
        }
        return try await request(endpoint: "\(AppConfig.Endpoints.alerts)\(Self.querySuffix(queryItems))")
    }

    /// Fetch a single alert by ``public_id`` (BE-1c — used for deep-linking).
    ///
    /// Args:
    ///   publicId: UUID7 of the alert event to load.
    ///
    /// Returns:
    ///   ``AlertEventResponse`` wrapping the one matching row.
    func fetchAlert(publicId: String) async throws -> AlertEventResponse {
        return try await request(endpoint: "\(AppConfig.Endpoints.alerts)/\(Self.encodePathSegment(publicId))")
    }

    /// Fetch active per-(alert_type, scope) prefs for the addressed
    /// device.
    ///
    /// Caller-scoped server-side: the backend filters by
    /// ``principal.user_public_id`` and 404s when the device is not
    /// owned. ``DeviceRegistrationService.currentDevicePublicId`` is
    /// the right input — never pass a foreign or stale id.
    ///
    /// Returns:
    ///   ``DeviceAlertPrefListResponse`` with zero-or-more prefs.
    func fetchDevicePrefs(devicePublicId: String) async throws -> DeviceAlertPrefListResponse {
        return try await request(
            endpoint: "\(AppConfig.Endpoints.devices)/\(Self.encodePathSegment(devicePublicId))/prefs"
        )
    }

    /// Upsert one per-(device, alert_type, scope) preference.
    ///
    /// Sends ``PATCH /api/devices/{public_id}/prefs`` with a full
    /// ``UpdateDevicePrefCommand`` envelope. The handler strips and
    /// re-mints provenance, so the iOS-side envelope fields are
    /// placeholder values (mirrors ``registerDevice``).
    func updateDevicePref(
        devicePublicId: String,
        command: UpdateDevicePrefCommand
    ) async throws -> DeviceAlertPrefResponse {
        return try await request(
            endpoint: "\(AppConfig.Endpoints.devices)/\(Self.encodePathSegment(devicePublicId))/prefs",
            method: "PATCH",
            body: command
        )
    }

    /// Fetch the caller's user-level alert default fallbacks.
    ///
    /// Empty list is the legitimate "no overrides" state — the
    /// alert-routing layer falls through to the in-app defaults.
    func fetchAlertDefaults() async throws -> UserAlertDefaultListResponse {
        return try await request(endpoint: AppConfig.Endpoints.alertDefaults)
    }

    /// Upsert one ``(user, alert_type)`` fallback default.
    ///
    /// Sends ``PATCH /api/alert_defaults``. ``user_public_id`` is
    /// sourced server-side from the principal; the
    /// ``UserAlertDefaultBody`` deliberately omits the field.
    func updateAlertDefault(
        command: UpdateUserAlertDefaultCommand
    ) async throws -> UserAlertDefaultResponse {
        return try await request(
            endpoint: AppConfig.Endpoints.alertDefaults,
            method: "PATCH",
            body: command
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
