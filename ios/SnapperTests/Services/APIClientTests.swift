import XCTest
@testable import Snapper

@MainActor
final class APIClientTests: XCTestCase {

    private var mockSession: URLSession!

    override func setUp() {
        super.setUp()
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [MockURLProtocol.self]
        mockSession = URLSession(configuration: config)
    }

    override func tearDown() {
        mockSession = nil
        MockURLProtocol.requestHandler = nil
        super.tearDown()
    }

    func testAPIErrorDescription() {
        XCTAssertEqual(APIError.invalidURL.errorDescription, "Invalid URL")
        XCTAssertEqual(APIError.invalidResponse.errorDescription, "Invalid response from server")
        XCTAssertEqual(APIError.httpError(404).errorDescription, "HTTP error: 404")
        XCTAssertEqual(APIError.serverError("Test error").errorDescription, "Test error")
        XCTAssertEqual(APIError.decodingError.errorDescription, "Failed to decode response")
    }

    /// First 401 triggers fresh-token fetch; second call returns 200.
    /// Request must execute twice; final 200 payload decodes; no logout.
    func test401TriggersRefreshAndRetry() async throws {
        let fakeAuth = FakeAuthService(nextToken: "new-token")
        let client = APIClient(session: mockSession, authService: fakeAuth)

        var callCount = 0
        MockURLProtocol.requestHandler = { request in
            callCount += 1
            if callCount == 1 {
                return MockURLProtocol.errorResponse(statusCode: 401, message: "Token expired")
            }
            return MockURLProtocol.jsonResponse(statusCode: 200, json: Self.orderListEnvelope())
        }

        let orders = try await client.fetchOrders()

        XCTAssertEqual(callCount, 2, "request must be retried exactly once after 401")
        XCTAssertEqual(orders.count, 1)
        let fetchCalls = await fakeAuth.fetchCalls
        let logoutCalls = await fakeAuth.logoutCalls
        XCTAssertEqual(fetchCalls, 1, "refresh should be requested once between 401 and retry")
        XCTAssertEqual(logoutCalls, 0, "successful retry must not trigger logout")
    }

    /// Second 401 after refresh → force logout + surface APIError.httpError(401).
    func test401TwiceTriggersLogout() async {
        let fakeAuth = FakeAuthService(nextToken: "fresh")
        let client = APIClient(session: mockSession, authService: fakeAuth)

        MockURLProtocol.requestHandler = { _ in
            return MockURLProtocol.errorResponse(statusCode: 401, message: "Token still bad")
        }

        do {
            _ = try await client.fetchOrders()
            XCTFail("expected APIError.httpError(401)")
        } catch let error as APIError {
            if case .httpError(let code) = error {
                XCTAssertEqual(code, 401)
            } else {
                XCTFail("wrong error: \(error)")
            }
        } catch {
            XCTFail("unexpected error type: \(error)")
        }

        let logoutCalls = await fakeAuth.logoutCalls
        XCTAssertEqual(logoutCalls, 1, "logout must fire after second 401")
    }

    /// Refresh returns nil (nextToken=nil) → immediate logout, no retry.
    func testRefreshNilTriggersLogoutWithoutRetry() async {
        let fakeAuth = FakeAuthService(nextToken: nil)
        let client = APIClient(session: mockSession, authService: fakeAuth)

        var callCount = 0
        MockURLProtocol.requestHandler = { _ in
            callCount += 1
            return MockURLProtocol.errorResponse(statusCode: 401, message: "Token expired")
        }

        do {
            _ = try await client.fetchOrders()
            XCTFail("expected APIError.httpError(401)")
        } catch let error as APIError {
            if case .httpError = error {} else { XCTFail("wrong error: \(error)") }
        } catch {
            XCTFail("unexpected error type: \(error)")
        }

        XCTAssertEqual(callCount, 1, "must NOT retry when refresh returns nil — infinite loop defence")
        let logoutCalls = await fakeAuth.logoutCalls
        XCTAssertEqual(logoutCalls, 1, "logout must fire when refresh returns nil")
    }

    /// Asserts that ``fetchWallets`` decodes the canonical
    /// ``WalletListResponse`` envelope and returns the inner
    /// ``WalletInfo`` payload preserving ``label`` + ``isPaper``.
    func testFetchWalletsParsesResponse() async throws {
        let fakeAuth = FakeAuthService(nextToken: "token")
        let client = APIClient(session: mockSession, authService: fakeAuth)

        MockURLProtocol.requestHandler = { _ in
            return MockURLProtocol.jsonResponse(statusCode: 200, json: Self.walletListEnvelope())
        }

        let wallets = try await client.fetchWallets()

        XCTAssertEqual(wallets.count, 2)
        XCTAssertEqual(wallets[0].label, "default")
        XCTAssertEqual(wallets[0].isPaper, false)
        XCTAssertEqual(wallets[1].label, "default-paper")
        XCTAssertEqual(wallets[1].isPaper, true)
    }

    private static func walletListEnvelope() -> [String: Any] {
        return [
            "sequence_id": 1,
            "public_id": "01961234-5678-7000-8000-000000000a00",
            "timestamp": "2025-11-22T10:00:00Z",
            "session_id": "session-wallets",
            "count": 2,
            "payload": [
                [
                    "sequence_id": 1,
                    "public_id": "01961234-5678-7000-8000-000000000a01",
                    "timestamp": "2025-11-22T10:00:00Z",
                    "session_id": "session-wallets",
                    "label": "default",
                    "is_paper": false
                ],
                [
                    "sequence_id": 2,
                    "public_id": "01961234-5678-7000-8000-000000000a02",
                    "timestamp": "2025-11-22T10:00:00Z",
                    "session_id": "session-wallets",
                    "label": "default-paper",
                    "is_paper": true
                ]
            ]
        ]
    }

    private static func orderListEnvelope() -> [String: Any] {
        return [
            "sequence_id": 1,
            "public_id": "01961234-5678-7000-8000-000000000900",
            "timestamp": "2025-11-22T10:00:00Z",
            "session_id": "session-retry",
            "count": 1,
            "payload": [
                [
                    "sequence_id": 1,
                    "public_id": "01961234-5678-7000-8000-000000000901",
                    "timestamp": "2025-11-22T10:00:00Z",
                    "session_id": "session-retry",
                    "instrument": "BTCUSD",
                    "exchange": "kraken",
                    "client_order_id": "cli-retry",
                    "side": "buy",
                    "order_type": "limit",
                    "size": 1.0,
                    "filled_size": 0.0,
                    "status": "open",
                    "created_at": "2025-11-22T10:00:00Z"
                ]
            ]
        ]
    }
}
