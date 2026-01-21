import XCTest
@testable import Snapper

final class APIClientNetworkTests: XCTestCase {

    var apiClient: APIClient!
    var mockSession: URLSession!
    var mockAuthService: AuthService!

    override func setUp() {
        super.setUp()

        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [MockURLProtocol.self]
        mockSession = URLSession(configuration: configuration)

        mockAuthService = AuthService(session: mockSession)

        apiClient = APIClient(session: mockSession, authService: mockAuthService)
    }

    override func tearDown() {
        apiClient = nil
        mockSession = nil
        mockAuthService = nil
        MockURLProtocol.requestHandler = nil
        super.tearDown()
    }

    func testFetchOrdersSuccess() async throws {

        MockURLProtocol.requestHandler = { request in
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 200,
                httpVersion: nil,
                headerFields: ["Content-Type": "application/json"]
            )!

            let json: [[String: Any]] = [
                [
                    "id": "order-1",
                    "symbol": "BTCUSD",
                    "side": "buy",
                    "order_type": "limit",
                    "quantity": 1.0,
                    "price": 50000.0,
                    "status": "open",
                    "filled_quantity": 0.0,
                    "average_price": NSNull(),
                    "created_at": "2025-11-22T10:00:00Z",
                    "updated_at": "2025-11-22T10:00:00Z"
                ]
            ]
            let data = try! JSONSerialization.data(withJSONObject: json)

            return (response, data)
        }

        let orders = try await apiClient.fetchOrders()

        XCTAssertEqual(orders.count, 1)
        XCTAssertEqual(orders[0].id, "order-1")
        XCTAssertEqual(orders[0].symbol, "BTCUSD")
    }

    func testFetchOrders401Error() async throws {

        MockURLProtocol.requestHandler = { request in
            MockURLProtocol.errorResponse(statusCode: 401, message: "Token expired")
        }

        do {
            _ = try await apiClient.fetchOrders()
            XCTFail("Should throw error")
        } catch let error as APIError {
            if case .serverError(let message) = error {
                XCTAssertEqual(message, "Token expired")
            } else {
                XCTFail("Wrong error type: \(error)")
            }
        }
    }

    func testFetchOrdersNetworkError() async throws {

        MockURLProtocol.requestHandler = { request in
            throw MockURLProtocol.networkError()
        }

        do {
            _ = try await apiClient.fetchOrders()
            XCTFail("Should throw error")
        } catch {

            XCTAssertNotNil(error)
        }
    }

    func testCreateOrderSuccess() async throws {

        MockURLProtocol.requestHandler = { request in
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 200,
                httpVersion: nil,
                headerFields: ["Content-Type": "application/json"]
            )!

            let json: [String: Any] = [
                "id": "order-123",
                "symbol": "BTCUSD",
                "side": "buy",
                "order_type": "limit",
                "quantity": 1.5,
                "price": 50000.0,
                "status": "pending",
                "filled_quantity": 0.0,
                "average_price": NSNull(),
                "created_at": "2025-11-22T10:00:00Z",
                "updated_at": "2025-11-22T10:00:00Z"
            ]
            let data = try! JSONSerialization.data(withJSONObject: json)

            return (response, data)
        }

        let orderRequest = CreateOrderRequest(
            symbol: "BTCUSD",
            side: "buy",
            quantity: 1.5,
            orderType: "limit",
            price: 50000.0
        )
        let order = try await apiClient.createOrder(orderRequest)

        XCTAssertEqual(order.id, "order-123")
        XCTAssertEqual(order.symbol, "BTCUSD")
        XCTAssertEqual(order.quantity, 1.5)
        XCTAssertEqual(order.status, "pending")
    }

    func testCreateOrderValidation() async throws {

        MockURLProtocol.requestHandler = { request in
            MockURLProtocol.errorResponse(
                statusCode: 422,
                message: "Invalid quantity"
            )
        }

        do {
            let orderRequest = CreateOrderRequest(
                symbol: "BTCUSD",
                side: "buy",
                quantity: -1.0,
                orderType: "limit",
                price: 50000.0
            )
            _ = try await apiClient.createOrder(orderRequest)
            XCTFail("Should throw error")
        } catch let error as APIError {
            if case .serverError(let message) = error {
                XCTAssertEqual(message, "Invalid quantity")
            } else {
                XCTFail("Wrong error type")
            }
        }
    }

    func testFetchPortfolioSuccess() async throws {

        MockURLProtocol.requestHandler = { request in
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 200,
                httpVersion: nil,
                headerFields: ["Content-Type": "application/json"]
            )!

            let json: [String: Any] = [
                "total_value": 100000.0,
                "cash_balance": 50000.0,
                "positions_value": 50000.0,
                "unrealized_pnl": 5000.0,
                "realized_pnl": 3000.0,
                "today_pnl": 1500.0,
                "today_pnl_percent": 1.5
            ]
            let data = try! JSONSerialization.data(withJSONObject: json)

            return (response, data)
        }

        let portfolio = try await apiClient.fetchPortfolio()

        XCTAssertEqual(portfolio.totalValue, 100000.0)
        XCTAssertEqual(portfolio.cashBalance, 50000.0)
        XCTAssertEqual(portfolio.unrealizedPnL, 5000.0)
    }

    func testRequestIncludesAuthHeader() async throws {

        var requestCount = 0
        MockURLProtocol.requestHandler = { request in
            requestCount += 1

            if requestCount == 1 {

                let json: [String: Any] = [
                    "access_token": "test_token_123",
                    "refresh_token": "refresh",
                    "token_type": "bearer"
                ]
                return MockURLProtocol.jsonResponse(statusCode: 200, json: json)
            } else if requestCount == 2 {

                let json: [String: Any] = [
                    "id": 1,
                    "username": "test",
                    "email": "test@example.com",
                    "is_active": true,
                    "is_admin": false
                ]
                return MockURLProtocol.jsonResponse(statusCode: 200, json: json)
            } else {

                return MockURLProtocol.jsonResponse(statusCode: 200, json: ["items": []])
            }
        }

        await mockAuthService.login(username: "test", password: "test")

        requestCount = 0
        var capturedRequest: URLRequest?
        MockURLProtocol.requestHandler = { request in
            capturedRequest = request
            return MockURLProtocol.jsonResponse(statusCode: 200, json: ["items": []])
        }

        do {
            let _: [Order] = try await apiClient.fetchOrders()
        } catch {

        }

        XCTAssertNotNil(capturedRequest)
        let authHeader = capturedRequest?.value(forHTTPHeaderField: "Authorization")
        XCTAssertNotNil(authHeader)
        XCTAssertTrue(authHeader?.starts(with: "Bearer ") ?? false)
    }
}
