import XCTest
@testable import Snapper

@MainActor
final class APIClientNetworkTests: XCTestCase {

    var apiClient: APIClient!
    var mockSession: URLSession!

    override func setUp() {
        super.setUp()

        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [MockURLProtocol.self]
        mockSession = URLSession(configuration: configuration)

        apiClient = APIClient(session: mockSession)
    }

    override func tearDown() {
        apiClient = nil
        mockSession = nil
        MockURLProtocol.requestHandler = nil
        super.tearDown()
    }

    func testFetchOrdersSuccess() async throws {

        MockURLProtocol.requestHandler = { request in
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 200,
                httpVersion: nil,
                headerFields: [AppConfig.HTTPHeader.contentType: AppConfig.ContentType.json]
            )!

            let json: [[String: Any]] = [
                [
                    "id": 1,
                    "instrument": "BTCUSD",
                    "exchange": "kraken",
                    "side": "buy",
                    "type": "limit",
                    "size": 1.0,
                    "status": "open",
                    "created_at": "2025-11-22T10:00:00Z"
                ]
            ]
            let data = try JSONSerialization.data(withJSONObject: json)

            return (response, data)
        }

        let orders = try await apiClient.fetchOrders()

        XCTAssertEqual(orders.count, 1)
        XCTAssertEqual(orders[0].id, 1)
        XCTAssertEqual(orders[0].instrument, "BTCUSD")
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

    func testFetchPositionsSuccess() async throws {

        MockURLProtocol.requestHandler = { request in
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 200,
                httpVersion: nil,
                headerFields: [AppConfig.HTTPHeader.contentType: AppConfig.ContentType.json]
            )!

            let json: [[String: Any]] = [
                [
                    "id": 1,
                    "instrument": "BTCUSD",
                    "exchange": "kraken",
                    "quantity": 1.5,
                    "average_price": 50000.0,
                    "unrealized_pnl": 500.0,
                    "realized_pnl": 0.0,
                    "updated_at": "2025-11-22T10:00:00Z"
                ]
            ]
            let data = try JSONSerialization.data(withJSONObject: json)

            return (response, data)
        }

        let positions = try await apiClient.fetchPositions()

        XCTAssertEqual(positions.count, 1)
        XCTAssertEqual(positions[0].instrument, "BTCUSD")
        XCTAssertEqual(positions[0].quantity, 1.5)
        XCTAssertEqual(positions[0].averagePrice, 50000.0)
    }

    func testFetchSignalsSuccess() async throws {

        MockURLProtocol.requestHandler = { request in
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 200,
                httpVersion: nil,
                headerFields: [AppConfig.HTTPHeader.contentType: AppConfig.ContentType.json]
            )!

            let json: [[String: Any]] = [
                [
                    "id": 1,
                    "instrument": "ETHUSD",
                    "exchange": "kraken",
                    "side": "buy",
                    "strength": 0.8,
                    "reason": "Strategy triggered",
                    "timestamp": "2025-11-22T10:00:00Z"
                ]
            ]
            let data = try JSONSerialization.data(withJSONObject: json)

            return (response, data)
        }

        let signals = try await apiClient.fetchSignals()

        XCTAssertEqual(signals.count, 1)
        XCTAssertEqual(signals[0].id, 1)
        XCTAssertEqual(signals[0].instrument, "ETHUSD")
    }

    func testRequestSetsCorrectContentType() async throws {

        var capturedRequest: URLRequest?
        MockURLProtocol.requestHandler = { request in
            capturedRequest = request

            let json: [[String: Any]] = [
                [
                    "id": 1,
                    "instrument": "BTCUSD",
                    "exchange": "kraken",
                    "side": "buy",
                    "type": "limit",
                    "size": 1.0,
                    "status": "open",
                    "created_at": "2025-11-22T10:00:00Z"
                ]
            ]
            let data = try JSONSerialization.data(withJSONObject: json)

            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 200,
                httpVersion: nil,
                headerFields: [AppConfig.HTTPHeader.contentType: AppConfig.ContentType.json]
            )!
            return (response, data)
        }

        _ = try await apiClient.fetchOrders()

        XCTAssertNotNil(capturedRequest)
        let contentType = capturedRequest?.value(forHTTPHeaderField: AppConfig.HTTPHeader.contentType)
        XCTAssertEqual(contentType, AppConfig.ContentType.json)
    }
}
