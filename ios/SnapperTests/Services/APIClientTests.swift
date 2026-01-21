import XCTest
@testable import Snapper

final class APIClientTests: XCTestCase {

    func testCreateOrderRequestEncoding() throws {
        let request = CreateOrderRequest(
            symbol: "BTCUSD",
            side: "buy",
            quantity: 1.5,
            orderType: "limit",
            price: 50000.0
        )

        let encoder = JSONEncoder()
        encoder.keyEncodingStrategy = .convertToSnakeCase
        let data = try encoder.encode(request)

        let json = try JSONSerialization.jsonObject(with: data) as! [String: Any]

        XCTAssertEqual(json["symbol"] as? String, "BTCUSD")
        XCTAssertEqual(json["side"] as? String, "buy")
        XCTAssertEqual(json["quantity"] as? Double, 1.5)
        XCTAssertEqual(json["order_type"] as? String, "limit")
        XCTAssertEqual(json["price"] as? Double, 50000.0)
    }

    func testCreateOrderRequestEncodingMarketOrder() throws {
        let request = CreateOrderRequest(
            symbol: "ETHUSD",
            side: "sell",
            quantity: 10.0,
            orderType: "market",
            price: nil
        )

        let encoder = JSONEncoder()
        encoder.keyEncodingStrategy = .convertToSnakeCase
        let data = try encoder.encode(request)

        let json = try JSONSerialization.jsonObject(with: data) as! [String: Any]

        XCTAssertEqual(json["symbol"] as? String, "ETHUSD")
        XCTAssertEqual(json["side"] as? String, "sell")
        XCTAssertEqual(json["quantity"] as? Double, 10.0)
        XCTAssertEqual(json["order_type"] as? String, "market")
        XCTAssertNil(json["price"])
    }

    func testAPIErrorDescription() {
        XCTAssertEqual(APIError.invalidURL.errorDescription, "Invalid URL")
        XCTAssertEqual(APIError.invalidResponse.errorDescription, "Invalid response from server")
        XCTAssertEqual(APIError.httpError(404).errorDescription, "HTTP error: 404")
        XCTAssertEqual(APIError.serverError("Test error").errorDescription, "Test error")
        XCTAssertEqual(APIError.decodingError.errorDescription, "Failed to decode response")
    }
}
