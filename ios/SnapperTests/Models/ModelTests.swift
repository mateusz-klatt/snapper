import XCTest
@testable import Snapper

final class ModelTests: XCTestCase {

    func testUserProfileDecoding() throws {
        let json = """
        {
            "id": "1",
            "username": "testuser",
            "email": "test@example.com",
            "role": "viewer",
            "is_active": true,
            "created_at": "2025-01-01T00:00:00Z",
            "last_login": null
        }
        """.data(using: .utf8)!

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601

        let user = try decoder.decode(UserProfile.self, from: json)

        XCTAssertEqual(user.id, "1")
        XCTAssertEqual(user.username, "testuser")
        XCTAssertEqual(user.email, "test@example.com")
        XCTAssertEqual(user.role, .viewer)
        XCTAssertEqual(user.isActive, true)
    }

    func testOrderStatusDecoding() throws {
        let json = """
        {
            "id": 123,
            "instrument": "BTCUSD",
            "exchange": "kraken",
            "client_order_id": "client-123",
            "exchange_order_id": "exchange-456",
            "created_at": "2025-11-22T10:00:00Z",
            "updated_at": "2025-11-22T10:00:00Z",
            "side": "buy",
            "type": "limit",
            "price": 50000.0,
            "size": 0.5,
            "status": "open"
        }
        """.data(using: .utf8)!

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601

        let order = try decoder.decode(OrderStatus.self, from: json)

        XCTAssertEqual(order.id, 123)
        XCTAssertEqual(order.instrument, "BTCUSD")
        XCTAssertEqual(order.side, "buy")
        XCTAssertEqual(order.type, "limit")
        XCTAssertEqual(order.size, 0.5)
        XCTAssertEqual(order.price, 50000.0)
        XCTAssertEqual(order.status, "open")
    }

    func testLoginResponseDecoding() throws {
        let json = """
        {
            "message": "Login successful",
            "expires_in": 900,
            "user": {
                "id": "1",
                "username": "testuser",
                "email": "test@example.com",
                "role": "admin",
                "is_active": true,
                "created_at": "2025-01-01T00:00:00Z",
                "last_login": "2025-11-22T10:00:00Z"
            }
        }
        """.data(using: .utf8)!

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601

        let response = try decoder.decode(LoginResponse.self, from: json)

        XCTAssertEqual(response.message, "Login successful")
        XCTAssertEqual(response.expiresIn, 900)
        XCTAssertEqual(response.user.username, "testuser")
        XCTAssertEqual(response.user.role, .admin)
    }

    func testBarEnvelopeDecoding() throws {
        let json = """
        {
            "type": "bar",
            "timestamp": "2025-11-22T10:00:00Z",
            "meta": null,
            "instrument": "BTCUSD",
            "timeframe": "1m",
            "open": 50000.0,
            "high": 50100.0,
            "low": 49900.0,
            "close": 50050.0,
            "volume": 100.5,
            "vwap": 50025.0,
            "trades": 150,
            "exchange": "kraken"
        }
        """.data(using: .utf8)!

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601

        let bar = try decoder.decode(BarEnvelope.self, from: json)

        XCTAssertEqual(bar.type, "bar")
        XCTAssertEqual(bar.instrument, "BTCUSD")
        XCTAssertEqual(bar.timeframe, "1m")
        XCTAssertEqual(bar.open, 50000.0)
        XCTAssertEqual(bar.high, 50100.0)
        XCTAssertEqual(bar.low, 49900.0)
        XCTAssertEqual(bar.close, 50050.0)
        XCTAssertEqual(bar.volume, 100.5)
        XCTAssertEqual(bar.exchange, "kraken")
    }

    func testRefreshResponseDecoding() throws {
        let json = """
        {
            "message": "Token refreshed",
            "ws_token": "ws_token_value",
            "ws_token_exp": "2025-11-22T11:00:00Z",
            "csrf_token": "csrf_token_value",
            "user": {
                "id": "1",
                "username": "testuser",
                "email": "test@example.com",
                "role": "viewer",
                "is_active": true,
                "created_at": "2025-01-01T00:00:00Z",
                "last_login": null
            }
        }
        """.data(using: .utf8)!

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601

        let response = try decoder.decode(RefreshResponse.self, from: json)

        XCTAssertEqual(response.message, "Token refreshed")
        XCTAssertEqual(response.wsToken, "ws_token_value")
        XCTAssertEqual(response.csrfToken, "csrf_token_value")
        XCTAssertEqual(response.user.username, "testuser")
    }
}
