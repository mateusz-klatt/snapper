import XCTest
@testable import Snapper

final class ModelTests: XCTestCase {

    func testUserProfileDecoding() throws {
        let json = """
        {
            "sequence_id": 1,
            "public_id": "01961234-5678-7000-8000-000000000001",
            "timestamp": "2025-01-01T00:00:00Z",
            "session_id": "session-1",
            "username": "testuser",
            "email": "test@example.com",
            "role": "viewer",
            "is_active": true,
            "created_at": "2025-01-01T00:00:00Z"
        }
        """.data(using: .utf8)!

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601

        let user = try decoder.decode(UserProfile.self, from: json)

        XCTAssertEqual(user.username, "testuser")
        XCTAssertEqual(user.email, "test@example.com")
        XCTAssertEqual(user.role, .viewer)
        XCTAssertEqual(user.isActive, true)
    }

    func testOrderStatusDecoding() throws {
        let json = """
        {
            "sequence_id": 1,
            "public_id": "01961234-5678-7000-8000-000000000001",
            "timestamp": "2025-11-22T10:00:00Z",
            "session_id": "session-1",
            "instrument": "BTCUSD",
            "exchange": "kraken",
            "client_order_id": "client-123",
            "exchange_order_id": "exchange-456",
            "created_at": "2025-11-22T10:00:00Z",
            "updated_at": "2025-11-22T10:00:00Z",
            "side": "buy",
            "order_type": "limit",
            "price": 50000.0,
            "size": 0.5,
            "filled_size": 0.0,
            "status": "open"
        }
        """.data(using: .utf8)!

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601

        let order = try decoder.decode(OrderStatus.self, from: json)

        XCTAssertEqual(order.publicId, "01961234-5678-7000-8000-000000000001")
        XCTAssertEqual(order.instrument, "BTCUSD")
        XCTAssertEqual(order.side, "buy")
        XCTAssertEqual(order.orderType, "limit")
        XCTAssertEqual(order.size, 0.5)
        XCTAssertEqual(order.price, 50000.0)
        XCTAssertEqual(order.status, "open")
    }

    func testLoginResponseDecoding() throws {
        let json = """
        {
            "sequence_id": 1,
            "public_id": "01961234-5678-7000-8000-000000000001",
            "timestamp": "2025-01-01T00:00:00Z",
            "session_id": "session-1",
            "payload": {
                "sequence_id": 1,
                "public_id": "01961234-5678-7000-8000-000000000002",
                "timestamp": "2025-01-01T00:00:00Z",
                "session_id": "session-1",
                "message": "Login successful",
                "expires_in": 900,
                "user": {
                    "sequence_id": 1,
                    "public_id": "01961234-5678-7000-8000-000000000003",
                    "timestamp": "2025-01-01T00:00:00Z",
                    "session_id": "session-1",
                    "username": "testuser",
                    "email": "test@example.com",
                    "role": "admin",
                    "is_active": true,
                    "created_at": "2025-01-01T00:00:00Z"
                }
            }
        }
        """.data(using: .utf8)!

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601

        let response = try decoder.decode(LoginResponse.self, from: json)

        XCTAssertEqual(response.payload.message, "Login successful")
        XCTAssertEqual(response.payload.expiresIn, 900)
        XCTAssertEqual(response.payload.user.username, "testuser")
        XCTAssertEqual(response.payload.user.role, .admin)
    }

    func testCandleEnvelopeDecoding() throws {
        let json = """
        {
            "type": "candle",
            "sequence_id": 1,
            "public_id": "01961234-5678-7000-8000-000000000010",
            "timestamp": "2025-11-22T10:00:00Z",
            "session_id": "session-1",
            "instrument": "BTCUSD",
            "timeframe": "1m",
            "open_at": "2025-11-22T10:00:00Z",
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

        let candle = try decoder.decode(CandleEnvelope.self, from: json)

        XCTAssertEqual(candle.type, "candle")
        XCTAssertEqual(candle.instrument, "BTCUSD")
        XCTAssertEqual(candle.timeframe, "1m")
        XCTAssertEqual(candle.open, 50000.0)
        XCTAssertEqual(candle.high, 50100.0)
        XCTAssertEqual(candle.low, 49900.0)
        XCTAssertEqual(candle.close, 50050.0)
        XCTAssertEqual(candle.volume, 100.5)
        XCTAssertEqual(candle.exchange, "kraken")
    }

    func testRefreshResponseDecoding() throws {
        let json = """
        {
            "sequence_id": 1,
            "public_id": "01961234-5678-7000-8000-000000000020",
            "timestamp": "2025-11-22T11:00:00Z",
            "session_id": "session-1",
            "payload": {
                "sequence_id": 1,
                "public_id": "01961234-5678-7000-8000-000000000021",
                "timestamp": "2025-11-22T11:00:00Z",
                "session_id": "session-1",
                "message": "Token refreshed",
                "ws_token": "ws_token_value",
                "ws_token_exp": "2025-11-22T11:00:00Z",
                "csrf_token": "csrf_token_value",
                "user": {
                    "sequence_id": 1,
                    "public_id": "01961234-5678-7000-8000-000000000022",
                    "timestamp": "2025-01-01T00:00:00Z",
                    "session_id": "session-1",
                    "username": "testuser",
                    "email": "test@example.com",
                    "role": "viewer",
                    "is_active": true,
                    "created_at": "2025-01-01T00:00:00Z"
                }
            }
        }
        """.data(using: .utf8)!

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601

        let response = try decoder.decode(RefreshResponse.self, from: json)

        XCTAssertEqual(response.payload.message, "Token refreshed")
        XCTAssertEqual(response.payload.wsToken, "ws_token_value")
        XCTAssertEqual(response.payload.csrfToken, "csrf_token_value")
        XCTAssertEqual(response.payload.user.username, "testuser")
    }
}
