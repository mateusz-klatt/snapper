import XCTest
@testable import Snapper

final class ModelTests: XCTestCase {

    func testUserDecoding() throws {
        let json = """
        {
            "id": 1,
            "username": "testuser",
            "email": "test@example.com",
            "is_active": true,
            "is_admin": false
        }
        """.data(using: .utf8)!

        let decoder = JSONDecoder()

        let user = try decoder.decode(User.self, from: json)

        XCTAssertEqual(user.id, 1)
        XCTAssertEqual(user.username, "testuser")
        XCTAssertEqual(user.email, "test@example.com")
        XCTAssertTrue(user.isActive)
        XCTAssertFalse(user.isAdmin)
    }

    func testOrderDecoding() throws {
        let json = """
        {
            "id": "order-123",
            "symbol": "BTCUSD",
            "side": "buy",
            "order_type": "limit",
            "quantity": 0.5,
            "price": 50000.0,
            "status": "open",
            "filled_quantity": 0.0,
            "average_price": null,
            "created_at": "2025-11-22T10:00:00Z",
            "updated_at": "2025-11-22T10:00:00Z"
        }
        """.data(using: .utf8)!

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601

        let order = try decoder.decode(Order.self, from: json)

        XCTAssertEqual(order.id, "order-123")
        XCTAssertEqual(order.symbol, "BTCUSD")
        XCTAssertEqual(order.side, "buy")
        XCTAssertEqual(order.orderType, "limit")
        XCTAssertEqual(order.quantity, 0.5)
        XCTAssertEqual(order.price, 50000.0)
        XCTAssertEqual(order.status, "open")
        XCTAssertEqual(order.filledQuantity, 0.0)
        XCTAssertNil(order.averagePrice)
    }

    func testOrderStatusColor() {
        let filledOrder = Order(
            id: "1",
            symbol: "BTCUSD",
            side: "buy",
            orderType: "market",
            quantity: 1.0,
            price: nil,
            status: "filled",
            filledQuantity: 1.0,
            averagePrice: 50000.0,
            createdAt: Date(),
            updatedAt: Date()
        )
        XCTAssertEqual(filledOrder.statusColor, "green")

        let cancelledOrder = Order(
            id: "2",
            symbol: "BTCUSD",
            side: "sell",
            orderType: "limit",
            quantity: 1.0,
            price: 51000.0,
            status: "cancelled",
            filledQuantity: 0.0,
            averagePrice: nil,
            createdAt: Date(),
            updatedAt: nil
        )
        XCTAssertEqual(cancelledOrder.statusColor, "red")
    }

    func testPortfolioDecoding() throws {
        let json = """
        {
            "total_value": 100000.0,
            "cash_balance": 50000.0,
            "positions_value": 50000.0,
            "unrealized_pnl": 5000.0,
            "realized_pnl": 3000.0,
            "today_pnl": 1500.0,
            "today_pnl_percent": 1.5
        }
        """.data(using: .utf8)!

        let decoder = JSONDecoder()

        let portfolio = try decoder.decode(Portfolio.self, from: json)

        XCTAssertEqual(portfolio.totalValue, 100000.0)
        XCTAssertEqual(portfolio.cashBalance, 50000.0)
        XCTAssertEqual(portfolio.positionsValue, 50000.0)
        XCTAssertEqual(portfolio.unrealizedPnL, 5000.0)
        XCTAssertEqual(portfolio.realizedPnL, 3000.0)
        XCTAssertEqual(portfolio.todayPnL, 1500.0)
        XCTAssertEqual(portfolio.todayPnLPercent, 1.5)
    }

    func testPositionPnLColor() {
        let profitPosition = Position(
            id: "1",
            symbol: "BTCUSD",
            quantity: 1.0,
            averagePrice: 50000.0,
            currentPrice: 55000.0,
            marketValue: 55000.0,
            unrealizedPnL: 5000.0,
            unrealizedPnLPercent: 10.0,
            costBasis: 50000.0
        )
        XCTAssertEqual(profitPosition.pnlColor, "green")

        let lossPosition = Position(
            id: "2",
            symbol: "ETHUSD",
            quantity: 10.0,
            averagePrice: 3000.0,
            currentPrice: 2800.0,
            marketValue: 28000.0,
            unrealizedPnL: -2000.0,
            unrealizedPnLPercent: -6.67,
            costBasis: 30000.0
        )
        XCTAssertEqual(lossPosition.pnlColor, "red")
    }
}
