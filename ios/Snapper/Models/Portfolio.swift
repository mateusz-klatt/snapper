import Foundation

struct Portfolio: Codable {
    let totalValue: Double
    let cashBalance: Double
    let positionsValue: Double
    let unrealizedPnL: Double
    let realizedPnL: Double
    let todayPnL: Double
    let todayPnLPercent: Double

    enum CodingKeys: String, CodingKey {
        case totalValue = "total_value"
        case cashBalance = "cash_balance"
        case positionsValue = "positions_value"
        case unrealizedPnL = "unrealized_pnl"
        case realizedPnL = "realized_pnl"
        case todayPnL = "today_pnl"
        case todayPnLPercent = "today_pnl_percent"
    }
}

struct Position: Codable, Identifiable {
    let id: String
    let symbol: String
    let quantity: Double
    let averagePrice: Double
    let currentPrice: Double
    let marketValue: Double
    let unrealizedPnL: Double
    let unrealizedPnLPercent: Double
    let costBasis: Double

    enum CodingKeys: String, CodingKey {
        case id
        case symbol
        case quantity
        case averagePrice = "average_price"
        case currentPrice = "current_price"
        case marketValue = "market_value"
        case unrealizedPnL = "unrealized_pnl"
        case unrealizedPnLPercent = "unrealized_pnl_percent"
        case costBasis = "cost_basis"
    }

    var pnlColor: String {
        return unrealizedPnL >= 0 ? "green" : "red"
    }
}
