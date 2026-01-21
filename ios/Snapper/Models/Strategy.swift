import Foundation

struct Strategy: Codable, Identifiable {
    let id: String
    let name: String
    let type: String
    let status: String
    let symbols: [String]
    let parameters: [String: String]?
    let performance: StrategyPerformance?
    let createdAt: Date
    let updatedAt: Date?

    enum CodingKeys: String, CodingKey {
        case id
        case name
        case type
        case status
        case symbols
        case parameters
        case performance
        case createdAt = "created_at"
        case updatedAt = "updated_at"
    }

    var isRunning: Bool {
        return status.lowercased() == "running"
    }
}

struct StrategyPerformance: Codable {
    let totalTrades: Int
    let winRate: Double
    let profitLoss: Double
    let profitLossPercent: Double
    let sharpeRatio: Double?

    enum CodingKeys: String, CodingKey {
        case totalTrades = "total_trades"
        case winRate = "win_rate"
        case profitLoss = "profit_loss"
        case profitLossPercent = "profit_loss_percent"
        case sharpeRatio = "sharpe_ratio"
    }
}
