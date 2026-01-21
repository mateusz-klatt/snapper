import Foundation

struct MarketData: Codable {
    let symbol: String
    let price: Double
    let bid: Double?
    let ask: Double?
    let volume: Double?
    let high24h: Double?
    let low24h: Double?
    let change24h: Double?
    let changePercent24h: Double?
    let timestamp: Date

    enum CodingKeys: String, CodingKey {
        case symbol
        case price
        case bid
        case ask
        case volume
        case high24h = "high_24h"
        case low24h = "low_24h"
        case change24h = "change_24h"
        case changePercent24h = "change_percent_24h"
        case timestamp
    }
}
