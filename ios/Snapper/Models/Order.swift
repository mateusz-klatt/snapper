import Foundation

struct Order: Codable, Identifiable {
    let id: String
    let symbol: String
    let side: String
    let orderType: String
    let quantity: Double
    let price: Double?
    let status: String
    let filledQuantity: Double
    let averagePrice: Double?
    let createdAt: Date
    let updatedAt: Date?

    enum CodingKeys: String, CodingKey {
        case id
        case symbol
        case side
        case orderType = "order_type"
        case quantity
        case price
        case status
        case filledQuantity = "filled_quantity"
        case averagePrice = "average_price"
        case createdAt = "created_at"
        case updatedAt = "updated_at"
    }

    var statusColor: String {
        switch status.lowercased() {
        case "filled":
            return "green"
        case "pending", "open":
            return "blue"
        case "cancelled", "rejected":
            return "red"
        case "partially_filled":
            return "orange"
        default:
            return "gray"
        }
    }

    var formattedStatus: String {
        return status.replacingOccurrences(of: "_", with: " ").capitalized
    }
}
