import Foundation

struct User: Codable, Identifiable {
    let id: Int
    let username: String
    let email: String?
    let isActive: Bool
    let isAdmin: Bool

    enum CodingKeys: String, CodingKey {
        case id
        case username
        case email
        case isActive = "is_active"
        case isAdmin = "is_admin"
    }
}
