import Foundation

enum AppConfig {

    static let baseURL = "http://localhost:8000"
    static let apiPrefix = "/snapper/api"

    static var apiBaseURL: String {
        return "\(baseURL)\(apiPrefix)"
    }

    static var wsBaseURL: String {
        let wsProtocol = baseURL.hasPrefix("https") ? "wss" : "ws"
        let urlWithoutProtocol = baseURL.replacingOccurrences(of: "http://", with: "")
            .replacingOccurrences(of: "https://", with: "")
        return "\(wsProtocol)://\(urlWithoutProtocol)\(apiPrefix)/ws"
    }

    enum Endpoints {
        static let login = "/auth/login"
        static let logout = "/auth/logout"
        static let refresh = "/auth/refresh"
        static let me = "/auth/me"
        static let orders = "/orders"
        static let portfolio = "/portfolio"
        static let positions = "/portfolio/positions"
        static let marketData = "/market/data"
        static let strategies = "/strategies"
    }
}
