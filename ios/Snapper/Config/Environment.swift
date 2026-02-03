import Foundation

enum AppConfig {

    static let baseURL = "https://snapper.ch"
    static let apiPrefix = "/api"
    static let wsPath = "/ws"

    enum URIScheme {
        static let http = "http://"
        static let https = "https://"
        static let ws = "ws://"
        static let wss = "wss://"
        static let httpsPrefix = "https"
    }

    enum ContentType {
        static let json = "application/json"
        static let formURLEncoded = "application/x-www-form-urlencoded"
    }

    enum HTTPHeader {
        static let contentType = "Content-Type"
        static let authorization = "Authorization"
    }

    static var apiBaseURL: String {
        return "\(baseURL)\(apiPrefix)"
    }

    static var wsBaseURL: String {
        let wsProtocol = baseURL.hasPrefix(URIScheme.httpsPrefix) ? URIScheme.wss : URIScheme.ws
        let urlWithoutProtocol = baseURL
            .replacingOccurrences(of: URIScheme.http, with: "")
            .replacingOccurrences(of: URIScheme.https, with: "")
        return "\(wsProtocol)\(urlWithoutProtocol)\(apiPrefix)\(wsPath)"
    }

    enum Endpoints {
        static let login = "/auth/login"
        static let logout = "/auth/logout"
        static let refresh = "/auth/refresh"
        static let me = "/auth/me"
        static let orders = "/orders"
        static let positions = "/positions"
        static let signals = "/signals"
        static let executions = "/executions"
        static let status = "/status"
        static let health = "/health"
    }
}
