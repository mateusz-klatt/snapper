import Foundation
import os

enum AppConfig {

    private static let logger = Logger(
        subsystem: Bundle.main.bundleIdentifier ?? "Snapper",
        category: "Config"
    )

    private static let configuration = AppConfiguration.load()

    static var baseURL: String {
        return configuration.baseURL
    }

    static var apiPrefix: String {
        return configuration.apiPrefix
    }

    static var wsPath: String {
        return configuration.wsPath
    }

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
        static var login: String {
            return configuration.endpoints.login
        }

        static var logout: String {
            return configuration.endpoints.logout
        }

        static var refresh: String {
            return configuration.endpoints.refresh
        }

        static var me: String {
            return configuration.endpoints.me
        }

        static var orders: String {
            return configuration.endpoints.orders
        }

        static var positions: String {
            return configuration.endpoints.positions
        }

        static var signals: String {
            return configuration.endpoints.signals
        }

        static var executions: String {
            return configuration.endpoints.executions
        }

        static var status: String {
            return configuration.endpoints.status
        }

        static var health: String {
            return configuration.endpoints.health
        }

        static var devices: String {
            return configuration.endpoints.devices
        }

        static var alerts: String {
            return configuration.endpoints.alerts
        }

        static var system: String {
            return configuration.endpoints.system
        }

        static var wallets: String {
            return configuration.endpoints.wallets
        }

        static var alertDefaults: String {
            return configuration.endpoints.alertDefaults
        }

        static var operators: String {
            return configuration.endpoints.operators
        }

        static var executionPlans: String {
            return configuration.endpoints.executionPlans
        }

        static var trailingStops: String {
            return configuration.endpoints.trailingStops
        }
    }

    private struct AppConfiguration {
        let baseURL: String
        let apiPrefix: String
        let wsPath: String
        let endpoints: EndpointConfiguration

        struct EndpointConfiguration {
            let login: String
            let logout: String
            let refresh: String
            let me: String
            let orders: String
            let positions: String
            let signals: String
            let executions: String
            let status: String
            let health: String
            let devices: String
            let alerts: String
            let system: String
            let wallets: String
            let alertDefaults: String
            let operators: String
            let executionPlans: String
            let trailingStops: String
        }

        static func load() -> AppConfiguration {
            guard
                let url = Bundle.main.url(forResource: "Configuration", withExtension: "plist"),
                let data = try? Data(contentsOf: url),
                let plist = try? PropertyListSerialization.propertyList(from: data, options: [], format: nil),
                let dict = plist as? [String: Any]
            else {
                let message = "Failed to load Configuration.plist from main bundle. Verify the file is included in target resources and contains valid plist data."
                AppConfig.logger.error("\(message, privacy: .public)")
                assertionFailure(message)
                return empty
            }

            let endpoints = dict["Endpoints"] as? [String: Any] ?? [:]

            let config = AppConfiguration(
                baseURL: dict["BaseURL"] as? String ?? "",
                apiPrefix: dict["APIPrefix"] as? String ?? "",
                wsPath: dict["WSPath"] as? String ?? "",
                endpoints: EndpointConfiguration(
                    login: endpoints["Login"] as? String ?? "",
                    logout: endpoints["Logout"] as? String ?? "",
                    refresh: endpoints["Refresh"] as? String ?? "",
                    me: endpoints["Me"] as? String ?? "",
                    orders: endpoints["Orders"] as? String ?? "",
                    positions: endpoints["Positions"] as? String ?? "",
                    signals: endpoints["Signals"] as? String ?? "",
                    executions: endpoints["Executions"] as? String ?? "",
                    status: endpoints["Status"] as? String ?? "",
                    health: endpoints["Health"] as? String ?? "",
                    devices: endpoints["Devices"] as? String ?? "",
                    alerts: endpoints["Alerts"] as? String ?? "",
                    system: endpoints["System"] as? String ?? "",
                    wallets: endpoints["Wallets"] as? String ?? "",
                    alertDefaults: endpoints["AlertDefaults"] as? String ?? "",
                    operators: endpoints["Operators"] as? String ?? "",
                    executionPlans: endpoints["ExecutionPlans"] as? String ?? "",
                    trailingStops: endpoints["TrailingStops"] as? String ?? ""
                )
            )

            if config.baseURL.isEmpty || config.apiPrefix.isEmpty {
                let message = "Configuration.plist missing critical fields. BaseURL=\"\(config.baseURL)\" APIPrefix=\"\(config.apiPrefix)\". Check ios/Snapper/Config/Configuration.plist."
                AppConfig.logger.error("\(message, privacy: .public)")
                assertionFailure(message)
            }

            return config
        }

        static var empty: AppConfiguration {
            return AppConfiguration(
                baseURL: "",
                apiPrefix: "",
                wsPath: "",
                endpoints: EndpointConfiguration(
                    login: "",
                    logout: "",
                    refresh: "",
                    me: "",
                    orders: "",
                    positions: "",
                    signals: "",
                    executions: "",
                    status: "",
                    health: "",
                    devices: "",
                    alerts: "",
                    system: "",
                    wallets: "",
                    alertDefaults: "",
                    operators: "",
                    executionPlans: "",
                    trailingStops: ""
                )
            )
        }
    }
}
