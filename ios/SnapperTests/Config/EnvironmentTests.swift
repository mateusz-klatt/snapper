import XCTest
@testable import Snapper

final class EnvironmentTests: XCTestCase {

    private static let expectedAPIBaseURL = AppConfig.baseURL + AppConfig.apiPrefix
    private static let httpsTestURL = AppConfig.URIScheme.https + "api.example.com"

    func testAPIBaseURL() {
        XCTAssertEqual(AppConfig.apiBaseURL, Self.expectedAPIBaseURL)
    }

    func testWebSocketBaseURL() {

        let wsURL = AppConfig.wsBaseURL
        XCTAssertTrue(wsURL.starts(with: AppConfig.URIScheme.ws))
        XCTAssertTrue(wsURL.contains("localhost:8000"))
        let expectedWsPath = AppConfig.apiPrefix + AppConfig.wsPath
        XCTAssertTrue(wsURL.contains(expectedWsPath))
    }

    func testWebSocketBaseURLWithHTTPS() {

        let httpsURL = Self.httpsTestURL
        let wsProtocol = httpsURL.hasPrefix(AppConfig.URIScheme.httpsPrefix) ? "wss" : "ws"
        XCTAssertEqual(wsProtocol, "wss")
    }

    func testEndpoints() {
        XCTAssertEqual(AppConfig.Endpoints.login, "/auth/login")
        XCTAssertEqual(AppConfig.Endpoints.logout, "/auth/logout")
        XCTAssertEqual(AppConfig.Endpoints.me, "/auth/me")
        XCTAssertEqual(AppConfig.Endpoints.orders, "/orders")
        XCTAssertEqual(AppConfig.Endpoints.positions, "/positions")
        XCTAssertEqual(AppConfig.Endpoints.signals, "/signals")
        XCTAssertEqual(AppConfig.Endpoints.executions, "/executions")
        XCTAssertEqual(AppConfig.Endpoints.status, "/status")
        XCTAssertEqual(AppConfig.Endpoints.health, "/health")
    }
}
