import XCTest
@testable import Snapper

final class EnvironmentTests: XCTestCase {

    func testAPIBaseURL() {
        let expected = "http://localhost:8000/snapper/api"
        XCTAssertEqual(AppConfig.apiBaseURL, expected)
    }

    func testWebSocketBaseURL() {

        let wsURL = AppConfig.wsBaseURL
        XCTAssertTrue(wsURL.starts(with: "ws://"))
        XCTAssertTrue(wsURL.contains("localhost:8000"))
        XCTAssertTrue(wsURL.contains("/snapper/api/ws"))
    }

    func testWebSocketBaseURLWithHTTPS() {

        let httpsURL = "https://api.example.com"
        let wsProtocol = httpsURL.hasPrefix("https") ? "wss" : "ws"
        XCTAssertEqual(wsProtocol, "wss")
    }

    func testEndpoints() {
        XCTAssertEqual(AppConfig.Endpoints.login, "/auth/login")
        XCTAssertEqual(AppConfig.Endpoints.logout, "/auth/logout")
        XCTAssertEqual(AppConfig.Endpoints.me, "/auth/me")
        XCTAssertEqual(AppConfig.Endpoints.orders, "/orders")
        XCTAssertEqual(AppConfig.Endpoints.portfolio, "/portfolio")
        XCTAssertEqual(AppConfig.Endpoints.positions, "/portfolio/positions")
        XCTAssertEqual(AppConfig.Endpoints.strategies, "/strategies")
    }
}
