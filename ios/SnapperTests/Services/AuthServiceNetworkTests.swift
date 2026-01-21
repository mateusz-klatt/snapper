import XCTest
@testable import Snapper

final class AuthServiceNetworkTests: XCTestCase {

    var authService: AuthService!
    var mockSession: URLSession!

    override func setUp() {
        super.setUp()

        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [MockURLProtocol.self]
        mockSession = URLSession(configuration: configuration)

        authService = AuthService(session: mockSession)
    }

    override func tearDown() {
        authService = nil
        mockSession = nil
        MockURLProtocol.requestHandler = nil
        super.tearDown()
    }

    func testLoginSuccess() async throws {

        var requestCount = 0
        MockURLProtocol.requestHandler = { request in
            requestCount += 1

            if requestCount == 1 {
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: nil,
                    headerFields: ["Content-Type": "application/json"]
                )!

                let json: [String: Any] = [
                    "access_token": "test_access_token_123",
                    "refresh_token": "test_refresh_token_456",
                    "token_type": "bearer"
                ]
                let data = try! JSONSerialization.data(withJSONObject: json)

                return (response, data)
            }

            else {
                let response = HTTPURLResponse(
                    url: request.url!,
                    statusCode: 200,
                    httpVersion: nil,
                    headerFields: ["Content-Type": "application/json"]
                )!

                let json: [String: Any] = [
                    "id": 1,
                    "username": "testuser",
                    "email": "test@example.com",
                    "is_active": true,
                    "is_admin": false
                ]
                let data = try! JSONSerialization.data(withJSONObject: json)

                return (response, data)
            }
        }

        await authService.login(username: "testuser", password: "testpass")

        await MainActor.run {
            XCTAssertTrue(authService.isAuthenticated)
            XCTAssertNil(authService.errorMessage)
            XCTAssertEqual(authService.getAccessToken(), "test_access_token_123")
        }
    }

    func testLoginInvalidCredentials() async throws {

        MockURLProtocol.requestHandler = { request in
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 401,
                httpVersion: nil,
                headerFields: ["Content-Type": "application/json"]
            )!

            let json = ["detail": "Invalid credentials"]
            let data = try! JSONSerialization.data(withJSONObject: json)

            return (response, data)
        }

        await authService.login(username: "wronguser", password: "wrongpass")

        await MainActor.run {
            XCTAssertFalse(authService.isAuthenticated)
            XCTAssertEqual(authService.errorMessage, "Invalid credentials")
            XCTAssertNil(authService.getAccessToken())
        }
    }

    func testLoginNetworkError() async throws {

        MockURLProtocol.requestHandler = { request in
            throw MockURLProtocol.networkError()
        }

        await authService.login(username: "testuser", password: "testpass")

        await MainActor.run {
            XCTAssertFalse(authService.isAuthenticated)
            XCTAssertNotNil(authService.errorMessage)
            XCTAssertTrue(authService.errorMessage?.contains("Network") ?? false)
        }
    }

    func testLoginServerError() async throws {

        MockURLProtocol.requestHandler = { request in
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 500,
                httpVersion: nil,
                headerFields: ["Content-Type": "application/json"]
            )!

            let json = ["detail": "Internal server error"]
            let data = try! JSONSerialization.data(withJSONObject: json)

            return (response, data)
        }

        await authService.login(username: "testuser", password: "testpass")

        await MainActor.run {
            XCTAssertFalse(authService.isAuthenticated)
            XCTAssertEqual(authService.errorMessage, "Internal server error")
        }
    }
}
