import XCTest
@testable import Snapper

@MainActor
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

        MockURLProtocol.requestHandler = { request in
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 200,
                httpVersion: nil,
                headerFields: [AppConfig.HTTPHeader.contentType: AppConfig.ContentType.json]
            )!

            let json: [String: Any] = [
                "message": "Login successful",
                "expires_in": 900,
                "user": [
                    "id": "1",
                    "username": "testuser",
                    "email": "test@example.com",
                    "role": "viewer",
                    "is_active": true,
                    "created_at": "2025-01-01T00:00:00Z"
                ]
            ]
            let data = try JSONSerialization.data(withJSONObject: json)

            return (response, data)
        }

        await authService.login(username: "testuser", password: "testpass")

        await MainActor.run {
            XCTAssertTrue(authService.isAuthenticated)
            XCTAssertNil(authService.errorMessage)
            XCTAssertEqual(authService.currentUser?.username, "testuser")
        }
    }

    func testLoginInvalidCredentials() async throws {

        MockURLProtocol.requestHandler = { request in
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 401,
                httpVersion: nil,
                headerFields: [AppConfig.HTTPHeader.contentType: AppConfig.ContentType.json]
            )!

            let json = ["detail": "Invalid credentials"]
            let data = try JSONSerialization.data(withJSONObject: json)

            return (response, data)
        }

        await authService.login(username: "wronguser", password: "wrongpass")

        await MainActor.run {
            XCTAssertFalse(authService.isAuthenticated)
            XCTAssertEqual(authService.errorMessage, "Invalid credentials")
            XCTAssertNil(authService.currentUser)
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
                headerFields: [AppConfig.HTTPHeader.contentType: AppConfig.ContentType.json]
            )!

            let json = ["detail": "Internal server error"]
            let data = try JSONSerialization.data(withJSONObject: json)

            return (response, data)
        }

        await authService.login(username: "testuser", password: "testpass")

        await MainActor.run {
            XCTAssertFalse(authService.isAuthenticated)
            XCTAssertEqual(authService.errorMessage, "Internal server error")
        }
    }
}
