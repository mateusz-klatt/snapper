import XCTest
@testable import Snapper

final class AuthServiceTests: XCTestCase {

    var authService: AuthService!

    override func setUp() {
        super.setUp()
    }

    override func tearDown() {
        authService = nil
        super.tearDown()
    }

    func testLoginResponseDecoding() throws {
        let json = """
        {
            "message": "Login successful",
            "expires_in": 900,
            "user": {
                "id": "1",
                "username": "testuser",
                "email": "test@example.com",
                "role": "viewer",
                "is_active": true,
                "created_at": "2025-01-01T00:00:00Z",
                "last_login": null
            }
        }
        """.data(using: .utf8)!

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let response = try decoder.decode(LoginResponse.self, from: json)

        XCTAssertEqual(response.message, "Login successful")
        XCTAssertEqual(response.expiresIn, 900)
        XCTAssertEqual(response.user.username, "testuser")
        XCTAssertEqual(response.user.email, "test@example.com")
        XCTAssertEqual(response.user.role, .viewer)
    }

    func testErrorResponseDecoding() throws {
        let json = """
        {
            "detail": "Invalid credentials"
        }
        """.data(using: .utf8)!

        let decoder = JSONDecoder()
        let response = try decoder.decode(ErrorResponse.self, from: json)

        XCTAssertEqual(response.detail, "Invalid credentials")
    }

}
