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
            "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
            "refresh_token": "refresh_token_value",
            "token_type": "bearer"
        }
        """.data(using: .utf8)!

        let decoder = JSONDecoder()
        let response = try decoder.decode(LoginResponse.self, from: json)

        XCTAssertEqual(response.accessToken, "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...")
        XCTAssertEqual(response.refreshToken, "refresh_token_value")
        XCTAssertEqual(response.tokenType, "bearer")
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
