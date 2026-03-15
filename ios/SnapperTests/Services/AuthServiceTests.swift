import XCTest
@testable import Snapper

@MainActor
final class AuthServiceTests: XCTestCase {

    var authService: AuthService!

    override func setUp() {
        super.setUp()
        authService = AuthService(session: .shared)
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
                "created_at": "2025-01-01T00:00:00Z"
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

    func testHasRoleHierarchy() {
        authService.currentUser = makeUser(role: .operatorRole)

        XCTAssertTrue(authService.hasRole(.viewer))
        XCTAssertTrue(authService.hasRole(.operatorRole))
        XCTAssertFalse(authService.hasRole(.admin))
    }

    func testHasPermissionUsesGeneratedRolePermissions() {
        authService.currentUser = makeUser(role: .viewer)

        XCTAssertTrue(authService.hasPermission(.readMarketData))
        XCTAssertFalse(authService.hasPermission(.manageUsers))
    }

    func testHasPermissionAdminAllowsAll() {
        authService.currentUser = makeUser(role: .admin)

        XCTAssertTrue(authService.hasPermission(.manageUsers))
        XCTAssertTrue(authService.hasPermission(.configureSystem))
    }

    func testCanAccessUsesGeneratedResourceAccess() {
        authService.currentUser = makeUser(role: .viewer)

        XCTAssertTrue(authService.canAccess("overview"))
        XCTAssertFalse(authService.canAccess("settings"))
    }

    func testRoleAndPermissionChecksReturnFalseWithoutUser() {
        authService.currentUser = nil

        XCTAssertFalse(authService.hasRole(.viewer))
        XCTAssertFalse(authService.hasPermission(.readMarketData))
        XCTAssertFalse(authService.canAccess("overview"))
    }

    private func makeUser(role: UserRole) -> UserProfile {
        UserProfile(
            username: "testuser",
            email: "test@example.com",
            role: role,
            isActive: true,
            createdAt: nil
        )
    }

}
