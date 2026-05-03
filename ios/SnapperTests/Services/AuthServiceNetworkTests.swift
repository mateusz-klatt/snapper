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
                "sequence_id": 1,
                "public_id": "01961234-5678-7000-8000-000000000500",
                "timestamp": "2025-01-01T00:00:00Z",
                "session_id": "session-1",
                "payload": [
                    "sequence_id": 1,
                    "public_id": "01961234-5678-7000-8000-000000000501",
                    "timestamp": "2025-01-01T00:00:00Z",
                    "session_id": "session-1",
                    "message": "Login successful",
                    "expires_in": 900,
                    "user": [
                        "sequence_id": 1,
                        "public_id": "01961234-5678-7000-8000-000000000502",
                        "timestamp": "2025-01-01T00:00:00Z",
                        "session_id": "session-1",
                        "username": "testuser",
                        "email": "test@example.com",
                        "role": "viewer",
                        "is_active": true,
                        "created_at": "2025-01-01T00:00:00Z"
                    ]
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

    /// Regression guard for the envelope/provenance contract enforced
    /// by ``snapper.api.schemas.base.StrictDataSchema``: outbound
    /// login requests must wrap credentials inside ``payload`` and
    /// stamp top-level provenance fields. A reverted change to plain
    /// ``{username, password}`` body would resurrect the production
    /// HTTP 422 we hit previously.
    func testLoginRequestUsesEnvelopeWithProvenanceAndNoTopLevelCredentials() async throws {
        let captured = CapturedBody()

        MockURLProtocol.requestHandler = { request in
            let body = Self.readBody(from: request) ?? Data()
            captured.set(body)

            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 401,
                httpVersion: nil,
                headerFields: [AppConfig.HTTPHeader.contentType: AppConfig.ContentType.json]
            )!
            let stub = try JSONSerialization.data(withJSONObject: ["detail": "stub"])
            return (response, stub)
        }

        await authService.login(username: "alice", password: "s3cret")

        let bodyData = captured.value
        XCTAssertGreaterThan(bodyData.count, 0, "Login request body must not be empty.")

        let parsed = try JSONSerialization.jsonObject(with: bodyData) as? [String: Any]
        XCTAssertNotNil(parsed, "Login body must be a JSON object envelope.")
        guard let parsed else { return }

        XCTAssertNil(parsed["username"], "Username must NOT appear at the top level.")
        XCTAssertNil(parsed["password"], "Password must NOT appear at the top level.")

        XCTAssertEqual(parsed["type"] as? String, "login_request")
        XCTAssertNotNil(parsed["public_id"] as? String)
        XCTAssertNotNil(parsed["session_id"] as? String)
        XCTAssertTrue(parsed["sequence_id"] is Int || parsed["sequence_id"] is NSNumber)
        XCTAssertNotNil(parsed["timestamp"] as? String)

        let payload = parsed["payload"] as? [String: Any]
        XCTAssertEqual(payload?["username"] as? String, "alice")
        XCTAssertEqual(payload?["password"] as? String, "s3cret")
    }

    private static func readBody(from request: URLRequest) -> Data? {
        if let body = request.httpBody {
            return body
        }
        guard let stream = request.httpBodyStream else { return nil }
        stream.open()
        defer { stream.close() }
        var data = Data()
        let bufferSize = 4096
        let buffer = UnsafeMutablePointer<UInt8>.allocate(capacity: bufferSize)
        defer { buffer.deallocate() }
        while stream.hasBytesAvailable {
            let read = stream.read(buffer, maxLength: bufferSize)
            if read <= 0 { break }
            data.append(buffer, count: read)
        }
        return data
    }
}

/// Captures bytes from inside the URLProtocol handler closure into a
/// thread-safe holder the test body can read after ``login()`` returns.
private final class CapturedBody: @unchecked Sendable {
    private let lock = NSLock()
    private var bytes: Data = Data()

    func set(_ data: Data) {
        lock.withLock { bytes = data }
    }

    var value: Data {
        lock.withLock { bytes }
    }
}
