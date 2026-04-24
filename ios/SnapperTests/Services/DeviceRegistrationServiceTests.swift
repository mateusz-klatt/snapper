import XCTest
@testable import Snapper

/// Tests for ``DeviceRegistrationService`` actor (Plan 2 iOS-1).
///
/// Covers the two-input handshake (APNs token + login state) +
/// idempotent re-registration + logout clears state. The APIClient
/// is swapped for a mock-session-backed instance via
/// `MockURLProtocol` so `registerDevice` actually round-trips HTTP
/// under the mocked 200 / 401 / network-error responses.
@MainActor
final class DeviceRegistrationServiceTests: XCTestCase {

    private var mockSession: URLSession!
    private var apiClient: APIClient!

    override func setUp() {
        super.setUp()
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [MockURLProtocol.self]
        mockSession = URLSession(configuration: config)
        apiClient = APIClient(session: mockSession, authService: FakeAuthService())
    }

    override func tearDown() {
        mockSession = nil
        apiClient = nil
        MockURLProtocol.requestHandler = nil
        super.tearDown()
    }

    /// Token received before login → stored; no registration yet.
    func testTokenBeforeLoginStoredButNotRegistered() async {
        let service = DeviceRegistrationService(apiClient: apiClient)
        let token = Data([0x01, 0x02, 0x03, 0x04])

        var callCount = 0
        MockURLProtocol.requestHandler = { _ in
            callCount += 1
            return MockURLProtocol.jsonResponse(statusCode: 200, json: _deviceResponseJSON)
        }

        await service.onTokenReceived(token)

        XCTAssertEqual(callCount, 0)
        let pid = await service.currentDevicePublicId()
        XCTAssertNil(pid)
    }

    /// Login before token → flag set; token arriving later triggers register.
    func testLoginBeforeTokenDoesNotRegisterUntilTokenArrives() async {
        let service = DeviceRegistrationService(apiClient: apiClient)
        let token = Data([0x01, 0x02, 0x03, 0x04])

        var calls: [URLRequest] = []
        MockURLProtocol.requestHandler = { request in
            calls.append(request)
            return MockURLProtocol.jsonResponse(statusCode: 200, json: _deviceResponseJSON)
        }

        await service.onLogin()
        XCTAssertEqual(calls.count, 0)

        await service.onTokenReceived(token)
        XCTAssertEqual(calls.count, 1)
        let pid = await service.currentDevicePublicId()
        XCTAssertEqual(pid, "dev-registered-pid")
    }

    /// Token + login both present → registration fires immediately.
    func testTokenThenLoginRegistersOnLogin() async {
        let service = DeviceRegistrationService(apiClient: apiClient)
        let token = Data([0x0a, 0x0b, 0x0c, 0x0d])

        var calls: [URLRequest] = []
        MockURLProtocol.requestHandler = { request in
            calls.append(request)
            return MockURLProtocol.jsonResponse(statusCode: 200, json: _deviceResponseJSON)
        }

        await service.onTokenReceived(token)
        XCTAssertEqual(calls.count, 0)

        await service.onLogin()
        XCTAssertEqual(calls.count, 1)
        XCTAssertEqual(calls[0].httpMethod, "POST")
    }

    /// Registration POSTs to the `/devices` endpoint.
    func testRegistrationHitsDevicesEndpoint() async {
        let service = DeviceRegistrationService(apiClient: apiClient)
        let token = Data([0xde, 0xad, 0xbe, 0xef])

        var capturedRequests: [URLRequest] = []
        MockURLProtocol.requestHandler = { request in
            capturedRequests.append(request)
            return MockURLProtocol.jsonResponse(statusCode: 200, json: _deviceResponseJSON)
        }

        await service.onLogin()
        await service.onTokenReceived(token)

        XCTAssertEqual(capturedRequests.count, 1)
        XCTAssertEqual(capturedRequests.first?.httpMethod, "POST")
        XCTAssertTrue(
            capturedRequests.first?.url?.path.hasSuffix("/devices") ?? false,
            "expected URL path to end with /devices"
        )
    }

    /// Logout clears both the login flag AND the pending token.
    func testLogoutClearsState() async {
        let service = DeviceRegistrationService(apiClient: apiClient)
        let token = Data([0x01])

        MockURLProtocol.requestHandler = { _ in
            return MockURLProtocol.jsonResponse(statusCode: 200, json: _deviceResponseJSON)
        }

        await service.onLogin()
        await service.onTokenReceived(token)
        await service.onLogout()

        var afterLogoutCalls = 0
        MockURLProtocol.requestHandler = { _ in
            afterLogoutCalls += 1
            return MockURLProtocol.jsonResponse(statusCode: 200, json: _deviceResponseJSON)
        }
        await service.onLogin()

        XCTAssertEqual(afterLogoutCalls, 0, "no re-registration after logout cleared the token")
    }

    /// Backend 500 error does NOT raise; next attempt re-tries cleanly.
    func testRegistrationServerErrorIsSwallowed() async {
        let service = DeviceRegistrationService(apiClient: apiClient)
        let token = Data([0x01])
        var attempts = 0

        MockURLProtocol.requestHandler = { _ in
            attempts += 1
            if attempts == 1 {
                return MockURLProtocol.errorResponse(statusCode: 500, message: "server down")
            }
            return MockURLProtocol.jsonResponse(statusCode: 200, json: _deviceResponseJSON)
        }

        await service.onLogin()
        await service.onTokenReceived(token)

        let firstPid = await service.currentDevicePublicId()
        XCTAssertNil(firstPid)

        await service.onTokenReceived(token)
        let secondPid = await service.currentDevicePublicId()
        XCTAssertEqual(secondPid, "dev-registered-pid")
    }
}

private nonisolated(unsafe) let _deviceResponseJSON: [String: Any] = [
    "type": "notification_device_response",
    "session_id": "s",
    "sequence_id": 1,
    "public_id": "env-1",
    "timestamp": "2026-04-24T12:00:00Z",
    "payload": [
        "session_id": "s",
        "sequence_id": 1,
        "public_id": "dev-registered-pid",
        "timestamp": "2026-04-24T12:00:00Z",
        "user_public_id": "user-1",
        "device_token": "deadbeef",
        "device_id": "did",
        "platform": "ios",
        "env": "sandbox",
        "app_version": "1.0",
        "previews_mode": "private",
        "registered_at": "2026-04-24T12:00:00Z",
        "last_seen_at": nil,
    ],
]
