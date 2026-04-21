import XCTest
@testable import Snapper

@MainActor
final class LoginViewModelTests: XCTestCase {

    var mockAuthService: AuthService!
    var mockSession: URLSession!

    override func setUp() {
        super.setUp()

        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [MockURLProtocol.self]
        mockSession = URLSession(configuration: configuration)

        mockAuthService = AuthService(session: mockSession)
    }

    override func tearDown() {
        mockAuthService = nil
        mockSession = nil
        MockURLProtocol.requestHandler = nil
        super.tearDown()
    }

    @MainActor
    func createViewModel() -> LoginViewModel {
        return LoginViewModel(authService: mockAuthService)
    }

    func testInitialState() async {
        let viewModel = await createViewModel()
        await MainActor.run {
            XCTAssertEqual(viewModel.username, "")
            XCTAssertEqual(viewModel.password, "")
            XCTAssertFalse(viewModel.isLoading)
            XCTAssertNil(viewModel.errorMessage)
            XCTAssertFalse(viewModel.isAuthenticated)
        }
    }

    func testIsLoginEnabledWhenFieldsEmpty() async {
        let viewModel = await createViewModel()
        await MainActor.run {
            viewModel.username = ""
            viewModel.password = ""
            XCTAssertFalse(viewModel.isLoginEnabled)
        }
    }

    func testIsLoginEnabledWhenUsernameEmpty() async {
        let viewModel = await createViewModel()
        await MainActor.run {
            viewModel.username = ""
            viewModel.password = "password123"
            XCTAssertFalse(viewModel.isLoginEnabled)
        }
    }

    func testIsLoginEnabledWhenPasswordEmpty() async {
        let viewModel = await createViewModel()
        await MainActor.run {
            viewModel.username = "testuser"
            viewModel.password = ""
            XCTAssertFalse(viewModel.isLoginEnabled)
        }
    }

    func testIsLoginEnabledWhenBothFieldsFilled() async {
        let viewModel = await createViewModel()
        await MainActor.run {
            viewModel.username = "testuser"
            viewModel.password = "password123"
            XCTAssertTrue(viewModel.isLoginEnabled)
        }
    }

    func testIsLoginDisabledWhenLoading() async {
        let viewModel = await createViewModel()
        await MainActor.run {
            viewModel.username = "testuser"
            viewModel.password = "password123"
            viewModel.isLoading = true
            XCTAssertFalse(viewModel.isLoginEnabled)
        }
    }

    func testLoginSuccess() async {
        let viewModel = await createViewModel()

        var requestCount = 0
        MockURLProtocol.requestHandler = { request in
            requestCount += 1

            if requestCount == 1 {

                let json: [String: Any] = [
                    "sequence_id": 1,
                    "public_id": "01961234-5678-7000-8000-000000000600",
                    "timestamp": "2025-01-01T00:00:00Z",
                    "session_id": "session-1",
                    "payload": [
                        "sequence_id": 1,
                        "public_id": "01961234-5678-7000-8000-000000000601",
                        "timestamp": "2025-01-01T00:00:00Z",
                        "session_id": "session-1",
                        "message": "Login successful",
                        "expires_in": 900,
                        "user": [
                            "sequence_id": 1,
                            "public_id": "01961234-5678-7000-8000-000000000602",
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
                return MockURLProtocol.jsonResponse(statusCode: 200, json: json)
            } else {

                let json: [String: Any] = [
                    "sequence_id": 2,
                    "public_id": "01961234-5678-7000-8000-000000000700",
                    "timestamp": "2025-11-22T11:00:00Z",
                    "session_id": "session-1",
                    "payload": [
                        "sequence_id": 2,
                        "public_id": "01961234-5678-7000-8000-000000000701",
                        "timestamp": "2025-11-22T11:00:00Z",
                        "session_id": "session-1",
                        "message": "Token refreshed",
                        "ws_token": "ws_token_value",
                        "ws_token_exp": "2025-11-22T11:00:00Z",
                        "csrf_token": "csrf_value",
                        "user": [
                            "sequence_id": 1,
                            "public_id": "01961234-5678-7000-8000-000000000702",
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
                return MockURLProtocol.jsonResponse(statusCode: 200, json: json)
            }
        }

        await MainActor.run {
            viewModel.username = "testuser"
            viewModel.password = "password123"
        }

        await viewModel.login()

        await MainActor.run {
            XCTAssertTrue(viewModel.isAuthenticated)
            XCTAssertNil(viewModel.errorMessage)
            XCTAssertFalse(viewModel.isLoading)
        }
    }

    func testLoginInvalidCredentials() async {
        let viewModel = await createViewModel()

        MockURLProtocol.requestHandler = { request in
            MockURLProtocol.errorResponse(statusCode: 401, message: "Invalid credentials")
        }

        await MainActor.run {
            viewModel.username = "wronguser"
            viewModel.password = "wrongpass"
        }

        await viewModel.login()

        await MainActor.run {
            XCTAssertFalse(viewModel.isAuthenticated)
            XCTAssertEqual(viewModel.errorMessage, "Invalid credentials")
            XCTAssertFalse(viewModel.isLoading)
        }
    }

    func testLoginNetworkError() async {
        let viewModel = await createViewModel()

        MockURLProtocol.requestHandler = { request in
            throw MockURLProtocol.networkError()
        }

        await MainActor.run {
            viewModel.username = "testuser"
            viewModel.password = "password123"
        }

        await viewModel.login()

        await MainActor.run {
            XCTAssertFalse(viewModel.isAuthenticated)
            XCTAssertNotNil(viewModel.errorMessage)
            XCTAssertTrue(viewModel.errorMessage?.contains("Network") ?? false)
            XCTAssertFalse(viewModel.isLoading)
        }
    }

    func testLoadingStateDuringLogin() async {
        let viewModel = await createViewModel()

        MockURLProtocol.requestHandler = { request in
            Thread.sleep(forTimeInterval: 0.1)
            return MockURLProtocol.jsonResponse(statusCode: 200, json: [
                "sequence_id": 1,
                "public_id": "01961234-5678-7000-8000-000000000800",
                "timestamp": "2025-01-01T00:00:00Z",
                "session_id": "session-1",
                "payload": [
                    "sequence_id": 1,
                    "public_id": "01961234-5678-7000-8000-000000000801",
                    "timestamp": "2025-01-01T00:00:00Z",
                    "session_id": "session-1",
                    "message": "Login successful",
                    "expires_in": 900,
                    "user": [
                        "sequence_id": 1,
                        "public_id": "01961234-5678-7000-8000-000000000802",
                        "timestamp": "2025-01-01T00:00:00Z",
                        "session_id": "session-1",
                        "username": "testuser",
                        "email": "test@example.com",
                        "role": "viewer",
                        "is_active": true,
                        "created_at": "2025-01-01T00:00:00Z"
                    ]
                ]
            ])
        }

        await MainActor.run {
            viewModel.username = "testuser"
            viewModel.password = "password123"
        }

        let loginTask = Task {
            await viewModel.login()
        }

        try? await Task.sleep(nanoseconds: 10_000_000)
        await MainActor.run {
            XCTAssertTrue(viewModel.isLoading)
        }

        await loginTask.value
        await MainActor.run {
            XCTAssertFalse(viewModel.isLoading)
        }
    }
}
