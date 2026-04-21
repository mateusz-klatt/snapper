import Foundation
import Combine
import os

@MainActor
class AuthService: ObservableObject {
    static let shared = AuthService()

    private let logger = Logger(subsystem: Bundle.main.bundleIdentifier ?? "Snapper", category: "Auth")

    @Published var isAuthenticated = false
    @Published var currentUser: UserProfile?
    @Published var errorMessage: String?

    private var wsToken: String?
    private let session: URLSession

    init(session: URLSession = .shared) {
        self.session = session
    }

    private convenience init() {
        self.init(session: .shared)
    }

    func login(username: String, password: String) async {
        guard let url = URL(string: "\(AppConfig.apiBaseURL)\(AppConfig.Endpoints.login)") else {
            errorMessage = "Invalid URL"
            return
        }

        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue(AppConfig.ContentType.json, forHTTPHeaderField: AppConfig.HTTPHeader.contentType)

        let body = ["username": username, "password": password]
        request.httpBody = try? JSONSerialization.data(withJSONObject: body)

        do {
            let (data, response) = try await session.data(for: request)

            guard let httpResponse = response as? HTTPURLResponse else {
                errorMessage = "Invalid response"
                return
            }

            if httpResponse.statusCode == 200 {
                let decoder = JSONDecoder()
                decoder.dateDecodingStrategy = .iso8601
                let loginResponse = try decoder.decode(LoginResponse.self, from: data)
                currentUser = loginResponse.payload.user
                errorMessage = nil
                isAuthenticated = true
            } else {
                let errorResponse = try? JSONDecoder().decode(ErrorResponse.self, from: data)
                errorMessage = errorResponse?.detail ?? "Login failed"
            }
        } catch {
            errorMessage = "Network error: \(error.localizedDescription)"
        }
    }

    func logout() async {
        await logoutFromServer()
        wsToken = nil
        currentUser = nil
        isAuthenticated = false
    }

    private func logoutFromServer() async {
        guard let url = URL(string: "\(AppConfig.apiBaseURL)\(AppConfig.Endpoints.logout)") else {
            return
        }

        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue(AppConfig.ContentType.json, forHTTPHeaderField: AppConfig.HTTPHeader.contentType)
        // Per plan §D8: bound the logout/refresh endpoints at 10s so a
        // dead network can't leave us wedged forever. `URLSession.shared`
        // is immutable, so set this per-URLRequest — the test session
        // is free to override via its own configuration.
        request.timeoutInterval = 10

        _ = try? await session.data(for: request)
    }

    private static let roleHierarchy: [UserRole: Int] = [
        .viewer: 1,
        .operatorRole: 2,
        .admin: 3,
    ]

    func hasRole(_ role: UserRole) -> Bool {
        guard let user = currentUser else { return false }
        let userLevel = Self.roleHierarchy[user.role] ?? 0
        let requiredLevel = Self.roleHierarchy[role] ?? 0
        return userLevel >= requiredLevel
    }

    func hasPermission(_ permission: Permission) -> Bool {
        guard let user = currentUser else { return false }
        if user.role == .admin { return true }
        let perms = rolePermissions[user.role] ?? []
        return perms.contains(permission)
    }

    func canAccess(_ resource: String) -> Bool {
        guard let user = currentUser else { return false }
        let allowed = resourceAccess[resource] ?? []
        return allowed.contains(user.role)
    }

    func getWsToken() -> String? {
        return wsToken
    }

    func fetchFreshWsToken() async -> String? {
        guard let url = URL(string: "\(AppConfig.apiBaseURL)\(AppConfig.Endpoints.refresh)") else {
            return nil
        }

        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue(AppConfig.ContentType.json, forHTTPHeaderField: AppConfig.HTTPHeader.contentType)
        // Per plan §D8: cap refresh attempts at 10s — see logoutFromServer
        // comment for rationale. Refresh payload is small, so 10s is
        // generous for real networks; legitimate hang == network dead.
        request.timeoutInterval = 10

        do {
            let (data, response) = try await session.data(for: request)

            guard let httpResponse = response as? HTTPURLResponse,
                  httpResponse.statusCode == 200 else {
                return nil
            }

            let decoder = JSONDecoder()
            decoder.dateDecodingStrategy = .iso8601
            let refreshResponse = try decoder.decode(RefreshResponse.self, from: data)
            wsToken = refreshResponse.payload.wsToken
            return refreshResponse.payload.wsToken
        } catch {
            logger.error("Failed to fetch fresh ws_token: \(error)")
            return nil
        }
    }
}

struct ErrorResponse: Codable {
    let detail: String
}

extension AuthService: AuthRefreshing {}
