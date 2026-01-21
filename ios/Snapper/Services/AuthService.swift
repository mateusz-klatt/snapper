import Foundation
import Combine

class AuthService: ObservableObject {
    static let shared = AuthService()

    @Published var isAuthenticated = false
    @Published var currentUser: User?
    @Published var errorMessage: String?

    private var accessToken: String?
    private var refreshToken: String?
    private let session: URLSession

    init(session: URLSession = .shared) {
        self.session = session

        loadTokensFromKeychain()
    }

    private convenience init() {
        self.init(session: .shared)
    }

    func login(username: String, password: String) async {
        guard let url = URL(string: "\(AppConfig.apiBaseURL)\(AppConfig.Endpoints.login)") else {
            await MainActor.run {
                errorMessage = "Invalid URL"
            }
            return
        }

        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/x-www-form-urlencoded", forHTTPHeaderField: "Content-Type")

        let bodyString = "username=\(username)&password=\(password)"
        request.httpBody = bodyString.data(using: .utf8)

        do {
            let (data, response) = try await session.data(for: request)

            guard let httpResponse = response as? HTTPURLResponse else {
                await MainActor.run { errorMessage = "Invalid response" }
                return
            }

            if httpResponse.statusCode == 200 {
                let loginResponse = try JSONDecoder().decode(AuthLoginResponse.self, from: data)
                await MainActor.run {
                    self.accessToken = loginResponse.accessToken
                    self.refreshToken = loginResponse.refreshToken
                    self.isAuthenticated = true
                    self.errorMessage = nil
                    saveTokensToKeychain()
                }

                await fetchCurrentUser()
            } else {
                let errorResponse = try? JSONDecoder().decode(ErrorResponse.self, from: data)
                await MainActor.run {
                    errorMessage = errorResponse?.detail ?? "Login failed"
                }
            }
        } catch {
            await MainActor.run {
                errorMessage = "Network error: \(error.localizedDescription)"
            }
        }
    }

    func logout() {
        accessToken = nil
        refreshToken = nil
        currentUser = nil
        isAuthenticated = false
        clearTokensFromKeychain()
    }

    private func fetchCurrentUser() async {
        guard let token = accessToken,
              let url = URL(string: "\(AppConfig.apiBaseURL)\(AppConfig.Endpoints.me)") else {
            return
        }

        var request = URLRequest(url: url)
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")

        do {
            let (data, _) = try await session.data(for: request)
            let user = try JSONDecoder().decode(User.self, from: data)
            await MainActor.run {
                self.currentUser = user
            }
        } catch {
            print("Failed to fetch current user: \(error)")
        }
    }

    func getAccessToken() -> String? {
        return accessToken
    }

    private func loadTokensFromKeychain() {

    }

    private func saveTokensToKeychain() {

    }

    private func clearTokensFromKeychain() {

    }
}

struct AuthLoginResponse: Codable {
    let accessToken: String
    let refreshToken: String
    let tokenType: String

    enum CodingKeys: String, CodingKey {
        case accessToken = "access_token"
        case refreshToken = "refresh_token"
        case tokenType = "token_type"
    }
}

struct ErrorResponse: Codable {
    let detail: String
}
