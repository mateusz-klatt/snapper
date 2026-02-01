import Foundation
import Combine

class AuthService: ObservableObject {
    static let shared = AuthService()

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
            await MainActor.run {
                errorMessage = "Invalid URL"
            }
            return
        }

        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue(AppConfig.ContentType.formURLEncoded, forHTTPHeaderField: AppConfig.HTTPHeader.contentType)

        let bodyString = "username=\(username)&password=\(password)"
        request.httpBody = bodyString.data(using: .utf8)

        do {
            let (data, response) = try await session.data(for: request)

            guard let httpResponse = response as? HTTPURLResponse else {
                await MainActor.run { errorMessage = "Invalid response" }
                return
            }

            if httpResponse.statusCode == 200 {
                let decoder = JSONDecoder()
                decoder.dateDecodingStrategy = .iso8601
                let loginResponse = try decoder.decode(LoginResponse.self, from: data)
                await MainActor.run {
                    self.currentUser = loginResponse.user
                    self.isAuthenticated = true
                    self.errorMessage = nil
                }

                await refreshTokens()
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
        wsToken = nil
        currentUser = nil
        isAuthenticated = false
    }

    func getWsToken() -> String? {
        return wsToken
    }

    private func refreshTokens() async {
        guard let url = URL(string: "\(AppConfig.apiBaseURL)\(AppConfig.Endpoints.refresh)") else {
            return
        }

        var request = URLRequest(url: url)
        request.httpMethod = "POST"

        do {
            let (data, response) = try await session.data(for: request)

            guard let httpResponse = response as? HTTPURLResponse,
                  httpResponse.statusCode == 200 else {
                return
            }

            let decoder = JSONDecoder()
            decoder.dateDecodingStrategy = .iso8601
            let refreshResponse = try decoder.decode(RefreshResponse.self, from: data)
            await MainActor.run {
                self.wsToken = refreshResponse.wsToken
            }
        } catch {
            print("Failed to refresh tokens: \(error)")
        }
    }
}

struct ErrorResponse: Codable {
    let detail: String
}
