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
        request.setValue(AppConfig.ContentType.json, forHTTPHeaderField: AppConfig.HTTPHeader.contentType)

        let body = ["username": username, "password": password]
        request.httpBody = try? JSONSerialization.data(withJSONObject: body)

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
                    self.errorMessage = nil
                }

                await refreshTokens()

                await MainActor.run {
                    self.isAuthenticated = true
                }
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

    func fetchFreshWsToken() async -> String? {
        guard let url = URL(string: "\(AppConfig.apiBaseURL)\(AppConfig.Endpoints.refresh)") else {
            return nil
        }

        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue(AppConfig.ContentType.json, forHTTPHeaderField: AppConfig.HTTPHeader.contentType)

        do {
            let (data, response) = try await session.data(for: request)

            guard let httpResponse = response as? HTTPURLResponse,
                  httpResponse.statusCode == 200 else {
                return nil
            }

            let decoder = JSONDecoder()
            decoder.dateDecodingStrategy = .iso8601
            let refreshResponse = try decoder.decode(RefreshResponse.self, from: data)
            await MainActor.run {
                self.wsToken = refreshResponse.wsToken
            }
            return refreshResponse.wsToken
        } catch {
            print("Failed to fetch fresh ws_token: \(error)")
            return nil
        }
    }

    private func refreshTokens() async {
        guard let url = URL(string: "\(AppConfig.apiBaseURL)\(AppConfig.Endpoints.refresh)") else {
            return
        }

        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue(AppConfig.ContentType.json, forHTTPHeaderField: AppConfig.HTTPHeader.contentType)

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
