import Foundation
import Combine

@MainActor
class LoginViewModel: ObservableObject {
    @Published var username = ""
    @Published var password = ""
    @Published var isLoading = false
    @Published var errorMessage: String?
    @Published var isAuthenticated = false

    private let authService: AuthService
    private var cancellables = Set<AnyCancellable>()

    init(authService: AuthService = .shared) {
        self.authService = authService

        authService.$isAuthenticated
            .assign(to: &$isAuthenticated)

        authService.$errorMessage
            .assign(to: &$errorMessage)
    }

    var isLoginEnabled: Bool {
        !username.isEmpty && !password.isEmpty && !isLoading
    }

    func login() async {
        isLoading = true
        errorMessage = nil

        await authService.login(username: username, password: password)

        isLoading = false
    }
}
