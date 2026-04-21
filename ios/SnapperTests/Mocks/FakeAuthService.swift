import Foundation
@testable import Snapper

/// Actor-based `AuthRefreshing` stub for `WebSocketManager` + `APIClient`
/// tests. Actor isolation is natural here — both protocol methods are
/// `async`, and test reads (`fetchCalls`, `logoutCalls`) are awaitable.
actor FakeAuthService: AuthRefreshing {
    var fetchCalls: Int = 0
    var logoutCalls: Int = 0
    var nextToken: String?

    init(nextToken: String? = nil) {
        self.nextToken = nextToken
    }

    func setNextToken(_ token: String?) {
        self.nextToken = token
    }

    func fetchFreshWsToken() async -> String? {
        fetchCalls += 1
        return nextToken
    }

    func logout() async {
        logoutCalls += 1
    }
}
