import SwiftUI

@main
struct SnapperApp: App {
    @StateObject private var authService = AuthService.shared
    @StateObject private var webSocketManager = WebSocketManager.shared

    var body: some Scene {
        WindowGroup {
            Group {
                if authService.isAuthenticated {
                    MainTabView()
                        .environmentObject(authService)
                        .environmentObject(webSocketManager)
                } else {
                    LoginView()
                        .environmentObject(authService)
                }
            }
            .tint(.brandRed)
        }
    }
}
