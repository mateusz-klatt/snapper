import SwiftUI

@main
struct SnapperApp: App {
    @UIApplicationDelegateAdaptor(AppDelegate.self) private var appDelegate
    @StateObject private var authService = AuthService.shared
    @StateObject private var webSocketManager = WebSocketManager.shared
    @StateObject private var notificationService = NotificationService.shared
    @StateObject private var navigationCoordinator = NavigationCoordinator.shared
    @Environment(\.scenePhase) private var scenePhase

    var body: some Scene {
        WindowGroup {
            Group {
                if authService.isAuthenticated {
                    MainTabView()
                        .environmentObject(authService)
                        .environmentObject(webSocketManager)
                        .environmentObject(notificationService)
                        .environmentObject(navigationCoordinator)
                } else {
                    LoginView()
                        .environmentObject(authService)
                }
            }
            .environment(AppState.shared)
            .tint(.brandGreen)
            .onChange(of: scenePhase) { _, newPhase in
                handleScenePhase(newPhase)
            }
            .onChange(of: authService.isAuthenticated) { _, isAuth in
                handleAuthChange(isAuth)
            }
        }
    }

    /// Connect on foreground / disconnect on background. Matches the
    /// iOS lifecycle: the socket must not hold the radio while the
    /// app is suspended.
    private func handleScenePhase(_ phase: ScenePhase) {
        switch phase {
        case .active:
            if authService.isAuthenticated {
                webSocketManager.connect()
            }
        case .background:
            webSocketManager.disconnect()
        case .inactive:
            break
        @unknown default:
            break
        }
    }

    /// Login flips `isAuthenticated` to true while the app is already
    /// `.active` — scenePhase doesn't fire, so we need a second
    /// observer to kick the WS connect. Logout is the mirror case.
    /// `disconnect()` is idempotent so the compound "logout +
    /// background" path is safe.
    private func handleAuthChange(_ isAuth: Bool) {
        if isAuth {
            webSocketManager.connect()
        } else {
            webSocketManager.disconnect()
        }
    }
}
