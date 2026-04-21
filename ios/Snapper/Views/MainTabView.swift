import SwiftUI

struct MainTabView: View {
    @EnvironmentObject var webSocketManager: WebSocketManager
    @EnvironmentObject var authService: AuthService

    var body: some View {
        TabView {
            if authService.canAccess("overview") {
                DashboardView()
                    .tabItem {
                        Label("Dashboard", systemImage: "chart.bar.fill")
                    }
            }

            if authService.canAccess("orders") {
                TradingView()
                    .tabItem {
                        Label("Trading", systemImage: "arrow.left.arrow.right")
                    }
            }

            if authService.canAccess("overview") {
                SettingsView()
                    .tabItem {
                        Label("Settings", systemImage: "gearshape.fill")
                    }
            }
        }
        // WS lifecycle is owned by `SnapperApp` (scenePhase + isAuthenticated
        // observers per plan §D8). Putting connect/disconnect here would
        // kill the socket whenever a modal sheet covered the tab view.
    }
}

struct MainTabView_Previews: PreviewProvider {
    static var previews: some View {
        MainTabView()
            .environmentObject(WebSocketManager.shared)
            .environmentObject(AuthService.shared)
    }
}
