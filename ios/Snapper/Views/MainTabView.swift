import SwiftUI

struct MainTabView: View {
    @EnvironmentObject var webSocketManager: WebSocketManager
    @EnvironmentObject var authService: AuthService
    @EnvironmentObject var navigationCoordinator: NavigationCoordinator

    @State private var selectedTab: String = "dashboard"

    var body: some View {
        TabView(selection: $selectedTab) {
            if authService.canAccess("overview") {
                DashboardView()
                    .tabItem {
                        Label("Dashboard", systemImage: "chart.bar.fill")
                    }
                    .tag("dashboard")
            }

            if authService.canAccess("orders") {
                TradingView()
                    .tabItem {
                        Label("Trading", systemImage: "arrow.left.arrow.right")
                    }
                    .tag("trading")
            }

            if authService.hasPermission(.readNotifications) {
                AlertsView()
                    .tabItem {
                        Label("Alerts", systemImage: "bell.fill")
                    }
                    .tag("alerts")
            }

            if authService.canAccess("overview") {
                SettingsView()
                    .tabItem {
                        Label("Settings", systemImage: "gearshape.fill")
                    }
                    .tag("settings")
            }
        }
        .onChange(of: navigationCoordinator.pendingDeepLink) { _, path in
            handleDeepLink(path: path)
        }
        // WS lifecycle is owned by `SnapperApp` (scenePhase + isAuthenticated
        // observers per plan §D8). Putting connect/disconnect here would
        // kill the socket whenever a modal sheet covered the tab view.
    }

    /// Route the coordinator's pending deep-link to the correct tab.
    ///
    /// Path prefixes map 1:1 to tabs:
    /// - ``/orders*`` → Trading
    /// - ``/alerts*`` → Alerts
    /// - ``/positions*`` / ``/system*`` → Dashboard (fallback until
    ///   iOS-3 introduces a dedicated Positions tab)
    /// - anything else → leave current tab untouched
    ///
    /// ``AlertsView`` does its own scroll-to-anchor once the tab is
    /// active, so this method only owns tab selection. Clearing the
    /// pending deep-link is the target view's responsibility — it
    /// fires after the scroll lands.
    private func handleDeepLink(path: String?) {
        guard let path else { return }
        if path.hasPrefix(AppConfig.Endpoints.alerts) {
            selectedTab = "alerts"
        } else if path.hasPrefix(AppConfig.Endpoints.orders) {
            selectedTab = "trading"
        } else if path.hasPrefix(AppConfig.Endpoints.positions)
            || path.hasPrefix(AppConfig.Endpoints.system)
        {
            selectedTab = "dashboard"
        }
    }
}

struct MainTabView_Previews: PreviewProvider {
    static var previews: some View {
        MainTabView()
            .environmentObject(WebSocketManager.shared)
            .environmentObject(AuthService.shared)
            .environmentObject(NavigationCoordinator.shared)
    }
}
