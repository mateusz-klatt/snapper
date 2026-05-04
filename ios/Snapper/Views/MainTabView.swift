import SwiftUI

/// 5-tab root after iOS-3: Home, Positions, Orders, Alerts, Settings.
///
/// Permission gating per tab:
/// - Home: ``canAccess("overview")``.
/// - Positions: ``readPositions`` / ``managePositions`` (every role
///   above ``aiDelegate`` already grants ``readPositions``).
/// - Orders: ``canAccess("orders")``.
/// - Alerts: ``readNotifications`` (preserves the iOS-4 push gating).
/// - Settings: ``canAccess("overview")`` — every authenticated role
///   per the permission catalog (preserves the iOS-5 contract that
///   any user can self-manage push notifications + device).
///
/// Deep-link routing pulls a pending path off
/// ``NavigationCoordinator.pendingDeepLink`` and selects the matching
/// tab. The mapping was generalised from the iOS-4 contract so the
/// dedicated Positions tab gets ``/positions*``, Home picks up
/// ``/system*``, and the existing ``/alerts*`` / ``/orders*`` rules
/// stay byte-identical to keep the APNs tap → AlertsView
/// scroll-to-anchor flow working.
struct MainTabView: View {
    @EnvironmentObject var webSocketManager: WebSocketManager
    @EnvironmentObject var authService: AuthService
    @EnvironmentObject var navigationCoordinator: NavigationCoordinator
    @Environment(AppState.self) private var appState

    @State private var selectedTab: String = "home"

    var body: some View {
        TabView(selection: $selectedTab) {
            if authService.canAccess("overview") {
                HomeView()
                    .tabItem {
                        Label("Home", systemImage: "house.fill")
                    }
                    .tag("home")
            }

            if authService.hasPermission(.readPositions) {
                PositionsView()
                    .tabItem {
                        Label("Positions", systemImage: "chart.line.uptrend.xyaxis")
                    }
                    .tag("positions")
            }

            if authService.canAccess("orders") {
                OrdersView()
                    .tabItem {
                        Label("Orders", systemImage: "arrow.left.arrow.right")
                    }
                    .tag("orders")
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
            if let nextTab = Self.routeDeepLink(
                path: path,
                alertsPrefix: AppConfig.Endpoints.alerts,
                ordersPrefix: AppConfig.Endpoints.orders,
                positionsPrefix: AppConfig.Endpoints.positions,
                systemPrefix: AppConfig.Endpoints.system
            ) {
                selectedTab = nextTab
            }
        }
        // WS lifecycle is owned by `SnapperApp` (scenePhase + isAuthenticated
        // observers). Putting connect/disconnect here would kill the
        // socket whenever a modal sheet covered the tab view.
    }

    /// Deep-link routing path-prefix mapping (post-iOS-3):
    /// - ``/alerts*`` → ``alerts`` (UNCHANGED, preserves iOS-4
    ///   AlertsView scroll-to-anchor contract).
    /// - ``/orders*`` → ``orders`` (renamed from "trading").
    /// - ``/positions*`` → ``positions`` (now its own tab — pre-iOS-3
    ///   this fell back to Dashboard).
    /// - ``/system*`` → ``home`` (renamed from "dashboard").
    /// - anything else → ``nil``, leaving the current tab unchanged.
    ///
    /// Extracted as a pure function so the routing table is unit
    /// tested without rendering the SwiftUI body — ViewInspector is
    /// not available in this project.
    static func routeDeepLink(
        path: String?,
        alertsPrefix: String,
        ordersPrefix: String,
        positionsPrefix: String,
        systemPrefix: String
    ) -> String? {
        guard let path else { return nil }
        if path.hasPrefix(alertsPrefix) {
            return "alerts"
        }
        if path.hasPrefix(ordersPrefix) {
            return "orders"
        }
        if path.hasPrefix(positionsPrefix) {
            return "positions"
        }
        if path.hasPrefix(systemPrefix) {
            return "home"
        }
        return nil
    }
}

struct MainTabView_Previews: PreviewProvider {
    static var previews: some View {
        MainTabView()
            .environmentObject(WebSocketManager.shared)
            .environmentObject(AuthService.shared)
            .environmentObject(NavigationCoordinator.shared)
            .environment(AppState.shared)
    }
}
