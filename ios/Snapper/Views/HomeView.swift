import SwiftUI
import os

struct HomeView: View {
    private let logger = Logger(subsystem: Bundle.main.bundleIdentifier ?? "Snapper", category: "Home")
    @EnvironmentObject var webSocketManager: WebSocketManager
    @Environment(AppState.self) private var appState
    @State private var systemStatus: SystemStatus?
    @State private var positions: [PositionSnapshot] = []
    @State private var orders: [OrderStatus] = []
    @State private var latestAlert: AlertEventInfo?
    @State private var isLoading = true
    @State private var errorMessage: String?

    var body: some View {
        NavigationView {
            ScrollView {
                VStack(spacing: 20) {
                    HStack {
                        WalletPicker()
                        Spacer()
                    }

                    LatestAlertCard(alert: latestAlert)

                    connectionStatusView

                    if let status = systemStatus {
                        systemStatusView(status: status)
                    } else if isLoading {
                        ProgressView("Loading status...")
                            .padding()
                    } else if let error = errorMessage {
                        Text("Error: \(error)")
                            .foregroundColor(.red)
                            .padding()
                    }

                    if !positions.isEmpty {
                        positionsView
                    }

                    if !orders.isEmpty {
                        recentOrdersView
                    }
                }
                .padding()
            }
            .background(Color.bgBase)
            .navigationTitle("Home")
            .refreshable {
                await loadData()
                await loadLatestAlert()
            }
        }
        .task {
            await loadData()
        }
        .task(id: appState.selectedWalletPublicId) {
            await loadLatestAlert()
        }
    }

    /// Pure helper extracted for unit testing — returns the first
    /// element of an alert history page, or ``nil`` for an empty
    /// page. The card collapses to ``EmptyView`` on ``nil`` so the
    /// Home layout stays compact when the user has no alerts yet.
    static func firstAlert(from history: AlertHistoryResponse) -> AlertEventInfo? {
        return history.payload.first
    }

    private func loadLatestAlert() async {
        do {
            let history = try await APIClient.shared.fetchAlertHistory(limit: 1)
            latestAlert = Self.firstAlert(from: history)
        } catch {
            logger.error("Failed to fetch latest alert: \(error)")
        }
    }

    private var connectionStatusView: some View {
        // TimelineView re-evaluates every 1s so heartbeat freshness color
        // transitions green → amber → red as time passes between frames
        // even when no new @Published update fires from `webSocketManager`.
        TimelineView(.periodic(from: .now, by: 1)) { context in
            HStack {
                Circle()
                    .fill(connectionColor(at: context.date))
                    .frame(width: 8, height: 8)

                Text(connectionText(at: context.date))
                    .font(.caption)
                    .foregroundColor(.textSecondary)

                Spacer()
            }
            .padding(.horizontal)
        }
    }

    /// Heartbeat-freshness thresholds for the connection-health badge.
    /// Green: heartbeat within 5s. Amber: 5–30s. Red: >30s or never arrived
    /// while the socket is already `.connected` (i.e. healthy connection,
    /// stale heartbeats — distinct from socket-level disconnect).
    private static let heartbeatFreshThreshold: TimeInterval = 5
    private static let heartbeatStaleThreshold: TimeInterval = 30

    private func connectionColor(at now: Date) -> Color {
        switch webSocketManager.connectionState {
        case .connected:
            return heartbeatColor(at: now)
        case .connecting, .authenticating:
            return .orange
        case .disconnected, .error, .authFailed:
            return .brandRed
        }
    }

    /// When the socket is `.connected`, the UI reflects heartbeat freshness
    /// rather than socket state alone — a connected-but-silent publisher
    /// surfaces as amber/red so operators notice stale data.
    private func heartbeatColor(at now: Date) -> Color {
        guard let last = webSocketManager.state.lastHeartbeatAt else {
            return .orange
        }
        let age = now.timeIntervalSince(last)
        if age <= Self.heartbeatFreshThreshold {
            return .brandGreen
        } else if age <= Self.heartbeatStaleThreshold {
            return .orange
        } else {
            return .brandRed
        }
    }

    private func connectionText(at now: Date) -> String {
        switch webSocketManager.connectionState {
        case .connected:
            if let last = webSocketManager.state.lastHeartbeatAt {
                let age = Int(now.timeIntervalSince(last))
                return "Connected · last heartbeat \(age)s ago"
            }
            return "Connected · waiting for heartbeat"
        case .connecting:
            return "Connecting..."
        case .authenticating:
            return "Authenticating..."
        case .disconnected:
            return "Disconnected"
        case .error(let message):
            return "Error: \(message)"
        case .authFailed(let message):
            return "Auth failed: \(message)"
        }
    }

    private func systemStatusView(status: SystemStatus) -> some View {
        VStack(spacing: 16) {
            HStack {
                Text("Trader Status")
                    .font(.headline)
                Spacer()
                Text(status.trader.status)
                    .font(.caption)
                    .padding(.horizontal, 8)
                    .padding(.vertical, 4)
                    .background(status.trader.status == "running" ? Color.brandGreen.opacity(0.2) : Color.brandRed.opacity(0.2))
                    .cornerRadius(4)
            }

            if let strategies = status.strategies, !strategies.isEmpty {
                Divider()
                HStack {
                    Text("Active Strategies")
                        .font(.subheadline)
                    Spacer()
                    Text("\(strategies.count)")
                        .font(.subheadline)
                        .fontWeight(.semibold)
                }
            }

            Divider()

            LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 16) {
                statView(title: "Open Positions", value: "\(positions.count)")
                statView(title: "Active Orders", value: "\(orders.filter { $0.status == "open" || $0.status == "pending" }.count)")
            }
        }
        .padding()
        .background(Color.bgSurface)
        .cornerRadius(12)
    }

    private func statView(title: String, value: String, color: Color = .textPrimary) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(title)
                .font(.caption)
                .foregroundColor(.textSecondary)

            Text(value)
                .font(.headline)
                .foregroundColor(color)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    private var positionsView: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Open Positions")
                .font(.headline)

            ForEach(positions, id: \.id) { position in
                HStack {
                    VStack(alignment: .leading, spacing: 4) {
                        Text(position.instrument)
                            .font(.subheadline)
                            .fontWeight(.medium)

                        Text(String(format: "Qty: %.4f @ %.2f", position.quantity, position.averagePrice))
                            .font(.caption)
                            .foregroundColor(.secondary)
                    }

                    Spacer()

                    VStack(alignment: .trailing, spacing: 4) {
                        Text(String(format: "$%.2f", position.unrealizedPnl))
                            .font(.subheadline)
                            .foregroundColor(position.unrealizedPnl >= 0 ? .profitGreen : .lossRed)

                        Text("Unrealized P&L")
                            .font(.caption2)
                            .foregroundColor(.secondary)
                    }
                }
                .padding(.vertical, 8)
                Divider()
            }
        }
        .padding()
        .background(Color.bgSurface)
        .cornerRadius(12)
    }

    private var recentOrdersView: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Recent Orders")
                .font(.headline)

            ForEach(orders.prefix(5), id: \.id) { order in
                HStack {
                    VStack(alignment: .leading, spacing: 4) {
                        Text(order.instrument)
                            .font(.subheadline)
                            .fontWeight(.medium)

                        Text(String(format: "%@ %.4f", order.side.uppercased(), order.size))
                            .font(.caption)
                            .foregroundColor(.secondary)
                    }

                    Spacer()

                    Text(order.status)
                        .font(.caption)
                        .padding(.horizontal, 8)
                        .padding(.vertical, 4)
                        .background(statusColor(for: order.status))
                        .foregroundColor(.white)
                        .cornerRadius(4)
                }
                .padding(.vertical, 8)
                Divider()
            }
        }
        .padding()
        .background(Color.bgSurface)
        .cornerRadius(12)
    }

    private func loadData() async {
        isLoading = true
        errorMessage = nil

        async let statusResult = APIClient.shared.fetchSystemStatus()
        async let positionsResult = APIClient.shared.fetchPositions()
        async let ordersResult = APIClient.shared.fetchOrders()

        do {
            systemStatus = try await statusResult
        } catch {
            errorMessage = error.localizedDescription
        }

        do {
            positions = try await positionsResult
        } catch {
            logger.error("Failed to fetch positions: \(error)")
        }

        do {
            orders = try await ordersResult
        } catch {
            logger.error("Failed to fetch orders: \(error)")
        }

        isLoading = false
    }

    private func statusColor(for status: String) -> Color {
        switch status.lowercased() {
        case "filled":
            return .brandGreen
        case "pending", "open":
            return .brandRed
        case "cancelled", "rejected":
            return .lossRed
        case "partially_filled":
            return .orange
        default:
            return .textSecondary
        }
    }
}

struct HomeView_Previews: PreviewProvider {
    static var previews: some View {
        HomeView()
            .environmentObject(WebSocketManager.shared)
            .environment(AppState.shared)
    }
}
