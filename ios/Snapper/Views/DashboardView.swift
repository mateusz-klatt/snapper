import SwiftUI
import os

struct DashboardView: View {
    private let logger = Logger(subsystem: Bundle.main.bundleIdentifier ?? "Snapper", category: "Dashboard")
    @EnvironmentObject var webSocketManager: WebSocketManager
    @State private var systemStatus: SystemStatus?
    @State private var positions: [PositionSnapshot] = []
    @State private var orders: [OrderStatus] = []
    @State private var isLoading = true
    @State private var errorMessage: String?

    var body: some View {
        NavigationView {
            ScrollView {
                VStack(spacing: 20) {

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
            .navigationTitle("Dashboard")
            .refreshable {
                await loadData()
            }
        }
        .task {
            await loadData()
        }
    }

    private var connectionStatusView: some View {
        HStack {
            Circle()
                .fill(connectionColor)
                .frame(width: 8, height: 8)

            Text(connectionText)
                .font(.caption)
                .foregroundColor(.secondary)

            Spacer()
        }
        .padding(.horizontal)
    }

    private var connectionColor: Color {
        switch webSocketManager.connectionState {
        case .connected:
            return .green
        case .connecting, .authenticating:
            return .orange
        case .disconnected, .error:
            return .red
        }
    }

    private var connectionText: String {
        switch webSocketManager.connectionState {
        case .connected:
            return "Connected"
        case .connecting:
            return "Connecting..."
        case .authenticating:
            return "Authenticating..."
        case .disconnected:
            return "Disconnected"
        case .error(let message):
            return "Error: \(message)"
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
                    .background(status.trader.status == "running" ? Color.green.opacity(0.2) : Color.red.opacity(0.2))
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
        .background(Color(uiColor: .systemGray6))
        .cornerRadius(12)
    }

    private func statView(title: String, value: String, color: Color = .primary) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(title)
                .font(.caption)
                .foregroundColor(.secondary)

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
                            .foregroundColor(position.unrealizedPnl >= 0 ? .green : .red)

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
        .background(Color(uiColor: .systemGray6))
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
        .background(Color(uiColor: .systemGray6))
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
            return .green
        case "pending", "open":
            return .blue
        case "cancelled", "rejected":
            return .red
        case "partially_filled":
            return .orange
        default:
            return .gray
        }
    }
}

struct DashboardView_Previews: PreviewProvider {
    static var previews: some View {
        DashboardView()
            .environmentObject(WebSocketManager.shared)
    }
}
