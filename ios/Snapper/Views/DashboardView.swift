import SwiftUI

struct DashboardView: View {
    @EnvironmentObject var webSocketManager: WebSocketManager
    @State private var portfolio: Portfolio?
    @State private var isLoading = true
    @State private var errorMessage: String?

    var body: some View {
        NavigationView {
            ScrollView {
                VStack(spacing: 20) {

                    connectionStatusView

                    if let portfolio = portfolio {
                        portfolioSummaryView(portfolio: portfolio)
                    } else if isLoading {
                        ProgressView("Loading portfolio...")
                            .padding()
                    } else if let error = errorMessage {
                        Text("Error: \(error)")
                            .foregroundColor(.red)
                            .padding()
                    }

                    if !webSocketManager.marketData.isEmpty {
                        marketDataView
                    }

                    if !webSocketManager.orderUpdates.isEmpty {
                        recentOrdersView
                    }
                }
                .padding()
            }
            .navigationTitle("Dashboard")
            .refreshable {
                await loadPortfolio()
            }
        }
        .task {
            await loadPortfolio()

            webSocketManager.subscribeToMarketData(symbols: ["BTCUSD", "ETHUSD", "PLNUSD"])
            webSocketManager.subscribeToOrders()
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
        case .connecting:
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
        case .disconnected:
            return "Disconnected"
        case .error(let message):
            return "Error: \(message)"
        }
    }

    private func portfolioSummaryView(portfolio: Portfolio) -> some View {
        VStack(spacing: 16) {

            VStack(spacing: 4) {
                Text("Total Value")
                    .font(.caption)
                    .foregroundColor(.secondary)

                Text(String(format: "$%.2f", portfolio.totalValue))
                    .font(.system(size: 36, weight: .bold))
            }

            HStack {
                VStack(alignment: .leading) {
                    Text("Today's P&L")
                        .font(.caption)
                        .foregroundColor(.secondary)

                    Text(String(format: "$%.2f", portfolio.todayPnL))
                        .font(.title3)
                        .fontWeight(.semibold)
                        .foregroundColor(portfolio.todayPnL >= 0 ? .green : .red)
                }

                Spacer()

                Text(String(format: "%@%.2f%%", portfolio.todayPnLPercent >= 0 ? "+" : "", portfolio.todayPnLPercent))
                    .font(.title3)
                    .fontWeight(.semibold)
                    .foregroundColor(portfolio.todayPnLPercent >= 0 ? .green : .red)
            }

            Divider()

            LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 16) {
                statView(title: "Cash", value: String(format: "$%.2f", portfolio.cashBalance))
                statView(title: "Positions", value: String(format: "$%.2f", portfolio.positionsValue))
                statView(title: "Unrealized P&L", value: String(format: "$%.2f", portfolio.unrealizedPnL), color: portfolio.unrealizedPnL >= 0 ? .green : .red)
                statView(title: "Realized P&L", value: String(format: "$%.2f", portfolio.realizedPnL), color: portfolio.realizedPnL >= 0 ? .green : .red)
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

    private var marketDataView: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Market Data")
                .font(.headline)

            ForEach(Array(webSocketManager.marketData.values.prefix(5)), id: \.symbol) { data in
                HStack {
                    Text(data.symbol)
                        .font(.subheadline)
                        .fontWeight(.medium)

                    Spacer()

                    Text(String(format: "$%.2f", data.price))
                        .font(.subheadline)

                    if let volume = data.volume {
                        Text(String(format: "Vol: %.0f", volume))
                            .font(.caption)
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

            ForEach(webSocketManager.orderUpdates.prefix(5)) { update in
                HStack {
                    VStack(alignment: .leading, spacing: 4) {
                        Text(update.symbol)
                            .font(.subheadline)
                            .fontWeight(.medium)

                        Text(String(format: "%@ %.4f", update.side.uppercased(), update.quantity))
                            .font(.caption)
                            .foregroundColor(.secondary)
                    }

                    Spacer()

                    Text(update.status)
                        .font(.caption)
                        .padding(.horizontal, 8)
                        .padding(.vertical, 4)
                        .background(statusColor(for: update.status))
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

    private func loadPortfolio() async {
        isLoading = true
        errorMessage = nil

        do {
            portfolio = try await APIClient.shared.fetchPortfolio()
            isLoading = false
        } catch {
            errorMessage = error.localizedDescription
            isLoading = false
        }
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
