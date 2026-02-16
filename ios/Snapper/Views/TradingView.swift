import SwiftUI
import os

struct TradingView: View {
    private let logger = Logger(subsystem: Bundle.main.bundleIdentifier ?? "Snapper", category: "Trading")
    @State private var orders: [OrderStatus] = []
    @State private var positions: [PositionSnapshot] = []
    @State private var signals: [TradingSignal] = []
    @State private var isLoading = false
    @State private var errorMessage: String?

    var body: some View {
        NavigationView {
            List {
                Section("Positions") {
                    if positions.isEmpty {
                        Text("No open positions")
                            .foregroundColor(.secondary)
                    } else {
                        ForEach(positions, id: \.id) { position in
                            PositionRowView(position: position)
                        }
                    }
                }

                Section("Orders") {
                    if orders.isEmpty {
                        Text("No orders")
                            .foregroundColor(.secondary)
                    } else {
                        ForEach(orders, id: \.id) { order in
                            OrderRowView(order: order)
                        }
                    }
                }

                Section("Signals") {
                    if signals.isEmpty {
                        Text("No recent signals")
                            .foregroundColor(.secondary)
                    } else {
                        ForEach(signals, id: \.id) { signal in
                            SignalRowView(signal: signal)
                        }
                    }
                }
            }
            .listStyle(.insetGrouped)
            .navigationTitle("Trading")
            .refreshable {
                await loadData()
            }
        }
        .task {
            await loadData()
        }
    }

    private func loadData() async {
        isLoading = true
        errorMessage = nil

        async let ordersResult = APIClient.shared.fetchOrders()
        async let positionsResult = APIClient.shared.fetchPositions()
        async let signalsResult = APIClient.shared.fetchSignals()

        do {
            orders = try await ordersResult
        } catch {
            logger.error("Failed to fetch orders: \(error)")
        }

        do {
            positions = try await positionsResult
        } catch {
            logger.error("Failed to fetch positions: \(error)")
        }

        do {
            signals = try await signalsResult
        } catch {
            logger.error("Failed to fetch signals: \(error)")
        }

        isLoading = false
    }
}

struct PositionRowView: View {
    let position: PositionSnapshot

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack {
                Text(position.instrument)
                    .font(.headline)
                Spacer()
                Text(position.quantity > 0 ? "Long" : "Short")
                    .font(.caption)
                    .foregroundColor(position.quantity > 0 ? .profitGreen : .lossRed)
            }
            HStack {
                Text("Qty: \(position.quantity, specifier: "%.4f")")
                Spacer()
                Text("Avg: \(position.averagePrice, specifier: "%.2f")")
            }
            .font(.caption)
            .foregroundColor(.secondary)
            HStack {
                Text("P&L: \(position.unrealizedPnl, specifier: "%.2f")")
                    .foregroundColor(position.unrealizedPnl >= 0 ? .profitGreen : .lossRed)
            }
            .font(.caption)
        }
        .padding(.vertical, 2)
    }
}

struct OrderRowView: View {
    let order: OrderStatus

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack {
                Text(order.instrument)
                    .font(.headline)
                Spacer()
                Text(order.status)
                    .font(.caption)
                    .padding(.horizontal, 6)
                    .padding(.vertical, 2)
                    .background(Color.brandRed.opacity(0.2))
                    .cornerRadius(4)
            }
            HStack {
                Text("\(order.side) \(order.type)")
                Spacer()
                Text("Size: \(order.size, specifier: "%.4f")")
            }
            .font(.caption)
            .foregroundColor(.secondary)
        }
        .padding(.vertical, 2)
    }
}

struct SignalRowView: View {
    let signal: TradingSignal

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack {
                Text(signal.instrument)
                    .font(.headline)
                Spacer()
                Text(signal.side)
                    .font(.caption)
                    .foregroundColor(signal.side == "buy" ? .profitGreen : .lossRed)
            }
            Text(signal.reason)
                .font(.caption)
                .foregroundColor(.secondary)
        }
        .padding(.vertical, 2)
    }
}

struct TradingView_Previews: PreviewProvider {
    static var previews: some View {
        TradingView()
    }
}
