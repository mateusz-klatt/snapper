import SwiftUI
import os

/// Orders + executions list, segmented by lifecycle state (iOS-3).
///
/// Picker tabs:
/// - Open: orders whose ``status`` is one of the live-lifecycle
///   members of the backend ``OrderStatusEnum`` (`new`, `submitted`,
///   `open`, `partially_filled`).
/// - Recent: every order the wallet selection grants visibility to,
///   newest-first, capped at 50 rows for paging discipline.
/// - Fills: ``ExecutionData`` rows from ``GET /api/executions``.
///
/// Wallet filter: orders + executions both expose
/// ``walletPublicId``, so the list is scoped to
/// ``AppState.selectedWalletPublicId`` whenever a wallet is picked.
/// Rows whose ``walletPublicId`` is ``nil`` (legacy / system rows
/// without an owner) pass through so the UI never silently drops
/// data.
struct OrdersView: View {
    @Environment(AppState.self) private var appState
    @State private var segment: OrdersSegment = .open
    @State private var orders: [OrderStatus] = []
    @State private var executions: [ExecutionRecord] = []
    @State private var isLoading = false
    @State private var errorMessage: String?

    private let logger = Logger(
        subsystem: Bundle.main.bundleIdentifier ?? "Snapper",
        category: "OrdersView"
    )

    var body: some View {
        NavigationStack {
            VStack(spacing: 0) {
                Picker("Segment", selection: $segment) {
                    ForEach(OrdersSegment.allCases) { seg in
                        Text(seg.title).tag(seg)
                    }
                }
                .pickerStyle(.segmented)
                .padding()

                List {
                    switch segment {
                    case .open:
                        ForEach(filteredOpen, id: \.publicId) { order in
                            OrderRow(order: order)
                        }
                    case .recent:
                        ForEach(filteredRecent, id: \.publicId) { order in
                            OrderRow(order: order)
                        }
                    case .fills:
                        ForEach(filteredFills, id: \.publicId) { execution in
                            ExecutionRow(execution: execution)
                        }
                    }
                }
                .listStyle(.insetGrouped)
                .scrollContentBackground(.hidden)
                .background(Color.bgBase)
                .refreshable { await load() }
            }
            .navigationTitle("Orders")
        }
        .task(id: appState.selectedWalletPublicId) {
            await load()
        }
    }

    var filteredOpen: [OrderStatus] {
        return Self.filterOpen(
            orders: orders,
            selectedWalletPublicId: appState.selectedWalletPublicId
        )
    }

    var filteredRecent: [OrderStatus] {
        return Self.filterRecent(
            orders: orders,
            selectedWalletPublicId: appState.selectedWalletPublicId
        )
    }

    var filteredFills: [ExecutionRecord] {
        return Self.filterFills(
            executions: executions,
            selectedWalletPublicId: appState.selectedWalletPublicId
        )
    }

    /// Backend-canonical "open" lifecycle states. Values mirror
    /// ``snapper.core.types.OrderStatusEnum`` members that have not
    /// reached a terminal state (``filled``, ``cancelled``,
    /// ``rejected``).
    static let openStatuses: Set<String> = ["new", "submitted", "open", "partially_filled"]

    static func isOpen(status: String) -> Bool {
        return openStatuses.contains(status)
    }

    static func walletMatches(rowWalletId: String?, selected: String?) -> Bool {
        guard let selected else { return true }
        guard let rowWalletId else { return true }
        return rowWalletId == selected
    }

    static func filterOpen(
        orders: [OrderStatus],
        selectedWalletPublicId: String?
    ) -> [OrderStatus] {
        return orders.filter { order in
            isOpen(status: order.status)
                && walletMatches(rowWalletId: order.walletPublicId, selected: selectedWalletPublicId)
        }
    }

    static func filterRecent(
        orders: [OrderStatus],
        selectedWalletPublicId: String?,
        limit: Int = 50
    ) -> [OrderStatus] {
        let scoped = orders.filter { order in
            walletMatches(rowWalletId: order.walletPublicId, selected: selectedWalletPublicId)
        }
        let sorted = scoped.sorted { $0.createdAt > $1.createdAt }
        return Array(sorted.prefix(limit))
    }

    static func filterFills(
        executions: [ExecutionRecord],
        selectedWalletPublicId: String?
    ) -> [ExecutionRecord] {
        return executions.filter { execution in
            walletMatches(
                rowWalletId: execution.walletPublicId,
                selected: selectedWalletPublicId
            )
        }
    }

    private func load() async {
        isLoading = true
        defer { isLoading = false }

        async let ordersResult = APIClient.shared.fetchOrders()
        async let executionsResult = APIClient.shared.fetchExecutions()

        do {
            orders = try await ordersResult
        } catch {
            logger.error("Failed to fetch orders: \(error)")
            errorMessage = error.localizedDescription
        }

        do {
            executions = try await executionsResult
        } catch {
            logger.error("Failed to fetch executions: \(error)")
        }
    }
}

enum OrdersSegment: String, CaseIterable, Identifiable {
    case open
    case recent
    case fills

    var id: String { rawValue }

    var title: String {
        switch self {
        case .open: return "Open"
        case .recent: return "Recent"
        case .fills: return "Fills"
        }
    }
}

private struct OrderRow: View {
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
                    .background(statusBackgroundColor)
                    .cornerRadius(4)
            }
            HStack {
                Text("\(order.side) \(order.orderType)")
                Spacer()
                Text(String(format: "Size: %.4f", order.size))
            }
            .font(.caption)
            .foregroundColor(.secondary)
            if let price = order.price {
                Text(String(format: "Price: %.4f", price))
                    .font(.caption2)
                    .foregroundColor(.secondary)
            }
        }
        .padding(.vertical, 2)
    }

    private var statusBackgroundColor: Color {
        switch order.status {
        case "filled":
            return .brandGreen.opacity(0.2)
        case "cancelled", "rejected":
            return .lossRed.opacity(0.2)
        case "partially_filled":
            return .orange.opacity(0.2)
        default:
            return .brandRed.opacity(0.2)
        }
    }
}

private struct ExecutionRow: View {
    let execution: ExecutionRecord

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack {
                Text(execution.instrument)
                    .font(.headline)
                Spacer()
                Text(execution.side)
                    .font(.caption)
                    .foregroundColor(execution.side == "buy" ? .profitGreen : .lossRed)
            }
            HStack {
                Text(String(format: "Filled %.4f @ %.4f", execution.lastSize, execution.lastPrice))
                Spacer()
                Text(String(format: "Fee: %.4f %@", execution.fee, execution.feeAsset))
            }
            .font(.caption)
            .foregroundColor(.secondary)
        }
        .padding(.vertical, 2)
    }
}

struct OrdersView_Previews: PreviewProvider {
    static var previews: some View {
        OrdersView()
            .environment(AppState.shared)
    }
}
