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
    @State private var presentingNewOrder = false
    @State private var pendingCancelOrder: OrderStatus?
    @State private var submitError: String?

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
                                .swipeActions(edge: .trailing, allowsFullSwipe: false) {
                                    Button(role: .destructive) {
                                        pendingCancelOrder = order
                                    } label: {
                                        Label("Cancel", systemImage: "xmark.circle")
                                    }
                                    .disabled(order.planPublicId == nil)
                                }
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
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Button {
                        presentingNewOrder = true
                    } label: {
                        Label("New order", systemImage: "plus.circle")
                    }
                    .disabled(resolvedWallet == nil)
                }
            }
        }
        .task(id: appState.selectedWalletPublicId) {
            await load()
        }
        .sheet(isPresented: $presentingNewOrder) {
            if let wallet = resolvedWallet {
                NewOrderSheet(
                    exchanges: derivedExchanges,
                    walletPublicId: wallet.publicId,
                    walletIsPaper: wallet.isPaper,
                    onSubmit: { body in
                        await submitNewOrder(body: body)
                    }
                )
            }
        }
        .alert(
            "Cancel order?",
            isPresented: Binding(
                get: { pendingCancelOrder != nil },
                set: { if !$0 { pendingCancelOrder = nil } }
            ),
            presenting: pendingCancelOrder
        ) { order in
            Button("Cancel order", role: .destructive) {
                Task { await submitCancel(order: order) }
            }
            Button("Keep open", role: .cancel) {
                pendingCancelOrder = nil
            }
        } message: { order in
            Text("\(order.instrument) \(order.side.uppercased()) \(String(format: "%.4f", order.size)) @ \(order.price.map { String(format: "%.4f", $0) } ?? "market") will be cancelled at the venue.")
        }
        .alert(
            "Submission failed",
            isPresented: Binding(
                get: { submitError != nil },
                set: { if !$0 { submitError = nil } }
            ),
            presenting: submitError
        ) { _ in
            Button("OK", role: .cancel) {
                submitError = nil
            }
        } message: { error in
            Text(error)
        }
    }

    /// Resolves the user's selected wallet against the live
    /// ``appState.availableWallets`` cache. A persisted public id
    /// pointing at a wallet that no longer exists (revoked,
    /// deleted, scope-handed-over) falls through to ``nil`` so the
    /// `+` button gate disables instead of presenting a sheet that
    /// cannot build a valid order body.
    var resolvedWallet: WalletInfo? {
        guard let id = appState.selectedWalletPublicId else { return nil }
        return appState.availableWallets.first { $0.publicId == id }
    }

    /// Unique exchange identifiers derived from the orders + executions
    /// already loaded into the view. Backend has no dedicated
    /// "list venues" endpoint mounted on iOS yet, so the picker
    /// bootstraps from the data already on screen — falls back to
    /// `["kraken"]` so the form is still usable on first load.
    var derivedExchanges: [String] {
        var seen: Set<String> = []
        var ordered: [String] = []
        for value in orders.map(\.exchange) + executions.map(\.exchange) {
            if seen.insert(value).inserted {
                ordered.append(value)
            }
        }
        return ordered.isEmpty ? ["kraken"] : ordered
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

    private func submitNewOrder(body: CreateOrderBody) async {
        do {
            let command = NewOrderSheet.makeCommand(body: body)
            _ = try await APIClient.shared.createOrder(command: command)
            await load()
        } catch {
            logger.error("Failed to submit new order: \(error.localizedDescription)")
            submitError = "Couldn't submit the order. Try again."
        }
    }

    private func submitCancel(order: OrderStatus) async {
        guard let planId = order.planPublicId else {
            submitError = "This order has no execution plan to cancel."
            return
        }
        do {
            _ = try await APIClient.shared.cancelOrder(planPublicId: planId)
            await load()
        } catch {
            logger.error("Failed to cancel order: \(error.localizedDescription)")
            submitError = "Couldn't cancel the order. Try again."
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
