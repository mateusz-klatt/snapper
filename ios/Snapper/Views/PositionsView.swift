import SwiftUI
import os

/// Display-only positions list (iOS-3).
///
/// Reduce / close mutations are deferred to a follow-up plan per the
/// plan v1.15 §S4 lock — this surface is read-only.
///
/// Wallet scoping: backend now exposes ``wallet_public_id`` on the
/// ``PositionData`` projection, so the list narrows to
/// ``AppState.selectedWalletPublicId`` whenever a wallet is picked.
/// Rows whose ``walletPublicId`` is ``nil`` (legacy / pre-projection
/// rows) pass through so the UI never silently drops data — mirrors
/// the policy ``OrdersView.walletMatches`` uses for the same edge.
struct PositionsView: View {
    @Environment(AppState.self) private var appState
    @State private var positions: [PositionSnapshot] = []
    @State private var isLoading = false
    @State private var loadError: APIError?

    private let logger = Logger(
        subsystem: Bundle.main.bundleIdentifier ?? "Snapper",
        category: "PositionsView"
    )

    var body: some View {
        NavigationStack {
            Group {
                if isLoading && filteredPositions.isEmpty {
                    ProgressView("Loading positions…")
                } else if filteredPositions.isEmpty {
                    ContentUnavailableView(
                        "No positions",
                        systemImage: "chart.line.flattrend.xyaxis",
                        description: Text("Open positions will appear here.")
                    )
                } else {
                    List(filteredPositions, id: \.publicId) { position in
                        PositionCard(position: position)
                    }
                    .listStyle(.insetGrouped)
                    .scrollContentBackground(.hidden)
                    .background(Color.bgBase)
                    .refreshable { await load() }
                }
            }
            .navigationTitle("Positions")
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    WalletPicker()
                }
            }
        }
        .task(id: appState.selectedWalletPublicId) {
            await load()
        }
    }

    var filteredPositions: [PositionSnapshot] {
        return Self.filter(
            positions: positions,
            selectedWalletPublicId: appState.selectedWalletPublicId
        )
    }

    /// Pure wallet-match helper extracted for unit testing — mirrors
    /// the policy in ``OrdersView.walletMatches``: ``nil`` selection
    /// passes through every row, and ``nil`` row-side wallet passes
    /// through so legacy / system rows are never silently dropped.
    static func walletMatches(rowWalletId: String?, selected: String?) -> Bool {
        guard let selected else { return true }
        guard let rowWalletId else { return true }
        return rowWalletId == selected
    }

    static func filter(
        positions: [PositionSnapshot],
        selectedWalletPublicId: String?
    ) -> [PositionSnapshot] {
        return positions.filter { position in
            walletMatches(
                rowWalletId: position.walletPublicId,
                selected: selectedWalletPublicId
            )
        }
    }

    private func load() async {
        isLoading = true
        defer { isLoading = false }
        do {
            positions = try await APIClient.shared.fetchPositions()
        } catch let error as APIError {
            loadError = error
            logger.error("Failed to fetch positions: \(error.localizedDescription)")
        } catch {
            loadError = .invalidResponse
            logger.error("Failed to fetch positions: \(error.localizedDescription)")
        }
    }
}

/// Single-row presentation of a ``PositionData`` projection.
struct PositionCard: View {
    let position: PositionSnapshot

    var body: some View {
        HStack(spacing: 12) {
            VStack(alignment: .leading, spacing: 4) {
                Text(position.instrument)
                    .font(.headline)
                Text(directionLabel)
                    .font(.caption)
                    .foregroundStyle(directionColor)
            }
            Spacer()
            VStack(alignment: .trailing, spacing: 4) {
                Text(String(format: "%.2f", position.unrealizedPnl))
                    .font(.body.monospaced())
                    .foregroundStyle(position.unrealizedPnl >= 0 ? Color.profitGreen : Color.lossRed)
                Text(String(format: "Avg %.4f", position.averagePrice))
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
        }
        .padding(.vertical, 4)
    }

    /// Pure helper used both in the body and unit tests so the
    /// quantity → side derivation has explicit coverage.
    static func direction(for quantity: Double) -> String {
        if quantity > 0 { return "Long" }
        if quantity < 0 { return "Short" }
        return "Flat"
    }

    private var directionLabel: String {
        return "\(Self.direction(for: position.quantity)) · \(String(format: "%.4f", position.quantity))"
    }

    private var directionColor: Color {
        if position.quantity > 0 { return .profitGreen }
        if position.quantity < 0 { return .lossRed }
        return .secondary
    }
}

struct PositionsView_Previews: PreviewProvider {
    static var previews: some View {
        PositionsView()
            .environment(AppState.shared)
    }
}
