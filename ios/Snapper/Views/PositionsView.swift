import SwiftUI
import os

/// Wrapper that pairs a ``PositionSnapshot`` with an ``Identifiable``
/// conformance for SwiftUI ``sheet(item:)`` driving without
/// retroactively conforming the generated ``PositionData`` type.
private struct IdentifiedPosition: Identifiable {
    let position: PositionSnapshot

    var id: String { position.publicId }
}

/// Positions list with reduce / close actions (iOS-Position-Mutations).
///
/// Wallet scoping: backend exposes ``wallet_public_id`` on the
/// ``PositionData`` projection, so the list narrows to
/// ``AppState.selectedWalletPublicId`` whenever a wallet is picked.
/// Rows whose ``walletPublicId`` is ``nil`` (legacy / pre-projection
/// rows) pass through so the UI never silently drops data — mirrors
/// the policy ``OrdersView.walletMatches`` uses for the same edge.
///
/// Mutations: tap a row to open an ActionSheet with two options.
///
/// - "Close position": confirms then submits a reduce-only market
///   order with the full ``abs(position.quantity)`` against the
///   opposite side.
/// - "Reduce position": opens ``ReducePositionView`` with a slider
///   bounded by the absolute quantity. Submits a reduce-only
///   market order with the chosen partial size.
///
/// Both actions go through the existing ``POST /api/orders`` route
/// with ``reduceOnly=true`` — no new backend route. After submit the
/// list refetches so the resulting (smaller / zero) position
/// reflects in the UI on the next bus tick.
struct PositionsView: View {
    @Environment(AppState.self) private var appState
    @State private var positions: [PositionSnapshot] = []
    @State private var isLoading = false
    @State private var loadError: APIError?
    @State private var actionSheetPosition: PositionSnapshot?
    @State private var reduceModalPosition: IdentifiedPosition?
    @State private var bracketModalPosition: IdentifiedPosition?
    @State private var trailingStopModalPosition: IdentifiedPosition?
    @State private var pendingClosePosition: PositionSnapshot?
    @State private var submitError: String?

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
                        Button {
                            actionSheetPosition = position
                        } label: {
                            PositionCard(position: position)
                        }
                        .buttonStyle(.plain)
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
        .confirmationDialog(
            actionSheetPosition.map { "\($0.instrument) on \($0.exchange)" } ?? "",
            isPresented: Binding(
                get: { actionSheetPosition != nil },
                set: { if !$0 { actionSheetPosition = nil } }
            ),
            presenting: actionSheetPosition
        ) { position in
            if Self.canSubmitReduce(position: position) {
                Button("Close position", role: .destructive) {
                    pendingClosePosition = position
                }
                Button("Reduce position") {
                    reduceModalPosition = IdentifiedPosition(position: position)
                }
            }
            if position.positionCyclePublicId != nil {
                Button("Attach SL / TP") {
                    bracketModalPosition = IdentifiedPosition(position: position)
                }
                Button("Attach trailing stop") {
                    trailingStopModalPosition = IdentifiedPosition(position: position)
                }
            }
            Button("Cancel", role: .cancel) {
                actionSheetPosition = nil
            }
        } message: { position in
            Text(
                "\(PositionCard.direction(for: position.quantity)) \(String(format: "%.4f", position.quantity)) @ \(String(format: "%.4f", position.averagePrice))"
            )
        }
        .alert(
            "Close \(pendingClosePosition?.instrument ?? "position")?",
            isPresented: Binding(
                get: { pendingClosePosition != nil },
                set: { if !$0 { pendingClosePosition = nil } }
            ),
            presenting: pendingClosePosition
        ) { position in
            Button("Close", role: .destructive) {
                Task { await submitMarketReduce(position: position, quantity: abs(position.quantity)) }
            }
            Button("Cancel", role: .cancel) {
                pendingClosePosition = nil
            }
        } message: { position in
            Text(
                "Submits a reduce-only market order for \(String(format: "%.4f", abs(position.quantity))) \(position.instrument)."
            )
        }
        .sheet(item: $reduceModalPosition) { wrapper in
            ReducePositionView(
                position: wrapper.position,
                onSubmit: { quantity in
                    await submitMarketReduce(position: wrapper.position, quantity: quantity)
                }
            )
        }
        .sheet(item: $bracketModalPosition) { wrapper in
            AttachBracketSheet(
                position: wrapper.position,
                onSubmit: { slPrice, tpPrice, idempotencyKey in
                    await submitBracket(
                        position: wrapper.position,
                        slPrice: slPrice,
                        tpPrice: tpPrice,
                        idempotencyKey: idempotencyKey
                    )
                }
            )
        }
        .sheet(item: $trailingStopModalPosition) { wrapper in
            AttachTrailingStopSheet(
                position: wrapper.position,
                onSubmit: { trailingPct, minLockPct, idempotencyKey in
                    await submitTrailingStop(
                        position: wrapper.position,
                        trailingPct: trailingPct,
                        minLockPct: minLockPct,
                        idempotencyKey: idempotencyKey
                    )
                }
            )
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

    private func submitMarketReduce(position: PositionSnapshot, quantity: Double) async {
        guard let command = Self.makeReduceCommand(position: position, quantity: quantity) else {
            logger.error("Refusing to submit reduce/close: position missing instrument or wallet public id")
            submitError = "Position is missing the wallet or instrument identifier the backend requires. Try refreshing the positions list."
            return
        }
        do {
            _ = try await APIClient.shared.createOrder(command: command)
            await load()
        } catch {
            logger.error("Failed to submit reduce/close: \(error.localizedDescription)")
            submitError = "Couldn't submit the order. Try again."
        }
    }

    /// Reduce / close eligibility: backend rejects ``CreateOrderBody``
    /// with empty ``instrumentPublicId`` or ``walletPublicId`` so the
    /// UI must hide the trading actions for legacy / system rows that
    /// arrive without those ids.
    static func canSubmitReduce(position: PositionSnapshot) -> Bool {
        guard
            let walletId = position.walletPublicId, !walletId.isEmpty,
            let instrumentId = position.instrumentPublicId, !instrumentId.isEmpty
        else {
            return false
        }
        return true
    }

    private func submitBracket(
        position: PositionSnapshot,
        slPrice: Double?,
        tpPrice: Double?,
        idempotencyKey: String
    ) async {
        guard let cycleId = position.positionCyclePublicId else {
            submitError = "Position has no active cycle to attach a bracket to."
            return
        }
        do {
            let command = AttachBracketSheet.makeCommand(
                positionCyclePublicId: cycleId,
                slPrice: slPrice,
                tpPrice: tpPrice,
                idempotencyKey: idempotencyKey
            )
            _ = try await APIClient.shared.createBracket(command: command)
            await load()
        } catch {
            logger.error("Failed to submit bracket: \(error.localizedDescription)")
            submitError = "Couldn't attach bracket. Try again."
        }
    }

    private func submitTrailingStop(
        position: PositionSnapshot,
        trailingPct: Double,
        minLockPct: Double?,
        idempotencyKey: String
    ) async {
        guard let cycleId = position.positionCyclePublicId else {
            submitError = "Position has no active cycle to attach a trailing stop to."
            return
        }
        do {
            let command = AttachTrailingStopSheet.makeCommand(
                positionCyclePublicId: cycleId,
                trailingPct: trailingPct,
                minLockPct: minLockPct,
                idempotencyKey: idempotencyKey
            )
            _ = try await APIClient.shared.createTrailingStop(command: command)
            await load()
        } catch {
            logger.error("Failed to submit trailing stop: \(error.localizedDescription)")
            submitError = "Couldn't attach trailing stop. Try again."
        }
    }

    /// Build the iOS-side ``CreateOrderCommand`` envelope for a
    /// reduce-only market order on the given position.
    ///
    /// The order side is the opposite of the position direction:
    /// long position (positive quantity) -> ``"sell"``; short
    /// (negative) -> ``"buy"``. ``reduceOnly`` is always true so a
    /// venue cannot accidentally flip the position into the
    /// opposite direction.
    @MainActor
    static func makeReduceCommand(
        position: PositionSnapshot,
        quantity: Double,
        provenance: EnvelopeMinter.Provenance? = nil
    ) -> CreateOrderCommand? {
        guard
            let walletId = position.walletPublicId, !walletId.isEmpty,
            let instrumentId = position.instrumentPublicId, !instrumentId.isEmpty
        else {
            return nil
        }
        let envelope = provenance ?? EnvelopeMinter.shared.next(.control)
        let side: String = position.quantity > 0 ? "sell" : "buy"
        return CreateOrderCommand(
            type: "create_order_command",
            sequenceId: envelope.sequenceId,
            publicId: envelope.publicId,
            timestamp: envelope.timestamp,
            sessionId: envelope.sessionId,
            topic: nil,
            payload: CreateOrderBody(
                instrument: position.instrument,
                instrumentPublicId: instrumentId,
                exchange: position.exchange,
                mode: position.mode,
                side: side,
                orderType: "market",
                quantity: quantity,
                price: nil,
                stopPrice: nil,
                timeInForce: "GTC",
                postOnly: false,
                leverage: nil,
                reduceOnly: true,
                walletPublicId: walletId,
                operatorPublicId: nil,
                idempotencyKey: nil,
                aiReviewPublicId: nil
            )
        )
    }
}

/// Modal sheet for partial-reduce of a position
/// (iOS-Position-Mutations).
///
/// Renders a slider bounded by ``[0, abs(position.quantity)]`` plus
/// preset buttons (25/50/75/100 %) for fast common reductions. The
/// ``onSubmit`` closure is fired with the chosen quantity and
/// dismisses the sheet on success — the parent ``PositionsView``
/// handles the actual ``APIClient.createOrder`` round trip + error
/// surface.
struct ReducePositionView: View {
    let position: PositionSnapshot
    let onSubmit: (Double) async -> Void

    @Environment(\.dismiss) private var dismiss
    @State private var quantity: Double
    @State private var isSubmitting = false

    init(
        position: PositionSnapshot,
        onSubmit: @escaping (Double) async -> Void
    ) {
        self.position = position
        self.onSubmit = onSubmit
        _quantity = State(initialValue: abs(position.quantity) * 0.5)
    }

    private var maxQuantity: Double {
        return abs(position.quantity)
    }

    var body: some View {
        NavigationStack {
            Form {
                Section("Position") {
                    HStack {
                        Text(position.instrument)
                            .font(.headline)
                        Spacer()
                        Text("\(PositionCard.direction(for: position.quantity)) \(String(format: "%.4f", position.quantity))")
                            .foregroundStyle(.secondary)
                    }
                    HStack {
                        Text("Average price")
                        Spacer()
                        Text(String(format: "%.4f", position.averagePrice))
                            .foregroundStyle(.secondary)
                    }
                }
                Section {
                    HStack {
                        Text("Reduce by")
                        Spacer()
                        Text(String(format: "%.4f", quantity))
                            .font(.body.monospaced())
                    }
                    Slider(value: $quantity, in: 0...maxQuantity)
                    HStack {
                        ForEach([0.25, 0.5, 0.75, 1.0], id: \.self) { ratio in
                            Button(String(format: "%.0f%%", ratio * 100)) {
                                quantity = maxQuantity * ratio
                            }
                            .buttonStyle(.bordered)
                            .frame(maxWidth: .infinity)
                        }
                    }
                } header: {
                    Text("Quantity")
                } footer: {
                    Text("100% closes the position. Submits a reduce-only market order against the opposite side.")
                }
            }
            .navigationTitle("Reduce position")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Submit") {
                        Task {
                            isSubmitting = true
                            await onSubmit(quantity)
                            isSubmitting = false
                            dismiss()
                        }
                    }
                    .disabled(quantity <= 0 || isSubmitting)
                }
            }
            .overlay {
                if isSubmitting {
                    ProgressView().controlSize(.large)
                }
            }
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
