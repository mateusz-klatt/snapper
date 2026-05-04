import SwiftUI
import os

/// Modal sheet to attach a stop-loss / take-profit bracket to an
/// open position cycle. Mirrors the web frontend's
/// ``AttachBracketModal`` at
/// `frontend/src/features/positions/AttachBracketModal.tsx`.
///
/// Backend route: ``POST /api/execution-plans`` carrying
/// ``BracketCreateCommand`` whose payload links to
/// ``position_cycle_public_id``. Backend rejects the request when
/// the cycle is already closed or when neither ``sl_price`` nor
/// ``tp_price`` is supplied.
///
/// Either price is optional individually — but the UI requires at
/// least one before enabling the submit button so the user does not
/// fire an empty bracket request that the backend would reject with
/// HTTP 400.
struct AttachBracketSheet: View {
    let position: PositionSnapshot
    let onSubmit: (Double?, Double?, String) async -> Void

    @Environment(\.dismiss) private var dismiss
    @State private var slPriceText: String = ""
    @State private var tpPriceText: String = ""
    @State private var isSubmitting = false

    /// Idempotency token minted once per sheet presentation. Stable
    /// across in-sheet retries so a network failure plus user re-tap
    /// dedups at the backend instead of creating two protective brackets.
    /// Re-presenting the sheet (cancel/dismiss/re-open) gets a fresh
    /// state value and a fresh key. Mirrors ``NewOrderSheet``.
    @State private var idempotencyKey: String = UUID().uuidString

    private var parsedSL: Double? {
        return Self.parsePrice(slPriceText)
    }

    private var parsedTP: Double? {
        return Self.parsePrice(tpPriceText)
    }

    private var canSubmit: Bool {
        return Self.canSubmit(slPrice: parsedSL, tpPrice: parsedTP, isSubmitting: isSubmitting)
    }

    var body: some View {
        NavigationStack {
            Form {
                Section("Position") {
                    HStack {
                        Text(position.instrument).font(.headline)
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
                        Text("Stop loss")
                        Spacer()
                        TextField("0.00", text: $slPriceText)
                            .keyboardType(.decimalPad)
                            .multilineTextAlignment(.trailing)
                            .frame(minWidth: 100)
                    }
                    HStack {
                        Text("Take profit")
                        Spacer()
                        TextField("0.00", text: $tpPriceText)
                            .keyboardType(.decimalPad)
                            .multilineTextAlignment(.trailing)
                            .frame(minWidth: 100)
                    }
                } header: {
                    Text("Bracket")
                } footer: {
                    Text("At least one of stop loss / take profit is required. Backend fires reduce-only protective orders when price crosses either level.")
                }
            }
            .navigationTitle("Attach bracket")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Submit") {
                        guard !isSubmitting else { return }
                        let sl = parsedSL
                        let tp = parsedTP
                        let key = idempotencyKey
                        isSubmitting = true
                        Task {
                            defer { isSubmitting = false }
                            await onSubmit(sl, tp, key)
                            dismiss()
                        }
                    }
                    .disabled(!canSubmit)
                }
            }
            .overlay {
                if isSubmitting {
                    ProgressView().controlSize(.large)
                }
            }
        }
    }

    /// Parses a free-text price string into a positive `Double`.
    /// Empty / non-numeric / non-positive input maps to `nil` so
    /// the caller treats it as "not supplied" rather than zero —
    /// the backend's bracket validator rejects zero-priced legs.
    static func parsePrice(_ text: String) -> Double? {
        let trimmed = text.trimmingCharacters(in: .whitespaces)
        guard !trimmed.isEmpty else { return nil }
        let normalized = trimmed.replacingOccurrences(of: ",", with: ".")
        guard let value = Double(normalized), value > 0 else { return nil }
        return value
    }

    /// Submit gate: at least one leg parsed AND not already in flight.
    /// Mirrors backend `BracketCreateBody` validator at
    /// ``snapper.api.schemas.commands`` requiring at least one of
    /// ``sl_price`` / ``tp_price``.
    static func canSubmit(slPrice: Double?, tpPrice: Double?, isSubmitting: Bool) -> Bool {
        guard !isSubmitting else { return false }
        return slPrice != nil || tpPrice != nil
    }
}

extension AttachBracketSheet {
    /// Build the iOS-side ``BracketCreateCommand`` envelope. The
    /// backend handler at ``snapper.server.execution_plan_routes``
    /// validates the cycle is open + caller has wallet access
    /// before spawning the protective plan.
    @MainActor
    static func makeCommand(
        positionCyclePublicId: String,
        slPrice: Double?,
        tpPrice: Double?,
        idempotencyKey: String,
        provenance: EnvelopeMinter.Provenance? = nil
    ) -> BracketCreateCommand {
        let envelope = provenance ?? EnvelopeMinter.shared.next(.control)
        return BracketCreateCommand(
            type: "create_bracket_command",
            sequenceId: envelope.sequenceId,
            publicId: envelope.publicId,
            timestamp: envelope.timestamp,
            sessionId: envelope.sessionId,
            topic: nil,
            payload: BracketCreateBody(
                positionCyclePublicId: positionCyclePublicId,
                slPrice: slPrice,
                tpPrice: tpPrice,
                idempotencyKey: idempotencyKey
            )
        )
    }
}
