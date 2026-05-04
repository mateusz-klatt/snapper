import SwiftUI

/// Modal sheet to attach a trailing-stop plan to an open position
/// cycle. Mirrors the web frontend's ``AttachTrailingStopModal`` at
/// `frontend/src/features/positions/AttachTrailingStopModal.tsx`.
///
/// Backend route: ``POST /api/trailing-stops`` carrying
/// ``TrailingStopCreateCommand`` whose payload links to
/// ``position_cycle_public_id`` and supplies ``trailing_pct``
/// (required) plus an optional ``min_lock_pct`` floor that prevents
/// the trail from moving below a locked-in profit threshold.
struct AttachTrailingStopSheet: View {
    let position: PositionSnapshot
    let onSubmit: (Double, Double?, String) async -> Void

    @Environment(\.dismiss) private var dismiss
    @State private var trailingPctText: String = ""
    @State private var minLockPctText: String = ""
    @State private var isSubmitting = false

    /// Idempotency token minted once per sheet presentation. Stable
    /// across in-sheet retries so a network failure plus user re-tap
    /// dedups at the backend instead of creating two trailing-stop plans.
    @State private var idempotencyKey: String = UUID().uuidString

    private var parsedTrailing: Double? {
        return Self.parsePercent(trailingPctText)
    }

    private var parsedMinLock: Double? {
        return Self.parsePercent(minLockPctText)
    }

    private var canSubmit: Bool {
        return Self.canSubmit(
            trailingPct: parsedTrailing,
            minLockPct: parsedMinLock,
            isSubmitting: isSubmitting
        )
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
                }
                Section {
                    HStack {
                        Text("Trailing distance")
                        Spacer()
                        TextField("1.5", text: $trailingPctText)
                            .keyboardType(.decimalPad)
                            .multilineTextAlignment(.trailing)
                            .frame(minWidth: 80)
                        Text("%").foregroundStyle(.secondary)
                    }
                    HStack {
                        Text("Min lock-in")
                        Spacer()
                        TextField("optional", text: $minLockPctText)
                            .keyboardType(.decimalPad)
                            .multilineTextAlignment(.trailing)
                            .frame(minWidth: 80)
                        Text("%").foregroundStyle(.secondary)
                    }
                } header: {
                    Text("Trailing stop")
                } footer: {
                    Text("Trailing distance is the gap the stop maintains as price moves favourably. Min lock-in (optional) prevents the stop from moving below that profit threshold once reached.")
                }
            }
            .navigationTitle("Attach trailing stop")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Submit") {
                        guard !isSubmitting, let trailing = parsedTrailing else { return }
                        let minLock = parsedMinLock
                        let key = idempotencyKey
                        isSubmitting = true
                        Task {
                            defer { isSubmitting = false }
                            await onSubmit(trailing, minLock, key)
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

    /// Parse a free-text percent into a positive `Double`. Empty /
    /// non-numeric / non-positive input maps to `nil` so the caller
    /// treats it as "not supplied" rather than zero — the backend
    /// rejects zero-distance trails.
    static func parsePercent(_ text: String) -> Double? {
        let trimmed = text.trimmingCharacters(in: .whitespaces)
        guard !trimmed.isEmpty else { return nil }
        let normalized = trimmed.replacingOccurrences(of: ",", with: ".")
        guard let value = Double(normalized), value > 0 else { return nil }
        return value
    }

    /// Submit gate: trailing distance present (the only required
    /// payload field) AND not already in flight.
    static func canSubmit(trailingPct: Double?, minLockPct: Double?, isSubmitting: Bool) -> Bool {
        _ = minLockPct
        guard !isSubmitting else { return false }
        return trailingPct != nil
    }
}

extension AttachTrailingStopSheet {
    /// Build the iOS-side ``TrailingStopCreateCommand`` envelope.
    @MainActor
    static func makeCommand(
        positionCyclePublicId: String,
        trailingPct: Double,
        minLockPct: Double?,
        idempotencyKey: String,
        provenance: EnvelopeMinter.Provenance? = nil
    ) -> TrailingStopCreateCommand {
        let envelope = provenance ?? EnvelopeMinter.shared.next(.control)
        return TrailingStopCreateCommand(
            type: "create_trailing_stop_command",
            sequenceId: envelope.sequenceId,
            publicId: envelope.publicId,
            timestamp: envelope.timestamp,
            sessionId: envelope.sessionId,
            topic: nil,
            payload: TrailingStopCreateBody(
                positionCyclePublicId: positionCyclePublicId,
                trailingPct: trailingPct,
                minLockPct: minLockPct,
                idempotencyKey: idempotencyKey
            )
        )
    }
}
