import Foundation

/// Mints provenance envelope fields (`public_id`, `session_id`,
/// `sequence_id`, `timestamp`) for outbound REST and WebSocket
/// messages, mirroring the contract enforced backend-side by
/// ``snapper.api.schemas.base.StrictDataSchema``.
///
/// Mirrors the bridge's `EnvelopeMinter` at
/// `integrations/snapper-mcp/src/envelope.ts` and the frontend's
/// `stampProvenance` at `frontend/src/lib/apiClient.ts`:
/// - `session_id` is minted once per app launch (in-memory).
///   Persisting only `session_id` without counters would create
///   duplicate or gap-filled sequence numbers under the same
///   session — backend gap detection would flag the client.
/// - `sequence_id` is per-counter monotonic. "control" frames
///   (auth, subscribe, REST commands) and "telemetry" frames
///   (ping/heartbeat) carry independent counters so a flood of
///   telemetry does not advance the control sequence.
/// - `public_id` is a fresh UUID v4 per request. Backend tolerates
///   v4 (existing iOS commands ship v4 today); v7 sortability is
///   not required at this layer.
///
/// `@MainActor` isolation gives synchronous `next()` access from
/// SwiftUI builders (`make*Command()`), `WebSocketManager` (already
/// `@MainActor`), and `AuthService` (already `@MainActor`). The
/// only off-main caller is `DeviceRegistrationService` (an actor)
/// which hops via `await MainActor.run` before minting.
@MainActor
final class EnvelopeMinter {
    static let shared = EnvelopeMinter()

    enum Counter {
        case control
        case telemetry
    }

    /// Provenance bundle stamped onto outbound envelopes.
    ///
    /// Carries both `timestamp: Date` for `JSONEncoder`-driven REST
    /// paths (which serialize the `Date` via the encoder's date
    /// strategy) AND a pre-formatted `timestampString` for WS
    /// frames built as `[String: Any]` and shipped through
    /// `JSONSerialization` (which cannot serialize `Date` directly).
    /// Both forms are millisecond-precision ISO 8601 to match the
    /// frontend's `new Date().toISOString()`.
    struct Provenance: Sendable {
        let publicId: String
        let sessionId: String
        let sequenceId: Int
        let timestamp: Date
        let timestampString: String
    }

    private static let iso8601: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }()

    static func formatTimestamp(_ date: Date) -> String {
        return iso8601.string(from: date)
    }

    private let sessionIdValue: String
    private var controlCounter: Int = 0
    private var telemetryCounter: Int = 0

    init(sessionId: String = UUID().uuidString) {
        self.sessionIdValue = sessionId
    }

    var sessionId: String {
        return sessionIdValue
    }

    func next(_ counter: Counter, now: Date = Date()) -> Provenance {
        let sequence: Int
        switch counter {
        case .control:
            controlCounter += 1
            sequence = controlCounter
        case .telemetry:
            telemetryCounter += 1
            sequence = telemetryCounter
        }
        return Provenance(
            publicId: UUID().uuidString,
            sessionId: sessionIdValue,
            sequenceId: sequence,
            timestamp: now,
            timestampString: Self.formatTimestamp(now)
        )
    }
}
