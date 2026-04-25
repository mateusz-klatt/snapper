import SwiftUI

/// Compact "latest alert" card on the Home tab (iOS-2).
///
/// Displays the single most recent ``AlertEventInfo`` — typically
/// fetched via ``APIClient.fetchAlertHistory(limit: 1)`` from
/// ``HomeView`` on appear and on wallet selection change. When the
/// alert is ``nil`` (no history yet, or fetch failed silently) the
/// card collapses to ``EmptyView`` so the Home layout shifts up
/// instead of leaving a placeholder.
///
/// Vocabulary mirrors ``AlertRow`` (title / body / timestamp) so the
/// summary on Home stays consistent with the full Alerts tab list.
struct LatestAlertCard: View {
    let alert: AlertEventInfo?

    var body: some View {
        if let alert {
            VStack(alignment: .leading, spacing: 8) {
                HStack(spacing: 6) {
                    Image(systemName: "bell.badge.fill")
                        .foregroundStyle(.tint)
                    Text("Latest alert")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    Spacer()
                    Text(Self.relativeTimestamp(for: alert.timestamp))
                        .font(.caption2)
                        .foregroundStyle(.tertiary)
                }
                Text(alert.title)
                    .font(.callout)
                    .fontWeight(.semibold)
                    .lineLimit(1)
                Text(alert.body)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(2)
            }
            .padding(12)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(Color(.systemGray6))
            .clipShape(RoundedRectangle(cornerRadius: 12))
            .overlay(alignment: .leading) {
                if alert.isSafetyCritical {
                    RoundedRectangle(cornerRadius: 12)
                        .stroke(.red, lineWidth: 1)
                }
            }
        } else {
            EmptyView()
        }
    }

    /// Relative-time helper extracted as a static so it can be unit
    /// tested independently of the SwiftUI view body.
    static func relativeTimestamp(for date: Date, relativeTo reference: Date = Date()) -> String {
        let formatter = RelativeDateTimeFormatter()
        formatter.unitsStyle = .short
        return formatter.localizedString(for: date, relativeTo: reference)
    }
}
