import SwiftUI

/// Single alert row for the Alerts tab.
///
/// Renders the four user-visible fields —
/// `alert_type` as a badge (colour-coded by priority),
/// `title`, `body`, and the relative-time `timestamp`.
///
/// Safety-critical alerts get a subtle red accent border so users
/// can scan the list for urgent items without reading every row.
/// The row is purely presentational — selection / deep-linking is
/// handled by the parent `AlertsView` via `onTapGesture`.
struct AlertRow: View {
    let alert: AlertEventInfo

    var body: some View {
        HStack(alignment: .top, spacing: 12) {
            priorityBadge
            VStack(alignment: .leading, spacing: 4) {
                Text(alert.title)
                    .font(.headline)
                    .foregroundStyle(.primary)
                Text(alert.body)
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
                    .lineLimit(3)
                Text(relativeTimestamp)
                    .font(.caption)
                    .foregroundStyle(.tertiary)
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }
        .padding(.vertical, 8)
        .overlay(alignment: .leading) {
            if alert.isSafetyCritical {
                Rectangle()
                    .fill(.red)
                    .frame(width: 3)
                    .padding(.vertical, 4)
            }
        }
        .accessibilityElement(children: .combine)
        .accessibilityLabel("\(alert.title). \(alert.body). \(alert.priority) priority.")
    }

    private var priorityBadge: some View {
        Circle()
            .fill(priorityColor)
            .frame(width: 10, height: 10)
            .padding(.top, 6)
    }

    private var priorityColor: Color {
        switch alert.priority {
        case "high": return .red
        case "medium": return .orange
        case "low": return .blue
        default: return .gray
        }
    }

    private var relativeTimestamp: String {
        let formatter = RelativeDateTimeFormatter()
        formatter.unitsStyle = .short
        return formatter.localizedString(for: alert.timestamp, relativeTo: Date())
    }
}
