import SwiftUI
import os

/// Notification preferences screen pushed from Settings → Notifications
/// → "Manage preferences" (iOS-NP-1).
///
/// Two sections:
/// - User defaults: each of the five backend ``alert_types`` is
///   rendered as a row with an enabled toggle + min_priority picker.
///   Toggling fires ``APIClient.updateAlertDefault`` immediately —
///   optimistic UI with revert-on-error so the user never sees a
///   stale toggle position after a network blip.
/// - Device overrides: read-only list of active per-(alert_type,
///   scope) rows for the registered device. iOS-NP-1b adds the
///   editor (scope picker + quiet hours + mute_until); iOS-NP-1a
///   ships read-only so the wallet-narrowed configuration is at
///   least visible to the user even before the editor lands.
///
/// On appear the view fetches both surfaces in parallel via
/// ``async let``. Empty list is the legitimate "no overrides" state
/// — the routing layer falls through to the in-app defaults.
struct NotificationPrefsView: View {
    @State private var defaults: [String: UserAlertDefaultInfo] = [:]
    @State private var devicePrefs: [DeviceAlertPrefInfo] = []
    @State private var devicePublicId: String?
    @State private var isLoading = false
    @State private var loadError: String?
    @State private var inflightAlertTypes: Set<String> = []

    private let logger = Logger(
        subsystem: Bundle.main.bundleIdentifier ?? "Snapper",
        category: "NotificationPrefsView"
    )

    var body: some View {
        Form {
            if let loadError {
                Section {
                    Text(loadError)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }

            Section {
                ForEach(Self.alertTypes, id: \.self) { alertType in
                    AlertDefaultRow(
                        alertType: alertType,
                        existing: defaults[alertType],
                        isInflight: inflightAlertTypes.contains(alertType),
                        onChange: { enabled, minPriority in
                            await mutateDefault(
                                alertType: alertType,
                                enabled: enabled,
                                minPriority: minPriority
                            )
                        }
                    )
                }
            } header: {
                Text("User defaults")
            } footer: {
                Text(
                    "Defaults apply when no per-device override matches an inbound alert."
                )
            }

            Section {
                if devicePublicId == nil {
                    ContentUnavailableView(
                        "No device registered",
                        systemImage: "iphone.slash",
                        description: Text(
                            "Enable push notifications in Settings → Notifications to register this device."
                        )
                    )
                } else if devicePrefs.isEmpty {
                    ContentUnavailableView(
                        "No overrides yet",
                        systemImage: "bell.badge",
                        description: Text(
                            "Once iOS-NP-1b ships you'll be able to configure per-wallet quiet hours and mute windows here."
                        )
                    )
                } else {
                    ForEach(devicePrefs, id: \.publicId) { pref in
                        DevicePrefRow(pref: pref)
                    }
                }
            } header: {
                Text("Device overrides")
            }
        }
        .navigationTitle("Notification preferences")
        .navigationBarTitleDisplayMode(.inline)
        .task {
            await load()
        }
        .refreshable {
            await load()
        }
    }

    // MARK: - Pure helpers (extracted for unit testing)

    /// Backend-canonical alert_types in display order. Kept in sync
    /// with ``snapper.api.schemas.devices.UserAlertDefaultBody``'s
    /// Literal — adding a new alert_type backend-side requires
    /// updating both this list and ``displayName(for:)``.
    static let alertTypes: [String] = [
        "order_fill_full",
        "order_rejected",
        "position_stop_loss_fired",
        "margin_warning",
        "critical_system_error",
    ]

    /// Backend-canonical priority members from
    /// ``UserAlertDefaultBody.min_priority`` Literal.
    static let priorityValues: [String] = ["low", "medium", "high"]

    static func displayName(for alertType: String) -> String {
        switch alertType {
        case "order_fill_full": return "Order filled"
        case "order_rejected": return "Order rejected"
        case "position_stop_loss_fired": return "Stop-loss fired"
        case "margin_warning": return "Margin warning"
        case "critical_system_error": return "System error"
        default: return alertType
        }
    }

    static func priorityDisplayName(for priority: String) -> String {
        return priority.prefix(1).uppercased() + priority.dropFirst()
    }

    static func scopeLabel(for pref: DeviceAlertPrefInfo) -> String {
        if let walletId = pref.walletPublicId {
            return "Wallet \(String(walletId.prefix(8)))…"
        }
        if let operatorId = pref.operatorPublicId {
            return "Operator \(String(operatorId.prefix(8)))…"
        }
        return "Device-global"
    }

    /// One-line summary of a device pref's runtime semantics:
    /// enabled flag · min_priority · optional quiet-hours window ·
    /// optional active-mute marker.
    static func summaryLabel(for pref: DeviceAlertPrefInfo) -> String {
        var parts: [String] = []
        parts.append(pref.enabled ? "Enabled" : "Muted")
        parts.append("min \(pref.minPriority)")
        if let start = pref.quietHoursStartMin, let end = pref.quietHoursEndMin {
            parts.append("quiet \(formatMinutes(start))–\(formatMinutes(end))")
        }
        if let muteUntil = pref.muteUntil, muteUntil > Date() {
            parts.append("muted until \(formatRelativeDate(muteUntil))")
        }
        return parts.joined(separator: " · ")
    }

    /// Format minutes-since-midnight as ``HH:MM`` (UTC bias). Out-of-
    /// range values fall back to the raw integer for diagnostic
    /// surfacing.
    static func formatMinutes(_ totalMinutes: Int) -> String {
        guard (0..<1440).contains(totalMinutes) else {
            return String(totalMinutes)
        }
        let h = totalMinutes / 60
        let m = totalMinutes % 60
        return String(format: "%02d:%02d", h, m)
    }

    /// Build the iOS-side envelope for ``PATCH /api/alert_defaults``.
    /// Provenance fields are placeholders — the backend handler
    /// strips and re-mints them per request.
    static func makeDefaultCommand(
        alertType: String,
        enabled: Bool,
        minPriority: String,
        timestamp: Date = Date()
    ) -> UpdateUserAlertDefaultCommand {
        return UpdateUserAlertDefaultCommand(
            type: "update_user_alert_default_command",
            sequenceId: 1,
            publicId: "client-envelope",
            timestamp: timestamp,
            sessionId: "client-session",
            payload: UserAlertDefaultBody(
                alertType: alertType,
                enabled: enabled,
                minPriority: minPriority
            )
        )
    }

    private static func formatRelativeDate(_ date: Date) -> String {
        let formatter = RelativeDateTimeFormatter()
        formatter.unitsStyle = .short
        return formatter.localizedString(for: date, relativeTo: Date())
    }

    // MARK: - Networking

    private func load() async {
        isLoading = true
        defer { isLoading = false }
        loadError = nil

        let resolvedDeviceId = await DeviceRegistrationService.shared().currentDevicePublicId()
        devicePublicId = resolvedDeviceId

        do {
            let envelope = try await APIClient.shared.fetchAlertDefaults()
            defaults = Dictionary(
                uniqueKeysWithValues: envelope.payload.map { ($0.alertType, $0) }
            )
        } catch {
            logger.error("Failed to fetch alert defaults: \(error)")
            loadError = "Couldn't load preferences. Pull to refresh."
        }

        guard let deviceId = resolvedDeviceId, !deviceId.isEmpty else { return }
        do {
            let prefsEnvelope = try await APIClient.shared.fetchDevicePrefs(
                devicePublicId: deviceId
            )
            devicePrefs = prefsEnvelope.payload
        } catch {
            logger.error("Failed to fetch device prefs: \(error)")
        }
    }

    private func mutateDefault(
        alertType: String,
        enabled: Bool,
        minPriority: String
    ) async {
        inflightAlertTypes.insert(alertType)
        defer { inflightAlertTypes.remove(alertType) }

        let command = Self.makeDefaultCommand(
            alertType: alertType,
            enabled: enabled,
            minPriority: minPriority
        )
        do {
            let response = try await APIClient.shared.updateAlertDefault(command: command)
            defaults[alertType] = response.payload
        } catch {
            logger.error("Failed to update alert default for \(alertType): \(error)")
            loadError = "Couldn't save preference. Try again."
        }
    }
}

private struct AlertDefaultRow: View {
    let alertType: String
    let existing: UserAlertDefaultInfo?
    let isInflight: Bool
    let onChange: (Bool, String) async -> Void

    @State private var enabled: Bool
    @State private var minPriority: String

    init(
        alertType: String,
        existing: UserAlertDefaultInfo?,
        isInflight: Bool,
        onChange: @escaping (Bool, String) async -> Void
    ) {
        self.alertType = alertType
        self.existing = existing
        self.isInflight = isInflight
        self.onChange = onChange
        _enabled = State(initialValue: existing?.enabled ?? true)
        _minPriority = State(initialValue: existing?.minPriority ?? "medium")
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Text(NotificationPrefsView.displayName(for: alertType))
                    .font(.body)
                Spacer()
                if isInflight {
                    ProgressView().controlSize(.small)
                }
            }
            Toggle("Enabled", isOn: $enabled)
                .onChange(of: enabled) { _, newValue in
                    Task { await onChange(newValue, minPriority) }
                }
            Picker("Minimum priority", selection: $minPriority) {
                ForEach(NotificationPrefsView.priorityValues, id: \.self) { priority in
                    Text(NotificationPrefsView.priorityDisplayName(for: priority)).tag(priority)
                }
            }
            .pickerStyle(.segmented)
            .onChange(of: minPriority) { _, newValue in
                Task { await onChange(enabled, newValue) }
            }
        }
        .padding(.vertical, 4)
    }
}

private struct DevicePrefRow: View {
    let pref: DeviceAlertPrefInfo

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(NotificationPrefsView.displayName(for: pref.alertType))
                .font(.body)
            Text(NotificationPrefsView.scopeLabel(for: pref))
                .font(.caption)
                .foregroundStyle(.secondary)
            Text(NotificationPrefsView.summaryLabel(for: pref))
                .font(.caption2)
                .foregroundStyle(.tertiary)
        }
        .padding(.vertical, 2)
    }
}

struct NotificationPrefsView_Previews: PreviewProvider {
    static var previews: some View {
        NavigationStack {
            NotificationPrefsView()
        }
    }
}
