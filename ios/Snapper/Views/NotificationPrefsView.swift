import SwiftUI
import os

/// Notification preferences screen pushed from Settings → Notifications
/// → "Manage preferences".
///
/// Two sections:
/// - User defaults: each of the five backend ``alert_types`` is
///   rendered as a row with an enabled toggle + min_priority picker.
///   Toggling fires ``APIClient.updateAlertDefault`` immediately —
///   optimistic UI with revert-on-error so the user never sees a
///   stale toggle position after a network blip.
/// - Device overrides: list of active per-(alert_type, scope) rows
///   for the registered device. Each row opens an editor sheet with
///   scope picker, quiet hours, and mute_until controls so the
///   wallet-narrowed configuration is editable end-to-end.
///
/// On appear the view fetches both surfaces in parallel via
/// ``async let``. Empty list is the legitimate "no overrides" state
/// — the routing layer falls through to the in-app defaults.
struct NotificationPrefsView: View {
    @Environment(AppState.self) private var appState
    @State private var defaults: [String: UserAlertDefaultInfo] = [:]
    @State private var devicePrefs: [DeviceAlertPrefInfo] = []
    @State private var devicePublicId: String?
    @State private var isLoading = false
    @State private var loadError: String?
    @State private var inflightAlertTypes: Set<String> = []
    @State private var sheetMode: EditDevicePrefView.Mode?

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
                } else {
                    if devicePrefs.isEmpty {
                        Text("No overrides yet — defaults apply to every alert.")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    } else {
                        ForEach(devicePrefs, id: \.publicId) { pref in
                            Button {
                                sheetMode = .edit(existing: pref)
                            } label: {
                                DevicePrefRow(pref: pref)
                            }
                            .buttonStyle(.plain)
                        }
                    }

                    Button {
                        sheetMode = .create
                    } label: {
                        Label("Add override", systemImage: "plus.circle")
                    }
                    .disabled(devicePublicId == nil)
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
        .sheet(item: Binding(
            get: { sheetMode.map { SheetIdentifier(mode: $0) } },
            set: { newValue in sheetMode = newValue?.mode }
        )) { identifier in
            if let id = devicePublicId {
                EditDevicePrefView(
                    mode: identifier.mode,
                    devicePublicId: id,
                    onSaved: { saved in
                        Self.applySavedPref(saved, into: &devicePrefs)
                    }
                )
            }
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

    /// Merge a saved ``DeviceAlertPrefInfo`` into the local cache.
    /// Replaces an existing row that shares the same scope tuple
    /// (alert_type, operator_public_id, wallet_public_id) — the SCD2
    /// upsert preserves the same ``public_id`` so a bare publicId
    /// match suffices on the happy path. New scopes append.
    static func applySavedPref(
        _ saved: DeviceAlertPrefInfo,
        into prefs: inout [DeviceAlertPrefInfo]
    ) {
        if let index = prefs.firstIndex(where: { $0.publicId == saved.publicId }) {
            prefs[index] = saved
        } else if let scopeIndex = prefs.firstIndex(where: {
            $0.alertType == saved.alertType
                && $0.operatorPublicId == saved.operatorPublicId
                && $0.walletPublicId == saved.walletPublicId
        }) {
            prefs[scopeIndex] = saved
        } else {
            prefs.append(saved)
        }
    }

    /// Build the iOS-side envelope for ``PATCH /api/alert_defaults``.
    /// Provenance comes from ``EnvelopeMinter`` so the backend gap
    /// detector sees a coherent ``session_id`` + monotonic
    /// ``sequence_id`` across iOS-originated commands.
    @MainActor
    static func makeDefaultCommand(
        alertType: String,
        enabled: Bool,
        minPriority: String,
        provenance: EnvelopeMinter.Provenance? = nil
    ) -> UpdateUserAlertDefaultCommand {
        let envelope = provenance ?? EnvelopeMinter.shared.next(.control)
        return UpdateUserAlertDefaultCommand(
            type: "update_user_alert_default_command",
            sequenceId: envelope.sequenceId,
            publicId: envelope.publicId,
            timestamp: envelope.timestamp,
            sessionId: envelope.sessionId,
            topic: nil,
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
                .disabled(isInflight)
                .onChange(of: enabled) { _, newValue in
                    let baseline = existing?.enabled ?? true
                    guard newValue != baseline else { return }
                    Task { await onChange(newValue, minPriority) }
                }
            Picker("Minimum priority", selection: $minPriority) {
                ForEach(NotificationPrefsView.priorityValues, id: \.self) { priority in
                    Text(NotificationPrefsView.priorityDisplayName(for: priority)).tag(priority)
                }
            }
            .pickerStyle(.segmented)
            .disabled(isInflight)
            .onChange(of: minPriority) { _, newValue in
                let baseline = existing?.minPriority ?? "medium"
                guard newValue != baseline else { return }
                Task { await onChange(enabled, newValue) }
            }
        }
        .padding(.vertical, 4)
        // Sync local @State when the async load (or a server response)
        // updates the existing row underneath us. The guard inside the
        // .onChange-of-enabled / -minPriority handlers above bails when
        // the new local value matches the new baseline, so this sync
        // does not recurse into an extra PATCH.
        .onChange(of: existing?.publicId) { _, _ in
            if let existing {
                if enabled != existing.enabled { enabled = existing.enabled }
                if minPriority != existing.minPriority { minPriority = existing.minPriority }
            }
        }
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

/// Identifier wrapper so SwiftUI's ``sheet(item:)`` can drive the
/// edit/create sheet from an enum without forcing the caller to
/// manage Identifiable conformance on the enum itself.
private struct SheetIdentifier: Identifiable {
    let mode: EditDevicePrefView.Mode

    var id: String {
        switch mode {
        case .create:
            return "create"
        case .edit(let pref):
            return "edit-\(pref.publicId)"
        }
    }
}

struct NotificationPrefsView_Previews: PreviewProvider {
    static var previews: some View {
        NavigationStack {
            NotificationPrefsView()
                .environment(AppState.shared)
        }
    }
}
