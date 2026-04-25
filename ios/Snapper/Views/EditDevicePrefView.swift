import SwiftUI
import os

/// Editor sheet for one device-scoped notification preference
/// (iOS-NP-1b).
///
/// Two entry points:
/// - Tap an existing row in
///   ``NotificationPrefsView``'s device-overrides section: opens in
///   edit mode with ``alert_type`` locked (the SCD2 key includes
///   alert_type, so editing the type would silently create a
///   sibling row instead of mutating the current one).
/// - "Add override" button: opens in new mode with the alert_type
///   picker enabled.
///
/// Scope picker surfaces ``Device-global`` plus every wallet the
/// caller has loaded in ``AppState.availableWallets``. Operator
/// scope is deferred to a future plan — the iOS surface has no
/// operator catalog today, and the backend SCD2 row's
/// ``operator_public_id`` column stays ``None`` until we add one.
struct EditDevicePrefView: View {
    enum Mode {
        case create
        case edit(existing: DeviceAlertPrefInfo)
    }

    enum ScopeKind: String, Hashable {
        case deviceGlobal
        case wallet
    }

    let mode: Mode
    let devicePublicId: String
    let onSaved: (DeviceAlertPrefInfo) -> Void
    let availableWallets: [WalletInfo]

    @Environment(\.dismiss) private var dismiss
    @State private var alertType: String
    @State private var scopeKind: ScopeKind
    @State private var selectedWalletId: String?
    @State private var enabled: Bool
    @State private var minPriority: String
    @State private var quietHoursEnabled: Bool
    @State private var quietHoursStart: Date
    @State private var quietHoursEnd: Date
    @State private var muteEnabled: Bool
    @State private var muteUntil: Date
    @State private var isSaving = false
    @State private var saveError: String?

    private let logger = Logger(
        subsystem: Bundle.main.bundleIdentifier ?? "Snapper",
        category: "EditDevicePrefView"
    )

    init(
        mode: Mode,
        devicePublicId: String,
        availableWallets: [WalletInfo],
        onSaved: @escaping (DeviceAlertPrefInfo) -> Void
    ) {
        self.mode = mode
        self.devicePublicId = devicePublicId
        self.availableWallets = availableWallets
        self.onSaved = onSaved

        let calendar = Calendar(identifier: .gregorian)
        let midnight = calendar.startOfDay(for: Date())

        switch mode {
        case .create:
            _alertType = State(initialValue: NotificationPrefsView.alertTypes[0])
            _scopeKind = State(initialValue: .deviceGlobal)
            _selectedWalletId = State(initialValue: nil)
            _enabled = State(initialValue: true)
            _minPriority = State(initialValue: "medium")
            _quietHoursEnabled = State(initialValue: false)
            _quietHoursStart = State(initialValue: midnight)
            _quietHoursEnd = State(initialValue: midnight)
            _muteEnabled = State(initialValue: false)
            _muteUntil = State(initialValue: Date())
        case .edit(let pref):
            _alertType = State(initialValue: pref.alertType)
            let kind: ScopeKind = pref.walletPublicId != nil ? .wallet : .deviceGlobal
            _scopeKind = State(initialValue: kind)
            _selectedWalletId = State(initialValue: pref.walletPublicId)
            _enabled = State(initialValue: pref.enabled)
            _minPriority = State(initialValue: pref.minPriority)
            let quietActive = pref.quietHoursStartMin != nil && pref.quietHoursEndMin != nil
            _quietHoursEnabled = State(initialValue: quietActive)
            _quietHoursStart = State(
                initialValue: Self.dateFromMinutes(pref.quietHoursStartMin ?? 0, base: midnight)
            )
            _quietHoursEnd = State(
                initialValue: Self.dateFromMinutes(pref.quietHoursEndMin ?? 0, base: midnight)
            )
            let muteActive = (pref.muteUntil ?? Date.distantPast) > Date()
            _muteEnabled = State(initialValue: muteActive)
            _muteUntil = State(initialValue: pref.muteUntil ?? Date())
        }
    }

    var body: some View {
        NavigationStack {
            Form {
                Section("Alert") {
                    if isAlertTypeLocked {
                        HStack {
                            Text(NotificationPrefsView.displayName(for: alertType))
                            Spacer()
                            Text("locked")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                    } else {
                        Picker("Type", selection: $alertType) {
                            ForEach(NotificationPrefsView.alertTypes, id: \.self) { type in
                                Text(NotificationPrefsView.displayName(for: type)).tag(type)
                            }
                        }
                    }
                }

                Section {
                    Picker("Apply to", selection: $scopeKind) {
                        Text("Device-global").tag(ScopeKind.deviceGlobal)
                        if !availableWallets.isEmpty {
                            Text("Specific wallet").tag(ScopeKind.wallet)
                        }
                    }
                    .pickerStyle(.segmented)

                    if scopeKind == .wallet {
                        Picker("Wallet", selection: $selectedWalletId) {
                            Text("Select…").tag(Optional<String>.none)
                            ForEach(availableWallets, id: \.publicId) { wallet in
                                Text(WalletPicker.walletDisplayName(wallet))
                                    .tag(Optional(wallet.publicId))
                            }
                        }
                    }
                } header: {
                    Text("Scope")
                } footer: {
                    if availableWallets.isEmpty {
                        Text("Open the Home tab once to load your wallets, then come back to scope this preference.")
                    } else {
                        Text(
                            "Wallet-narrowed preferences override the device-global default for that wallet's alerts."
                        )
                    }
                }

                Section {
                    Toggle("Enabled", isOn: $enabled)
                    Picker("Minimum priority", selection: $minPriority) {
                        ForEach(NotificationPrefsView.priorityValues, id: \.self) { priority in
                            Text(NotificationPrefsView.priorityDisplayName(for: priority))
                                .tag(priority)
                        }
                    }
                    .pickerStyle(.segmented)
                } header: {
                    Text("Delivery")
                }

                Section {
                    Toggle("Quiet hours", isOn: $quietHoursEnabled)
                    if quietHoursEnabled {
                        DatePicker(
                            "Start",
                            selection: $quietHoursStart,
                            displayedComponents: [.hourAndMinute]
                        )
                        DatePicker(
                            "End",
                            selection: $quietHoursEnd,
                            displayedComponents: [.hourAndMinute]
                        )
                    }
                } header: {
                    Text("Quiet hours")
                } footer: {
                    Text("Non-safety-critical alerts are deferred during the window, in the device timezone.")
                }

                Section {
                    Toggle("Hard mute", isOn: $muteEnabled)
                    if muteEnabled {
                        DatePicker(
                            "Until",
                            selection: $muteUntil,
                            in: Date()...,
                            displayedComponents: [.date, .hourAndMinute]
                        )
                    }
                } header: {
                    Text("Mute")
                } footer: {
                    Text("Hard mute overrides quiet hours and disables every alert in this scope until the chosen time.")
                }

                if let saveError {
                    Section {
                        Text(saveError)
                            .font(.caption)
                            .foregroundStyle(.red)
                    }
                }
            }
            .navigationTitle(navigationTitle)
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Save") {
                        Task { await save() }
                    }
                    .disabled(!canSave || isSaving)
                }
            }
            .overlay {
                if isSaving {
                    ProgressView().controlSize(.large)
                }
            }
        }
    }

    // MARK: - Pure helpers (extracted for unit testing)

    var isAlertTypeLocked: Bool {
        if case .edit = mode { return true }
        return false
    }

    var navigationTitle: String {
        return isAlertTypeLocked ? "Edit override" : "New override"
    }

    var canSave: Bool {
        if scopeKind == .wallet, selectedWalletId == nil { return false }
        return true
    }

    /// Convert a ``DatePicker`` Date to the minutes-since-midnight
    /// integer the backend expects for quiet-hours windows.
    static func minutesSinceMidnight(for date: Date, calendar: Calendar = .current) -> Int {
        let components = calendar.dateComponents([.hour, .minute], from: date)
        return (components.hour ?? 0) * 60 + (components.minute ?? 0)
    }

    /// Inverse of ``minutesSinceMidnight`` — used to seed
    /// ``DatePicker`` from a backend-supplied minute offset.
    static func dateFromMinutes(_ totalMinutes: Int, base: Date, calendar: Calendar = .current) -> Date {
        let clamped = max(0, min(1439, totalMinutes))
        let hour = clamped / 60
        let minute = clamped % 60
        return calendar.date(
            bySettingHour: hour, minute: minute, second: 0, of: base
        ) ?? base
    }

    /// Build the iOS-side envelope for ``PATCH /api/devices/{id}/prefs``.
    /// Provenance fields are placeholders — the backend handler
    /// strips and re-mints them per request.
    static func makeDeviceCommand(
        alertType: String,
        operatorPublicId: String? = nil,
        walletPublicId: String? = nil,
        enabled: Bool,
        minPriority: String,
        quietHoursStartMin: Int? = nil,
        quietHoursEndMin: Int? = nil,
        muteUntil: Date? = nil,
        timestamp: Date = Date()
    ) -> UpdateDevicePrefCommand {
        return UpdateDevicePrefCommand(
            type: "update_device_pref_command",
            sequenceId: 1,
            publicId: "client-envelope",
            timestamp: timestamp,
            sessionId: "client-session",
            payload: DeviceAlertPrefBody(
                alertType: alertType,
                operatorPublicId: operatorPublicId,
                walletPublicId: walletPublicId,
                enabled: enabled,
                minPriority: minPriority,
                quietHoursStartMin: quietHoursStartMin,
                quietHoursEndMin: quietHoursEndMin,
                muteUntil: muteUntil,
                timezone: "UTC"
            )
        )
    }

    // MARK: - Save

    private func save() async {
        isSaving = true
        defer { isSaving = false }
        saveError = nil

        let command = Self.makeDeviceCommand(
            alertType: alertType,
            walletPublicId: scopeKind == .wallet ? selectedWalletId : nil,
            enabled: enabled,
            minPriority: minPriority,
            quietHoursStartMin: quietHoursEnabled
                ? Self.minutesSinceMidnight(for: quietHoursStart) : nil,
            quietHoursEndMin: quietHoursEnabled
                ? Self.minutesSinceMidnight(for: quietHoursEnd) : nil,
            muteUntil: muteEnabled ? muteUntil : nil
        )
        do {
            let response = try await APIClient.shared.updateDevicePref(
                devicePublicId: devicePublicId,
                command: command
            )
            onSaved(response.payload)
            dismiss()
        } catch {
            logger.error("Failed to save device pref: \(error)")
            saveError = "Couldn't save preference. Try again."
        }
    }
}
