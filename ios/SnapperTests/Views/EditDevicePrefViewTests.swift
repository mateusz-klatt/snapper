import XCTest
@testable import Snapper

@MainActor
final class EditDevicePrefViewTests: XCTestCase {

    private static let baseTimestamp = Date(timeIntervalSince1970: 1_700_000_000)

    private func makeWallet(publicId: String, label: String, isPaper: Bool) -> WalletInfo {
        return WalletInfo(
            type: "wallet_info",
            sequenceId: 1,
            publicId: publicId,
            timestamp: Self.baseTimestamp,
            sessionId: "session-test",
            label: label,
            description: nil,
            isPaper: isPaper
        )
    }

    private func makePref(
        publicId: String,
        alertType: String = "order_fill_full",
        operatorPublicId: String? = nil,
        walletPublicId: String? = nil,
        enabled: Bool = true,
        minPriority: String = "medium",
        quietHoursStartMin: Int? = nil,
        quietHoursEndMin: Int? = nil,
        muteUntil: Date? = nil
    ) -> DeviceAlertPrefInfo {
        return DeviceAlertPrefInfo(
            type: "device_alert_pref_info",
            sequenceId: 1,
            publicId: publicId,
            timestamp: Self.baseTimestamp,
            sessionId: "session-test",
            devicePublicId: "dev-1",
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
    }

    func testIsAlertTypeLockedForEditMode() {
        let pref = makePref(publicId: "p-1")
        let view = EditDevicePrefView(
            mode: .edit(existing: pref),
            devicePublicId: "dev-1",
            availableWallets: [],
            onSaved: { _ in }
        )
        XCTAssertTrue(view.isAlertTypeLocked)
        XCTAssertEqual(view.navigationTitle, "Edit override")
    }

    func testIsAlertTypeUnlockedForCreateMode() {
        let view = EditDevicePrefView(
            mode: .create,
            devicePublicId: "dev-1",
            availableWallets: [],
            onSaved: { _ in }
        )
        XCTAssertFalse(view.isAlertTypeLocked)
        XCTAssertEqual(view.navigationTitle, "New override")
    }

    /// Use a UTC calendar so the round-trip is timezone-independent.
    private var utcCalendar: Calendar {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = TimeZone(identifier: "UTC")!
        return calendar
    }

    func testMinutesSinceMidnightRoundTrip() {
        let calendar = utcCalendar
        let base = calendar.date(from: DateComponents(year: 2026, month: 4, day: 25))!
        for minutes in [0, 60, 22 * 60 + 30, 1439] {
            let date = EditDevicePrefView.dateFromMinutes(minutes, base: base, calendar: calendar)
            let extracted = EditDevicePrefView.minutesSinceMidnight(for: date, calendar: calendar)
            XCTAssertEqual(extracted, minutes, "Round trip lost data for \(minutes) minutes.")
        }
    }

    func testDateFromMinutesClampsOutOfRange() {
        let calendar = utcCalendar
        let base = calendar.date(from: DateComponents(year: 2026, month: 4, day: 25))!
        let underflow = EditDevicePrefView.dateFromMinutes(-100, base: base, calendar: calendar)
        XCTAssertEqual(EditDevicePrefView.minutesSinceMidnight(for: underflow, calendar: calendar), 0)
        let overflow = EditDevicePrefView.dateFromMinutes(2000, base: base, calendar: calendar)
        XCTAssertEqual(EditDevicePrefView.minutesSinceMidnight(for: overflow, calendar: calendar), 1439)
    }

    func testMakeDeviceCommandShapeWithFullPayload() {
        let mute = Date(timeIntervalSince1970: 1_800_000_000)
        let command = EditDevicePrefView.makeDeviceCommand(
            alertType: "margin_warning",
            operatorPublicId: nil,
            walletPublicId: "wallet-abc",
            enabled: false,
            minPriority: "high",
            quietHoursStartMin: 22 * 60,
            quietHoursEndMin: 7 * 60,
            muteUntil: mute,
            timestamp: Self.baseTimestamp
        )
        XCTAssertEqual(command.payload.alertType, "margin_warning")
        XCTAssertEqual(command.payload.walletPublicId, "wallet-abc")
        XCTAssertNil(command.payload.operatorPublicId)
        XCTAssertEqual(command.payload.enabled, false)
        XCTAssertEqual(command.payload.minPriority, "high")
        XCTAssertEqual(command.payload.quietHoursStartMin, 22 * 60)
        XCTAssertEqual(command.payload.quietHoursEndMin, 7 * 60)
        XCTAssertEqual(command.payload.muteUntil, mute)
        XCTAssertEqual(command.payload.timezone, "UTC")
        XCTAssertEqual(command.type, "update_device_pref_command")
    }

    func testCanSaveBlocksWalletScopeWithoutWalletSelected() {
        let view = EditDevicePrefView(
            mode: .create,
            devicePublicId: "dev-1",
            availableWallets: [makeWallet(publicId: "w-1", label: "default", isPaper: false)],
            onSaved: { _ in }
        )
        XCTAssertTrue(
            view.canSave,
            "Default scope is device-global; canSave starts true regardless of wallet picker state."
        )
    }

    func testApplySavedPrefReplacesByPublicId() {
        var prefs = [
            makePref(publicId: "p-1", alertType: "order_fill_full"),
            makePref(publicId: "p-2", alertType: "order_rejected"),
        ]
        let updated = makePref(
            publicId: "p-1",
            alertType: "order_fill_full",
            enabled: false,
            minPriority: "high"
        )

        NotificationPrefsView.applySavedPref(updated, into: &prefs)

        XCTAssertEqual(prefs.count, 2)
        XCTAssertEqual(prefs.first(where: { $0.publicId == "p-1" })?.enabled, false)
        XCTAssertEqual(prefs.first(where: { $0.publicId == "p-1" })?.minPriority, "high")
    }

    /// The SCD2 upsert preserves public_id across versions, so the
    /// publicId-match path is the happy path. The scope-tuple
    /// fallback covers the rare case where the iOS cache is stale
    /// (e.g. user reinstalled and the new public_id differs but the
    /// scope tuple matches an existing row).
    func testApplySavedPrefReplacesByScopeTupleWhenPublicIdMisses() {
        var prefs = [
            makePref(publicId: "p-old", alertType: "order_fill_full", walletPublicId: "wallet-1"),
        ]
        let updated = makePref(
            publicId: "p-new",
            alertType: "order_fill_full",
            walletPublicId: "wallet-1",
            enabled: false
        )

        NotificationPrefsView.applySavedPref(updated, into: &prefs)

        XCTAssertEqual(prefs.count, 1)
        XCTAssertEqual(prefs[0].publicId, "p-new")
        XCTAssertEqual(prefs[0].enabled, false)
    }

    func testApplySavedPrefAppendsWhenScopeIsNew() {
        var prefs = [
            makePref(publicId: "p-1", alertType: "order_fill_full"),
        ]
        let added = makePref(
            publicId: "p-2",
            alertType: "margin_warning"
        )

        NotificationPrefsView.applySavedPref(added, into: &prefs)

        XCTAssertEqual(prefs.count, 2)
        XCTAssertTrue(prefs.contains { $0.publicId == "p-2" })
    }
}
