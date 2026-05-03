import XCTest
@testable import Snapper

@MainActor
final class WalletPickerTests: XCTestCase {

    private static let baseTimestamp = Date(timeIntervalSince1970: 1_700_000_000)

    /// Construct a minimal ``WalletInfo`` for picker label assertions.
    /// Generated envelope fields (sequence_id, timestamp, session_id)
    /// are filled with placeholders — the picker only cares about
    /// ``publicId``, ``label`` and ``isPaper``.
    private func makeWallet(
        publicId: String,
        label: String,
        isPaper: Bool
    ) -> WalletInfo {
        return WalletInfo(
            type: "wallet_info",
            sequenceId: 1,
            publicId: publicId,
            timestamp: Self.baseTimestamp,
            sessionId: "session-test",
            topic: nil,
            label: label,
            description: nil,
            isPaper: isPaper
        )
    }

    func testCurrentLabelDerivation() {
        let live = makeWallet(publicId: "p-1", label: "default", isPaper: false)
        let paper = makeWallet(publicId: "p-2", label: "default-paper", isPaper: true)

        XCTAssertEqual(
            WalletPicker.currentLabel(wallets: [], selected: nil),
            "Loading wallets..."
        )
        XCTAssertEqual(
            WalletPicker.currentLabel(wallets: [live, paper], selected: nil),
            "Select wallet"
        )
        XCTAssertEqual(
            WalletPicker.currentLabel(wallets: [live, paper], selected: "p-1"),
            "default"
        )
        XCTAssertEqual(
            WalletPicker.currentLabel(wallets: [live, paper], selected: "p-2"),
            "default-paper (paper)"
        )
        XCTAssertEqual(
            WalletPicker.currentLabel(wallets: [live, paper], selected: "missing"),
            "Select wallet",
            "Stale or invalid selection must fall back to the empty-selection prompt."
        )
    }

    func testWalletDisplayNamePaperSuffix() {
        let live = makeWallet(publicId: "p-3", label: "firm", isPaper: false)
        let paper = makeWallet(publicId: "p-4", label: "firm", isPaper: true)

        XCTAssertEqual(WalletPicker.walletDisplayName(live), "firm")
        XCTAssertEqual(
            WalletPicker.walletDisplayName(paper),
            "firm (paper)",
            "Paper wallets must surface the (paper) suffix to mirror backend (label, is_paper) disambiguation."
        )
    }
}
