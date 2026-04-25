import XCTest
@testable import Snapper

@MainActor
final class AppStateTests: XCTestCase {

    private static let walletKey = "selected_wallet_public_id"

    /// Build a UserDefaults instance backed by an ephemeral suite so
    /// concurrent test runs and stale state from prior runs cannot
    /// leak into the assertion under test.
    private func makeIsolatedDefaults() -> UserDefaults {
        let suiteName = "test.AppStateTests.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defaults.removePersistentDomain(forName: suiteName)
        return defaults
    }

    func testSelectedWalletDefaultsToNil() {
        let defaults = makeIsolatedDefaults()
        let state = AppState(userDefaults: defaults)
        XCTAssertNil(state.selectedWalletPublicId)
    }

    func testSelectedWalletPersistsAcrossInstances() {
        let defaults = makeIsolatedDefaults()
        let writer = AppState(userDefaults: defaults)
        writer.selectedWalletPublicId = "wallet-abc"
        let reader = AppState(userDefaults: defaults)
        XCTAssertEqual(reader.selectedWalletPublicId, "wallet-abc")
    }

    func testSettingSelectedWalletToNilRemovesPersistence() {
        let defaults = makeIsolatedDefaults()
        let state = AppState(userDefaults: defaults)
        state.selectedWalletPublicId = "wallet-xyz"
        XCTAssertEqual(defaults.string(forKey: Self.walletKey), "wallet-xyz")
        state.selectedWalletPublicId = nil
        XCTAssertNil(defaults.string(forKey: Self.walletKey))
    }
}
