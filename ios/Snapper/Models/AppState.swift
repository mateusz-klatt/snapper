import Foundation
import Observation

/// Per-user UX state shared across tabs (iOS-2).
///
/// Lives outside `WSState` because it does not track live WebSocket
/// frames — only the wallet the user picked from `WalletPicker`. The
/// selection persists across app launches via `UserDefaults` so the
/// Home tab opens on the same wallet the user last selected.
///
/// `availableWallets` is a cache populated by `WalletPicker` after
/// `APIClient.fetchWallets()` succeeds. Treat it as a read-through
/// snapshot, not a source of truth — the picker re-fetches on view
/// appear.
///
/// `UserDefaults` is injected for test isolation; production code
/// uses `AppState.shared` which falls through to `.standard`.
@MainActor
@Observable
final class AppState {
    static let shared = AppState()

    private static let walletKey = "selected_wallet_public_id"
    private let userDefaults: UserDefaults

    var selectedWalletPublicId: String? {
        didSet {
            if let id = selectedWalletPublicId {
                userDefaults.set(id, forKey: Self.walletKey)
            } else {
                userDefaults.removeObject(forKey: Self.walletKey)
            }
        }
    }

    var availableWallets: [WalletInfo] = []

    /// Operators the caller may act AS, populated by
    /// ``EditDevicePrefView`` on appearance via
    /// ``APIClient.fetchOperators``. Treated as a read-through
    /// cache — the editor refetches every time the sheet opens so
    /// stale catalogs do not narrow the scope picker.
    var availableOperators: [OperatorInfo] = []

    init(userDefaults: UserDefaults = .standard) {
        self.userDefaults = userDefaults
        self.selectedWalletPublicId = userDefaults.string(forKey: Self.walletKey)
    }
}
