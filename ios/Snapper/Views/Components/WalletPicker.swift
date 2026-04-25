import SwiftUI

/// Compact wallet selector surfaced in the Home tab toolbar (iOS-2).
///
/// Reads + writes ``AppState.selectedWalletPublicId`` so other tabs
/// (Positions, Orders) can filter to the wallet the user picked here.
/// Loads wallets on first appearance via ``APIClient.fetchWallets``;
/// on success, falls through to the first wallet when the user has
/// not yet picked one (first-launch UX).
///
/// Paper wallets are tagged ``(paper)`` after the label so the user
/// cannot confuse a paper-mode session with a live one — backend
/// disambiguation is ``(label, is_paper)``-keyed and the UI mirrors
/// that exactly.
struct WalletPicker: View {
    @Environment(AppState.self) private var appState
    @State private var loadError: APIError?

    var body: some View {
        Menu {
            ForEach(appState.availableWallets, id: \.publicId) { wallet in
                Button {
                    appState.selectedWalletPublicId = wallet.publicId
                } label: {
                    HStack {
                        Text(Self.walletDisplayName(wallet))
                        if wallet.publicId == appState.selectedWalletPublicId {
                            Image(systemName: "checkmark")
                        }
                    }
                }
            }
        } label: {
            HStack(spacing: 6) {
                Image(systemName: "wallet.bifold")
                Text(Self.currentLabel(
                    wallets: appState.availableWallets,
                    selected: appState.selectedWalletPublicId
                ))
                Image(systemName: "chevron.down")
            }
            .padding(.horizontal, 12)
            .padding(.vertical, 6)
            .background(Color(.systemGray6))
            .clipShape(Capsule())
        }
        .task {
            await loadWallets()
        }
    }

    /// Pure label-derivation helper extracted for unit testing — the
    /// SwiftUI body is not directly testable without ViewInspector,
    /// but the visible string is covered via this function.
    static func currentLabel(wallets: [WalletInfo], selected: String?) -> String {
        guard
            let id = selected,
            let wallet = wallets.first(where: { $0.publicId == id })
        else {
            return wallets.isEmpty ? "Loading wallets..." : "Select wallet"
        }
        return walletDisplayName(wallet)
    }

    static func walletDisplayName(_ wallet: WalletInfo) -> String {
        return wallet.isPaper ? "\(wallet.label) (paper)" : wallet.label
    }

    private func loadWallets() async {
        do {
            let wallets = try await APIClient.shared.fetchWallets()
            appState.availableWallets = wallets
            if appState.selectedWalletPublicId == nil, let first = wallets.first {
                appState.selectedWalletPublicId = first.publicId
            }
        } catch let error as APIError {
            loadError = error
        } catch {
            loadError = .invalidResponse
        }
    }
}
