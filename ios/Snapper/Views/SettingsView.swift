import SwiftUI
import UIKit
import UserNotifications

struct SettingsView: View {
    @EnvironmentObject var authService: AuthService
    @EnvironmentObject var webSocketManager: WebSocketManager
    @EnvironmentObject var notificationService: NotificationService

    @State private var showingLogoutAlert = false
    @State private var registeredDevicePublicId: String?

    var body: some View {
        NavigationView {
            Form {

                Section("Account") {
                    if let user = authService.currentUser {
                        HStack {
                            Text("Username")
                            Spacer()
                            Text(user.username)
                                .foregroundColor(.secondary)
                        }

                        if let email = user.email {
                            HStack {
                                Text("Email")
                                Spacer()
                                Text(email)
                                    .foregroundColor(.secondary)
                            }
                        }

                        HStack {
                            Text("Role")
                            Spacer()
                            Text(user.role.rawValue.capitalized)
                                .foregroundColor(.secondary)
                        }
                    }
                }

                Section("Connection") {
                    HStack {
                        Text("WebSocket Status")
                        Spacer()
                        connectionStatusView
                    }

                    HStack {
                        Text("Backend URL")
                        Spacer()
                        Text(AppConfig.baseURL)
                            .font(.caption)
                            .foregroundColor(.secondary)
                    }
                }

                Section("Notifications") {
                    HStack {
                        Text("Permission")
                        Spacer()
                        notificationStatusView
                    }

                    if notificationService.authorizationStatus == .notDetermined {
                        Button("Enable push notifications") {
                            Task {
                                await notificationService.requestAuthorization()
                            }
                        }
                    } else if notificationService.authorizationStatus == .denied {
                        Button("Open Settings") {
                            if let url = URL(string: UIApplication.openSettingsURLString) {
                                UIApplication.shared.open(url)
                            }
                        }
                    }

                    if let pid = registeredDevicePublicId {
                        HStack {
                            Text("Device")
                            Spacer()
                            Text(String(pid.prefix(12)) + "…")
                                .font(.caption.monospaced())
                                .foregroundColor(.secondary)
                        }
                    } else {
                        HStack {
                            Text("Device")
                            Spacer()
                            Text("Not registered")
                                .font(.caption)
                                .foregroundColor(.secondary)
                        }
                    }

                    NavigationLink("Manage preferences") {
                        NotificationPrefsView()
                    }
                }

                Section("App Information") {
                    HStack {
                        Text("Version")
                        Spacer()
                        Text(Bundle.main.appVersion)
                            .foregroundColor(.secondary)
                    }

                    HStack {
                        Text("Build")
                        Spacer()
                        Text(Bundle.main.buildVersion)
                            .foregroundColor(.secondary)
                    }
                }

                Section {
                    Button(role: .destructive, action: { showingLogoutAlert = true }) {
                        HStack {
                            Spacer()
                            Text("Logout")
                            Spacer()
                        }
                    }
                }
            }
            .navigationTitle("Settings")
            .scrollContentBackground(.hidden)
            .background(Color.bgBase)
            .task {
                await notificationService.refreshAuthorizationStatus()
                registeredDevicePublicId = await DeviceRegistrationService.shared().currentDevicePublicId()
            }
        }
        .alert("Logout", isPresented: $showingLogoutAlert) {
            Button("Cancel", role: .cancel) { /* Dismiss alert with no action */ }
            Button("Logout", role: .destructive) {
                logout()
            }
        } message: {
            Text("Are you sure you want to logout?")
        }
    }

    private var notificationStatusView: some View {
        HStack(spacing: 6) {
            Circle()
                .fill(notificationStatusColor)
                .frame(width: 8, height: 8)
            Text(notificationStatusText)
                .font(.caption)
                .foregroundColor(.secondary)
        }
    }

    private var notificationStatusColor: Color {
        switch notificationService.authorizationStatus {
        case .authorized, .provisional, .ephemeral:
            return .brandGreen
        case .notDetermined:
            return .orange
        case .denied:
            return .brandRed
        @unknown default:
            return .gray
        }
    }

    private var notificationStatusText: String {
        switch notificationService.authorizationStatus {
        case .authorized:
            return "Enabled"
        case .provisional:
            return "Quiet"
        case .ephemeral:
            return "Ephemeral"
        case .notDetermined:
            return "Not set"
        case .denied:
            return "Disabled"
        @unknown default:
            return "Unknown"
        }
    }

    private var connectionStatusView: some View {
        HStack(spacing: 6) {
            Circle()
                .fill(connectionColor)
                .frame(width: 8, height: 8)

            Text(connectionText)
                .font(.caption)
        }
    }

    private var connectionColor: Color {
        switch webSocketManager.connectionState {
        case .connected:
            return .brandGreen
        case .connecting, .authenticating:
            return .orange
        case .disconnected, .error, .authFailed:
            return .brandRed
        }
    }

    private var connectionText: String {
        switch webSocketManager.connectionState {
        case .connected:
            return "Connected"
        case .connecting:
            return "Connecting"
        case .authenticating:
            return "Authenticating"
        case .disconnected:
            return "Disconnected"
        case .error:
            return "Error"
        case .authFailed:
            return "Auth failed"
        }
    }

    private func logout() {
        webSocketManager.disconnect()
        Task {
            await authService.logout()
        }
    }
}

extension Bundle {
    var appVersion: String {
        return infoDictionary?["CFBundleShortVersionString"] as? String ?? "Unknown"
    }

    var buildVersion: String {
        return infoDictionary?["CFBundleVersion"] as? String ?? "Unknown"
    }
}

struct SettingsView_Previews: PreviewProvider {
    static var previews: some View {
        SettingsView()
            .environmentObject(AuthService.shared)
            .environmentObject(WebSocketManager.shared)
            .environmentObject(NotificationService.shared)
    }
}
