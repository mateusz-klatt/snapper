import SwiftUI

struct StrategiesView: View {
    @State private var strategies: [Strategy] = []
    @State private var isLoading = false
    @State private var errorMessage: String?

    var body: some View {
        NavigationView {
            Group {
                if isLoading {
                    ProgressView("Loading strategies...")
                } else if let error = errorMessage {
                    VStack {
                        Text("Error loading strategies")
                            .font(.headline)
                        Text(error)
                            .font(.caption)
                            .foregroundColor(.secondary)

                        Button("Retry") {
                            Task { await loadStrategies() }
                        }
                        .buttonStyle(.bordered)
                    }
                } else if strategies.isEmpty {
                    VStack {
                        Image(systemName: "brain.head.profile")
                            .font(.system(size: 60))
                            .foregroundColor(.gray)

                        Text("No strategies")
                            .font(.headline)
                            .padding(.top)

                        Text("Configure strategies in the web dashboard")
                            .font(.caption)
                            .foregroundColor(.secondary)
                            .multilineTextAlignment(.center)
                            .padding(.horizontal)
                    }
                } else {
                    List(strategies) { strategy in
                        StrategyRowView(
                            strategy: strategy,
                            onToggle: { await toggleStrategy(strategy) }
                        )
                    }
                    .listStyle(.plain)
                }
            }
            .navigationTitle("Strategies")
            .refreshable {
                await loadStrategies()
            }
        }
        .task {
            await loadStrategies()
        }
    }

    private func loadStrategies() async {
        isLoading = true
        errorMessage = nil

        do {
            strategies = try await APIClient.shared.fetchStrategies()
            isLoading = false
        } catch {
            errorMessage = error.localizedDescription
            isLoading = false
        }
    }

    private func toggleStrategy(_ strategy: Strategy) async {
        do {
            if strategy.isRunning {
                try await APIClient.shared.stopStrategy(strategyId: strategy.id)
            } else {
                try await APIClient.shared.startStrategy(strategyId: strategy.id)
            }
            await loadStrategies()
        } catch {
            errorMessage = "Failed to toggle strategy: \(error.localizedDescription)"
        }
    }
}

struct StrategyRowView: View {
    let strategy: Strategy
    let onToggle: () async -> Void

    @State private var isToggling = false

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                VStack(alignment: .leading, spacing: 4) {
                    Text(strategy.name)
                        .font(.headline)

                    Text(strategy.type.capitalized)
                        .font(.caption)
                        .foregroundColor(.secondary)
                }

                Spacer()

                if isToggling {
                    ProgressView()
                } else {
                    Toggle("", isOn: .constant(strategy.isRunning))
                        .labelsHidden()
                        .onChange(of: strategy.isRunning) { oldValue, newValue in
                            Task {
                                isToggling = true
                                await onToggle()
                                isToggling = false
                            }
                        }
                }
            }

            if !strategy.symbols.isEmpty {
                HStack {
                    Image(systemName: "chart.xyaxis.line")
                        .font(.caption)
                        .foregroundColor(.secondary)

                    Text(strategy.symbols.joined(separator: ", "))
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
            }

            if let performance = strategy.performance {
                Divider()

                LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 8) {
                    PerformanceItem(label: "Trades", value: "\(performance.totalTrades)")
                    PerformanceItem(label: "Win Rate", value: String(format: "%.1f%%", performance.winRate))
                    PerformanceItem(
                        label: "P&L",
                        value: String(format: "$%.2f", performance.profitLoss),
                        color: performance.profitLoss >= 0 ? .green : .red
                    )
                    PerformanceItem(
                        label: "Return",
                        value: String(format: "%@%.2f%%", performance.profitLossPercent >= 0 ? "+" : "", performance.profitLossPercent),
                        color: performance.profitLossPercent >= 0 ? .green : .red
                    )
                }
            }

            HStack {
                Circle()
                    .fill(strategy.isRunning ? Color.green : Color.gray)
                    .frame(width: 8, height: 8)

                Text(strategy.status.capitalized)
                    .font(.caption2)
                    .foregroundColor(.secondary)

                Spacer()

                Text(strategy.updatedAt ?? strategy.createdAt, style: .relative)
                    .font(.caption2)
                    .foregroundColor(.secondary)
            }
        }
        .padding(.vertical, 8)
    }
}

struct PerformanceItem: View {
    let label: String
    let value: String
    var color: Color = .primary

    var body: some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(label)
                .font(.caption2)
                .foregroundColor(.secondary)

            Text(value)
                .font(.caption)
                .fontWeight(.medium)
                .foregroundColor(color)
        }
    }
}

struct StrategiesView_Previews: PreviewProvider {
    static var previews: some View {
        StrategiesView()
    }
}
