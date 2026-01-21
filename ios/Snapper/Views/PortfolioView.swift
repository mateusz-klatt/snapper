import SwiftUI

struct PortfolioView: View {
    @State private var portfolio: Portfolio?
    @State private var positions: [Position] = []
    @State private var isLoading = false
    @State private var errorMessage: String?

    var body: some View {
        NavigationView {
            ScrollView {
                VStack(spacing: 20) {

                    if let portfolio = portfolio {
                        portfolioHeaderView(portfolio: portfolio)
                    }

                    if !positions.isEmpty {
                        positionsListView
                    } else if isLoading {
                        ProgressView("Loading positions...")
                            .padding()
                    } else {
                        Text("No positions")
                            .foregroundColor(.secondary)
                            .padding()
                    }

                    if let error = errorMessage {
                        Text("Error: \(error)")
                            .foregroundColor(.red)
                            .font(.caption)
                            .padding()
                    }
                }
                .padding()
            }
            .navigationTitle("Portfolio")
            .refreshable {
                await loadData()
            }
        }
        .task {
            await loadData()
        }
    }

    private func portfolioHeaderView(portfolio: Portfolio) -> some View {
        VStack(spacing: 16) {
            VStack(spacing: 4) {
                Text("Total Value")
                    .font(.caption)
                    .foregroundColor(.secondary)

                Text(String(format: "$%.2f", portfolio.totalValue))
                    .font(.system(size: 32, weight: .bold))
            }

            LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 12) {
                StatCard(title: "Cash", value: String(format: "$%.2f", portfolio.cashBalance))
                StatCard(title: "Positions", value: String(format: "$%.2f", portfolio.positionsValue))
                StatCard(
                    title: "Unrealized P&L",
                    value: String(format: "$%.2f", portfolio.unrealizedPnL),
                    color: portfolio.unrealizedPnL >= 0 ? .green : .red
                )
                StatCard(
                    title: "Realized P&L",
                    value: String(format: "$%.2f", portfolio.realizedPnL),
                    color: portfolio.realizedPnL >= 0 ? .green : .red
                )
            }
        }
        .padding()
        .background(Color(uiColor: .systemGray6))
        .cornerRadius(12)
    }

    private var positionsListView: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Positions")
                .font(.headline)
                .padding(.horizontal)

            ForEach(positions) { position in
                PositionRowView(position: position)
                    .padding(.horizontal)
                Divider()
            }
        }
    }

    private func loadData() async {
        isLoading = true
        errorMessage = nil

        async let portfolioResult = try? await APIClient.shared.fetchPortfolio()
        async let positionsResult = try? await APIClient.shared.fetchPositions()

        let (portfolioData, positionsData) = await (portfolioResult, positionsResult)

        portfolio = portfolioData
        positions = positionsData ?? []
        isLoading = false
    }
}

struct StatCard: View {
    let title: String
    let value: String
    var color: Color = .primary

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(title)
                .font(.caption)
                .foregroundColor(.secondary)

            Text(value)
                .font(.headline)
                .foregroundColor(color)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding()
        .background(Color(uiColor: .systemBackground))
        .cornerRadius(8)
    }
}

struct PositionRowView: View {
    let position: Position

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Text(position.symbol)
                    .font(.headline)

                Spacer()

                VStack(alignment: .trailing, spacing: 2) {
                    Text(String(format: "$%.2f", position.marketValue))
                        .font(.headline)

                    Text(String(format: "%@$%.2f (%.2f%%)", position.unrealizedPnL >= 0 ? "+" : "", position.unrealizedPnL, position.unrealizedPnLPercent))
                        .font(.caption)
                        .foregroundColor(Color(position.pnlColor))
                }
            }

            HStack {
                VStack(alignment: .leading, spacing: 4) {
                    Text("Quantity: \(position.quantity, specifier: "%.4f")")
                        .font(.caption)
                        .foregroundColor(.secondary)

                    Text("Avg Price: $\(position.averagePrice, specifier: "%.2f")")
                        .font(.caption)
                        .foregroundColor(.secondary)
                }

                Spacer()

                VStack(alignment: .trailing, spacing: 4) {
                    Text("Current: $\(position.currentPrice, specifier: "%.2f")")
                        .font(.caption)
                        .foregroundColor(.secondary)

                    Text("Cost Basis: $\(position.costBasis, specifier: "%.2f")")
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
            }
        }
        .padding(.vertical, 4)
    }
}

struct PortfolioView_Previews: PreviewProvider {
    static var previews: some View {
        PortfolioView()
    }
}
