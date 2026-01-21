import SwiftUI

struct OrdersView: View {
    @State private var orders: [Order] = []
    @State private var isLoading = false
    @State private var errorMessage: String?
    @State private var showingCreateOrder = false

    var body: some View {
        NavigationView {
            Group {
                if isLoading {
                    ProgressView("Loading orders...")
                } else if let error = errorMessage {
                    VStack {
                        Text("Error loading orders")
                            .font(.headline)
                        Text(error)
                            .font(.caption)
                            .foregroundColor(.secondary)

                        Button("Retry") {
                            Task { await loadOrders() }
                        }
                        .buttonStyle(.bordered)
                    }
                } else if orders.isEmpty {
                    VStack {
                        Image(systemName: "tray")
                            .font(.system(size: 60))
                            .foregroundColor(.gray)

                        Text("No orders")
                            .font(.headline)
                            .padding(.top)

                        Text("Create your first order")
                            .font(.caption)
                            .foregroundColor(.secondary)
                    }
                } else {
                    List {
                        ForEach(orders) { order in
                            OrderRowView(order: order)
                                .swipeActions(edge: .trailing) {
                                    if order.status.lowercased() == "open" || order.status.lowercased() == "pending" {
                                        Button(role: .destructive) {
                                            Task {
                                                await cancelOrder(order)
                                            }
                                        } label: {
                                            Label("Cancel", systemImage: "xmark")
                                        }
                                    }
                                }
                        }
                    }
                    .listStyle(.plain)
                }
            }
            .navigationTitle("Orders")
            .toolbar {
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button {
                        showingCreateOrder = true
                    } label: {
                        Image(systemName: "plus")
                    }
                }
            }
            .refreshable {
                await loadOrders()
            }
            .sheet(isPresented: $showingCreateOrder) {
                CreateOrderView(onOrderCreated: {
                    Task { await loadOrders() }
                })
            }
        }
        .task {
            await loadOrders()
        }
    }

    private func loadOrders() async {
        isLoading = true
        errorMessage = nil

        do {
            orders = try await APIClient.shared.fetchOrders()
            isLoading = false
        } catch {
            errorMessage = error.localizedDescription
            isLoading = false
        }
    }

    private func cancelOrder(_ order: Order) async {
        do {
            try await APIClient.shared.cancelOrder(orderId: order.id)
            await loadOrders()
        } catch {
            errorMessage = "Failed to cancel order: \(error.localizedDescription)"
        }
    }
}

struct OrderRowView: View {
    let order: Order

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Text(order.symbol)
                    .font(.headline)

                Spacer()

                Text(order.formattedStatus)
                    .font(.caption)
                    .padding(.horizontal, 8)
                    .padding(.vertical, 4)
                    .background(Color(order.statusColor))
                    .foregroundColor(.white)
                    .cornerRadius(4)
            }

            HStack {
                Label(order.side.uppercased(), systemImage: order.side == "buy" ? "arrow.up.circle.fill" : "arrow.down.circle.fill")
                    .font(.subheadline)
                    .foregroundColor(order.side == "buy" ? .green : .red)

                Spacer()

                Text("\(order.quantity, specifier: "%.4f")")
                    .font(.subheadline)
            }

            if let price = order.price {
                Text("Price: $\(price, specifier: "%.2f")")
                    .font(.caption)
                    .foregroundColor(.secondary)
            }

            if order.filledQuantity > 0 {
                HStack {
                    Text("Filled: \(order.filledQuantity, specifier: "%.4f")")

                    if let avgPrice = order.averagePrice {
                        Text("@ $\(avgPrice, specifier: "%.2f")")
                    }
                }
                .font(.caption)
                .foregroundColor(.secondary)
            }

            Text(order.createdAt, style: .relative)
                .font(.caption2)
                .foregroundColor(.secondary)
        }
        .padding(.vertical, 4)
    }
}

struct CreateOrderView: View {
    @Environment(\.dismiss) var dismiss
    let onOrderCreated: () -> Void

    @State private var symbol = ""
    @State private var side = "buy"
    @State private var orderType = "market"
    @State private var quantity = ""
    @State private var price = ""
    @State private var isSubmitting = false
    @State private var errorMessage: String?

    let sides = ["buy", "sell"]
    let orderTypes = ["market", "limit"]

    var body: some View {
        NavigationView {
            Form {
                Section("Order Details") {
                    TextField("Symbol", text: $symbol)
                        .textInputAutocapitalization(.characters)

                    Picker("Side", selection: $side) {
                        ForEach(sides, id: \.self) { side in
                            Text(side.capitalized).tag(side)
                        }
                    }
                    .pickerStyle(.segmented)

                    Picker("Type", selection: $orderType) {
                        ForEach(orderTypes, id: \.self) { type in
                            Text(type.capitalized).tag(type)
                        }
                    }
                    .pickerStyle(.segmented)

                    TextField("Quantity", text: $quantity)
                        .keyboardType(.decimalPad)

                    if orderType == "limit" {
                        TextField("Price", text: $price)
                            .keyboardType(.decimalPad)
                    }
                }

                if let error = errorMessage {
                    Section {
                        Text(error)
                            .foregroundColor(.red)
                            .font(.caption)
                    }
                }

                Section {
                    Button(action: submitOrder) {
                        if isSubmitting {
                            ProgressView()
                        } else {
                            Text("Submit Order")
                                .frame(maxWidth: .infinity)
                        }
                    }
                    .disabled(isSubmitting || !isValid)
                }
            }
            .navigationTitle("New Order")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .navigationBarLeading) {
                    Button("Cancel") {
                        dismiss()
                    }
                }
            }
        }
    }

    private var isValid: Bool {
        !symbol.isEmpty && !quantity.isEmpty && (orderType == "market" || !price.isEmpty)
    }

    private func submitOrder() {
        guard let qty = Double(quantity) else {
            errorMessage = "Invalid quantity"
            return
        }

        let priceValue: Double? = orderType == "limit" ? Double(price) : nil

        let request = CreateOrderRequest(
            symbol: symbol,
            side: side,
            quantity: qty,
            orderType: orderType,
            price: priceValue
        )

        isSubmitting = true
        errorMessage = nil

        Task {
            do {
                _ = try await APIClient.shared.createOrder(request)
                isSubmitting = false
                onOrderCreated()
                dismiss()
            } catch {
                errorMessage = error.localizedDescription
                isSubmitting = false
            }
        }
    }
}

struct OrdersView_Previews: PreviewProvider {
    static var previews: some View {
        OrdersView()
    }
}
