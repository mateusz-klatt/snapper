import SwiftUI
import os

/// Modal sheet for entering a fresh trading order. Mirrors the web
/// frontend's ``NewOrderModal`` at
/// `frontend/src/features/orders/NewOrderModal.tsx`.
///
/// Backend route: ``POST /api/orders`` carrying
/// ``CreateOrderCommand``. The submit flow:
/// 1. User picks exchange (loaded from ``APIClient.fetchInstruments``
///    per the recently-traded venues — currently bootstrapped from
///    the wallet picker selection).
/// 2. User picks instrument (filtered to ``can_trade == true`` rows
///    so TradFi mirrors are disabled at row level).
/// 3. User picks side / order type / quantity / optional price /
///    optional stop price / leverage / reduce-only toggle.
/// 4. Submit fires ``APIClient.createOrder`` with provenance from
///    ``EnvelopeMinter``.
struct NewOrderSheet: View {
    let exchanges: [String]
    let walletPublicId: String
    let walletIsPaper: Bool
    let onSubmit: (CreateOrderBody) async -> Void

    @Environment(\.dismiss) private var dismiss

    @State private var selectedExchange: String
    @State private var availableInstruments: [InstrumentDetailData] = []
    @State private var selectedInstrument: InstrumentDetailData?
    @State private var side: String = "buy"
    @State private var orderType: String = "limit"
    @State private var quantityText: String = ""
    @State private var priceText: String = ""
    @State private var stopPriceText: String = ""
    @State private var leverageText: String = ""
    @State private var reduceOnly: Bool = false
    @State private var isLoadingInstruments: Bool = false
    @State private var isSubmitting: Bool = false
    @State private var loadError: String?

    /// Idempotency token minted once per sheet presentation. Stable
    /// across retries within the same sheet so a network failure +
    /// re-submit by the user dedups at the backend instead of
    /// creating two live orders. Re-presenting the sheet (cancel /
    /// dismiss / re-open) gets a fresh state value and a fresh key.
    @State private var idempotencyKey: String = UUID().uuidString

    private let logger = Logger(
        subsystem: Bundle.main.bundleIdentifier ?? "Snapper",
        category: "NewOrderSheet"
    )

    init(
        exchanges: [String],
        walletPublicId: String,
        walletIsPaper: Bool,
        defaultExchange: String? = nil,
        onSubmit: @escaping (CreateOrderBody) async -> Void
    ) {
        self.exchanges = exchanges
        self.walletPublicId = walletPublicId
        self.walletIsPaper = walletIsPaper
        self.onSubmit = onSubmit
        _selectedExchange = State(initialValue: defaultExchange ?? exchanges.first ?? "")
    }

    private var needsPrice: Bool {
        return Self.needsPrice(orderType: orderType)
    }

    private var needsStopPrice: Bool {
        return Self.needsStopPrice(orderType: orderType)
    }

    private var canSubmit: Bool {
        return Self.canSubmit(
            instrument: selectedInstrument,
            selectedExchange: selectedExchange,
            isLoadingInstruments: isLoadingInstruments,
            quantityText: quantityText,
            priceText: priceText,
            stopPriceText: stopPriceText,
            orderType: orderType,
            isSubmitting: isSubmitting
        )
    }

    var body: some View {
        NavigationStack {
            Form {
                if let loadError {
                    Section {
                        Text(loadError).font(.caption).foregroundStyle(.secondary)
                    }
                }

                Section("Venue") {
                    Picker("Exchange", selection: $selectedExchange) {
                        ForEach(exchanges, id: \.self) { exchange in
                            Text(exchange).tag(exchange)
                        }
                    }
                    Picker("Instrument", selection: Binding(
                        get: { selectedInstrument?.instrumentPublicId ?? "" },
                        set: { newId in
                            selectedInstrument = availableInstruments.first {
                                $0.instrumentPublicId == newId
                            }
                        }
                    )) {
                        Text(isLoadingInstruments ? "Loading…" : "Select instrument")
                            .tag("")
                        ForEach(availableInstruments.filter { $0.canTrade }, id: \.instrumentPublicId) { row in
                            Text(row.symbol).tag(row.instrumentPublicId)
                        }
                    }
                }

                Section("Order") {
                    Picker("Side", selection: $side) {
                        Text("Buy").tag("buy")
                        Text("Sell").tag("sell")
                    }
                    .pickerStyle(.segmented)

                    Picker("Type", selection: $orderType) {
                        ForEach(Self.orderTypes, id: \.self) { type in
                            Text(Self.displayName(forOrderType: type)).tag(type)
                        }
                    }

                    HStack {
                        Text("Quantity")
                        Spacer()
                        TextField("0.0", text: $quantityText)
                            .keyboardType(.decimalPad)
                            .multilineTextAlignment(.trailing)
                            .frame(minWidth: 100)
                    }

                    if needsPrice {
                        HStack {
                            Text("Price")
                            Spacer()
                            TextField("0.0", text: $priceText)
                                .keyboardType(.decimalPad)
                                .multilineTextAlignment(.trailing)
                                .frame(minWidth: 100)
                        }
                    }

                    if needsStopPrice {
                        HStack {
                            Text("Stop price")
                            Spacer()
                            TextField("0.0", text: $stopPriceText)
                                .keyboardType(.decimalPad)
                                .multilineTextAlignment(.trailing)
                                .frame(minWidth: 100)
                        }
                    }
                }

                Section("Risk") {
                    HStack {
                        Text("Leverage")
                        Spacer()
                        TextField("optional", text: $leverageText)
                            .keyboardType(.numberPad)
                            .multilineTextAlignment(.trailing)
                            .frame(minWidth: 80)
                    }
                    Toggle("Reduce-only", isOn: $reduceOnly)
                }
            }
            .navigationTitle("New order")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Submit") {
                        guard !isSubmitting, let body = buildBody() else { return }
                        isSubmitting = true
                        Task {
                            defer { isSubmitting = false }
                            await onSubmit(body)
                            dismiss()
                        }
                    }
                    .disabled(!canSubmit)
                }
            }
            .overlay {
                if isSubmitting {
                    ProgressView().controlSize(.large)
                }
            }
            .task(id: selectedExchange) {
                await loadInstruments()
            }
        }
    }

    private func loadInstruments() async {
        guard !selectedExchange.isEmpty else { return }
        // Synchronously clear stale rows BEFORE the await so the user
        // cannot tap Submit during the fetch and ship the previous
        // exchange's instrument under a new exchange's name.
        let exchangeBeingLoaded = selectedExchange
        if selectedInstrument?.exchange != exchangeBeingLoaded {
            selectedInstrument = nil
        }
        availableInstruments = []
        isLoadingInstruments = true
        defer { isLoadingInstruments = false }
        do {
            let fetched = try await APIClient.shared.fetchInstruments(exchange: exchangeBeingLoaded)
            // If the user changed exchange while we were waiting, the
            // .task(id: selectedExchange) modifier already started a
            // fresh task — but defensively bail if our load races
            // past a newer one anyway.
            guard exchangeBeingLoaded == selectedExchange else { return }
            availableInstruments = fetched
            if let current = selectedInstrument,
               !fetched.contains(where: { $0.instrumentPublicId == current.instrumentPublicId }) {
                selectedInstrument = nil
            }
        } catch {
            logger.error("Failed to load instruments for \(exchangeBeingLoaded): \(error)")
            loadError = "Couldn't load instruments. Pull to refresh."
            availableInstruments = []
        }
    }

    private func buildBody() -> CreateOrderBody? {
        return Self.buildBody(
            instrument: selectedInstrument,
            selectedExchange: selectedExchange,
            walletPublicId: walletPublicId,
            walletIsPaper: walletIsPaper,
            side: side,
            orderType: orderType,
            quantityText: quantityText,
            priceText: priceText,
            stopPriceText: stopPriceText,
            leverageText: leverageText,
            reduceOnly: reduceOnly,
            idempotencyKey: idempotencyKey
        )
    }

    // MARK: - Pure helpers (extracted for unit testing)

    static let orderTypes: [String] = ["limit", "market", "stop", "stop_limit"]

    static func displayName(forOrderType orderType: String) -> String {
        switch orderType {
        case "limit": return "Limit"
        case "market": return "Market"
        case "stop": return "Stop"
        case "stop_limit": return "Stop limit"
        default: return orderType
        }
    }

    static func needsPrice(orderType: String) -> Bool {
        return orderType == "limit" || orderType == "stop_limit"
    }

    static func needsStopPrice(orderType: String) -> Bool {
        return orderType == "stop" || orderType == "stop_limit"
    }

    static func parsePositive(_ text: String) -> Double? {
        let trimmed = text.trimmingCharacters(in: .whitespaces)
        guard !trimmed.isEmpty else { return nil }
        let normalized = trimmed.replacingOccurrences(of: ",", with: ".")
        guard let value = Double(normalized), value > 0 else { return nil }
        return value
    }

    static func parsePositiveInt(_ text: String) -> Int? {
        let trimmed = text.trimmingCharacters(in: .whitespaces)
        guard !trimmed.isEmpty, let value = Int(trimmed), value > 0 else { return nil }
        return value
    }

    static func canSubmit(
        instrument: InstrumentDetailData?,
        selectedExchange: String,
        isLoadingInstruments: Bool,
        quantityText: String,
        priceText: String,
        stopPriceText: String,
        orderType: String,
        isSubmitting: Bool
    ) -> Bool {
        guard !isSubmitting,
              !isLoadingInstruments,
              let instrument,
              instrument.canTrade,
              instrument.exchange == selectedExchange,
              parsePositive(quantityText) != nil
        else { return false }
        if needsPrice(orderType: orderType), parsePositive(priceText) == nil {
            return false
        }
        if needsStopPrice(orderType: orderType), parsePositive(stopPriceText) == nil {
            return false
        }
        return true
    }

    /// Pure builder for ``CreateOrderBody`` extracted for unit
    /// testing. Returns ``nil`` when the form fails the same gate
    /// that drives the submit-button disabled state, so the
    /// generated body never violates backend's validators.
    ///
    /// ``walletIsPaper`` derives the ``mode`` literal so a paper
    /// wallet never accidentally routes as a live order. The
    /// backend default at ``snapper.api.schemas.orders.CreateOrderBody``
    /// is ``"live"`` — leaving ``mode`` at ``nil`` would route
    /// real money on the user's behalf.
    static func buildBody(
        instrument: InstrumentDetailData?,
        selectedExchange: String,
        walletPublicId: String,
        walletIsPaper: Bool,
        side: String,
        orderType: String,
        quantityText: String,
        priceText: String,
        stopPriceText: String,
        leverageText: String,
        reduceOnly: Bool,
        idempotencyKey: String
    ) -> CreateOrderBody? {
        guard let instrument,
              instrument.canTrade,
              instrument.exchange == selectedExchange
        else { return nil }
        guard let quantity = parsePositive(quantityText) else { return nil }
        let price: Double? = needsPrice(orderType: orderType) ? parsePositive(priceText) : nil
        if needsPrice(orderType: orderType), price == nil { return nil }
        let stopPrice: Double? = needsStopPrice(orderType: orderType) ? parsePositive(stopPriceText) : nil
        if needsStopPrice(orderType: orderType), stopPrice == nil { return nil }
        let leverage: Int? = parsePositiveInt(leverageText)
        return CreateOrderBody(
            instrument: instrument.symbol,
            instrumentPublicId: instrument.instrumentPublicId,
            exchange: instrument.exchange,
            mode: walletIsPaper ? "paper" : "live",
            side: side,
            orderType: orderType,
            quantity: quantity,
            price: price,
            stopPrice: stopPrice,
            timeInForce: "GTC",
            postOnly: false,
            leverage: leverage,
            reduceOnly: reduceOnly,
            walletPublicId: walletPublicId,
            operatorPublicId: nil,
            idempotencyKey: idempotencyKey,
            aiReviewPublicId: nil
        )
    }

    /// Wrap a ``CreateOrderBody`` in a provenance-stamped
    /// ``CreateOrderCommand`` envelope. Provenance comes from the
    /// shared ``EnvelopeMinter`` so the backend gap detector sees a
    /// coherent ``session_id`` + monotonic ``sequence_id`` across
    /// iOS-originated commands.
    @MainActor
    static func makeCommand(
        body: CreateOrderBody,
        provenance: EnvelopeMinter.Provenance? = nil
    ) -> CreateOrderCommand {
        let envelope = provenance ?? EnvelopeMinter.shared.next(.control)
        return CreateOrderCommand(
            type: "create_order_command",
            sequenceId: envelope.sequenceId,
            publicId: envelope.publicId,
            timestamp: envelope.timestamp,
            sessionId: envelope.sessionId,
            topic: nil,
            payload: body
        )
    }
}
