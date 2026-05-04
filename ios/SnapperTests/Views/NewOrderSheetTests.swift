import XCTest
@testable import Snapper

@MainActor
final class NewOrderSheetTests: XCTestCase {

    private static let baseTimestamp = Date(timeIntervalSince1970: 1_700_000_000)

    private static let fixedProvenance = EnvelopeMinter.Provenance(
        publicId: "test-public-id",
        sessionId: "session-test",
        sequenceId: 27,
        timestamp: baseTimestamp,
        timestampString: "2023-11-14T22:13:20.000Z"
    )

    private static func makeInstrument(
        canTrade: Bool = true,
        instrumentPublicId: String = "inst-1",
        symbol: String = "BTC-USD",
        exchange: String = "kraken"
    ) -> InstrumentDetailData {
        return InstrumentDetailData(
            type: "instrument_detail",
            sequenceId: 1,
            publicId: "envelope-id",
            timestamp: baseTimestamp,
            sessionId: "session-test",
            topic: nil,
            instrumentPublicId: instrumentPublicId,
            symbolPublicId: "sym-1",
            symbol: symbol,
            exchange: exchange,
            canTrade: canTrade,
            canMarketData: true,
            instrumentResolved: true,
            instrumentKind: "spot",
            expiryAt: nil
        )
    }

    private static let testIdempotencyKey: String = "test-idempotency-key"

    /// Two venues can list the same `symbol` (e.g. "BTC-USD" exists
    /// on both kraken-spot and kraken-futures) with distinct
    /// `instrumentPublicId`. Identifying the picked row by symbol
    /// alone routes orders to the wrong venue when the user
    /// switches exchanges. The builder MUST use the picked row's
    /// `instrumentPublicId` + `exchange` verbatim — no symbol
    /// reconciliation.
    func testBuildBodyPropagatesPickedInstrumentPublicIdNotSymbol() {
        let krakenSpot = Self.makeInstrument(
            instrumentPublicId: "inst-kraken-spot-btc",
            symbol: "BTC-USD",
            exchange: "kraken"
        )
        let krakenFutures = Self.makeInstrument(
            instrumentPublicId: "inst-kraken-futures-btc",
            symbol: "BTC-USD",
            exchange: "kraken_futures"
        )
        let bodySpot = NewOrderSheet.buildBody(
            instrument: krakenSpot,
            selectedExchange: "kraken",
            walletPublicId: "w", walletIsPaper: false,
            side: "buy", orderType: "market",
            quantityText: "1", priceText: "", stopPriceText: "",
            leverageText: "", reduceOnly: false,
            idempotencyKey: Self.testIdempotencyKey
        )
        let bodyFutures = NewOrderSheet.buildBody(
            instrument: krakenFutures,
            selectedExchange: "kraken_futures",
            walletPublicId: "w", walletIsPaper: false,
            side: "buy", orderType: "market",
            quantityText: "1", priceText: "", stopPriceText: "",
            leverageText: "", reduceOnly: false,
            idempotencyKey: Self.testIdempotencyKey
        )
        XCTAssertEqual(bodySpot?.instrumentPublicId, "inst-kraken-spot-btc")
        XCTAssertEqual(bodySpot?.exchange, "kraken")
        XCTAssertEqual(bodyFutures?.instrumentPublicId, "inst-kraken-futures-btc")
        XCTAssertEqual(bodyFutures?.exchange, "kraken_futures")
    }

    /// Cross-exchange race regression guard: when the picked
    /// instrument's exchange does NOT match the currently-selected
    /// exchange (because the instruments fetch is still in flight
    /// after a venue switch), the builder must refuse to build a
    /// body — submitting one venue's instrument under another
    /// venue's name is the worst case the original symbol-keyed bug
    /// could produce.
    func testBuildBodyRejectsExchangeMismatch() {
        XCTAssertNil(NewOrderSheet.buildBody(
            instrument: Self.makeInstrument(exchange: "kraken"),
            selectedExchange: "kraken_futures",
            walletPublicId: "w", walletIsPaper: false,
            side: "buy", orderType: "market",
            quantityText: "1", priceText: "", stopPriceText: "",
            leverageText: "", reduceOnly: false,
            idempotencyKey: Self.testIdempotencyKey
        ))
    }

    /// `needsPrice` mirrors the frontend rule at
    /// `NewOrderModal.tsx:114` — limit + stop_limit require a price.
    func testNeedsPriceFollowsBackendOrderTypes() {
        XCTAssertTrue(NewOrderSheet.needsPrice(orderType: "limit"))
        XCTAssertTrue(NewOrderSheet.needsPrice(orderType: "stop_limit"))
        XCTAssertFalse(NewOrderSheet.needsPrice(orderType: "market"))
        XCTAssertFalse(NewOrderSheet.needsPrice(orderType: "stop"))
    }

    func testNeedsStopPriceFollowsBackendOrderTypes() {
        XCTAssertTrue(NewOrderSheet.needsStopPrice(orderType: "stop"))
        XCTAssertTrue(NewOrderSheet.needsStopPrice(orderType: "stop_limit"))
        XCTAssertFalse(NewOrderSheet.needsStopPrice(orderType: "limit"))
        XCTAssertFalse(NewOrderSheet.needsStopPrice(orderType: "market"))
    }

    /// Submit gate: instrument must be tradable, exchange must
    /// match the picker, instruments fetch must be settled,
    /// quantity must parse positive, and any required price field
    /// must parse positive. Mirrors the disabled-state rules in
    /// the SwiftUI Form so the caller cannot fire requests that
    /// the backend would 422.
    func testCanSubmitRejectsMissingInstrument() {
        XCTAssertFalse(NewOrderSheet.canSubmit(
            instrument: nil,
            selectedExchange: "kraken",
            isLoadingInstruments: false,
            quantityText: "1", priceText: "100", stopPriceText: "",
            orderType: "limit",
            isSubmitting: false
        ))
    }

    func testCanSubmitRejectsMarketDataOnlyInstrument() {
        XCTAssertFalse(NewOrderSheet.canSubmit(
            instrument: Self.makeInstrument(canTrade: false),
            selectedExchange: "kraken",
            isLoadingInstruments: false,
            quantityText: "1", priceText: "", stopPriceText: "",
            orderType: "market",
            isSubmitting: false
        ))
    }

    func testCanSubmitRejectsExchangeMismatch() {
        XCTAssertFalse(NewOrderSheet.canSubmit(
            instrument: Self.makeInstrument(exchange: "kraken"),
            selectedExchange: "kraken_futures",
            isLoadingInstruments: false,
            quantityText: "1", priceText: "", stopPriceText: "",
            orderType: "market",
            isSubmitting: false
        ))
    }

    /// While instruments are still loading after an exchange
    /// switch, submit must stay disabled so a quick tap cannot
    /// fire the previously-selected instrument under a new
    /// exchange's name.
    func testCanSubmitBlockedWhileInstrumentsLoading() {
        XCTAssertFalse(NewOrderSheet.canSubmit(
            instrument: Self.makeInstrument(),
            selectedExchange: "kraken",
            isLoadingInstruments: true,
            quantityText: "1", priceText: "", stopPriceText: "",
            orderType: "market",
            isSubmitting: false
        ))
    }

    func testCanSubmitRejectsZeroOrMissingQuantity() {
        let instrument = Self.makeInstrument()
        XCTAssertFalse(NewOrderSheet.canSubmit(
            instrument: instrument,
            selectedExchange: "kraken",
            isLoadingInstruments: false,
            quantityText: "", priceText: "", stopPriceText: "",
            orderType: "market", isSubmitting: false
        ))
        XCTAssertFalse(NewOrderSheet.canSubmit(
            instrument: instrument,
            selectedExchange: "kraken",
            isLoadingInstruments: false,
            quantityText: "0", priceText: "", stopPriceText: "",
            orderType: "market", isSubmitting: false
        ))
    }

    func testCanSubmitRequiresPriceForLimit() {
        let instrument = Self.makeInstrument()
        XCTAssertFalse(NewOrderSheet.canSubmit(
            instrument: instrument,
            selectedExchange: "kraken",
            isLoadingInstruments: false,
            quantityText: "1", priceText: "", stopPriceText: "",
            orderType: "limit", isSubmitting: false
        ))
        XCTAssertTrue(NewOrderSheet.canSubmit(
            instrument: instrument,
            selectedExchange: "kraken",
            isLoadingInstruments: false,
            quantityText: "1", priceText: "100", stopPriceText: "",
            orderType: "limit", isSubmitting: false
        ))
    }

    func testCanSubmitRequiresStopPriceForStopLimit() {
        let instrument = Self.makeInstrument()
        XCTAssertFalse(NewOrderSheet.canSubmit(
            instrument: instrument,
            selectedExchange: "kraken",
            isLoadingInstruments: false,
            quantityText: "1", priceText: "100", stopPriceText: "",
            orderType: "stop_limit", isSubmitting: false
        ))
        XCTAssertTrue(NewOrderSheet.canSubmit(
            instrument: instrument,
            selectedExchange: "kraken",
            isLoadingInstruments: false,
            quantityText: "1", priceText: "100", stopPriceText: "95",
            orderType: "stop_limit", isSubmitting: false
        ))
    }

    func testCanSubmitMarketOrderJustNeedsQuantity() {
        let instrument = Self.makeInstrument()
        XCTAssertTrue(NewOrderSheet.canSubmit(
            instrument: instrument,
            selectedExchange: "kraken",
            isLoadingInstruments: false,
            quantityText: "0.25", priceText: "", stopPriceText: "",
            orderType: "market", isSubmitting: false
        ))
    }

    /// Builder propagates picker selections + parsed numeric inputs
    /// onto a `CreateOrderBody` whose shape matches the backend's
    /// validators. `time_in_force` defaults to GTC and `post_only`
    /// to false so the API contract sees explicit non-null values
    /// (avoids ambiguous "use venue defaults" semantics on the
    /// server side).
    func testBuildBodyPropagatesPickerSelectionsAndParsedNumbers() {
        let body = NewOrderSheet.buildBody(
            instrument: Self.makeInstrument(),
            selectedExchange: "kraken",
            walletPublicId: "wallet-9",
            walletIsPaper: false,
            side: "sell",
            orderType: "limit",
            quantityText: "1.25",
            priceText: "65000.5",
            stopPriceText: "",
            leverageText: "5",
            reduceOnly: true,
            idempotencyKey: Self.testIdempotencyKey
        )
        XCTAssertNotNil(body)
        guard let body else { return }
        XCTAssertEqual(body.instrument, "BTC-USD")
        XCTAssertEqual(body.instrumentPublicId, "inst-1")
        XCTAssertEqual(body.exchange, "kraken")
        XCTAssertEqual(body.side, "sell")
        XCTAssertEqual(body.orderType, "limit")
        XCTAssertEqual(body.quantity, 1.25)
        XCTAssertEqual(body.price, 65000.5)
        XCTAssertNil(body.stopPrice)
        XCTAssertEqual(body.leverage, 5)
        XCTAssertEqual(body.reduceOnly, true)
        XCTAssertEqual(body.walletPublicId, "wallet-9")
        XCTAssertEqual(body.timeInForce, "GTC")
        XCTAssertEqual(body.postOnly, false)
        XCTAssertEqual(body.idempotencyKey, Self.testIdempotencyKey)
    }

    func testBuildBodyReturnsNilWhenGateFails() {
        XCTAssertNil(NewOrderSheet.buildBody(
            instrument: Self.makeInstrument(canTrade: false),
            selectedExchange: "kraken",
            walletPublicId: "wallet-9",
            walletIsPaper: false,
            side: "buy", orderType: "market",
            quantityText: "1", priceText: "", stopPriceText: "",
            leverageText: "", reduceOnly: false,
            idempotencyKey: Self.testIdempotencyKey
        ))
    }

    /// Critical safety regression guard: a paper wallet must always
    /// produce a body with ``mode == "paper"`` so the backend's
    /// ``CreateOrderBody.mode`` default of ``"live"`` cannot route
    /// real money on a paper-wallet user's behalf.
    func testBuildBodyForPaperWalletStampsPaperMode() {
        let body = NewOrderSheet.buildBody(
            instrument: Self.makeInstrument(),
            selectedExchange: "kraken",
            walletPublicId: "paper-wallet",
            walletIsPaper: true,
            side: "buy", orderType: "market",
            quantityText: "0.1", priceText: "", stopPriceText: "",
            leverageText: "", reduceOnly: false,
            idempotencyKey: Self.testIdempotencyKey
        )
        XCTAssertEqual(body?.mode, "paper")
    }

    func testBuildBodyForLiveWalletStampsLiveMode() {
        let body = NewOrderSheet.buildBody(
            instrument: Self.makeInstrument(),
            selectedExchange: "kraken",
            walletPublicId: "live-wallet",
            walletIsPaper: false,
            side: "buy", orderType: "market",
            quantityText: "0.1", priceText: "", stopPriceText: "",
            leverageText: "", reduceOnly: false,
            idempotencyKey: Self.testIdempotencyKey
        )
        XCTAssertEqual(body?.mode, "live")
    }

    /// Idempotency key propagates verbatim from the caller into the
    /// outbound body so a network retry collides with the server's
    /// dedup index instead of creating a duplicate live order.
    func testBuildBodyPropagatesIdempotencyKey() {
        let body = NewOrderSheet.buildBody(
            instrument: Self.makeInstrument(),
            selectedExchange: "kraken",
            walletPublicId: "w", walletIsPaper: false,
            side: "buy", orderType: "market",
            quantityText: "1", priceText: "", stopPriceText: "",
            leverageText: "", reduceOnly: false,
            idempotencyKey: "stable-key-42"
        )
        XCTAssertEqual(body?.idempotencyKey, "stable-key-42")
    }

    func testMakeCommandStampsProvenance() {
        let body = CreateOrderBody(
            instrument: "BTC-USD",
            instrumentPublicId: "inst-1",
            exchange: "kraken",
            mode: nil,
            side: "buy",
            orderType: "market",
            quantity: 0.5,
            price: nil,
            stopPrice: nil,
            timeInForce: "GTC",
            postOnly: false,
            leverage: nil,
            reduceOnly: false,
            walletPublicId: "wallet-9",
            operatorPublicId: nil,
            idempotencyKey: nil,
            aiReviewPublicId: nil
        )
        let command = NewOrderSheet.makeCommand(body: body, provenance: Self.fixedProvenance)
        XCTAssertEqual(command.type, "create_order_command")
        XCTAssertEqual(command.sessionId, "session-test")
        XCTAssertEqual(command.sequenceId, 27)
        XCTAssertEqual(command.publicId, "test-public-id")
        XCTAssertEqual(command.payload.quantity, 0.5)
        XCTAssertEqual(command.payload.instrument, "BTC-USD")
    }
}
