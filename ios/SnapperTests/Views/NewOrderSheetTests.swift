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

    private static func makeInstrument(canTrade: Bool = true) -> InstrumentDetailData {
        return InstrumentDetailData(
            type: "instrument_detail",
            sequenceId: 1,
            publicId: "envelope-id",
            timestamp: baseTimestamp,
            sessionId: "session-test",
            topic: nil,
            instrumentPublicId: "inst-1",
            symbolPublicId: "sym-1",
            symbol: "BTC-USD",
            exchange: "kraken",
            canTrade: canTrade,
            canMarketData: true,
            instrumentResolved: true,
            instrumentKind: "spot",
            expiryAt: nil
        )
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

    /// Submit gate: instrument must be tradable, quantity must parse
    /// positive, and any required price field must parse positive.
    /// Mirrors the disabled-state rules in the SwiftUI Form so the
    /// caller cannot fire requests that the backend would 422.
    func testCanSubmitRejectsMissingInstrument() {
        XCTAssertFalse(NewOrderSheet.canSubmit(
            instrument: nil,
            quantityText: "1",
            priceText: "100",
            stopPriceText: "",
            orderType: "limit",
            isSubmitting: false
        ))
    }

    func testCanSubmitRejectsMarketDataOnlyInstrument() {
        XCTAssertFalse(NewOrderSheet.canSubmit(
            instrument: Self.makeInstrument(canTrade: false),
            quantityText: "1",
            priceText: "",
            stopPriceText: "",
            orderType: "market",
            isSubmitting: false
        ))
    }

    func testCanSubmitRejectsZeroOrMissingQuantity() {
        let instrument = Self.makeInstrument()
        XCTAssertFalse(NewOrderSheet.canSubmit(
            instrument: instrument, quantityText: "",
            priceText: "", stopPriceText: "",
            orderType: "market", isSubmitting: false
        ))
        XCTAssertFalse(NewOrderSheet.canSubmit(
            instrument: instrument, quantityText: "0",
            priceText: "", stopPriceText: "",
            orderType: "market", isSubmitting: false
        ))
    }

    func testCanSubmitRequiresPriceForLimit() {
        let instrument = Self.makeInstrument()
        XCTAssertFalse(NewOrderSheet.canSubmit(
            instrument: instrument, quantityText: "1",
            priceText: "", stopPriceText: "",
            orderType: "limit", isSubmitting: false
        ))
        XCTAssertTrue(NewOrderSheet.canSubmit(
            instrument: instrument, quantityText: "1",
            priceText: "100", stopPriceText: "",
            orderType: "limit", isSubmitting: false
        ))
    }

    func testCanSubmitRequiresStopPriceForStopLimit() {
        let instrument = Self.makeInstrument()
        XCTAssertFalse(NewOrderSheet.canSubmit(
            instrument: instrument, quantityText: "1",
            priceText: "100", stopPriceText: "",
            orderType: "stop_limit", isSubmitting: false
        ))
        XCTAssertTrue(NewOrderSheet.canSubmit(
            instrument: instrument, quantityText: "1",
            priceText: "100", stopPriceText: "95",
            orderType: "stop_limit", isSubmitting: false
        ))
    }

    func testCanSubmitMarketOrderJustNeedsQuantity() {
        let instrument = Self.makeInstrument()
        XCTAssertTrue(NewOrderSheet.canSubmit(
            instrument: instrument, quantityText: "0.25",
            priceText: "", stopPriceText: "",
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
            walletPublicId: "wallet-9",
            side: "sell",
            orderType: "limit",
            quantityText: "1.25",
            priceText: "65000.5",
            stopPriceText: "",
            leverageText: "5",
            reduceOnly: true
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
    }

    func testBuildBodyReturnsNilWhenGateFails() {
        XCTAssertNil(NewOrderSheet.buildBody(
            instrument: Self.makeInstrument(canTrade: false),
            walletPublicId: "wallet-9",
            side: "buy", orderType: "market",
            quantityText: "1", priceText: "", stopPriceText: "",
            leverageText: "", reduceOnly: false
        ))
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
