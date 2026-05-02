// This file was auto-generated from backend schemas.
// DO NOT EDIT - regenerate with: make ios-gen-types

import Foundation

enum AlertEventDataAlertType: String, Codable, Sendable {
    case orderFillFull = "order_fill_full"
    case orderRejected = "order_rejected"
    case positionStopLossFired = "position_stop_loss_fired"
    case marginWarning = "margin_warning"
    case criticalSystemError = "critical_system_error"
}

enum AlertEventDataPriority: String, Codable, Sendable {
    case low
    case medium
    case high
}

enum BacktestProgressDataEvent: String, Codable, Sendable {
    case started
    case progress
    case milestone
    case completed
    case failed
    case cancelled
}

enum CandleDataExchange: String, Codable, Sendable {
    case kraken
    case krakenFutures = "kraken_futures"
    case krakenEquities = "kraken_equities"
    case walutomat
    case polygon
}

enum FundingAccrualDataExchange: String, Codable, Sendable {
    case paper
    case kraken
    case krakenFutures = "kraken_futures"
    case walutomat
}

enum FundingAccrualDataMode: String, Codable, Sendable {
    case live
    case paper
}

enum FundingAccrualDataAccrualType: String, Codable, Sendable {
    case funding
    case rollover
    case borrow
}

enum HeartbeatDataStatus: String, Codable, Sendable {
    case healthy
    case warning
    case error
}

enum OrderCancelDataExchange: String, Codable, Sendable {
    case paper
    case kraken
    case krakenFutures = "kraken_futures"
    case walutomat
}

enum OrderEventDataExchange: String, Codable, Sendable {
    case paper
    case kraken
    case krakenFutures = "kraken_futures"
    case walutomat
}

enum OrderEventDataEvent: String, Codable, Sendable {
    case submitted
    case accepted
    case rejected
    case cancelled
    case expired
    case replaced
}

enum OrderReplaceDataExchange: String, Codable, Sendable {
    case paper
    case kraken
    case krakenFutures = "kraken_futures"
    case walutomat
}

enum OrderRequestDataExchange: String, Codable, Sendable {
    case paper
    case kraken
    case krakenFutures = "kraken_futures"
    case walutomat
}

enum OrderRequestDataMode: String, Codable, Sendable {
    case live
    case paper
}

enum OrderRequestDataSide: String, Codable, Sendable {
    case buy
    case sell
}

enum OrderRequestDataOrderType: String, Codable, Sendable {
    case market
    case limit
    case stop
    case stopLimit = "stop_limit"
}

enum ScopeRevokedDataScopeKind: String, Codable, Sendable {
    case underlying
    case instrument
}

enum TickDataExchange: String, Codable, Sendable {
    case kraken
    case krakenFutures = "kraken_futures"
    case krakenEquities = "kraken_equities"
    case walutomat
    case polygon
}

enum TradeDataExchange: String, Codable, Sendable {
    case kraken
    case krakenFutures = "kraken_futures"
    case krakenEquities = "kraken_equities"
    case walutomat
    case polygon
}

enum WSSubscriptionSuccessResponseAction: String, Codable, Sendable {
    case subscribe
    case unsubscribe
}

enum WSSubscriptionSuccessResponseStatus: String, Codable, Sendable {
    case subscribed
    case unsubscribed
    case partial
    case denied
    case noTopics = "no_topics"
}

struct WsMessageBase: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
    }
}

struct AiReviewCapsViolationFrameData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let reviewPublicId: String
    let userPublicId: String
    let strategyPublicId: String
    let walletPublicId: String
    let instrumentPublicId: String
    let capType: String
    let attempted: Double
    let limit: Double
    let dispatchVersion: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case reviewPublicId = "review_public_id"
        case userPublicId = "user_public_id"
        case strategyPublicId = "strategy_public_id"
        case walletPublicId = "wallet_public_id"
        case instrumentPublicId = "instrument_public_id"
        case capType = "cap_type"
        case attempted
        case limit
        case dispatchVersion = "dispatch_version"
    }
}

struct AiReviewDecisionAckFrameData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let reviewPublicId: String
    let userPublicId: String
    let strategyPublicId: String
    let walletPublicId: String
    let instrumentPublicId: String
    let respondingDelegatePublicId: String
    let decision: String
    let newStatus: String
    let resolutionMode: String
    let rationale: String?
    let dispatchVersion: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case reviewPublicId = "review_public_id"
        case userPublicId = "user_public_id"
        case strategyPublicId = "strategy_public_id"
        case walletPublicId = "wallet_public_id"
        case instrumentPublicId = "instrument_public_id"
        case respondingDelegatePublicId = "responding_delegate_public_id"
        case decision
        case newStatus = "new_status"
        case resolutionMode = "resolution_mode"
        case rationale
        case dispatchVersion = "dispatch_version"
    }
}

struct AiReviewDecisionData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let reviewPublicId: String
    let respondingDelegatePublicId: String
    let decision: String
    let newStatus: String
    let resolutionMode: String
    let dispatchVersion: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case reviewPublicId = "review_public_id"
        case respondingDelegatePublicId = "responding_delegate_public_id"
        case decision
        case newStatus = "new_status"
        case resolutionMode = "resolution_mode"
        case dispatchVersion = "dispatch_version"
    }
}

struct AiReviewRequestFrameData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let reviewPublicId: String
    let userPublicId: String
    let strategyPublicId: String
    let walletPublicId: String
    let instrumentPublicId: String
    let selectedDelegatePublicId: String
    let deadline: Date
    let signalEnvelope: JsonObject
    let instrumentMetadata: JsonObject
    let dispatchVersion: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case reviewPublicId = "review_public_id"
        case userPublicId = "user_public_id"
        case strategyPublicId = "strategy_public_id"
        case walletPublicId = "wallet_public_id"
        case instrumentPublicId = "instrument_public_id"
        case selectedDelegatePublicId = "selected_delegate_public_id"
        case deadline
        case signalEnvelope = "signal_envelope"
        case instrumentMetadata = "instrument_metadata"
        case dispatchVersion = "dispatch_version"
    }
}

struct AlertEventData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let userPublicId: String
    let operatorPublicId: String?
    let walletPublicId: String?
    let alertType: String
    let priority: String?
    let isSafetyCritical: Bool?
    let title: String
    let body: String
    let payload: JsonObject?
    let dedupKey: String?
    let threadKey: String?
    let sourceTopic: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case userPublicId = "user_public_id"
        case operatorPublicId = "operator_public_id"
        case walletPublicId = "wallet_public_id"
        case alertType = "alert_type"
        case priority
        case isSafetyCritical = "is_safety_critical"
        case title
        case body
        case payload
        case dedupKey = "dedup_key"
        case threadKey = "thread_key"
        case sourceTopic = "source_topic"
    }
}

struct BacktestProgressData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let runPublicId: String
    let walletPublicId: String
    let event: String
    let milestone: String?
    let candlesDone: Int
    let totalCandles: Int?
    let signalsCount: Int
    let tradesCount: Int
    let equity: Double
    let progressPct: Double

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case runPublicId = "run_public_id"
        case walletPublicId = "wallet_public_id"
        case event
        case milestone
        case candlesDone = "candles_done"
        case totalCandles = "total_candles"
        case signalsCount = "signals_count"
        case tradesCount = "trades_count"
        case equity
        case progressPct = "progress_pct"
    }
}

struct CandleData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let instrument: String
    let exchange: String
    let timeframe: String
    let openAt: Date
    let open: Double
    let high: Double
    let low: Double
    let close: Double
    let volume: Double
    let vwap: Double?
    let trades: Int?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case instrument
        case exchange
        case timeframe
        case openAt = "open_at"
        case open
        case high
        case low
        case close
        case volume
        case vwap
        case trades
    }
}

struct CapsViolationAfterAiApproveData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let reviewPublicId: String
    let userPublicId: String
    let strategyPublicId: String
    let walletPublicId: String
    let instrumentPublicId: String
    let capType: String
    let attempted: Double
    let limit: Double
    let dispatchVersion: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case reviewPublicId = "review_public_id"
        case userPublicId = "user_public_id"
        case strategyPublicId = "strategy_public_id"
        case walletPublicId = "wallet_public_id"
        case instrumentPublicId = "instrument_public_id"
        case capType = "cap_type"
        case attempted
        case limit
        case dispatchVersion = "dispatch_version"
    }
}

struct DelegateOfflineData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let userPublicId: String
    let delegatePublicId: String
    let lastSeenAt: Date

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case userPublicId = "user_public_id"
        case delegatePublicId = "delegate_public_id"
        case lastSeenAt = "last_seen_at"
    }
}

struct ExecutionPlanDecisionEventData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let decisionPublicId: String
    let planPublicId: String
    let decisionType: String
    let triggerType: String
    let reason: String
    let triggeredAt: Date

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case decisionPublicId = "decision_public_id"
        case planPublicId = "plan_public_id"
        case decisionType = "decision_type"
        case triggerType = "trigger_type"
        case reason
        case triggeredAt = "triggered_at"
    }
}

struct FundingAccrualData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let instrument: String
    let exchange: String
    let mode: String
    let accrualType: String
    let accruedAt: Date
    let amount: Double
    let amountAsset: String
    let rate: Double
    let notional: Double
    let positionQuantity: Double

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case instrument
        case exchange
        case mode
        case accrualType = "accrual_type"
        case accruedAt = "accrued_at"
        case amount
        case amountAsset = "amount_asset"
        case rate
        case notional
        case positionQuantity = "position_quantity"
    }
}

struct HeartbeatData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let component: String
    let sequence: Int
    let status: String
    let lagMs: Int
    let meta: JsonObject?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case component
        case sequence
        case status
        case lagMs = "lag_ms"
        case meta
    }
}

struct InstrumentCapabilityData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let instrumentPublicId: String
    let exchange: String
    let supportedOrderTypes: [String]
    let supportsPostOnly: Bool
    let supportsReduceOnly: Bool
    let supportsAmendInPlace: Bool
    let supportsNativeStopLoss: Bool
    let supportsNativeTakeProfit: Bool
    let supportsTrailingStopClientSide: Bool
    let supportsMarketMaking: Bool
    let supportsShortSelling: Bool
    let supportsLeverage: Bool
    let maxLeverageLong: Double
    let maxLeverageShort: Double
    let minNotional: Double?
    let maxOrderSize: Double?
    let topOfBookQuality: String

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case instrumentPublicId = "instrument_public_id"
        case exchange
        case supportedOrderTypes = "supported_order_types"
        case supportsPostOnly = "supports_post_only"
        case supportsReduceOnly = "supports_reduce_only"
        case supportsAmendInPlace = "supports_amend_in_place"
        case supportsNativeStopLoss = "supports_native_stop_loss"
        case supportsNativeTakeProfit = "supports_native_take_profit"
        case supportsTrailingStopClientSide = "supports_trailing_stop_client_side"
        case supportsMarketMaking = "supports_market_making"
        case supportsShortSelling = "supports_short_selling"
        case supportsLeverage = "supports_leverage"
        case maxLeverageLong = "max_leverage_long"
        case maxLeverageShort = "max_leverage_short"
        case minNotional = "min_notional"
        case maxOrderSize = "max_order_size"
        case topOfBookQuality = "top_of_book_quality"
    }
}

struct OrderCancelData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let exchange: String
    let instrument: String
    let exchangeOrderId: String
    let clientOrderId: String
    let walletPublicId: String?
    let operatorPublicId: String?
    let userPublicId: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case exchange
        case instrument
        case exchangeOrderId = "exchange_order_id"
        case clientOrderId = "client_order_id"
        case walletPublicId = "wallet_public_id"
        case operatorPublicId = "operator_public_id"
        case userPublicId = "user_public_id"
    }
}

struct OrderEventData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let exchangeOrderId: String
    let clientOrderId: String
    let exchange: String
    let instrument: String
    let event: String
    let reason: String?
    let walletPublicId: String?
    let operatorPublicId: String?
    let userPublicId: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case exchangeOrderId = "exchange_order_id"
        case clientOrderId = "client_order_id"
        case exchange
        case instrument
        case event
        case reason
        case walletPublicId = "wallet_public_id"
        case operatorPublicId = "operator_public_id"
        case userPublicId = "user_public_id"
    }
}

struct OrderReplaceData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let exchange: String
    let instrument: String
    let exchangeOrderId: String
    let clientOrderId: String
    let newQuantity: Double?
    let newPrice: Double?
    let walletPublicId: String?
    let operatorPublicId: String?
    let userPublicId: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case exchange
        case instrument
        case exchangeOrderId = "exchange_order_id"
        case clientOrderId = "client_order_id"
        case newQuantity = "new_quantity"
        case newPrice = "new_price"
        case walletPublicId = "wallet_public_id"
        case operatorPublicId = "operator_public_id"
        case userPublicId = "user_public_id"
    }
}

struct OrderRequestData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let strategyId: String
    let exchange: String
    let instrument: String
    let mode: String
    let side: String
    let orderType: String
    let quantity: Double
    let price: Double?
    let clientOrderId: String
    let signaledAt: Date?
    let strategyTag: String?
    let leverage: Int?
    let reduceOnly: Bool?
    let walletPublicId: String?
    let operatorPublicId: String?
    let userPublicId: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case strategyId = "strategy_id"
        case exchange
        case instrument
        case mode
        case side
        case orderType = "order_type"
        case quantity
        case price
        case clientOrderId = "client_order_id"
        case signaledAt = "signaled_at"
        case strategyTag = "strategy_tag"
        case leverage
        case reduceOnly = "reduce_only"
        case walletPublicId = "wallet_public_id"
        case operatorPublicId = "operator_public_id"
        case userPublicId = "user_public_id"
    }
}

struct ReplayEndData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
    }
}

struct ReplayStartData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let startedAt: Date?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case startedAt = "started_at"
    }
}

struct ScopeRevokedData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let grantPublicId: String
    let operatorPublicId: String
    let walletPublicId: String
    let scopeKind: String
    let underlyingPublicId: String?
    let instrumentPublicId: String?
    let revokedAt: Date
    let revokedByUserPublicId: String?
    let reason: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case grantPublicId = "grant_public_id"
        case operatorPublicId = "operator_public_id"
        case walletPublicId = "wallet_public_id"
        case scopeKind = "scope_kind"
        case underlyingPublicId = "underlying_public_id"
        case instrumentPublicId = "instrument_public_id"
        case revokedAt = "revoked_at"
        case revokedByUserPublicId = "revoked_by_user_public_id"
        case reason
    }
}

struct SettingChangedData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let key: String
    let value: String
    let category: String
    let updatedBy: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case key
        case value
        case category
        case updatedBy = "updated_by"
    }
}

struct SymbolAliasUpdateData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let event: String
    let action: String

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case event
        case action
    }
}

struct TickData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let instrument: String
    let exchange: String
    let volume: Double
    let bid: Double?
    let ask: Double?
    let last: Double?
    let isDelayed: Bool?
    let isExtendedHours: Bool?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case instrument
        case exchange
        case volume
        case bid
        case ask
        case last
        case isDelayed = "is_delayed"
        case isExtendedHours = "is_extended_hours"
    }
}

struct TradeData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let instrument: String
    let exchange: String
    let executedAt: Date?
    let price: Double
    let volume: Double
    let side: String?
    let tradeId: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case instrument
        case exchange
        case executedAt = "executed_at"
        case price
        case volume
        case side
        case tradeId = "trade_id"
    }
}

struct UserDeactivatedData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let userPublicId: String
    let deactivatedAt: Date
    let reason: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case userPublicId = "user_public_id"
        case deactivatedAt = "deactivated_at"
        case reason
    }
}

struct VenueFeeScheduleData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let exchange: String
    let instrumentPublicId: String?
    let feeTier: String
    let makerBps: Double
    let takerBps: Double
    let minVolume30D: Double?
    let currency: String

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case exchange
        case instrumentPublicId = "instrument_public_id"
        case feeTier = "fee_tier"
        case makerBps = "maker_bps"
        case takerBps = "taker_bps"
        case minVolume30D = "min_volume_30d"
        case currency
    }
}

struct WSAuthCompleteResponse: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let availableTopics: [String]
    let userRole: UserRole
    let sessionExpiresAt: Date?
    let wsTokenExp: Date

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case availableTopics = "available_topics"
        case userRole = "user_role"
        case sessionExpiresAt = "session_expires_at"
        case wsTokenExp = "ws_token_exp"
    }
}

struct WSAuthExpiredResponse: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
    }
}

struct WSAuthFailedResponse: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let reason: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case reason
    }
}

struct WSAuthOkResponse: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let exp: Date

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case exp
    }
}

struct WSAuthRequiredResponse: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let timeout: Int?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case timeout
    }
}

struct WSAuthenticateRequest: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let wsToken: String

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case wsToken = "ws_token"
    }
}

struct WSErrorResponse: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let message: String

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case message
    }
}

struct WSGetSubscriptionsRequest: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
    }
}

struct WSPingRequest: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
    }
}

struct WSPongResponse: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let activeConnections: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case activeConnections = "active_connections"
    }
}

struct WSReauthOkResponse: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let exp: Date

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case exp
    }
}

struct WSReauthRequest: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let wsToken: String

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case wsToken = "ws_token"
    }
}

struct WSReauthRequiredResponse: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let deadline: Date

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case deadline
    }
}

struct WSSubscribeRequest: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let topics: [String]

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case topics
    }
}

struct WSSubscriptionSuccessResponse: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let action: String
    let status: String
    let topics: [String]
    let deniedTopics: [String]?
    let activeSubscriptions: [String]
    let message: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case action
        case status
        case topics
        case deniedTopics = "denied_topics"
        case activeSubscriptions = "active_subscriptions"
        case message
    }
}

struct WSSubscriptionsListResponse: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let subscriptions: [String]
    let availableTopics: [String]
    let totalAvailable: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case subscriptions
        case availableTopics = "available_topics"
        case totalAvailable = "total_available"
    }
}

struct WSUnsubscribeRequest: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let topics: [String]

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case topics
    }
}
