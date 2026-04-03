// This file was auto-generated from backend schemas.
// DO NOT EDIT - regenerate with: make ios-gen-types

import Foundation

enum CandleDataExchange: String, Codable, Sendable {
    case kraken
    case krakenFutures = "kraken_futures"
    case krakenEquities = "kraken_equities"
    case zonda
    case walutomat
    case polygon
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
    case zonda
    case walutomat
}

enum OrderEventDataExchange: String, Codable, Sendable {
    case paper
    case kraken
    case krakenFutures = "kraken_futures"
    case zonda
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
    case zonda
    case walutomat
}

enum OrderRequestDataExchange: String, Codable, Sendable {
    case paper
    case kraken
    case krakenFutures = "kraken_futures"
    case zonda
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

enum TickDataExchange: String, Codable, Sendable {
    case kraken
    case krakenFutures = "kraken_futures"
    case krakenEquities = "kraken_equities"
    case zonda
    case walutomat
    case polygon
}

enum TradeDataExchange: String, Codable, Sendable {
    case kraken
    case krakenFutures = "kraken_futures"
    case krakenEquities = "kraken_equities"
    case zonda
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

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
    }
}

struct CandleData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
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

struct HeartbeatData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
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
        case component
        case sequence
        case status
        case lagMs = "lag_ms"
        case meta
    }
}

struct OrderCancelData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let exchange: String
    let instrument: String
    let exchangeOrderId: String
    let clientOrderId: String

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case exchange
        case instrument
        case exchangeOrderId = "exchange_order_id"
        case clientOrderId = "client_order_id"
    }
}

struct OrderEventData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let exchangeOrderId: String
    let clientOrderId: String
    let exchange: String
    let instrument: String
    let event: String
    let reason: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case exchangeOrderId = "exchange_order_id"
        case clientOrderId = "client_order_id"
        case exchange
        case instrument
        case event
        case reason
    }
}

struct OrderReplaceData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let exchange: String
    let instrument: String
    let exchangeOrderId: String
    let clientOrderId: String
    let newQuantity: Double?
    let newPrice: Double?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case exchange
        case instrument
        case exchangeOrderId = "exchange_order_id"
        case clientOrderId = "client_order_id"
        case newQuantity = "new_quantity"
        case newPrice = "new_price"
    }
}

struct OrderRequestData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
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

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
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
    }
}

struct ReplayEndData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
    }
}

struct ReplayStartData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let startedAt: Date?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case startedAt = "started_at"
    }
}

struct SettingChangedData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
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
    let event: String
    let action: String

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
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
    let instrument: String
    let exchange: String
    let volume: Double
    let bid: Double?
    let ask: Double?
    let last: Double?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case instrument
        case exchange
        case volume
        case bid
        case ask
        case last
    }
}

struct TradeData: Codable, Sendable {
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
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
        case instrument
        case exchange
        case executedAt = "executed_at"
        case price
        case volume
        case side
        case tradeId = "trade_id"
    }
}

struct WSAuthCompleteResponse: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    /// Topics available for subscription
    let availableTopics: [String]
    /// Authenticated user role
    let userRole: UserRole
    /// Session expiration (ISO 8601)
    let sessionExpiresAt: Date?
    /// WS token expiration (ISO 8601)
    let wsTokenExp: Date

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case availableTopics = "available_topics"
        case userRole = "user_role"
        case sessionExpiresAt = "session_expires_at"
        case wsTokenExp = "ws_token_exp"
    }
}

struct WSAuthExpiredResponse: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
    }
}

struct WSAuthFailedResponse: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    /// Failure reason
    let reason: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case reason
    }
}

struct WSAuthOkResponse: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    /// Token expiration (ISO 8601)
    let exp: Date

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case exp
    }
}

struct WSAuthRequiredResponse: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    /// Authentication timeout in seconds
    let timeout: Int?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case timeout
    }
}

struct WSAuthenticateRequest: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    /// WebSocket authentication token
    let wsToken: String

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case wsToken = "ws_token"
    }
}

struct WSErrorResponse: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    /// Error description
    let message: String

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case message
    }
}

struct WSGetSubscriptionsRequest: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
    }
}

struct WSPingRequest: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
    }
}

struct WSPongResponse: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let sequenceId: Int
    let publicId: String
    /// Server timestamp (ISO 8601)
    let timestamp: Date
    let sessionId: String
    /// Number of active WebSocket connections
    let activeConnections: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case activeConnections = "active_connections"
    }
}

struct WSReauthOkResponse: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    /// New token expiration (ISO 8601)
    let exp: Date

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case exp
    }
}

struct WSReauthRequest: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    /// New WebSocket authentication token
    let wsToken: String

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case wsToken = "ws_token"
    }
}

struct WSReauthRequiredResponse: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    /// Deadline for reauthentication (ISO 8601)
    let deadline: Date

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case deadline
    }
}

struct WSSubscribeRequest: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    /// Topics to subscribe to
    let topics: [String]

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topics
    }
}

struct WSSubscriptionSuccessResponse: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    /// The subscription action performed
    let action: String
    /// Result status of the subscription operation
    let status: String
    /// Topics that were successfully processed
    let topics: [String]
    /// Topics that were denied due to permissions
    let deniedTopics: [String]?
    /// Current list of active subscriptions
    let activeSubscriptions: [String]
    /// Optional message with additional details
    let message: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case action
        case status
        case topics
        case deniedTopics = "denied_topics"
        case activeSubscriptions = "active_subscriptions"
        case message
    }
}

struct WSSubscriptionsListResponse: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    /// Current active subscriptions
    let subscriptions: [String]
    /// Topics available for subscription
    let availableTopics: [String]
    /// Total number of available topics
    let totalAvailable: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case subscriptions
        case availableTopics = "available_topics"
        case totalAvailable = "total_available"
    }
}

struct WSUnsubscribeRequest: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    /// Topics to unsubscribe from
    let topics: [String]

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topics
    }
}
