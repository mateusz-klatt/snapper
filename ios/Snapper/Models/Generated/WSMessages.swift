// This file was auto-generated from backend schemas.
// DO NOT EDIT - regenerate with: make ios-gen-types

import Foundation

enum UserRole: String, Codable, Sendable {
    case viewer
    case operatorRole = "operator"
    case admin
}

enum CandleDataExchange: String, Codable, Sendable {
    case kraken
    case zonda
    case walutomat
    case polygon
}

enum ExecutionDataExchange: String, Codable, Sendable {
    case paper
    case kraken
    case zonda
    case walutomat
}

enum ExecutionDataSide: String, Codable, Sendable {
    case buy
    case sell
}

enum ExecutionDataStatus: String, Codable, Sendable {
    case filled
    case partial
}

enum HeartbeatDataStatus: String, Codable, Sendable {
    case healthy
    case warning
    case error
}

enum OrderCancelDataExchange: String, Codable, Sendable {
    case paper
    case kraken
    case zonda
    case walutomat
}

enum OrderDataExchange: String, Codable, Sendable {
    case paper
    case kraken
    case zonda
    case walutomat
}

enum OrderDataSide: String, Codable, Sendable {
    case buy
    case sell
}

enum OrderDataOrderType: String, Codable, Sendable {
    case market
    case limit
    case stop
    case stopLimit = "stop_limit"
}

enum OrderEventDataExchange: String, Codable, Sendable {
    case paper
    case kraken
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
    case zonda
    case walutomat
}

enum OrderRequestDataExchange: String, Codable, Sendable {
    case paper
    case kraken
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

enum PositionDataExchange: String, Codable, Sendable {
    case paper
    case kraken
    case zonda
    case walutomat
}

enum SignalDataExchange: String, Codable, Sendable {
    case paper
    case kraken
    case zonda
    case walutomat
}

enum SignalDataSide: String, Codable, Sendable {
    case buy
    case sell
}

enum TickDataExchange: String, Codable, Sendable {
    case kraken
    case zonda
    case walutomat
    case polygon
}

enum TradeDataExchange: String, Codable, Sendable {
    case kraken
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
    let timestamp: Date?
}

struct CandleData: Codable, Sendable {
    let publicId: String?
    let type: String
    let timestamp: Date?
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
        case publicId = "public_id"
        case type
        case timestamp
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

struct ExecutionData: Codable, Sendable {
    let publicId: String?
    let type: String
    let timestamp: Date?
    let tradeId: String?
    let exchangeOrderId: String?
    let clientOrderId: String
    let instrument: String
    let exchange: String
    let side: String
    let size: Double
    let price: Double
    let fee: Double
    let feeAsset: String
    let status: String
    let executedAt: Date?

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case tradeId = "trade_id"
        case exchangeOrderId = "exchange_order_id"
        case clientOrderId = "client_order_id"
        case instrument
        case exchange
        case side
        case size
        case price
        case fee
        case feeAsset = "fee_asset"
        case status
        case executedAt = "executed_at"
    }
}

struct HeartbeatData: Codable, Sendable {
    let publicId: String?
    let type: String
    let timestamp: Date?
    let component: String
    let sequence: Int
    let status: String
    let lagMs: Int
    let meta: [String: AnyCodable]?

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case component
        case sequence
        case status
        case lagMs = "lag_ms"
        case meta
    }
}

struct OrderCancelData: Codable, Sendable {
    let publicId: String?
    let type: String
    let timestamp: Date?
    let exchange: String
    let instrument: String
    let exchangeOrderId: String
    let clientOrderId: String

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case exchange
        case instrument
        case exchangeOrderId = "exchange_order_id"
        case clientOrderId = "client_order_id"
    }
}

struct OrderData: Codable, Sendable {
    let publicId: String?
    let type: String
    let timestamp: Date?
    let exchangeOrderId: String?
    let clientOrderId: String
    let instrument: String
    let exchange: String
    let side: String
    let status: String
    let orderType: String
    let size: Double
    let filledSize: Double
    let price: Double?
    let averagePrice: Double?
    let reason: String?
    let timeInForce: String?
    let error: String?
    let createdAt: Date?
    let updatedAt: Date?

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case exchangeOrderId = "exchange_order_id"
        case clientOrderId = "client_order_id"
        case instrument
        case exchange
        case side
        case status
        case orderType = "order_type"
        case size
        case filledSize = "filled_size"
        case price
        case averagePrice = "average_price"
        case reason
        case timeInForce = "time_in_force"
        case error
        case createdAt = "created_at"
        case updatedAt = "updated_at"
    }
}

struct OrderEventData: Codable, Sendable {
    let publicId: String?
    let type: String
    let timestamp: Date?
    let exchangeOrderId: String
    let clientOrderId: String
    let exchange: String
    let instrument: String
    let event: String
    let reason: String?

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case exchangeOrderId = "exchange_order_id"
        case clientOrderId = "client_order_id"
        case exchange
        case instrument
        case event
        case reason
    }
}

struct OrderReplaceData: Codable, Sendable {
    let publicId: String?
    let type: String
    let timestamp: Date?
    let exchange: String
    let instrument: String
    let exchangeOrderId: String
    let clientOrderId: String
    let newQuantity: Double?
    let newPrice: Double?

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case exchange
        case instrument
        case exchangeOrderId = "exchange_order_id"
        case clientOrderId = "client_order_id"
        case newQuantity = "new_quantity"
        case newPrice = "new_price"
    }
}

struct OrderRequestData: Codable, Sendable {
    let publicId: String?
    let type: String
    let timestamp: Date?
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
        case publicId = "public_id"
        case type
        case timestamp
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

struct PositionData: Codable, Sendable {
    let publicId: String?
    let type: String
    let timestamp: Date?
    let instrument: String
    let exchange: String
    let quantity: Double
    let averagePrice: Double
    let unrealizedPnl: Double
    let realizedPnl: Double

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case instrument
        case exchange
        case quantity
        case averagePrice = "average_price"
        case unrealizedPnl = "unrealized_pnl"
        case realizedPnl = "realized_pnl"
    }
}

struct ReplayEndData: Codable, Sendable {
    let publicId: String?
    let type: String
    let timestamp: Date?

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
    }
}

struct ReplayStartData: Codable, Sendable {
    let publicId: String?
    let type: String
    let timestamp: Date?
    let startedAt: Date?

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case startedAt = "started_at"
    }
}

struct SettingChangedData: Codable, Sendable {
    let publicId: String?
    let type: String
    let timestamp: Date?
    let key: String
    let value: String
    let category: String
    let updatedBy: String?

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case key
        case value
        case category
        case updatedBy = "updated_by"
    }
}

struct SignalData: Codable, Sendable {
    let publicId: String?
    let type: String
    let timestamp: Date?
    let instrument: String
    let exchange: String
    let side: String
    let strength: Double
    let reason: String
    let price: Double?
    let strategyName: String?
    let firedAt: Date?

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case instrument
        case exchange
        case side
        case strength
        case reason
        case price
        case strategyName = "strategy_name"
        case firedAt = "fired_at"
    }
}

struct SymbolAliasUpdateData: Codable, Sendable {
    let publicId: String?
    let type: String
    let timestamp: Date?
    let event: String
    let action: String

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case event
        case action
    }
}

struct TickData: Codable, Sendable {
    let publicId: String?
    let type: String
    let timestamp: Date?
    let instrument: String
    let exchange: String
    let volume: Double
    let bid: Double?
    let ask: Double?
    let last: Double?

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case instrument
        case exchange
        case volume
        case bid
        case ask
        case last
    }
}

struct TradeData: Codable, Sendable {
    let publicId: String?
    let type: String
    let timestamp: Date?
    let instrument: String
    let exchange: String
    let executedAt: Date?
    let price: Double
    let volume: Double
    let side: String?

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case instrument
        case exchange
        case executedAt = "executed_at"
        case price
        case volume
        case side
    }
}

struct WSAuthCompleteResponse: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let timestamp: Date?
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
        case timestamp
        case availableTopics = "available_topics"
        case userRole = "user_role"
        case sessionExpiresAt = "session_expires_at"
        case wsTokenExp = "ws_token_exp"
    }
}

struct WSAuthExpiredResponse: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let timestamp: Date?
}

struct WSAuthFailedResponse: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    /// Failure reason
    let reason: String?
}

struct WSAuthOkResponse: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    /// Token expiration (ISO 8601)
    let exp: Date
}

struct WSAuthRequiredResponse: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    /// Authentication timeout in seconds
    let timeout: Int?
}

struct WSAuthenticateRequest: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    /// WebSocket authentication token
    let wsToken: String

    enum CodingKeys: String, CodingKey {
        case type
        case timestamp
        case wsToken = "ws_token"
    }
}

struct WSErrorResponse: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    /// Error description
    let message: String
}

struct WSGetSubscriptionsRequest: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let timestamp: Date?
}

struct WSGetTopicSuggestionsRequest: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    /// Search prefix for topics
    let prefix: String?
}

struct WSPingRequest: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let timestamp: Date?
}

struct WSPongResponse: Codable, Sendable {
    /// Message type discriminator
    let type: String
    /// Server timestamp (ISO 8601)
    let timestamp: Date
    /// Number of active WebSocket connections
    let activeConnections: Int

    enum CodingKeys: String, CodingKey {
        case type
        case timestamp
        case activeConnections = "active_connections"
    }
}

struct WSReauthOkResponse: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    /// New token expiration (ISO 8601)
    let exp: Date
}

struct WSReauthRequest: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    /// New WebSocket authentication token
    let wsToken: String

    enum CodingKeys: String, CodingKey {
        case type
        case timestamp
        case wsToken = "ws_token"
    }
}

struct WSReauthRequiredResponse: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    /// Deadline for reauthentication (ISO 8601)
    let deadline: Date
}

struct WSSubscribeRequest: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    /// Topics to subscribe to
    let topics: [String]
}

struct WSSubscriptionSuccessResponse: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let timestamp: Date?
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
    /// ZMQ topics that were mapped
    let zmqTopics: [String]?
    /// Optional message with additional details
    let message: String?

    enum CodingKeys: String, CodingKey {
        case type
        case timestamp
        case action
        case status
        case topics
        case deniedTopics = "denied_topics"
        case activeSubscriptions = "active_subscriptions"
        case zmqTopics = "zmq_topics"
        case message
    }
}

struct WSSubscriptionsListResponse: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    /// Current active subscriptions
    let subscriptions: [String]
    /// Topics available for subscription
    let availableTopics: [String]
    /// Total number of available topics
    let totalAvailable: Int

    enum CodingKeys: String, CodingKey {
        case type
        case timestamp
        case subscriptions
        case availableTopics = "available_topics"
        case totalAvailable = "total_available"
    }
}

struct WSTopicSuggestionsResponse: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    /// Search prefix that was used
    let prefix: String
    /// Matching topic names
    let suggestions: [String]
}

struct WSUnsubscribeRequest: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    /// Topics to unsubscribe from
    let topics: [String]
}
