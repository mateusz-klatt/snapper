// This file was auto-generated from backend schemas.
// DO NOT EDIT - regenerate with: make ios-gen-types

import Foundation

enum FillEnvelopeSide: String, Codable, Sendable {
    case buy
    case sell
}

enum FillEnvelopeStatus: String, Codable, Sendable {
    case filled
    case partial
}

enum HeartbeatEnvelopeStatus: String, Codable, Sendable {
    case healthy
    case warning
    case error
}

enum OrderCancelEnvelopeExchange: String, Codable, Sendable {
    case paper
    case kraken
    case zonda
    case walutomat
}

enum OrderEventEnvelopeExchange: String, Codable, Sendable {
    case paper
    case kraken
    case zonda
    case walutomat
}

enum OrderEventEnvelopeEvent: String, Codable, Sendable {
    case submitted
    case accepted
    case rejected
    case cancelled
    case expired
    case replaced
}

enum OrderReplaceEnvelopeExchange: String, Codable, Sendable {
    case paper
    case kraken
    case zonda
    case walutomat
}

enum OrderRequestEnvelopeExchange: String, Codable, Sendable {
    case paper
    case kraken
    case zonda
    case walutomat
}

enum OrderRequestEnvelopeMode: String, Codable, Sendable {
    case live
    case paper
}

enum OrderRequestEnvelopeSide: String, Codable, Sendable {
    case buy
    case sell
}

enum OrderRequestEnvelopeOrderType: String, Codable, Sendable {
    case market
    case limit
    case stop
    case stopLimit = "stop_limit"
}

enum OrderStatusEnvelopeSide: String, Codable, Sendable {
    case buy
    case sell
}

enum OrderStatusEnvelopeStatus: String, Codable, Sendable {
    case submitted
    case accepted
    case rejected
    case cancelled
    case expired
    case replaced
}

enum OrderStatusEnvelopeOrderType: String, Codable, Sendable {
    case market
    case limit
    case stop
    case stopLimit = "stop_limit"
}

enum SignalEnvelopeSide: String, Codable, Sendable {
    case buy
    case sell
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

struct BarEnvelope: Codable, Sendable {
    let type: String
    let timestamp: Date?
    let meta: [String: AnyCodable]?
    let instrument: String
    let timeframe: String
    let open: Double
    let high: Double
    let low: Double
    let close: Double
    let volume: Double
    let vwap: Double?
    let trades: Int?
    let exchange: String
}

struct FillEnvelope: Codable, Sendable {
    let type: String
    let timestamp: Date?
    let meta: [String: AnyCodable]?
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
        case type
        case timestamp
        case meta
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

struct HeartbeatEnvelope: Codable, Sendable {
    let type: String
    let timestamp: Date?
    let meta: [String: AnyCodable]?
    let component: String
    let sequence: Int
    let status: String
    let lagMs: Int

    enum CodingKeys: String, CodingKey {
        case type
        case timestamp
        case meta
        case component
        case sequence
        case status
        case lagMs = "lag_ms"
    }
}

struct OrderCancelEnvelope: Codable, Sendable {
    let type: String
    let timestamp: Date?
    let meta: [String: AnyCodable]?
    let exchange: String
    let instrument: String
    let exchangeOrderId: String
    let clientOrderId: String

    enum CodingKeys: String, CodingKey {
        case type
        case timestamp
        case meta
        case exchange
        case instrument
        case exchangeOrderId = "exchange_order_id"
        case clientOrderId = "client_order_id"
    }
}

struct OrderEventEnvelope: Codable, Sendable {
    let type: String
    let timestamp: Date?
    let meta: [String: AnyCodable]?
    let exchangeOrderId: String
    let clientOrderId: String
    let exchange: String
    let instrument: String
    let event: String
    let reason: String?

    enum CodingKeys: String, CodingKey {
        case type
        case timestamp
        case meta
        case exchangeOrderId = "exchange_order_id"
        case clientOrderId = "client_order_id"
        case exchange
        case instrument
        case event
        case reason
    }
}

struct OrderReplaceEnvelope: Codable, Sendable {
    let type: String
    let timestamp: Date?
    let meta: [String: AnyCodable]?
    let exchange: String
    let instrument: String
    let exchangeOrderId: String
    let clientOrderId: String
    let newQuantity: Double?
    let newPrice: Double?

    enum CodingKeys: String, CodingKey {
        case type
        case timestamp
        case meta
        case exchange
        case instrument
        case exchangeOrderId = "exchange_order_id"
        case clientOrderId = "client_order_id"
        case newQuantity = "new_quantity"
        case newPrice = "new_price"
    }
}

struct OrderRequestEnvelope: Codable, Sendable {
    let type: String
    let timestamp: Date?
    let meta: [String: AnyCodable]?
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
        case timestamp
        case meta
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

struct OrderStatusEnvelope: Codable, Sendable {
    let type: String
    let timestamp: Date?
    let meta: [String: AnyCodable]?
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
    let createdAt: Date?
    let updatedAt: Date?

    enum CodingKeys: String, CodingKey {
        case type
        case timestamp
        case meta
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
        case createdAt = "created_at"
        case updatedAt = "updated_at"
    }
}

struct ReplayEndEnvelope: Codable, Sendable {
    let type: String
    let timestamp: Date?
    let meta: [String: AnyCodable]?
}

struct ReplayStartEnvelope: Codable, Sendable {
    let type: String
    let timestamp: Date?
    let meta: [String: AnyCodable]?
    let startedAt: Date?

    enum CodingKeys: String, CodingKey {
        case type
        case timestamp
        case meta
        case startedAt = "started_at"
    }
}

struct SettingChangedEnvelope: Codable, Sendable {
    let type: String
    let timestamp: Date?
    let meta: [String: AnyCodable]?
    let key: String
    let value: String
    let category: String
    let updatedBy: String?

    enum CodingKeys: String, CodingKey {
        case type
        case timestamp
        case meta
        case key
        case value
        case category
        case updatedBy = "updated_by"
    }
}

struct SignalEnvelope: Codable, Sendable {
    let type: String
    let timestamp: Date?
    let meta: [String: AnyCodable]?
    let instrument: String
    let side: String
    let strength: Double
    let reason: String
    let price: Double?
    let strategyName: String?
    let id: String?
    let exchange: String

    enum CodingKeys: String, CodingKey {
        case type
        case timestamp
        case meta
        case instrument
        case side
        case strength
        case reason
        case price
        case strategyName = "strategy_name"
        case id
        case exchange
    }
}

struct SymbolMappingUpdateEnvelope: Codable, Sendable {
    let type: String
    let timestamp: Date?
    let meta: [String: AnyCodable]?
    let event: String
    let action: String
}

struct TickEnvelope: Codable, Sendable {
    let type: String
    let timestamp: Date?
    let meta: [String: AnyCodable]?
    let instrument: String
    let volume: Double
    let bid: Double?
    let ask: Double?
    let last: Double?
    let exchange: String
}

struct TradeEnvelope: Codable, Sendable {
    let type: String
    let timestamp: Date?
    let meta: [String: AnyCodable]?
    let instrument: String
    let price: Double
    let volume: Double
    let side: String?
    let exchange: String
}

struct WSAuthCompleteResponse: Codable, Sendable {
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    /// Topics available for subscription
    let availableTopics: [String]
    /// Authenticated user role
    let userRole: String
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
