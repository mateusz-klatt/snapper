// This file was auto-generated from backend schemas.
// DO NOT EDIT - regenerate with: make ios-gen-types

import Foundation

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
    let publicId: String?
    let type: String
    let timestamp: Date?
    let sessionId: String?
    let sequenceId: Int?

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
    }
}

struct HeartbeatData: Codable, Sendable {
    let publicId: String?
    let type: String
    let timestamp: Date?
    let sessionId: String
    let sequenceId: Int
    let component: String
    let sequence: Int
    let status: String
    let lagMs: Int
    let meta: [String: AnyCodable]?

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
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
    let sessionId: String
    let sequenceId: Int
    let exchange: String
    let instrument: String
    let exchangeOrderId: String
    let clientOrderId: String

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
        case exchange
        case instrument
        case exchangeOrderId = "exchange_order_id"
        case clientOrderId = "client_order_id"
    }
}

struct OrderEventData: Codable, Sendable {
    let publicId: String?
    let type: String
    let timestamp: Date?
    let sessionId: String
    let sequenceId: Int
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
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
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
    let sessionId: String
    let sequenceId: Int
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
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
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
    let sessionId: String
    let sequenceId: Int
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
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
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
    let publicId: String?
    let type: String
    let timestamp: Date?
    let sessionId: String
    let sequenceId: Int

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
    }
}

struct ReplayStartData: Codable, Sendable {
    let publicId: String?
    let type: String
    let timestamp: Date?
    let sessionId: String
    let sequenceId: Int
    let startedAt: Date?

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
        case startedAt = "started_at"
    }
}

struct SettingChangedData: Codable, Sendable {
    let publicId: String?
    let type: String
    let timestamp: Date?
    let sessionId: String
    let sequenceId: Int
    let key: String
    let value: String
    let category: String
    let updatedBy: String?

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
        case key
        case value
        case category
        case updatedBy = "updated_by"
    }
}

struct SymbolAliasUpdateData: Codable, Sendable {
    let publicId: String?
    let type: String
    let timestamp: Date?
    let sessionId: String
    let sequenceId: Int
    let event: String
    let action: String

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
        case event
        case action
    }
}

struct TickData: Codable, Sendable {
    let publicId: String?
    let type: String
    let timestamp: Date?
    let sessionId: String
    let sequenceId: Int
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
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
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
    let sessionId: String
    let sequenceId: Int
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
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
        case instrument
        case exchange
        case executedAt = "executed_at"
        case price
        case volume
        case side
    }
}

struct WSAuthCompleteResponse: Codable, Sendable {
    let publicId: String?
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    let sessionId: String?
    let sequenceId: Int?
    /// Topics available for subscription
    let availableTopics: [String]
    /// Authenticated user role
    let userRole: UserRole
    /// Session expiration (ISO 8601)
    let sessionExpiresAt: Date?
    /// WS token expiration (ISO 8601)
    let wsTokenExp: Date

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
        case availableTopics = "available_topics"
        case userRole = "user_role"
        case sessionExpiresAt = "session_expires_at"
        case wsTokenExp = "ws_token_exp"
    }
}

struct WSAuthExpiredResponse: Codable, Sendable {
    let publicId: String?
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    let sessionId: String?
    let sequenceId: Int?

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
    }
}

struct WSAuthFailedResponse: Codable, Sendable {
    let publicId: String?
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    let sessionId: String?
    let sequenceId: Int?
    /// Failure reason
    let reason: String?

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
        case reason
    }
}

struct WSAuthOkResponse: Codable, Sendable {
    let publicId: String?
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    let sessionId: String?
    let sequenceId: Int?
    /// Token expiration (ISO 8601)
    let exp: Date

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
        case exp
    }
}

struct WSAuthRequiredResponse: Codable, Sendable {
    let publicId: String?
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    let sessionId: String?
    let sequenceId: Int?
    /// Authentication timeout in seconds
    let timeout: Int?

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
        case timeout
    }
}

struct WSAuthenticateRequest: Codable, Sendable {
    let publicId: String?
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    let sessionId: String?
    let sequenceId: Int?
    /// WebSocket authentication token
    let wsToken: String

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
        case wsToken = "ws_token"
    }
}

struct WSErrorResponse: Codable, Sendable {
    let publicId: String?
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    let sessionId: String?
    let sequenceId: Int?
    /// Error description
    let message: String

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
        case message
    }
}

struct WSGetSubscriptionsRequest: Codable, Sendable {
    let publicId: String?
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    let sessionId: String?
    let sequenceId: Int?

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
    }
}

struct WSGetTopicSuggestionsRequest: Codable, Sendable {
    let publicId: String?
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    let sessionId: String?
    let sequenceId: Int?
    /// Search prefix for topics
    let prefix: String?

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
        case prefix
    }
}

struct WSPingRequest: Codable, Sendable {
    let publicId: String?
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    let sessionId: String?
    let sequenceId: Int?

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
    }
}

struct WSPongResponse: Codable, Sendable {
    let publicId: String?
    /// Message type discriminator
    let type: String
    /// Server timestamp (ISO 8601)
    let timestamp: Date
    let sessionId: String?
    let sequenceId: Int?
    /// Number of active WebSocket connections
    let activeConnections: Int

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
        case activeConnections = "active_connections"
    }
}

struct WSReauthOkResponse: Codable, Sendable {
    let publicId: String?
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    let sessionId: String?
    let sequenceId: Int?
    /// New token expiration (ISO 8601)
    let exp: Date

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
        case exp
    }
}

struct WSReauthRequest: Codable, Sendable {
    let publicId: String?
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    let sessionId: String?
    let sequenceId: Int?
    /// New WebSocket authentication token
    let wsToken: String

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
        case wsToken = "ws_token"
    }
}

struct WSReauthRequiredResponse: Codable, Sendable {
    let publicId: String?
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    let sessionId: String?
    let sequenceId: Int?
    /// Deadline for reauthentication (ISO 8601)
    let deadline: Date

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
        case deadline
    }
}

struct WSSubscribeRequest: Codable, Sendable {
    let publicId: String?
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    let sessionId: String?
    let sequenceId: Int?
    /// Topics to subscribe to
    let topics: [String]

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
        case topics
    }
}

struct WSSubscriptionSuccessResponse: Codable, Sendable {
    let publicId: String?
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    let sessionId: String?
    let sequenceId: Int?
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
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
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
    let publicId: String?
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    let sessionId: String?
    let sequenceId: Int?
    /// Current active subscriptions
    let subscriptions: [String]
    /// Topics available for subscription
    let availableTopics: [String]
    /// Total number of available topics
    let totalAvailable: Int

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
        case subscriptions
        case availableTopics = "available_topics"
        case totalAvailable = "total_available"
    }
}

struct WSTopicSuggestionsResponse: Codable, Sendable {
    let publicId: String?
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    let sessionId: String?
    let sequenceId: Int?
    /// Search prefix that was used
    let prefix: String
    /// Matching topic names
    let suggestions: [String]

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
        case prefix
        case suggestions
    }
}

struct WSUnsubscribeRequest: Codable, Sendable {
    let publicId: String?
    /// Message type discriminator
    let type: String
    let timestamp: Date?
    let sessionId: String?
    let sequenceId: Int?
    /// Topics to unsubscribe from
    let topics: [String]

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case type
        case timestamp
        case sessionId = "session_id"
        case sequenceId = "sequence_id"
        case topics
    }
}
