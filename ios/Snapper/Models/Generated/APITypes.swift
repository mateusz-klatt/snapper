// This file was auto-generated from backend schemas.
// DO NOT EDIT - regenerate with: make ios-gen-types

import Foundation

enum AvailableProcessLifecycle: String, Codable, Sendable {
    case longRunning = "long_running"
    case oneShot = "one_shot"
}

enum AvailableProcessRole: String, Codable, Sendable {
    case core
    case task
    case strategy
    case backtest
}

enum CandleSnapshotExchange: String, Codable, Sendable {
    case kraken
    case zonda
    case walutomat
    case polygon
}

enum ConfiguredProcessMode: String, Codable, Sendable {
    case thread
    case process
}

enum ConfiguredProcessLifecycle: String, Codable, Sendable {
    case longRunning = "long_running"
    case oneShot = "one_shot"
}

enum ConfiguredProcessRole: String, Codable, Sendable {
    case core
    case task
    case strategy
    case backtest
}

enum ExecutionRecordSide: String, Codable, Sendable {
    case buy
    case sell
}

enum ExecutionRecordExchange: String, Codable, Sendable {
    case paper
    case kraken
    case zonda
    case walutomat
}

enum HealthCheckResponseStatus: String, Codable, Sendable {
    case healthy
    case warning
    case error
}

enum OrderStatusExchange: String, Codable, Sendable {
    case paper
    case kraken
    case zonda
    case walutomat
}

enum OrderStatusSide: String, Codable, Sendable {
    case buy
    case sell
}

enum OrderStatusType: String, Codable, Sendable {
    case market
    case limit
    case stop
    case stopLimit = "stop_limit"
}

enum OrderStatusStatus: String, Codable, Sendable {
    case new
    case submitted
    case openStatus = "open"
    case filled
    case partiallyFilled = "partially_filled"
    case cancelled
    case rejected
}

enum PositionSnapshotExchange: String, Codable, Sendable {
    case paper
    case kraken
    case zonda
    case walutomat
}

enum ProcessRunStatus: String, Codable, Sendable {
    case running
    case succeeded
    case failed
    case cancelled
}

enum ProcessRunRole: String, Codable, Sendable {
    case core
    case task
    case strategy
    case backtest
}

enum ProcessRunLifecycle: String, Codable, Sendable {
    case longRunning = "long_running"
    case oneShot = "one_shot"
}

enum ProcessSchemaResponseDefaultMode: String, Codable, Sendable {
    case thread
    case process
}

enum ProcessSchemaResponseLifecycle: String, Codable, Sendable {
    case longRunning = "long_running"
    case oneShot = "one_shot"
}

enum ProcessStartResponseStatus: String, Codable, Sendable {
    case success
    case alreadyRunning = "already_running"
    case error
}

enum ProcessStatusStatus: String, Codable, Sendable {
    case notRunning = "not_running"
    case running
    case stopped
    case completed
    case error
}

enum ProcessStopResponseStatus: String, Codable, Sendable {
    case success
    case notRunning = "not_running"
    case error
}

enum TradingSignalExchange: String, Codable, Sendable {
    case paper
    case kraken
    case zonda
    case walutomat
}

enum TradingSignalSide: String, Codable, Sendable {
    case buy
    case sell
}

enum ZmqComponentsZmqContext: String, Codable, Sendable {
    case ok
    case error
}

enum ZmqComponentsWebsocketManager: String, Codable, Sendable {
    case ok
    case error
}

enum ZmqHealthResponseStatus: String, Codable, Sendable {
    case healthy
    case warning
    case error
}

struct AdminResetPasswordRequest: Codable, Sendable {
    let newPassword: String

    enum CodingKeys: String, CodingKey {
        case newPassword = "new_password"
    }
}

struct AvailableProcess: Codable, Sendable {
    /// Process identifier
    let name: String
    /// Full Python class path
    let classPath: String
    /// Entry point method name
    let method: String
    /// Human-readable description
    let description: String
    /// Process lifecycle type
    let lifecycle: String
    /// Process role category
    let role: String
    /// Categorization tags
    let tags: [String]?
    /// JSON Schema for parameters
    let parametersSchema: [String: AnyCodable]?

    enum CodingKeys: String, CodingKey {
        case name
        case classPath = "class_path"
        case method
        case description
        case lifecycle
        case role
        case tags
        case parametersSchema = "parameters_schema"
    }
}

struct AvailableProcessesResponse: Codable, Sendable {
    let processes: [AvailableProcess]
    let count: Int
}

struct CandleSnapshot: Codable, Sendable {
    let instrument: String
    let exchange: String
    let timeframe: String
    let timestamp: Date
    let open: Double
    let high: Double
    let low: Double
    let close: Double
    let volume: Double
    let vwap: Double?
    let trades: Int?
}

struct ChangePasswordRequest: Codable, Sendable {
    let currentPassword: String
    let newPassword: String

    enum CodingKeys: String, CodingKey {
        case currentPassword = "current_password"
        case newPassword = "new_password"
    }
}

struct ConfiguredProcess: Codable, Sendable {
    /// Unique process name
    let name: String
    /// Whether process autostarts on boot
    let enabled: Bool
    /// Whether process is currently running
    let running: Bool
    /// Execution mode (thread/process)
    let mode: String
    /// Full Python class path
    let classPath: String
    /// Entry point method name
    let method: String
    /// Constructor arguments
    let args: [AnyCodable]?
    /// Constructor kwargs
    let kwargs: [String: AnyCodable]?
    /// Optional note
    let note: String?
    /// Process lifecycle type
    let lifecycle: String
    /// Process role category
    let role: String
    /// Categorization tags
    let tags: [String]?
    /// JSON Schema for parameters
    let parametersSchema: [String: AnyCodable]?
    /// Whether process is one-shot task
    let isOneShot: Bool
    /// Active run ID if running
    let activeRunId: String?

    enum CodingKeys: String, CodingKey {
        case name
        case enabled
        case running
        case mode
        case classPath = "class_path"
        case method
        case args
        case kwargs
        case note
        case lifecycle
        case role
        case tags
        case parametersSchema = "parameters_schema"
        case isOneShot = "is_one_shot"
        case activeRunId = "active_run_id"
    }
}

struct ConfiguredProcessesResponse: Codable, Sendable {
    let processes: [ConfiguredProcess]
    let count: Int
}

struct ConnectionStatsSchema: Codable, Sendable {
    /// Active WebSocket connections
    let activeConnections: Int?
    /// Active ZMQ subscriber sockets
    let zmqSubscribers: Int?
    /// Running subscriber tasks
    let subscriberTasks: Int?
    /// Topics with subscribers
    let activeTopics: Int?
    /// Unique connected clients
    let activeClients: Int?

    enum CodingKeys: String, CodingKey {
        case activeConnections = "active_connections"
        case zmqSubscribers = "zmq_subscribers"
        case subscriberTasks = "subscriber_tasks"
        case activeTopics = "active_topics"
        case activeClients = "active_clients"
    }
}

struct CreateUserRequest: Codable, Sendable {
    let username: String
    let email: String?
    let password: String
    let role: UserRole
    let isActive: Bool?

    enum CodingKeys: String, CodingKey {
        case username
        case email
        case password
        case role
        case isActive = "is_active"
    }
}

struct ExecutionRecord: Codable, Sendable {
    let id: Int
    let orderId: Int
    let timestamp: Date
    let price: Double
    let size: Double
    let fee: Double
    let feeAsset: String
    let instrument: String
    let side: String
    let exchange: String

    enum CodingKeys: String, CodingKey {
        case id
        case orderId = "order_id"
        case timestamp
        case price
        case size
        case fee
        case feeAsset = "fee_asset"
        case instrument
        case side
        case exchange
    }
}

struct HTTPValidationError: Codable, Sendable {
    let detail: [ValidationError]?
}

struct HealthCheckResponse: Codable, Sendable {
    /// Overall service health status
    let status: String
    /// Timestamp of the health check
    let timestamp: Date
    /// Application version
    let version: String
    /// Connection statistics
    let connections: ConnectionStatsSchema
    /// Topics availability
    let topics: HealthTopics
}

struct HealthTopics: Codable, Sendable {
    /// Total number of available topics
    let available: Int
    /// Number of currently active topics
    let active: Int
}

struct LoginRequest: Codable, Sendable {
    let username: String
    let password: String
    let rememberMe: Bool?

    enum CodingKeys: String, CodingKey {
        case username
        case password
        case rememberMe = "remember_me"
    }
}

struct LoginResponse: Codable, Sendable {
    let message: String
    let expiresIn: Int
    let user: UserProfile

    enum CodingKeys: String, CodingKey {
        case message
        case expiresIn = "expires_in"
        case user
    }
}

struct MessageResponse: Codable, Sendable {
    let message: String
}

struct OrderStatus: Codable, Sendable {
    let id: Int
    let instrument: String
    let exchange: String
    let clientOrderId: String?
    let exchangeOrderId: String?
    let createdAt: Date
    let updatedAt: Date?
    let side: String
    let type: String
    let price: Double?
    let size: Double
    let status: String
    let timeInForce: String?
    let error: String?

    enum CodingKeys: String, CodingKey {
        case id
        case instrument
        case exchange
        case clientOrderId = "client_order_id"
        case exchangeOrderId = "exchange_order_id"
        case createdAt = "created_at"
        case updatedAt = "updated_at"
        case side
        case type
        case price
        case size
        case status
        case timeInForce = "time_in_force"
        case error
    }
}

struct PositionSnapshot: Codable, Sendable {
    let id: Int
    let instrument: String
    let exchange: String
    let quantity: Double
    let averagePrice: Double
    let unrealizedPnl: Double
    let realizedPnl: Double
    let updatedAt: Date

    enum CodingKeys: String, CodingKey {
        case id
        case instrument
        case exchange
        case quantity
        case averagePrice = "average_price"
        case unrealizedPnl = "unrealized_pnl"
        case realizedPnl = "realized_pnl"
        case updatedAt = "updated_at"
    }
}

struct ProcessCategoryCount: Codable, Sendable {
    /// Number of currently running processes
    let running: Int
    /// Total number of configured processes
    let total: Int
}

struct ProcessCreateRequest: Codable, Sendable {
    /// Unique process name
    let name: String
    /// Registered process identifier used as template
    let template: String
    /// Whether process should autostart on boot
    let enabled: Bool?
    /// Execution mode override (thread/process)
    let mode: String?
    /// Constructor positional arguments
    let args: [AnyCodable]?
    /// Constructor keyword arguments
    let kwargs: [String: AnyCodable]?
    /// Optional note stored alongside configuration
    let note: String?
}

struct ProcessCreateResponse: Codable, Sendable {
    /// Operation status
    let status: String
    /// Created process info
    let process: ProcessCreatedInfo
}

struct ProcessCreatedInfo: Codable, Sendable {
    /// Unique process name
    let name: String
    /// Template used for creation
    let template: String
}

struct ProcessRun: Codable, Sendable {
    /// Unique run identifier
    let runId: String
    /// Process name
    let processName: String
    /// Run status
    let status: String
    /// Process role
    let role: String
    /// Process lifecycle
    let lifecycle: String
    /// Run parameters
    let parameters: [String: AnyCodable]?
    /// Run result if completed
    let result: [String: AnyCodable]?
    /// Error message if failed
    let error: String?
    /// Process tags
    let tags: [String]?
    /// Start time in ISO format
    let startedAt: String
    /// Completion time if finished
    let completedAt: String?

    enum CodingKeys: String, CodingKey {
        case runId = "run_id"
        case processName = "process_name"
        case status
        case role
        case lifecycle
        case parameters
        case result
        case error
        case tags
        case startedAt = "started_at"
        case completedAt = "completed_at"
    }
}

struct ProcessRunsResponse: Codable, Sendable {
    let runs: [ProcessRun]
    let count: Int
}

struct ProcessSchemaResponse: Codable, Sendable {
    /// Process identifier
    let name: String
    /// Human-readable description
    let description: String
    /// Full Python class path
    let classPath: String
    /// Entry point method name
    let method: String
    /// Default autostart setting
    let defaultEnabled: Bool
    /// Default execution mode
    let defaultMode: String
    /// Default arguments
    let defaultArgs: [AnyCodable]?
    /// Default kwargs
    let defaultKwargs: [String: AnyCodable]?
    /// Process lifecycle type
    let lifecycle: String

    enum CodingKeys: String, CodingKey {
        case name
        case description
        case classPath = "class_path"
        case method
        case defaultEnabled = "default_enabled"
        case defaultMode = "default_mode"
        case defaultArgs = "default_args"
        case defaultKwargs = "default_kwargs"
        case lifecycle
    }
}

struct ProcessStartRequest: Codable, Sendable {
    /// Execution mode (thread/process) - for ProcessLauncherService, not constructor
    let mode: String?
    /// Constructor positional arguments override
    let args: [AnyCodable]?
    /// Constructor keyword arguments override
    let kwargs: [String: AnyCodable]?
    /// Toggle autostart flag; None keeps stored value
    let autostart: Bool?
}

struct ProcessStartResponse: Codable, Sendable {
    /// Operation status (success, already_running, error)
    let status: String
    /// Process name
    let name: String
    /// Run ID if started
    let runId: String?
    /// Additional message
    let message: String?

    enum CodingKeys: String, CodingKey {
        case status
        case name
        case runId = "run_id"
        case message
    }
}

struct ProcessStatus: Codable, Sendable {
    /// Process status: not_running, running, stopped, completed, error
    let status: String
    /// Process ID if running
    let pid: Int?
    /// Start time in ISO format
    let startedAt: String?
    /// Command that was executed
    let command: String?
    /// Exit code if stopped
    let exitCode: Int?
    /// Error message if failed
    let error: String?

    enum CodingKeys: String, CodingKey {
        case status
        case pid
        case startedAt = "started_at"
        case command
        case exitCode = "exit_code"
        case error
    }
}

struct ProcessStopResponse: Codable, Sendable {
    /// Operation status (success, not_running, error)
    let status: String
    /// Process name
    let name: String
    /// Additional message
    let message: String?
}

struct ProcessSummaryResponse: Codable, Sendable {
    /// Feed publisher process counts
    let feeds: ProcessCategoryCount
    /// Strategy process counts
    let strategies: ProcessCategoryCount
    /// Executor process counts
    let executors: ProcessCategoryCount
    /// Broker process counts
    let brokers: ProcessCategoryCount
}

struct RefreshResponse: Codable, Sendable {
    let message: String
    let wsToken: String
    let wsTokenExp: Date
    let csrfToken: String
    let user: UserProfile

    enum CodingKeys: String, CodingKey {
        case message
        case wsToken = "ws_token"
        case wsTokenExp = "ws_token_exp"
        case csrfToken = "csrf_token"
        case user
    }
}

struct SettingCategoriesResponse: Codable, Sendable {
    /// List of unique setting categories
    let categories: [String]
}

struct SettingRead: Codable, Sendable {
    let key: String
    let value: String
    let category: String
    let description: String?
    let updatedAt: Date
    let updatedBy: String?

    enum CodingKeys: String, CodingKey {
        case key
        case value
        case category
        case description
        case updatedAt = "updated_at"
        case updatedBy = "updated_by"
    }
}

struct SettingUpdate: Codable, Sendable {
    /// Setting value as string
    let value: String
    /// Setting category
    let category: String?
    /// Setting description
    let description: String?
}

struct StrategyStatusPayload: Codable, Sendable {
    /// Strategy name
    let strategyName: String
    /// Current strategy status
    let status: String
    /// Full raw status
    let details: [String: AnyCodable]?
    /// Signals generated count
    let signalsGenerated: Int?
    /// Trades executed count
    let tradesExecuted: Int?
    /// Last signal description
    let lastSignal: String?
    /// Last signal timestamp
    let lastSignalTime: String?
    /// Current PnL
    let pnl: Double?
    /// Process ID
    let pid: Int?
    /// Process uptime
    let uptime: String?

    enum CodingKeys: String, CodingKey {
        case strategyName = "strategy_name"
        case status
        case details
        case signalsGenerated = "signals_generated"
        case tradesExecuted = "trades_executed"
        case lastSignal = "last_signal"
        case lastSignalTime = "last_signal_time"
        case pnl
        case pid
        case uptime
    }
}

struct SubscriptionsStats: Codable, Sendable {
    /// Subscriber count per topic
    let perTopic: [String: Int]
    /// Topics subscribed per client
    let perClient: [String: [String]]

    enum CodingKeys: String, CodingKey {
        case perTopic = "per_topic"
        case perClient = "per_client"
    }
}

struct SystemStatus: Codable, Sendable {
    let trader: ProcessStatus
    let backtests: [String: ProcessStatus]
    /// List of active strategies from strategy_runner
    let strategies: [StrategyStatusPayload]?
}

struct TopicMetricSnapshotSchema: Codable, Sendable {
    /// Current subscriber count
    let activeSubscribers: Int?
    /// Total messages received
    let received: Int?
    /// Messages forwarded to clients
    let forwarded: Int?
    /// Messages dropped by throttling
    let throttled: Int?
    /// Messages dropped by backpressure
    let dropped: Int?
    /// Messages timed out during send
    let timeout: Int?
    /// Errors encountered
    let errors: Int?
    /// Last message timestamp
    let lastMessageTs: Double?
    /// Throttle interval ms
    let throttleMs: Int?
    /// ZMQ subscription pattern
    let pattern: String?

    enum CodingKeys: String, CodingKey {
        case activeSubscribers = "active_subscribers"
        case received
        case forwarded
        case throttled
        case dropped
        case timeout
        case errors
        case lastMessageTs = "last_message_ts"
        case throttleMs = "throttle_ms"
        case pattern
    }
}

struct TradingSignal: Codable, Sendable {
    let id: Int
    let instrument: String
    let exchange: String
    let timestamp: Date
    let side: String
    let strength: Double
    let reason: String
    let strategyName: String?
    let price: Double?

    enum CodingKeys: String, CodingKey {
        case id
        case instrument
        case exchange
        case timestamp
        case side
        case strength
        case reason
        case strategyName = "strategy_name"
        case price
    }
}

struct UpdateUserRequest: Codable, Sendable {
    let email: String?
    let role: UserRole?
    let isActive: Bool?

    enum CodingKeys: String, CodingKey {
        case email
        case role
        case isActive = "is_active"
    }
}

struct UserListResponse: Codable, Sendable {
    let users: [UserProfile]
    let totalCount: Int

    enum CodingKeys: String, CodingKey {
        case users
        case totalCount = "total_count"
    }
}

struct UserProfile: Codable, Sendable {
    let id: String
    let username: String
    let email: String?
    let role: UserRole
    let isActive: Bool?
    let createdAt: Date?
    let lastLogin: Date?

    enum CodingKeys: String, CodingKey {
        case id
        case username
        case email
        case role
        case isActive = "is_active"
        case createdAt = "created_at"
        case lastLogin = "last_login"
    }
}

struct ValidationError: Codable, Sendable {
    let loc: [AnyCodable?]
    let msg: String
    let type: String
    let input: AnyCodable?
    let ctx: [String: AnyCodable]?
}

struct WebSocketStats: Codable, Sendable {
    /// Number of active WebSocket connections
    let activeConnections: Int
    /// Subscriber count per topic
    let topicSubscribers: [String: Int]
    /// Total client count
    let clientCount: Int

    enum CodingKeys: String, CodingKey {
        case activeConnections = "active_connections"
        case topicSubscribers = "topic_subscribers"
        case clientCount = "client_count"
    }
}

struct WsStatsConfig: Codable, Sendable {
    /// ZMQ broker XPUB endpoint
    let brokerXpub: String
    /// Heartbeat interval in milliseconds
    let heartbeatIntervalMs: Int

    enum CodingKeys: String, CodingKey {
        case brokerXpub = "broker_xpub"
        case heartbeatIntervalMs = "heartbeat_interval_ms"
    }
}

struct WsStatsResponse: Codable, Sendable {
    /// WebSocket statistics
    let websocket: WebSocketStats
    /// ZMQ bridge statistics
    let zmqBridge: ZmqBridgeStats
    /// Connection statistics
    let connections: ConnectionStatsSchema
    /// Topic message statistics
    let topics: [String: TopicMetricSnapshotSchema]
    /// Subscription details
    let subscriptions: SubscriptionsStats
    /// Configuration details
    let config: WsStatsConfig

    enum CodingKeys: String, CodingKey {
        case websocket
        case zmqBridge = "zmq_bridge"
        case connections
        case topics
        case subscriptions
        case config
    }
}

struct ZmqBridgeStats: Codable, Sendable {
    /// Number of active ZMQ topics
    let activeTopics: Int
    /// Number of subscriber tasks
    let subscriberTasks: Int
    /// List of available topics
    let availableTopics: [String]

    enum CodingKeys: String, CodingKey {
        case activeTopics = "active_topics"
        case subscriberTasks = "subscriber_tasks"
        case availableTopics = "available_topics"
    }
}

struct ZmqComponents: Codable, Sendable {
    /// ZMQ context status
    let zmqContext: String
    /// WebSocket manager status
    let websocketManager: String
    /// Number of active WebSocket connections
    let activeConnections: Int

    enum CodingKeys: String, CodingKey {
        case zmqContext = "zmq_context"
        case websocketManager = "websocket_manager"
        case activeConnections = "active_connections"
    }
}

struct ZmqConfig: Codable, Sendable {
    /// List of available ZMQ topics
    let availableTopics: [String]

    enum CodingKeys: String, CodingKey {
        case availableTopics = "available_topics"
    }
}

struct ZmqHealthResponse: Codable, Sendable {
    /// Overall ZMQ bridge health status
    let status: String
    /// Timestamp of the health check
    let timestamp: Date
    /// Component status details
    let components: ZmqComponents
    /// ZMQ configuration
    let config: ZmqConfig
    /// Connection statistics
    let connections: ConnectionStatsSchema
    /// Message statistics per topic
    let messageStats: [String: TopicMetricSnapshotSchema]
    /// Error messages if not healthy
    let errors: [String]?

    enum CodingKeys: String, CodingKey {
        case status
        case timestamp
        case components
        case config
        case connections
        case messageStats = "message_stats"
        case errors
    }
}
