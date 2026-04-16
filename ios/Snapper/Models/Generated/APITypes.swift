// This file was auto-generated from backend schemas.
// DO NOT EDIT - regenerate with: make ios-gen-types

import Foundation

enum RelationshipTypeEnum: String, Codable, Sendable {
    case exact
    case derivative
    case proxy
}

enum UserRole: String, Codable, Sendable {
    case viewer
    case operatorRole = "operator"
    case admin
}

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

enum ExecutionDataExchange: String, Codable, Sendable {
    case paper
    case kraken
    case krakenFutures = "kraken_futures"
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

enum HealthCheckDataStatus: String, Codable, Sendable {
    case healthy
    case warning
    case error
}

enum OrderDataExchange: String, Codable, Sendable {
    case paper
    case kraken
    case krakenFutures = "kraken_futures"
    case zonda
    case walutomat
}

enum OrderDataMode: String, Codable, Sendable {
    case live
    case paper
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

enum PositionDataExchange: String, Codable, Sendable {
    case paper
    case kraken
    case krakenFutures = "kraken_futures"
    case zonda
    case walutomat
}

enum PositionDataMode: String, Codable, Sendable {
    case live
    case paper
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

enum ProcessSchemaDataDefaultMode: String, Codable, Sendable {
    case thread
    case process
}

enum ProcessSchemaDataLifecycle: String, Codable, Sendable {
    case longRunning = "long_running"
    case oneShot = "one_shot"
}

enum ProcessStartDataStatus: String, Codable, Sendable {
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

enum ProcessStopDataStatus: String, Codable, Sendable {
    case success
    case notRunning = "not_running"
    case error
}

enum SignalDataExchange: String, Codable, Sendable {
    case paper
    case kraken
    case krakenFutures = "kraken_futures"
    case zonda
    case walutomat
}

enum SignalDataSide: String, Codable, Sendable {
    case buy
    case sell
}

enum SignalDiffEntryLeg: String, Codable, Sendable {
    case a
    case b
    case common
}

enum StrategyProcessMode: String, Codable, Sendable {
    case thread
    case process
}

enum TradeDiffEntryLeg: String, Codable, Sendable {
    case a
    case b
    case common
}

enum ZmqComponentsZmqContext: String, Codable, Sendable {
    case ok
    case error
}

enum ZmqComponentsWebsocketManager: String, Codable, Sendable {
    case ok
    case error
}

enum ZmqHealthDataStatus: String, Codable, Sendable {
    case healthy
    case warning
    case error
}

enum BacktestCompareBodyMode: String, Codable, Sendable {
    case manual
    case auto
}

enum CreateCredentialBodyCredentialType: String, Codable, Sendable {
    case apiKeySecret = "api_key_secret"
    case rsaPem = "rsa_pem"
    case oauth
    case paper
}

enum CreateOrderBodyMode: String, Codable, Sendable {
    case live
    case paper
}

enum CreateOrderBodySide: String, Codable, Sendable {
    case buy
    case sell
}

enum CreateOrderBodyOrderType: String, Codable, Sendable {
    case market
    case limit
    case stop
    case stopLimit = "stop_limit"
}

enum CreateScopeGrantBodyScopeKind: String, Codable, Sendable {
    case underlying
    case instrument
}

struct AvailableProcess: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
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
    let parametersSchema: JsonObject?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
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
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [AvailableProcess]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct BacktestComparisonData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let walletPublicId: String
    let runAPublicId: String
    let runBPublicId: String
    let configHash: String?
    let pairingMode: String
    let anchorRunPublicId: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case walletPublicId = "wallet_public_id"
        case runAPublicId = "run_a_public_id"
        case runBPublicId = "run_b_public_id"
        case configHash = "config_hash"
        case pairingMode = "pairing_mode"
        case anchorRunPublicId = "anchor_run_public_id"
    }
}

struct BacktestComparisonDetailResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: BacktestComparisonDetailResponseData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct BacktestComparisonDetailResponseData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let comparison: BacktestComparisonData
    let runA: BacktestRunData
    let runB: BacktestRunData
    let metricsDiff: [MetricDiffRow]
    let equityOverlay: [EquityOverlayPoint]
    let tradesDiff: [TradeDiffEntry]
    let signalsDiff: [SignalDiffEntry]

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case comparison
        case runA = "run_a"
        case runB = "run_b"
        case metricsDiff = "metrics_diff"
        case equityOverlay = "equity_overlay"
        case tradesDiff = "trades_diff"
        case signalsDiff = "signals_diff"
    }
}

struct BacktestComparisonListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [BacktestComparisonData]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct BacktestComparisonResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: BacktestComparisonData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct BacktestEquityPointInline: Codable, Sendable {
    let pointTime: Date
    let equity: Double
    let cash: Double
    let positionValue: Double?
    let drawdown: Double?

    enum CodingKeys: String, CodingKey {
        case pointTime = "point_time"
        case equity
        case cash
        case positionValue = "position_value"
        case drawdown
    }
}

struct BacktestEquityPointListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [BacktestEquityPointInline]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct BacktestEventData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let runPublicId: String
    let eventType: String
    let detail: [String: AnyCodable]?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case runPublicId = "run_public_id"
        case eventType = "event_type"
        case detail
    }
}

struct BacktestEventListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [BacktestEventData]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct BacktestResultInline: Codable, Sendable {
    let totalTrades: Int
    let winningTrades: Int
    let losingTrades: Int
    let totalPnl: Double
    let maxDrawdown: Double
    let sharpeRatio: Double?
    let winRate: Double?
    let profitFactor: Double?
    let finalEquity: Double
    let maxEquity: Double
    let sortinoRatio: Double?
    let cagr: Double?
    let calmarRatio: Double?
    let expectancy: Double?
    let avgTradePnl: Double?
    let maxDrawdownDurationSeconds: Double?
    let exposureRatio: Double?
    let turnoverRatio: Double?
    let extraMetrics: JsonObject?

    enum CodingKeys: String, CodingKey {
        case totalTrades = "total_trades"
        case winningTrades = "winning_trades"
        case losingTrades = "losing_trades"
        case totalPnl = "total_pnl"
        case maxDrawdown = "max_drawdown"
        case sharpeRatio = "sharpe_ratio"
        case winRate = "win_rate"
        case profitFactor = "profit_factor"
        case finalEquity = "final_equity"
        case maxEquity = "max_equity"
        case sortinoRatio = "sortino_ratio"
        case cagr
        case calmarRatio = "calmar_ratio"
        case expectancy
        case avgTradePnl = "avg_trade_pnl"
        case maxDrawdownDurationSeconds = "max_drawdown_duration_seconds"
        case exposureRatio = "exposure_ratio"
        case turnoverRatio = "turnover_ratio"
        case extraMetrics = "extra_metrics"
    }
}

struct BacktestRunData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let walletPublicId: String
    let strategyName: String
    let strategyParams: JsonObject?
    let instrumentPublicId: String
    let exchange: String
    let timeframe: String
    let startDate: Date
    let endDate: Date
    let initialCash: Double
    let status: String
    let executionMode: String?
    let fillModel: String?
    let slippageBps: Double?
    let commissionBps: Double?
    let configHash: String?
    let startedAt: Date?
    let completedAt: Date?
    let error: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case walletPublicId = "wallet_public_id"
        case strategyName = "strategy_name"
        case strategyParams = "strategy_params"
        case instrumentPublicId = "instrument_public_id"
        case exchange
        case timeframe
        case startDate = "start_date"
        case endDate = "end_date"
        case initialCash = "initial_cash"
        case status
        case executionMode = "execution_mode"
        case fillModel = "fill_model"
        case slippageBps = "slippage_bps"
        case commissionBps = "commission_bps"
        case configHash = "config_hash"
        case startedAt = "started_at"
        case completedAt = "completed_at"
        case error
    }
}

struct BacktestRunDetailData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let walletPublicId: String
    let strategyName: String
    let strategyParams: JsonObject?
    let instrumentPublicId: String
    let exchange: String
    let timeframe: String
    let startDate: Date
    let endDate: Date
    let initialCash: Double
    let status: String
    let executionMode: String?
    let fillModel: String?
    let slippageBps: Double?
    let commissionBps: Double?
    let configHash: String?
    let startedAt: Date?
    let completedAt: Date?
    let error: String?
    let result: BacktestResultInline?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case walletPublicId = "wallet_public_id"
        case strategyName = "strategy_name"
        case strategyParams = "strategy_params"
        case instrumentPublicId = "instrument_public_id"
        case exchange
        case timeframe
        case startDate = "start_date"
        case endDate = "end_date"
        case initialCash = "initial_cash"
        case status
        case executionMode = "execution_mode"
        case fillModel = "fill_model"
        case slippageBps = "slippage_bps"
        case commissionBps = "commission_bps"
        case configHash = "config_hash"
        case startedAt = "started_at"
        case completedAt = "completed_at"
        case error
        case result
    }
}

struct BacktestRunDetailResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: BacktestRunDetailData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct BacktestRunListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [BacktestRunData]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct BacktestRunResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: BacktestRunData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct BacktestSignalData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let runPublicId: String
    let signalTime: Date
    let signalType: String
    let instrument: String
    let price: Double
    let indicators: [String: AnyCodable]?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case runPublicId = "run_public_id"
        case signalTime = "signal_time"
        case signalType = "signal_type"
        case instrument
        case price
        case indicators
    }
}

struct BacktestSignalListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [BacktestSignalData]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct BacktestTradeData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let runPublicId: String
    let executedAt: Date
    let instrument: String
    let side: String
    let quantity: Double
    let price: Double
    let fee: Double
    let pnl: Double?
    let positionAfter: Double?
    let signalPublicId: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case runPublicId = "run_public_id"
        case executedAt = "executed_at"
        case instrument
        case side
        case quantity
        case price
        case fee
        case pnl
        case positionAfter = "position_after"
        case signalPublicId = "signal_public_id"
    }
}

struct BacktestTradeListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [BacktestTradeData]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct ConfiguredProcess: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
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
    /// Constructor parameters
    let parameters: JsonObject?
    /// Optional note
    let note: String?
    /// Process lifecycle type
    let lifecycle: String
    /// Process role category
    let role: String
    /// Categorization tags
    let tags: [String]?
    /// JSON Schema for parameters
    let parametersSchema: JsonObject?
    /// Whether process is one-shot task
    let isOneShot: Bool
    /// Active public ID if running
    let activePublicId: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case name
        case enabled
        case running
        case mode
        case classPath = "class_path"
        case method
        case parameters
        case note
        case lifecycle
        case role
        case tags
        case parametersSchema = "parameters_schema"
        case isOneShot = "is_one_shot"
        case activePublicId = "active_public_id"
    }
}

struct ConfiguredProcessesResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [ConfiguredProcess]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct ConnectionStats: Codable, Sendable {
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

struct ContinuousCandleData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let openAt: Date
    let timeframe: String
    let open: Double
    let high: Double
    let low: Double
    let close: Double
    let volume: Double
    let vwap: Double?
    let trades: Int?
    let sourceContract: String
    let adjustmentFactor: Double?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case openAt = "open_at"
        case timeframe
        case open
        case high
        case low
        case close
        case volume
        case vwap
        case trades
        case sourceContract = "source_contract"
        case adjustmentFactor = "adjustment_factor"
    }
}

struct ContinuousCandleListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [ContinuousCandleData]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct ContinuousSeriesPartialResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [ContinuousCandleData]
    let count: Int
    let failedRoll: RollPointDetail
    let message: String

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
        case failedRoll = "failed_roll"
        case message
    }
}

struct ContractData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let instrumentPublicId: String
    let nativeSymbol: String
    let exchange: String
    let expiryAt: Date?
    let instrumentKind: String?
    let relationshipType: String
    let contractFamily: String?
    let isFrontMonth: Bool

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case instrumentPublicId = "instrument_public_id"
        case nativeSymbol = "native_symbol"
        case exchange
        case expiryAt = "expiry_at"
        case instrumentKind = "instrument_kind"
        case relationshipType = "relationship_type"
        case contractFamily = "contract_family"
        case isFrontMonth = "is_front_month"
    }
}

struct ContractListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [ContractData]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct CredentialListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [CredentialSummary]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct CredentialResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: CredentialSummary

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct CredentialSummary: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let walletPublicId: String
    let exchange: String
    let credentialType: String
    let label: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case walletPublicId = "wallet_public_id"
        case exchange
        case credentialType = "credential_type"
        case label
    }
}

struct EquityOverlayPoint: Codable, Sendable {
    let pointTime: Date
    let equityA: Double?
    let equityB: Double?

    enum CodingKeys: String, CodingKey {
        case pointTime = "point_time"
        case equityA = "equity_a"
        case equityB = "equity_b"
    }
}

struct ExchangeListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [String]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct ExecutionData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let tradeId: String?
    let exchangeOrderId: String?
    let clientOrderId: String
    let instrument: String
    let exchange: String
    let side: String
    let size: Double
    let price: Double
    let lastSize: Double
    let lastPrice: Double
    let fee: Double
    let feeAsset: String
    let status: String
    let executedAt: Date
    let walletPublicId: String?
    let operatorPublicId: String?
    let userPublicId: String?
    let liquidityRole: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case tradeId = "trade_id"
        case exchangeOrderId = "exchange_order_id"
        case clientOrderId = "client_order_id"
        case instrument
        case exchange
        case side
        case size
        case price
        case lastSize = "last_size"
        case lastPrice = "last_price"
        case fee
        case feeAsset = "fee_asset"
        case status
        case executedAt = "executed_at"
        case walletPublicId = "wallet_public_id"
        case operatorPublicId = "operator_public_id"
        case userPublicId = "user_public_id"
        case liquidityRole = "liquidity_role"
    }
}

struct ExecutionListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [ExecutionData]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct ExecutionPlanData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let planType: String
    let status: String
    let instrumentPublicId: String
    let exchange: String
    let mode: String
    let side: String
    let totalQuantity: Double
    let filledQuantity: Double
    let createdAt: Date
    let createdVia: String
    let walletPublicId: String
    let operatorPublicId: String?
    let params: [String: AnyCodable]
    let positionCyclePublicId: String?
    let parentPlanPublicId: String?
    let lastError: String?
    let idempotencyKey: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case planType = "plan_type"
        case status
        case instrumentPublicId = "instrument_public_id"
        case exchange
        case mode
        case side
        case totalQuantity = "total_quantity"
        case filledQuantity = "filled_quantity"
        case createdAt = "created_at"
        case createdVia = "created_via"
        case walletPublicId = "wallet_public_id"
        case operatorPublicId = "operator_public_id"
        case params
        case positionCyclePublicId = "position_cycle_public_id"
        case parentPlanPublicId = "parent_plan_public_id"
        case lastError = "last_error"
        case idempotencyKey = "idempotency_key"
    }
}

struct ExecutionPlanResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: ExecutionPlanData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct FrontMonthData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let instrumentPublicId: String
    let nativeSymbol: String
    let exchange: String
    let expiryAt: Date
    let relationshipType: String
    let contractFamily: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case instrumentPublicId = "instrument_public_id"
        case nativeSymbol = "native_symbol"
        case exchange
        case expiryAt = "expiry_at"
        case relationshipType = "relationship_type"
        case contractFamily = "contract_family"
    }
}

struct FrontMonthResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: FrontMonthData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct GapDetectionStats: Codable, Sendable {
    /// ZMQ bridge gap detection stats
    let bridge: GapStats
    /// Per-session REST client gap stats
    let restClients: [String: GapStats]?

    enum CodingKeys: String, CodingKey {
        case bridge
        case restClients = "rest_clients"
    }
}

struct GapStats: Codable, Sendable {
    /// Total missing messages detected
    let gapsDetected: Int?
    /// Producer session resets observed
    let sessionResets: Int?
    /// Duplicate or reordered messages
    let duplicates: Int?
    /// Subscriptions started mid-stream
    let midStreamJoins: Int?
    /// Messages without provenance
    let rejectedUnstamped: Int?

    enum CodingKeys: String, CodingKey {
        case gapsDetected = "gaps_detected"
        case sessionResets = "session_resets"
        case duplicates
        case midStreamJoins = "mid_stream_joins"
        case rejectedUnstamped = "rejected_unstamped"
    }
}

struct HTTPValidationError: Codable, Sendable {
    let detail: [ValidationError]?
}

struct HandoverScopeGrantResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: HandoverScopeGrantResult

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct HandoverScopeGrantResult: Codable, Sendable {
    let closedGrant: ScopeGrantInfo
    let newGrant: ScopeGrantInfo

    enum CodingKeys: String, CodingKey {
        case closedGrant = "closed_grant"
        case newGrant = "new_grant"
    }
}

struct HealthCheckData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    /// Overall service health status
    let status: String
    /// Application version
    let version: String
    /// Connection statistics
    let connections: ConnectionStats
    /// Topics availability
    let topics: HealthTopics
    /// Gap detection statistics
    let gapDetection: GapDetectionStats

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case status
        case version
        case connections
        case topics
        case gapDetection = "gap_detection"
    }
}

struct HealthCheckResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: HealthCheckData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct HealthTopics: Codable, Sendable {
    /// Number of currently active topics
    let active: Int
}

struct InstrumentListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [String]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct JsonObject: Codable, Sendable {
}

struct LoginData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let message: String
    let expiresIn: Int
    let user: UserProfile

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case message
        case expiresIn = "expires_in"
        case user
    }
}

struct LoginResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: LoginData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct MessageResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: String

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct MetricDiffRow: Codable, Sendable {
    let name: String
    let runA: Double?
    let runB: Double?
    let delta: Double?
    let pct: Double?

    enum CodingKeys: String, CodingKey {
        case name
        case runA = "run_a"
        case runB = "run_b"
        case delta
        case pct
    }
}

struct OperatorInfo: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let label: String
    let description: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case label
        case description
    }
}

struct OperatorListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [OperatorInfo]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct OrderData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let exchangeOrderId: String?
    let clientOrderId: String
    let instrument: String
    let exchange: String
    let mode: String?
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
    let createdAt: Date
    let updatedAt: Date?
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
        case exchangeOrderId = "exchange_order_id"
        case clientOrderId = "client_order_id"
        case instrument
        case exchange
        case mode
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
        case leverage
        case reduceOnly = "reduce_only"
        case walletPublicId = "wallet_public_id"
        case operatorPublicId = "operator_public_id"
        case userPublicId = "user_public_id"
    }
}

struct OrderListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [OrderData]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct OrphanSweepResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: OrphanSweepResultData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct OrphanSweepResultData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let closedCount: Int
    let closedCycleIds: [String]

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case closedCount = "closed_count"
        case closedCycleIds = "closed_cycle_ids"
    }
}

struct PositionCycleData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let cyclePublicId: String
    let shardKey: String
    let instrumentPublicId: String
    let exchange: String
    let mode: String
    let walletPublicId: String
    let operatorPublicId: String?
    let direction: String
    let maxQty: Double
    let openedAt: String
    let ageHours: Double

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case cyclePublicId = "cycle_public_id"
        case shardKey = "shard_key"
        case instrumentPublicId = "instrument_public_id"
        case exchange
        case mode
        case walletPublicId = "wallet_public_id"
        case operatorPublicId = "operator_public_id"
        case direction
        case maxQty = "max_qty"
        case openedAt = "opened_at"
        case ageHours = "age_hours"
    }
}

struct PositionCycleListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [PositionCycleData]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct PositionData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let instrument: String
    let exchange: String
    let mode: String?
    let quantity: Double
    let averagePrice: Double
    let unrealizedPnl: Double
    let realizedPnl: Double
    let positionCyclePublicId: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case instrument
        case exchange
        case mode
        case quantity
        case averagePrice = "average_price"
        case unrealizedPnl = "unrealized_pnl"
        case realizedPnl = "realized_pnl"
        case positionCyclePublicId = "position_cycle_public_id"
    }
}

struct PositionListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [PositionData]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct ProcessCategoryCount: Codable, Sendable {
    /// Number of currently running processes
    let running: Int
    /// Total number of configured processes
    let total: Int
}

struct ProcessCreateData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    /// Operation status
    let status: String
    /// Created process info
    let process: ProcessCreatedInfo

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case status
        case process
    }
}

struct ProcessCreateResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: ProcessCreateData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct ProcessCreatedInfo: Codable, Sendable {
    /// Unique process name
    let name: String
    /// Template used for creation
    let template: String
}

struct ProcessRun: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    /// Unique run identifier
    let publicId: String
    let timestamp: Date
    let sessionId: String
    /// Process name
    let processName: String
    /// Run status
    let status: String
    /// Process role
    let role: String
    /// Process lifecycle
    let lifecycle: String
    /// Run parameters
    let parameters: JsonObject?
    /// Run result if completed
    let result: JsonObject?
    /// Error message if failed
    let error: String?
    /// Process tags
    let tags: [String]?
    /// Start time in ISO format
    let startedAt: String
    /// Completion time if finished
    let completedAt: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
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
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [ProcessRun]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct ProcessSchemaData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
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
    /// Default parameters
    let defaultParameters: JsonObject?
    /// Process lifecycle type
    let lifecycle: String

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case name
        case description
        case classPath = "class_path"
        case method
        case defaultEnabled = "default_enabled"
        case defaultMode = "default_mode"
        case defaultParameters = "default_parameters"
        case lifecycle
    }
}

struct ProcessSchemaResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: ProcessSchemaData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct ProcessStartData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    /// Operation status (success, already_running, error)
    let status: String
    /// Process name
    let name: String
    /// Public ID if started
    let processPublicId: String?
    /// Additional message
    let message: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case status
        case name
        case processPublicId = "process_public_id"
        case message
    }
}

struct ProcessStartResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: ProcessStartData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
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

struct ProcessStopData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    /// Operation status (success, not_running, error)
    let status: String
    /// Process name
    let name: String
    /// Additional message
    let message: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case status
        case name
        case message
    }
}

struct ProcessStopResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: ProcessStopData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct ProcessSummaryData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    /// Feed publisher process counts
    let feeds: ProcessCategoryCount
    /// Strategy process counts
    let strategies: ProcessCategoryCount
    /// Executor process counts
    let executors: ProcessCategoryCount
    /// Broker process counts
    let brokers: ProcessCategoryCount

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case feeds
        case strategies
        case executors
        case brokers
    }
}

struct ProcessSummaryResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: ProcessSummaryData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct RefreshData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let message: String
    let wsToken: String
    let wsTokenExp: Date
    let csrfToken: String
    let user: UserProfile

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case message
        case wsToken = "ws_token"
        case wsTokenExp = "ws_token_exp"
        case csrfToken = "csrf_token"
        case user
    }
}

struct RefreshResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: RefreshData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct RollPointDetail: Codable, Sendable {
    let fromContract: String
    let toContract: String
    let rollAt: String

    enum CodingKeys: String, CodingKey {
        case fromContract = "from_contract"
        case toContract = "to_contract"
        case rollAt = "roll_at"
    }
}

struct ScopeGrantInfo: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let operatorPublicId: String
    let walletPublicId: String
    let grantedByUserPublicId: String
    let scopeKind: String
    let underlyingPublicId: String?
    let instrumentPublicId: String?
    let note: String?
    let knownTo: Date

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case operatorPublicId = "operator_public_id"
        case walletPublicId = "wallet_public_id"
        case grantedByUserPublicId = "granted_by_user_public_id"
        case scopeKind = "scope_kind"
        case underlyingPublicId = "underlying_public_id"
        case instrumentPublicId = "instrument_public_id"
        case note
        case knownTo = "known_to"
    }
}

struct ScopeGrantListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [ScopeGrantInfo]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct ScopeGrantResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: ScopeGrantInfo

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct SettingCategoriesResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [String]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct SettingListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [SettingRead]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct SettingRead: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let key: String
    let value: String
    let category: String
    let description: String?
    let updatedAt: Date
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
        case description
        case updatedAt = "updated_at"
        case updatedBy = "updated_by"
    }
}

struct SettingResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: SettingRead

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct SignalData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let instrument: String
    let exchange: String
    let side: String
    let strength: Double
    let reason: String
    let price: Double?
    let strategyName: String?
    let firedAt: Date
    let walletPublicId: String?
    let operatorPublicId: String?
    let userPublicId: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case instrument
        case exchange
        case side
        case strength
        case reason
        case price
        case strategyName = "strategy_name"
        case firedAt = "fired_at"
        case walletPublicId = "wallet_public_id"
        case operatorPublicId = "operator_public_id"
        case userPublicId = "user_public_id"
    }
}

struct SignalDiffEntry: Codable, Sendable {
    let instrument: String
    let signalTime: Date
    let signalType: String
    let leg: String

    enum CodingKeys: String, CodingKey {
        case instrument
        case signalTime = "signal_time"
        case signalType = "signal_type"
        case leg
    }
}

struct SignalListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [SignalData]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct StrategyListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [StrategyProcess]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct StrategyProcess: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    /// Unique process name
    let name: String
    /// Whether process is currently running
    let running: Bool
    /// Whether process autostarts on boot
    let enabled: Bool
    /// Execution mode (thread/process)
    let mode: String

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case name
        case running
        case enabled
        case mode
    }
}

struct StrategyStatusPayload: Codable, Sendable {
    /// Strategy name
    let strategyName: String
    /// Current strategy status
    let status: String
    /// Full raw status
    let details: JsonObject?
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

struct SystemStatusData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let trader: ProcessStatus
    let backtests: [String: ProcessStatus]
    /// List of active strategies from strategy_runner
    let strategies: [StrategyStatusPayload]?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case trader
        case backtests
        case strategies
    }
}

struct SystemStatusResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: SystemStatusData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct TopicMetricSnapshot: Codable, Sendable {
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
    /// Messages with unparseable envelope
    let invalidMessages: Int?
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
        case invalidMessages = "invalid_messages"
        case lastMessageTs = "last_message_ts"
        case throttleMs = "throttle_ms"
        case pattern
    }
}

struct TradeDiffEntry: Codable, Sendable {
    let instrument: String
    let executedAt: Date
    let side: String
    let quantity: Double
    let price: Double
    let leg: String
    let pnlA: Double?
    let pnlB: Double?
    let pnlDelta: Double?

    enum CodingKeys: String, CodingKey {
        case instrument
        case executedAt = "executed_at"
        case side
        case quantity
        case price
        case leg
        case pnlA = "pnl_a"
        case pnlB = "pnl_b"
        case pnlDelta = "pnl_delta"
    }
}

struct TrailingStopStateData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let planPublicId: String
    let status: String
    let trailingPct: Double
    let minLockPct: Double
    let entryPrice: Double
    let peakPrice: Double
    let currentStop: Double
    let side: String

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case planPublicId = "plan_public_id"
        case status
        case trailingPct = "trailing_pct"
        case minLockPct = "min_lock_pct"
        case entryPrice = "entry_price"
        case peakPrice = "peak_price"
        case currentStop = "current_stop"
        case side
    }
}

struct TrailingStopStateResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: TrailingStopStateData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct UnderlyingAssetData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let ticker: String
    let name: String
    let assetClass: String
    let sector: String?
    let instrumentCount: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case ticker
        case name
        case assetClass = "asset_class"
        case sector
        case instrumentCount = "instrument_count"
    }
}

struct UnderlyingAssetListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [UnderlyingAssetData]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct UnderlyingInstrumentData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let instrumentPublicId: String
    let nativeSymbol: String
    let exchange: String
    let assetType: String
    let relationshipType: String
    let contractFamily: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case instrumentPublicId = "instrument_public_id"
        case nativeSymbol = "native_symbol"
        case exchange
        case assetType = "asset_type"
        case relationshipType = "relationship_type"
        case contractFamily = "contract_family"
    }
}

struct UnderlyingInstrumentListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [UnderlyingInstrumentData]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct UserListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [UserProfile]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct UserProfile: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let username: String
    let email: String?
    let role: UserRole
    let isActive: Bool?
    let createdAt: Date
    let operatorPublicIds: [String]?
    let primaryOperatorPublicId: String?
    let activeWalletPublicId: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case username
        case email
        case role
        case isActive = "is_active"
        case createdAt = "created_at"
        case operatorPublicIds = "operator_public_ids"
        case primaryOperatorPublicId = "primary_operator_public_id"
        case activeWalletPublicId = "active_wallet_public_id"
    }
}

struct UserResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: UserProfile

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct ValidationError: Codable, Sendable {
    let loc: [AnyCodable?]
    let msg: String
    let type: String
    let input: AnyCodable?
    let ctx: [String: AnyCodable]?
}

struct WalletInfo: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let label: String
    let description: String?
    let isPaper: Bool

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case label
        case description
        case isPaper = "is_paper"
    }
}

struct WalletListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: [WalletInfo]
    /// Number of items in payload
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
        case count
    }
}

struct WalletResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: WalletInfo

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
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

struct WsStatsData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    /// WebSocket statistics
    let websocket: WebSocketStats
    /// ZMQ bridge statistics
    let zmqBridge: ZmqBridgeStats
    /// Connection statistics
    let connections: ConnectionStats
    /// Topic message statistics
    let topics: [String: TopicMetricSnapshot]
    /// Subscription details
    let subscriptions: SubscriptionsStats
    /// Configuration details
    let config: WsStatsConfig

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case websocket
        case zmqBridge = "zmq_bridge"
        case connections
        case topics
        case subscriptions
        case config
    }
}

struct WsStatsResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: WsStatsData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
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

struct ZmqHealthData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    /// Overall ZMQ bridge health status
    let status: String
    /// Component status details
    let components: ZmqComponents
    /// ZMQ configuration
    let config: ZmqConfig
    /// Connection statistics
    let connections: ConnectionStats
    /// Message statistics per topic
    let messageStats: [String: TopicMetricSnapshot]
    /// Error messages if not healthy
    let errors: [String]?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case status
        case components
        case config
        case connections
        case messageStats = "message_stats"
        case errors
    }
}

struct ZmqHealthResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: ZmqHealthData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct LoginRequest: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: LoginBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct LoginBody: Codable, Sendable {
    let username: String
    let password: String
    let rememberMe: Bool?

    enum CodingKeys: String, CodingKey {
        case username
        case password
        case rememberMe = "remember_me"
    }
}

struct RefreshTokenRequest: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: RefreshTokenPayload

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct RefreshTokenPayload: Codable, Sendable {
    let activeWalletPublicId: String?
    let clearActiveWallet: Bool?

    enum CodingKeys: String, CodingKey {
        case activeWalletPublicId = "active_wallet_public_id"
        case clearActiveWallet = "clear_active_wallet"
    }
}

struct CreateUserRequest: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: CreateUserBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct CreateUserBody: Codable, Sendable {
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

struct UpdateUserRequest: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: UpdateUserBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct UpdateUserBody: Codable, Sendable {
    let email: String?
    let role: UserRole?
    let isActive: Bool?

    enum CodingKeys: String, CodingKey {
        case email
        case role
        case isActive = "is_active"
    }
}

struct DeactivateUserRequest: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: DeactivateUserBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct DeactivateUserBody: Codable, Sendable {
}

struct ChangePasswordRequest: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: ChangePasswordBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct ChangePasswordBody: Codable, Sendable {
    let currentPassword: String
    let newPassword: String

    enum CodingKeys: String, CodingKey {
        case currentPassword = "current_password"
        case newPassword = "new_password"
    }
}

struct AdminResetPasswordRequest: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: AdminResetPasswordBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct AdminResetPasswordBody: Codable, Sendable {
    let newPassword: String

    enum CodingKeys: String, CodingKey {
        case newPassword = "new_password"
    }
}

struct SettingUpdate: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: SettingUpdateBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct SettingUpdateBody: Codable, Sendable {
    /// Setting value as string
    let value: String
    /// Setting category
    let category: String?
    /// Setting description
    let description: String?
}

struct RemoveSettingRequest: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: RemoveSettingBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct RemoveSettingBody: Codable, Sendable {
}

struct BacktestCreateCommand: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: BacktestCreateBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct BacktestCreateBody: Codable, Sendable {
    let strategyClass: String
    let instrumentPublicId: String
    let exchange: String
    let timeframe: String?
    let startDate: Date
    let endDate: Date
    let initialCash: Double?
    let strategyParams: JsonObject?
    let executionMode: String?
    let fillModel: String?
    let slippageBps: Double?
    let commissionBps: Double?

    enum CodingKeys: String, CodingKey {
        case strategyClass = "strategy_class"
        case instrumentPublicId = "instrument_public_id"
        case exchange
        case timeframe
        case startDate = "start_date"
        case endDate = "end_date"
        case initialCash = "initial_cash"
        case strategyParams = "strategy_params"
        case executionMode = "execution_mode"
        case fillModel = "fill_model"
        case slippageBps = "slippage_bps"
        case commissionBps = "commission_bps"
    }
}

struct BacktestCompareRequest: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: BacktestCompareBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct BacktestCompareBody: Codable, Sendable {
    let mode: String
    let runAPublicId: String?
    let runBPublicId: String?
    let configHash: String?
    let anchorRunPublicId: String?

    enum CodingKeys: String, CodingKey {
        case mode
        case runAPublicId = "run_a_public_id"
        case runBPublicId = "run_b_public_id"
        case configHash = "config_hash"
        case anchorRunPublicId = "anchor_run_public_id"
    }
}

struct BacktestCancelCommand: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: BacktestCancelBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct BacktestCancelBody: Codable, Sendable {
    let reason: String?
}

struct CreateCredentialCommand: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: CreateCredentialBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct CreateCredentialBody: Codable, Sendable {
    let exchange: String
    let credentialType: String
    /// Plaintext credential fields, encrypted server-side
    let credentialPayload: [String: String]
    let label: String?

    enum CodingKeys: String, CodingKey {
        case exchange
        case credentialType = "credential_type"
        case credentialPayload = "credential_payload"
        case label
    }
}

struct RotateCredentialCommand: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: RotateCredentialBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct RotateCredentialBody: Codable, Sendable {
    /// New plaintext credential fields, encrypted server-side
    let credentialPayload: [String: String]
    let label: String?

    enum CodingKeys: String, CodingKey {
        case credentialPayload = "credential_payload"
        case label
    }
}

struct BracketCreateCommand: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: BracketCreateBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct BracketCreateBody: Codable, Sendable {
    let positionCyclePublicId: String
    let slPrice: Double?
    let tpPrice: Double?
    let idempotencyKey: String?

    enum CodingKeys: String, CodingKey {
        case positionCyclePublicId = "position_cycle_public_id"
        case slPrice = "sl_price"
        case tpPrice = "tp_price"
        case idempotencyKey = "idempotency_key"
    }
}

struct BracketCancelCommand: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: BracketCancelBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct BracketCancelBody: Codable, Sendable {
    let reason: String?
}

struct CreateOrderCommand: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: CreateOrderBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct CreateOrderBody: Codable, Sendable {
    let instrument: String
    let instrumentPublicId: String
    let exchange: String
    let mode: String?
    let side: String
    let orderType: String
    let quantity: Double
    let price: Double?
    let stopPrice: Double?
    let timeInForce: String?
    let postOnly: Bool?
    let leverage: Int?
    let reduceOnly: Bool?
    let walletPublicId: String
    let operatorPublicId: String?
    let idempotencyKey: String?

    enum CodingKeys: String, CodingKey {
        case instrument
        case instrumentPublicId = "instrument_public_id"
        case exchange
        case mode
        case side
        case orderType = "order_type"
        case quantity
        case price
        case stopPrice = "stop_price"
        case timeInForce = "time_in_force"
        case postOnly = "post_only"
        case leverage
        case reduceOnly = "reduce_only"
        case walletPublicId = "wallet_public_id"
        case operatorPublicId = "operator_public_id"
        case idempotencyKey = "idempotency_key"
    }
}

struct CancelOrderCommand: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: CancelOrderBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct CancelOrderBody: Codable, Sendable {
    let reason: String?
}

struct ProcessCreateRequest: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: ProcessCreateBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct ProcessCreateBody: Codable, Sendable {
    /// Unique process name
    let name: String
    /// Registered process identifier used as template
    let template: String
    /// Whether process should autostart on boot
    let enabled: Bool?
    /// Execution mode override (thread/process)
    let mode: String?
    /// Constructor parameters
    let parameters: JsonObject?
    /// Optional note stored alongside configuration
    let note: String?
}

struct ProcessStartRequest: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: ProcessStartBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct ProcessStartBody: Codable, Sendable {
    /// Execution mode (thread/process) override for this run
    let mode: String?
    /// Constructor parameters override for this run
    let parameters: JsonObject?
}

struct CreateScopeGrantCommand: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: CreateScopeGrantBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct CreateScopeGrantBody: Codable, Sendable {
    let operatorPublicId: String
    let walletPublicId: String
    let scopeKind: String
    let underlyingPublicId: String?
    let instrumentPublicId: String?
    let note: String?

    enum CodingKeys: String, CodingKey {
        case operatorPublicId = "operator_public_id"
        case walletPublicId = "wallet_public_id"
        case scopeKind = "scope_kind"
        case underlyingPublicId = "underlying_public_id"
        case instrumentPublicId = "instrument_public_id"
        case note
    }
}

struct HandoverScopeGrantCommand: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: HandoverScopeGrantBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct HandoverScopeGrantBody: Codable, Sendable {
    let fromGrantPublicId: String
    let toOperatorPublicId: String
    let reason: String?

    enum CodingKeys: String, CodingKey {
        case fromGrantPublicId = "from_grant_public_id"
        case toOperatorPublicId = "to_operator_public_id"
        case reason
    }
}

struct TrailingStopCreateCommand: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: TrailingStopCreateBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct TrailingStopCreateBody: Codable, Sendable {
    let positionCyclePublicId: String
    let trailingPct: Double
    let minLockPct: Double?
    let idempotencyKey: String?

    enum CodingKeys: String, CodingKey {
        case positionCyclePublicId = "position_cycle_public_id"
        case trailingPct = "trailing_pct"
        case minLockPct = "min_lock_pct"
        case idempotencyKey = "idempotency_key"
    }
}

struct TrailingStopCancelCommand: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: TrailingStopCancelBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct TrailingStopCancelBody: Codable, Sendable {
    let reason: String?
}

struct CreateWalletCommand: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let payload: CreateWalletBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case payload
    }
}

struct CreateWalletBody: Codable, Sendable {
    let label: String
    let description: String?
    let isPaper: Bool?

    enum CodingKeys: String, CodingKey {
        case label
        case description
        case isPaper = "is_paper"
    }
}
