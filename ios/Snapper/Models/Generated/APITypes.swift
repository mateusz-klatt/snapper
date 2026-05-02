// This file was auto-generated from backend schemas.
// DO NOT EDIT - regenerate with: make ios-gen-types

import Foundation

enum RelationshipTypeEnum: String, Codable, Sendable {
    case exact
    case derivative
    case proxy
}

enum UserRole: String, Codable, Sendable {
    case aiDelegate = "ai_delegate"
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

enum DeviceAlertPrefBodyAlertType: String, Codable, Sendable {
    case orderFillFull = "order_fill_full"
    case orderRejected = "order_rejected"
    case positionStopLossFired = "position_stop_loss_fired"
    case marginWarning = "margin_warning"
    case criticalSystemError = "critical_system_error"
}

enum DeviceAlertPrefBodyMinPriority: String, Codable, Sendable {
    case low
    case medium
    case high
}

enum ExecutionDataExchange: String, Codable, Sendable {
    case paper
    case kraken
    case krakenFutures = "kraken_futures"
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

enum RegisterDeviceBodyEnv: String, Codable, Sendable {
    case sandbox
    case prod
}

enum RegisterDeviceBodyPreviewsMode: String, Codable, Sendable {
    case privateValue = "private"
    case publicValue = "public"
}

enum SignalDataExchange: String, Codable, Sendable {
    case paper
    case kraken
    case krakenFutures = "kraken_futures"
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

enum TableStatsItemTableKind: String, Codable, Sendable {
    case event
    case state
}

enum TradeDiffEntryLeg: String, Codable, Sendable {
    case a
    case b
    case common
}

enum UserAlertDefaultBodyAlertType: String, Codable, Sendable {
    case orderFillFull = "order_fill_full"
    case orderRejected = "order_rejected"
    case positionStopLossFired = "position_stop_loss_fired"
    case marginWarning = "margin_warning"
    case criticalSystemError = "critical_system_error"
}

enum UserAlertDefaultBodyMinPriority: String, Codable, Sendable {
    case low
    case medium
    case high
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

struct AiReviewDecisionResponse: Codable, Sendable {
    let success: Bool
    let errorCode: String?
    let message: String
    let details: JsonObject

    enum CodingKeys: String, CodingKey {
        case success
        case errorCode = "error_code"
        case message
        case details
    }
}

struct AlertEventInfo: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let userPublicId: String
    let operatorPublicId: String?
    let walletPublicId: String?
    let alertType: String
    let priority: String
    let isSafetyCritical: Bool
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

struct AlertEventResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: AlertEventInfo

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct AlertHistoryResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: [AlertEventInfo]
    let count: Int
    let nextCursor: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
        case count
        case nextCursor = "next_cursor"
    }
}

struct AsyncioMetrics: Codable, Sendable {
    let activeTasks: Int
    let pendingTasks: Int

    enum CodingKeys: String, CodingKey {
        case activeTasks = "active_tasks"
        case pendingTasks = "pending_tasks"
    }
}

struct AvailableProcess: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let name: String
    let classPath: String
    let method: String
    let description: String
    let lifecycle: String
    let role: String
    let tags: [String]?
    let parametersSchema: JsonObject?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: [AvailableProcess]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
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
        case topic
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
    let topic: String?
    let payload: BacktestComparisonDetailResponseData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct BacktestComparisonDetailResponseData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
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
        case topic
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
    let topic: String?
    let payload: [BacktestComparisonData]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: BacktestComparisonData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: [BacktestEquityPointInline]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let runPublicId: String
    let eventType: String
    let detail: [String: AnyCodable]?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: [BacktestEventData]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
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
    let targetExecutionExchange: String?
    let startedAt: Date?
    let completedAt: Date?
    let error: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
        case targetExecutionExchange = "target_execution_exchange"
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
    let topic: String?
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
    let targetExecutionExchange: String?
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
        case topic
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
        case targetExecutionExchange = "target_execution_exchange"
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
    let topic: String?
    let payload: BacktestRunDetailData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct BacktestRunListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: [BacktestRunData]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: BacktestRunData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct BacktestSignalData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
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
        case topic
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
    let topic: String?
    let payload: [BacktestSignalData]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
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
        case topic
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
    let topic: String?
    let payload: [BacktestTradeData]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let name: String
    let enabled: Bool
    let running: Bool
    let mode: String
    let classPath: String
    let method: String
    let parameters: JsonObject?
    let note: String?
    let lifecycle: String
    let role: String
    let tags: [String]?
    let parametersSchema: JsonObject?
    let isOneShot: Bool
    let activePublicId: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: [ConfiguredProcess]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
        case count
    }
}

struct ConnectionStats: Codable, Sendable {
    let activeConnections: Int?
    let zmqSubscribers: Int?
    let subscriberTasks: Int?
    let activeTopics: Int?
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
    let topic: String?
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
        case topic
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
    let topic: String?
    let payload: [ContinuousCandleData]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
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
        case topic
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
    let topic: String?
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
        case topic
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
    let topic: String?
    let payload: [ContractData]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
        case count
    }
}

struct CpuMetrics: Codable, Sendable {
    let processPercent: Double
    let userTimeSeconds: Double
    let systemTimeSeconds: Double
    let cgroupQuotaMicroseconds: Int?
    let cgroupThrottledCount: Int?

    enum CodingKeys: String, CodingKey {
        case processPercent = "process_percent"
        case userTimeSeconds = "user_time_seconds"
        case systemTimeSeconds = "system_time_seconds"
        case cgroupQuotaMicroseconds = "cgroup_quota_microseconds"
        case cgroupThrottledCount = "cgroup_throttled_count"
    }
}

struct CredentialListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: [CredentialSummary]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: CredentialSummary

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct CredentialSummary: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
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
        case topic
        case walletPublicId = "wallet_public_id"
        case exchange
        case credentialType = "credential_type"
        case label
    }
}

struct DbInternalMetrics: Codable, Sendable {
    let aiosqliteLiveConnections: Int
    let poolSize: Int?
    let poolCheckedOut: Int?

    enum CodingKeys: String, CodingKey {
        case aiosqliteLiveConnections = "aiosqlite_live_connections"
        case poolSize = "pool_size"
        case poolCheckedOut = "pool_checked_out"
    }
}

struct DbStatsData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let snapshotStartedAt: Date
    let snapshotCompletedAt: Date
    let intervalSeconds: Int
    let tables: [TableStatsItem]

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case snapshotStartedAt = "snapshot_started_at"
        case snapshotCompletedAt = "snapshot_completed_at"
        case intervalSeconds = "interval_seconds"
        case tables
    }
}

struct DbStatsResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: DbStatsData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct DelegateCapsBody: Codable, Sendable {
    let maxOrderQuantityPerInstrument: JsonObject?
    let maxOpenOrders: Int?
    let maxDailyNotionalUsd: Double?
    let maxCancelsPerMinute: Int?

    enum CodingKeys: String, CodingKey {
        case maxOrderQuantityPerInstrument = "max_order_quantity_per_instrument"
        case maxOpenOrders = "max_open_orders"
        case maxDailyNotionalUsd = "max_daily_notional_usd"
        case maxCancelsPerMinute = "max_cancels_per_minute"
    }
}

struct DelegateCreatedPayload: Codable, Sendable {
    let delegate: DelegateRead
    let accessToken: String
    let expiresIn: Int

    enum CodingKeys: String, CodingKey {
        case delegate
        case accessToken = "access_token"
        case expiresIn = "expires_in"
    }
}

struct DelegateCreatedResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: DelegateCreatedPayload

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct DelegateListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: [DelegateRead]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
        case count
    }
}

struct DelegateRead: Codable, Sendable {
    let publicId: String
    let username: String
    let label: String
    let createdByUserPublicId: String
    let createdAt: Date
    let isActive: Bool
    let caps: DelegateCapsBody

    enum CodingKeys: String, CodingKey {
        case publicId = "public_id"
        case username
        case label
        case createdByUserPublicId = "created_by_user_public_id"
        case createdAt = "created_at"
        case isActive = "is_active"
        case caps
    }
}

struct DelegateResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: DelegateRead

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct DeviceAlertPrefBody: Codable, Sendable {
    let alertType: String
    let operatorPublicId: String?
    let walletPublicId: String?
    let enabled: Bool?
    let minPriority: String?
    let quietHoursStartMin: Int?
    let quietHoursEndMin: Int?
    let muteUntil: Date?
    let timezone: String?

    enum CodingKeys: String, CodingKey {
        case alertType = "alert_type"
        case operatorPublicId = "operator_public_id"
        case walletPublicId = "wallet_public_id"
        case enabled
        case minPriority = "min_priority"
        case quietHoursStartMin = "quiet_hours_start_min"
        case quietHoursEndMin = "quiet_hours_end_min"
        case muteUntil = "mute_until"
        case timezone
    }
}

struct DeviceAlertPrefInfo: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let devicePublicId: String
    let alertType: String
    let operatorPublicId: String?
    let walletPublicId: String?
    let enabled: Bool
    let minPriority: String
    let quietHoursStartMin: Int?
    let quietHoursEndMin: Int?
    let muteUntil: Date?
    let timezone: String

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case devicePublicId = "device_public_id"
        case alertType = "alert_type"
        case operatorPublicId = "operator_public_id"
        case walletPublicId = "wallet_public_id"
        case enabled
        case minPriority = "min_priority"
        case quietHoursStartMin = "quiet_hours_start_min"
        case quietHoursEndMin = "quiet_hours_end_min"
        case muteUntil = "mute_until"
        case timezone
    }
}

struct DeviceAlertPrefListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: [DeviceAlertPrefInfo]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
        case count
    }
}

struct DeviceAlertPrefResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: DeviceAlertPrefInfo

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
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
    let topic: String?
    let payload: [String]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
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
        case topic
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
    let topic: String?
    let payload: [ExecutionData]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
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
        case topic
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
    let topic: String?
    let payload: ExecutionPlanData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct FeatureFlagsPayload: Codable, Sendable {
    let aiIntegrationEnabled: Bool

    enum CodingKeys: String, CodingKey {
        case aiIntegrationEnabled = "ai_integration_enabled"
    }
}

struct FeatureFlagsResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: FeatureFlagsPayload

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct FrontMonthData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
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
        case topic
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
    let topic: String?
    let payload: FrontMonthData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct GapDetectionStats: Codable, Sendable {
    let bridge: GapStats
    let restClients: [String: GapStats]?

    enum CodingKeys: String, CodingKey {
        case bridge
        case restClients = "rest_clients"
    }
}

struct GapStats: Codable, Sendable {
    let gapsDetected: Int?
    let sessionResets: Int?
    let duplicates: Int?
    let midStreamJoins: Int?
    let rejectedUnstamped: Int?

    enum CodingKeys: String, CodingKey {
        case gapsDetected = "gaps_detected"
        case sessionResets = "session_resets"
        case duplicates
        case midStreamJoins = "mid_stream_joins"
        case rejectedUnstamped = "rejected_unstamped"
    }
}

struct GcMetrics: Codable, Sendable {
    let collectionsGen0: Int
    let collectionsGen1: Int
    let collectionsGen2: Int
    let uncollectable: Int
    let currentObjects: Int

    enum CodingKeys: String, CodingKey {
        case collectionsGen0 = "collections_gen0"
        case collectionsGen1 = "collections_gen1"
        case collectionsGen2 = "collections_gen2"
        case uncollectable
        case currentObjects = "current_objects"
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
    let topic: String?
    let payload: HandoverScopeGrantResult

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let status: String
    let version: String
    let connections: ConnectionStats
    let topics: HealthTopics
    let gapDetection: GapDetectionStats

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: HealthCheckData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct HealthTopics: Codable, Sendable {
    let active: Int
}

struct InstrumentDetailData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let instrumentPublicId: String
    let symbolPublicId: String
    let symbol: String
    let exchange: String
    let canTrade: Bool
    let canMarketData: Bool
    let instrumentResolved: Bool
    let instrumentKind: String?
    let expiryAt: Date?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case instrumentPublicId = "instrument_public_id"
        case symbolPublicId = "symbol_public_id"
        case symbol
        case exchange
        case canTrade = "can_trade"
        case canMarketData = "can_market_data"
        case instrumentResolved = "instrument_resolved"
        case instrumentKind = "instrument_kind"
        case expiryAt = "expiry_at"
    }
}

struct InstrumentDetailListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: [InstrumentDetailData]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
        case count
    }
}

struct InstrumentListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: [String]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
        case count
    }
}

struct JsonObject: Codable, Sendable {
}

struct LimitsMetrics: Codable, Sendable {
    let rlimitNproc: Int
    let rlimitNofile: Int
    let rlimitAsBytes: Int

    enum CodingKeys: String, CodingKey {
        case rlimitNproc = "rlimit_nproc"
        case rlimitNofile = "rlimit_nofile"
        case rlimitAsBytes = "rlimit_as_bytes"
    }
}

struct LoginData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let message: String
    let expiresIn: Int
    let user: UserProfile
    let accessToken: String?
    let refreshToken: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case message
        case expiresIn = "expires_in"
        case user
        case accessToken = "access_token"
        case refreshToken = "refresh_token"
    }
}

struct LoginResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: LoginData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct MemoryMetrics: Codable, Sendable {
    let rssBytes: Int
    let rssPeakBytes: Int
    let vmsBytes: Int
    let pythonTracedBytes: Int?
    let nativeBytes: Int?
    let cgroupLimitBytes: Int?
    let cgroupCurrentBytes: Int?
    let saturationPct: Double?

    enum CodingKeys: String, CodingKey {
        case rssBytes = "rss_bytes"
        case rssPeakBytes = "rss_peak_bytes"
        case vmsBytes = "vms_bytes"
        case pythonTracedBytes = "python_traced_bytes"
        case nativeBytes = "native_bytes"
        case cgroupLimitBytes = "cgroup_limit_bytes"
        case cgroupCurrentBytes = "cgroup_current_bytes"
        case saturationPct = "saturation_pct"
    }
}

struct MessageResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: String

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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

struct NotificationDeviceInfo: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let userPublicId: String
    let deviceToken: String
    let deviceId: String
    let platform: String
    let env: String
    let appVersion: String?
    let previewsMode: String
    let registeredAt: Date
    let lastSeenAt: Date?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case userPublicId = "user_public_id"
        case deviceToken = "device_token"
        case deviceId = "device_id"
        case platform
        case env
        case appVersion = "app_version"
        case previewsMode = "previews_mode"
        case registeredAt = "registered_at"
        case lastSeenAt = "last_seen_at"
    }
}

struct NotificationDeviceListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: [NotificationDeviceInfo]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
        case count
    }
}

struct NotificationDeviceResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: NotificationDeviceInfo

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct NotificationMetricsData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let deliverySuccessTotal: Int
    let deliveryFailedTotal: Int
    let delivery410UnregisteredTotal: Int
    let deliveryCancelledScopeTotal: Int
    let outboxQueuedDepth: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case deliverySuccessTotal = "delivery_success_total"
        case deliveryFailedTotal = "delivery_failed_total"
        case delivery410UnregisteredTotal = "delivery_410_unregistered_total"
        case deliveryCancelledScopeTotal = "delivery_cancelled_scope_total"
        case outboxQueuedDepth = "outbox_queued_depth"
    }
}

struct NotificationMetricsResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: NotificationMetricsData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct OperatorInfo: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let label: String
    let description: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: [OperatorInfo]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
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
    let planPublicId: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
        case planPublicId = "plan_public_id"
    }
}

struct OrderListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: [OrderData]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: OrphanSweepResultData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct OrphanSweepResultData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let closedCount: Int
    let closedCycleIds: [String]

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case closedCount = "closed_count"
        case closedCycleIds = "closed_cycle_ids"
    }
}

struct PendingReviewListResponse: Codable, Sendable {
    let items: [PendingReviewSummaryItem]
    let count: Int
}

struct PendingReviewSummaryItem: Codable, Sendable {
    let reviewPublicId: String
    let selectedDelegatePublicId: String
    let walletPublicId: String
    let dispatchVersion: Int
    let status: String
    let deadline: Date
    let fanoutAfter: Date

    enum CodingKeys: String, CodingKey {
        case reviewPublicId = "review_public_id"
        case selectedDelegatePublicId = "selected_delegate_public_id"
        case walletPublicId = "wallet_public_id"
        case dispatchVersion = "dispatch_version"
        case status
        case deadline
        case fanoutAfter = "fanout_after"
    }
}

struct PositionCycleData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
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
        case topic
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
    let topic: String?
    let payload: [PositionCycleData]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let instrument: String
    let instrumentPublicId: String?
    let exchange: String
    let mode: String?
    let quantity: Double
    let averagePrice: Double
    let unrealizedPnl: Double
    let realizedPnl: Double
    let positionCyclePublicId: String?
    let walletPublicId: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case instrument
        case instrumentPublicId = "instrument_public_id"
        case exchange
        case mode
        case quantity
        case averagePrice = "average_price"
        case unrealizedPnl = "unrealized_pnl"
        case realizedPnl = "realized_pnl"
        case positionCyclePublicId = "position_cycle_public_id"
        case walletPublicId = "wallet_public_id"
    }
}

struct PositionListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: [PositionData]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
        case count
    }
}

struct ProcessCategoryCount: Codable, Sendable {
    let running: Int
    let total: Int
}

struct ProcessCreateData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let status: String
    let process: ProcessCreatedInfo

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: ProcessCreateData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct ProcessCreatedInfo: Codable, Sendable {
    let name: String
    let template: String
}

struct ProcessMetrics: Codable, Sendable {
    let pid: Int
    let uptimeSeconds: Double
    let status: String
    let numThreads: Int
    let numFds: Int
    let numConnections: Int

    enum CodingKeys: String, CodingKey {
        case pid
        case uptimeSeconds = "uptime_seconds"
        case status
        case numThreads = "num_threads"
        case numFds = "num_fds"
        case numConnections = "num_connections"
    }
}

struct ProcessRun: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let processName: String
    let status: String
    let role: String
    let lifecycle: String
    let parameters: JsonObject?
    let result: JsonObject?
    let error: String?
    let tags: [String]?
    let startedAt: String
    let completedAt: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: [ProcessRun]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let name: String
    let description: String
    let classPath: String
    let method: String
    let defaultEnabled: Bool
    let defaultMode: String
    let defaultParameters: JsonObject?
    let lifecycle: String

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: ProcessSchemaData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct ProcessStartData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let status: String
    let name: String
    let processPublicId: String?
    let message: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: ProcessStartData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct ProcessStatus: Codable, Sendable {
    let status: String
    let pid: Int?
    let startedAt: String?
    let command: String?
    let exitCode: Int?
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
    let topic: String?
    let status: String
    let name: String
    let message: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: ProcessStopData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct ProcessSummaryData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let feeds: ProcessCategoryCount
    let strategies: ProcessCategoryCount
    let executors: ProcessCategoryCount
    let brokers: ProcessCategoryCount

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: ProcessSummaryData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct PushBetaConfigRead: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let enabled: Bool
    let userPublicIds: [String]

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case enabled
        case userPublicIds = "user_public_ids"
    }
}

struct PushBetaConfigResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: PushBetaConfigRead

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct RefreshData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let message: String
    let wsToken: String
    let wsTokenExp: Date
    let csrfToken: String
    let user: UserProfile
    let accessToken: String?
    let refreshToken: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case message
        case wsToken = "ws_token"
        case wsTokenExp = "ws_token_exp"
        case csrfToken = "csrf_token"
        case user
        case accessToken = "access_token"
        case refreshToken = "refresh_token"
    }
}

struct RefreshResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: RefreshData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct RegisterDeviceBody: Codable, Sendable {
    let deviceToken: String
    let deviceId: String
    let env: String
    let appVersion: String?
    let previewsMode: String?

    enum CodingKeys: String, CodingKey {
        case deviceToken = "device_token"
        case deviceId = "device_id"
        case env
        case appVersion = "app_version"
        case previewsMode = "previews_mode"
    }
}

struct RegisterDeviceCommand: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: RegisterDeviceBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct RestRateData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let exchanges: [String: RestRateExchangeStats]

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case exchanges
    }
}

struct RestRateExchangeStats: Codable, Sendable {
    let rps1S: Double
    let rps10S: Double
    let rps60S: Double
    let limitRps: Double?
    let utilization: Double?

    enum CodingKeys: String, CodingKey {
        case rps1S = "rps_1s"
        case rps10S = "rps_10s"
        case rps60S = "rps_60s"
        case limitRps = "limit_rps"
        case utilization
    }
}

struct RestRateResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: RestRateData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct RetentionPolicyResult: Codable, Sendable {
    let table: String
    let retainDays: Int
    let backlogLookbackDays: Int
    let dayStart: String?
    let dayEnd: String?
    let archivedRows: Int
    let purgedRows: Int
    let filesWritten: Int
    let error: String?

    enum CodingKeys: String, CodingKey {
        case table
        case retainDays = "retain_days"
        case backlogLookbackDays = "backlog_lookback_days"
        case dayStart = "day_start"
        case dayEnd = "day_end"
        case archivedRows = "archived_rows"
        case purgedRows = "purged_rows"
        case filesWritten = "files_written"
        case error
    }
}

struct RetentionRunData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let runStartedAt: Date
    let runCompletedAt: Date
    let dryRun: Bool
    let results: [RetentionPolicyResult]

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case runStartedAt = "run_started_at"
        case runCompletedAt = "run_completed_at"
        case dryRun = "dry_run"
        case results
    }
}

struct RetentionRunResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: RetentionRunData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct RevokeScopeGrantResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: ScopeGrantInfo

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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

struct SaturationMetrics: Codable, Sendable {
    let threadsPct: Double?
    let fdsPct: Double?

    enum CodingKeys: String, CodingKey {
        case threadsPct = "threads_pct"
        case fdsPct = "fds_pct"
    }
}

struct ScopeGrantInfo: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
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
        case topic
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
    let topic: String?
    let payload: [ScopeGrantInfo]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: ScopeGrantInfo

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct SettingCategoriesResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: [String]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: [SettingRead]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
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
        case topic
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
    let topic: String?
    let payload: SettingRead

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct SignalData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
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
    let aiReviewPublicId: String?
    let aiReviewDispatchVersion: Int?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
        case aiReviewPublicId = "ai_review_public_id"
        case aiReviewDispatchVersion = "ai_review_dispatch_version"
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
    let topic: String?
    let payload: [SignalData]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: [StrategyProcess]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let name: String
    let running: Bool
    let enabled: Bool
    let mode: String

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case name
        case running
        case enabled
        case mode
    }
}

struct StrategyStatusPayload: Codable, Sendable {
    let strategyName: String
    let status: String
    let details: JsonObject?
    let signalsGenerated: Int?
    let tradesExecuted: Int?
    let lastSignal: String?
    let lastSignalTime: String?
    let pnl: Double?
    let pid: Int?
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
    let perTopic: [String: Int]
    let perClient: [String: [String]]

    enum CodingKeys: String, CodingKey {
        case perTopic = "per_topic"
        case perClient = "per_client"
    }
}

struct SystemMetricsData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let busTime: Date
    let process: ProcessMetrics
    let cpu: CpuMetrics
    let memory: MemoryMetrics
    let asyncio: AsyncioMetrics
    let gc: GcMetrics
    let limits: LimitsMetrics
    let saturation: SaturationMetrics
    let dbInternal: DbInternalMetrics
    let tracemallocActive: Bool
    let cgroupVersion: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case busTime = "bus_time"
        case process
        case cpu
        case memory
        case asyncio
        case gc
        case limits
        case saturation
        case dbInternal = "db_internal"
        case tracemallocActive = "tracemalloc_active"
        case cgroupVersion = "cgroup_version"
    }
}

struct SystemMetricsHistoryItem: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let busTime: Date
    let process: ProcessMetrics
    let cpu: CpuMetrics
    let memory: MemoryMetrics
    let asyncio: AsyncioMetrics
    let gc: GcMetrics
    let limits: LimitsMetrics
    let saturation: SaturationMetrics
    let dbInternal: DbInternalMetrics
    let tracemallocActive: Bool
    let cgroupVersion: String?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case busTime = "bus_time"
        case process
        case cpu
        case memory
        case asyncio
        case gc
        case limits
        case saturation
        case dbInternal = "db_internal"
        case tracemallocActive = "tracemalloc_active"
        case cgroupVersion = "cgroup_version"
    }
}

struct SystemMetricsHistoryResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: [SystemMetricsHistoryItem]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
        case count
    }
}

struct SystemMetricsResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: SystemMetricsData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct SystemStatusData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let trader: ProcessStatus
    let backtests: [String: ProcessStatus]
    let strategies: [StrategyStatusPayload]?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: SystemStatusData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct TableStatsItem: Codable, Sendable {
    let table: String
    let tableKind: String
    let total: Int?
    let current: Int?
    let closed: Int?
    let archivable: Int?
    let isStale: Bool
    let lastSampledAt: Date

    enum CodingKeys: String, CodingKey {
        case table
        case tableKind = "table_kind"
        case total
        case current
        case closed
        case archivable
        case isStale = "is_stale"
        case lastSampledAt = "last_sampled_at"
    }
}

struct TopicMetricSnapshot: Codable, Sendable {
    let activeSubscribers: Int?
    let received: Int?
    let forwarded: Int?
    let throttled: Int?
    let dropped: Int?
    let timeout: Int?
    let errors: Int?
    let invalidMessages: Int?
    let lastMessageTs: Double?
    let throttleMs: Int?
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

struct TracemallocState: Codable, Sendable {
    let active: Bool
    let requestedDurationSeconds: Double?

    enum CodingKeys: String, CodingKey {
        case active
        case requestedDurationSeconds = "requested_duration_seconds"
    }
}

struct TracemallocStateResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: TracemallocState

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
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
    let topic: String?
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
        case topic
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
    let topic: String?
    let payload: TrailingStopStateData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct UnderlyingAssetData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
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
        case topic
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
    let topic: String?
    let payload: [UnderlyingAssetData]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
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
        case topic
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
    let topic: String?
    let payload: [UnderlyingInstrumentData]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
        case count
    }
}

struct UpdateDevicePrefCommand: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: DeviceAlertPrefBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct UpdateUserAlertDefaultCommand: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: UserAlertDefaultBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct UserAlertDefaultBody: Codable, Sendable {
    let alertType: String
    let enabled: Bool?
    let minPriority: String?

    enum CodingKeys: String, CodingKey {
        case alertType = "alert_type"
        case enabled
        case minPriority = "min_priority"
    }
}

struct UserAlertDefaultInfo: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let userPublicId: String
    let alertType: String
    let enabled: Bool
    let minPriority: String

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case userPublicId = "user_public_id"
        case alertType = "alert_type"
        case enabled
        case minPriority = "min_priority"
    }
}

struct UserAlertDefaultListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: [UserAlertDefaultInfo]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
        case count
    }
}

struct UserAlertDefaultResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: UserAlertDefaultInfo

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct UserListResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: [UserProfile]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
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
        case topic
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
    let topic: String?
    let payload: UserProfile

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let label: String
    let description: String?
    let isPaper: Bool

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: [WalletInfo]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: WalletInfo

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct WebSocketStats: Codable, Sendable {
    let activeConnections: Int
    let topicSubscribers: [String: Int]
    let clientCount: Int

    enum CodingKeys: String, CodingKey {
        case activeConnections = "active_connections"
        case topicSubscribers = "topic_subscribers"
        case clientCount = "client_count"
    }
}

struct WsStatsConfig: Codable, Sendable {
    let brokerXpub: String
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
    let topic: String?
    let websocket: WebSocketStats
    let zmqBridge: ZmqBridgeStats
    let connections: ConnectionStats
    let topics: [String: TopicMetricSnapshot]
    let subscriptions: SubscriptionsStats
    let config: WsStatsConfig

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: WsStatsData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct WsTokenData: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let message: String
    let wsToken: String
    let wsTokenExp: Date
    let expiresIn: Int

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case message
        case wsToken = "ws_token"
        case wsTokenExp = "ws_token_exp"
        case expiresIn = "expires_in"
    }
}

struct WsTokenResponse: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: WsTokenData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct ZmqBridgeStats: Codable, Sendable {
    let activeTopics: Int
    let subscriberTasks: Int
    let availableTopics: [String]

    enum CodingKeys: String, CodingKey {
        case activeTopics = "active_topics"
        case subscriberTasks = "subscriber_tasks"
        case availableTopics = "available_topics"
    }
}

struct ZmqComponents: Codable, Sendable {
    let zmqContext: String
    let websocketManager: String
    let activeConnections: Int

    enum CodingKeys: String, CodingKey {
        case zmqContext = "zmq_context"
        case websocketManager = "websocket_manager"
        case activeConnections = "active_connections"
    }
}

struct ZmqConfig: Codable, Sendable {
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
    let topic: String?
    let status: String
    let components: ZmqComponents
    let config: ZmqConfig
    let connections: ConnectionStats
    let messageStats: [String: TopicMetricSnapshot]
    let errors: [String]?

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: ZmqHealthData

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct LoginRequest: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: LoginBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: RefreshTokenPayload

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: CreateUserBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: UpdateUserBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: DeactivateUserBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct DeactivateUserBody: Codable, Sendable {
    let reason: String?
}

struct ChangePasswordRequest: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: ChangePasswordBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: AdminResetPasswordBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: SettingUpdateBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct SettingUpdateBody: Codable, Sendable {
    let value: String
    let category: String?
    let description: String?
}

struct UpdatePushBetaUsersCommand: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: PushBetaUsersBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct PushBetaUsersBody: Codable, Sendable {
    let enabled: Bool
    let userPublicIds: [String]?

    enum CodingKeys: String, CodingKey {
        case enabled
        case userPublicIds = "user_public_ids"
    }
}

struct RemoveSettingRequest: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: RemoveSettingBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct RemoveSettingBody: Codable, Sendable {
}

struct DelegateCreateRequest: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: DelegateCreateBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct DelegateCreateBody: Codable, Sendable {
    let label: String
    let caps: DelegateCapsBody?
    let operatorPublicId: String?

    enum CodingKeys: String, CodingKey {
        case label
        case caps
        case operatorPublicId = "operator_public_id"
    }
}

struct DelegateCapsUpdateRequest: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: DelegateCapsUpdateBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct DelegateCapsUpdateBody: Codable, Sendable {
    let caps: DelegateCapsBody
}

struct DelegateDeactivateRequest: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: DelegateDeactivateBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct DelegateDeactivateBody: Codable, Sendable {
    let reason: String?
}

struct AiReviewDecisionRequest: Codable, Sendable {
    let decision: String
    let rationale: String?
}

struct BacktestCreateCommand: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: BacktestCreateBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let targetExecutionExchange: String?

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
        case targetExecutionExchange = "target_execution_exchange"
    }
}

struct BacktestCompareRequest: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: BacktestCompareBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: BacktestCancelBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: CreateCredentialBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct CreateCredentialBody: Codable, Sendable {
    let exchange: String
    let credentialType: String
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
    let topic: String?
    let payload: RotateCredentialBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct RotateCredentialBody: Codable, Sendable {
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
    let topic: String?
    let payload: BracketCreateBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: BracketCancelBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: CreateOrderBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let aiReviewPublicId: String?

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
        case aiReviewPublicId = "ai_review_public_id"
    }
}

struct CancelOrderCommand: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: CancelOrderBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: ProcessCreateBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct ProcessCreateBody: Codable, Sendable {
    let name: String
    let template: String
    let enabled: Bool?
    let mode: String?
    let parameters: JsonObject?
    let note: String?
}

struct ProcessStartRequest: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: ProcessStartBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct ProcessStartBody: Codable, Sendable {
    let mode: String?
    let parameters: JsonObject?
}

struct CreateScopeGrantCommand: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: CreateScopeGrantBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: HandoverScopeGrantBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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

struct RevokeScopeGrantCommand: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: RevokeScopeGrantBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
        case payload
    }
}

struct RevokeScopeGrantBody: Codable, Sendable {
    let reason: String?
}

struct TrailingStopCreateCommand: Codable, Sendable {
    let type: String?
    let sequenceId: Int
    let publicId: String
    let timestamp: Date
    let sessionId: String
    let topic: String?
    let payload: TrailingStopCreateBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: TrailingStopCancelBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
    let topic: String?
    let payload: CreateWalletBody

    enum CodingKeys: String, CodingKey {
        case type
        case sequenceId = "sequence_id"
        case publicId = "public_id"
        case timestamp
        case sessionId = "session_id"
        case topic
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
