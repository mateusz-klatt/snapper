import Foundation

typealias OrderStatus = OrderData
typealias PositionSnapshot = PositionData
typealias TradingSignal = SignalData
typealias ExecutionRecord = ExecutionData
typealias CandleEnvelope = CandleData
typealias SystemStatus = SystemStatusData

extension OrderData: Identifiable {
    var id: String { publicId ?? clientOrderId }
}

extension PositionData: Identifiable {
    var id: String { publicId ?? "\(instrument):\(exchange)" }
}

extension SignalData: Identifiable {
    var id: String { publicId ?? "\(instrument):\(exchange):\(side)" }
}

extension ExecutionData: Identifiable {
    var id: String { publicId ?? clientOrderId }
}

extension CandleData: Identifiable {
    var id: String { publicId ?? "\(instrument):\(exchange):\(timeframe)" }
}
