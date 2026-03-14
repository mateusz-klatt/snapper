/**
 * Entity types re-exported from generated file.
 * This file provides a stable import path for entities.
 *
 * The actual entity definitions are auto-generated in entities.generated.ts
 * Regenerate with: make ui-gen-entities
 */

export type {
  // WS Data entities (backend naming)
  Candle,
  Execution,
  Heartbeat,
  OrderRequest,
  Order,
  ReplayEnd,
  ReplayStart,
  SettingChanged,
  Signal,
  SymbolAliasUpdate,
  Tick,
  Trade,
  // Request entities
  AdminResetPassword,
  ChangePassword,
  CreateUser,
  Login,
  ProcessCreate,
  ProcessStart,
  UpdateUser,
  // Re-exported common types
  TradeSide,
  OrderType,
  HeartbeatStatus,
} from './entities.generated'

/**
 * Canonical Position entity.
 * Derived from REST PositionData (no WS envelope exists for positions).
 */
export interface Position {
  id: string | number
  instrument: string
  exchange: string
  quantity: number
  averagePrice: number
  unrealizedPnl: number
  realizedPnl: number
  updatedAt: Date
}
