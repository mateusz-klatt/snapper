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
  Position,
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
