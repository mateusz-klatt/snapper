/**
 * Generated permission types from backend source of truth.
 * DO NOT EDIT - regenerate with: make ui-gen-permissions
 */

export const Permission = {
  READ_MARKET_DATA: 'read:market_data',
  READ_ORDERS: 'read:orders',
  CREATE_ORDERS: 'create:orders',
  CANCEL_ORDERS: 'cancel:orders',
  READ_POSITIONS: 'read:positions',
  MANAGE_POSITIONS: 'manage:positions',
  READ_STRATEGIES: 'read:strategies',
  START_STRATEGIES: 'start:strategies',
  STOP_STRATEGIES: 'stop:strategies',
  CONFIGURE_STRATEGIES: 'configure:strategies',
  READ_SYSTEM_STATUS: 'read:system_status',
  MANAGE_PROCESSES: 'manage:processes',
  CONFIGURE_SYSTEM: 'configure:system',
  MANAGE_USERS: 'manage:users',
} as const

export type Permission = (typeof Permission)[keyof typeof Permission]

type UserRole = 'viewer' | 'operator' | 'admin'

export const ROLE_PERMISSIONS: Record<UserRole, readonly Permission[]> = {
  viewer: ['read:market_data', 'read:orders', 'read:positions', 'read:strategies', 'read:system_status'],
  operator: ['cancel:orders', 'create:orders', 'manage:positions', 'manage:processes', 'read:market_data', 'read:orders', 'read:positions', 'read:strategies', 'read:system_status', 'start:strategies', 'stop:strategies'],
  admin: ['cancel:orders', 'configure:strategies', 'configure:system', 'create:orders', 'manage:positions', 'manage:processes', 'manage:users', 'read:market_data', 'read:orders', 'read:positions', 'read:strategies', 'read:system_status', 'start:strategies', 'stop:strategies'],
} as const

export const RESOURCE_ACCESS: Record<string, readonly UserRole[]> = {
  overview: ['viewer', 'operator', 'admin'],
  market: ['viewer', 'operator', 'admin'],
  processes: ['operator', 'admin'],
  strategies: ['viewer', 'operator', 'admin'],
  orders: ['viewer', 'operator', 'admin'],
  positions: ['viewer', 'operator', 'admin'],
  signals: ['viewer', 'operator', 'admin'],
  health: ['viewer', 'operator', 'admin'],
  admin: ['admin'],
  settings: ['admin'],
} as const
