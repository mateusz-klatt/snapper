import type { Components } from '../types/api.generated'

type Provenance = {
  type: string
  sequence_id: number
  public_id: string
  timestamp: string
  session_id: string
}

const BASE_PROVENANCE: Omit<Provenance, 'type'> = {
  sequence_id: 0,
  public_id: 'test-pid',
  timestamp: '2024-01-01T00:00:00Z',
  session_id: 'test-sid',
}

function stamp<T extends Record<string, unknown>>(type: string, data: T): Provenance & T {
  return { type, ...BASE_PROVENANCE, ...data }
}

const DEFAULT_USER_OVERRIDES: Partial<Components['schemas']['UserProfile']> & {
  username: string
} = { username: 'test' }

export function makeUserProfile(
  overrides: Partial<Components['schemas']['UserProfile']> & {
    username: string
  } = DEFAULT_USER_OVERRIDES
): Components['schemas']['UserProfile'] {
  return stamp('user_profile', {
    role: 'viewer' as const,
    is_active: true,
    created_at: overrides.created_at ?? '2026-01-01T00:00:00Z',
    ...overrides,
  })
}

export function makeStrategyProcess(
  overrides: Partial<Omit<Components['schemas']['StrategyProcess'], keyof Provenance>> & {
    name: string
  }
): Components['schemas']['StrategyProcess'] {
  return stamp('strategy_process', {
    running: false,
    enabled: true,
    mode: 'thread' as const,
    ...overrides,
  }) as Components['schemas']['StrategyProcess']
}

export function makeConfiguredProcess(
  overrides: Partial<Omit<Components['schemas']['ConfiguredProcess'], keyof Provenance>> & {
    name: string
  }
): Components['schemas']['ConfiguredProcess'] {
  return stamp('configured_process', {
    enabled: true,
    running: false,
    mode: 'thread' as const,
    class_path: 'test.module.TestClass',
    method: 'run',
    lifecycle: 'long_running' as const,
    role: 'core' as const,
    tags: [],
    is_one_shot: false,
    ...overrides,
  }) as Components['schemas']['ConfiguredProcess']
}

export function makeAvailableProcess(
  overrides: Partial<Omit<Components['schemas']['AvailableProcess'], keyof Provenance>> & {
    name: string
    description: string
  }
): Components['schemas']['AvailableProcess'] {
  return stamp('available_process', {
    class_path: 'test.module.TestClass',
    method: 'run',
    lifecycle: 'long_running' as const,
    role: 'core' as const,
    tags: [],
    ...overrides,
  }) as Components['schemas']['AvailableProcess']
}

export function makeProcessRun(
  overrides: Partial<Omit<Components['schemas']['ProcessRun'], keyof Provenance>> & {
    process_name: string
    status: Components['schemas']['ProcessRun']['status']
    role: Components['schemas']['ProcessRun']['role']
    lifecycle: Components['schemas']['ProcessRun']['lifecycle']
    started_at: string
  }
): Components['schemas']['ProcessRun'] {
  return stamp('process_run', {
    ...overrides,
  }) as Components['schemas']['ProcessRun']
}

export function makeHeartbeat(overrides: {
  component: string
  status: 'healthy' | 'warning' | 'error'
  lag_ms?: number
  sequence?: number
}): {
  type: 'heartbeat'
  sequence_id: number
  public_id: string
  timestamp: string
  session_id: string
  component: string
  sequence: number
  status: 'healthy' | 'warning' | 'error'
  lag_ms: number
} {
  return stamp('heartbeat', {
    sequence: overrides.sequence ?? 1,
    lag_ms: overrides.lag_ms ?? 0,
    ...overrides,
  }) as ReturnType<typeof makeHeartbeat>
}

export function makeSettingRead(
  overrides: Partial<Omit<Components['schemas']['SettingRead'], keyof Provenance>> & {
    key: string
    value: string
  }
): Components['schemas']['SettingRead'] {
  return stamp('setting_read', {
    category: 'system',
    updated_at: '2024-01-01T00:00:00Z',
    ...overrides,
  }) as Components['schemas']['SettingRead']
}

export function makeEnvelope<T>(
  type: string,
  payload: T,
  extra?: Partial<Provenance>
): Provenance & { payload: T } {
  return { type, ...BASE_PROVENANCE, ...extra, payload }
}

export function makeListEnvelope<T>(
  type: string,
  payload: T[],
  extra?: Partial<Provenance>
): Provenance & { payload: T[]; count: number } {
  return { type, ...BASE_PROVENANCE, ...extra, payload, count: payload.length }
}

export { BASE_PROVENANCE, stamp }
