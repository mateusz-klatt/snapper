export function createAuthRequired(overrides: { timeout?: number } = {}) {
  return {
    type: 'auth_required' as const,
    session_id: '',
    sequence_id: 0,
    timeout: overrides.timeout ?? 30,
  }
}

export function createAuthOk(overrides: { exp?: string } = {}) {
  return {
    type: 'auth_ok' as const,
    session_id: '',
    sequence_id: 0,
    exp: overrides.exp ?? new Date(Date.now() + 3600000).toISOString(),
  }
}

export function createAuthComplete(
  overrides: {
    available_topics?: string[]
    user_role?: 'viewer' | 'operator' | 'admin'
    ws_token_exp?: string
    session_expires_at?: string | null
  } = {}
) {
  return {
    type: 'auth_complete' as const,
    session_id: '',
    sequence_id: 0,
    available_topics: overrides.available_topics ?? [],
    user_role: overrides.user_role ?? 'operator',
    ws_token_exp: overrides.ws_token_exp ?? new Date(Date.now() + 3600000).toISOString(),
    session_expires_at:
      overrides.session_expires_at ?? new Date(Date.now() + 86400000).toISOString(),
  }
}

export function createAuthFailed(overrides: { reason?: string | null } = {}) {
  return {
    type: 'auth_failed' as const,
    session_id: '',
    sequence_id: 0,
    reason: overrides.reason ?? null,
  }
}

export function createAuthExpired() {
  return {
    type: 'auth_expired' as const,
    session_id: '',
    sequence_id: 0,
  }
}

export function createReauthRequired(overrides: { deadline?: string } = {}) {
  return {
    type: 'reauth_required' as const,
    session_id: '',
    sequence_id: 0,
    deadline: overrides.deadline ?? new Date(Date.now() + 60000).toISOString(),
  }
}

export function createReauthOk(overrides: { exp?: string } = {}) {
  return {
    type: 'reauth_ok' as const,
    session_id: '',
    sequence_id: 0,
    exp: overrides.exp ?? new Date(Date.now() + 3600000).toISOString(),
  }
}

export function createSubscribed(overrides: { topics?: string[] } = {}) {
  return {
    type: 'subscribed' as const,
    session_id: '',
    sequence_id: 0,
    topics: overrides.topics ?? ['market.test.BTC-USD'],
  }
}

export function createUnsubscribed(overrides: { topics?: string[] } = {}) {
  return {
    type: 'unsubscribed' as const,
    session_id: '',
    sequence_id: 0,
    topics: overrides.topics ?? ['market.test.BTC-USD'],
  }
}

export function createSubscriptionsList(
  overrides: {
    subscriptions?: string[]
    available_topics?: string[]
    total_available?: number
  } = {}
) {
  return {
    type: 'subscriptions_list' as const,
    session_id: '',
    sequence_id: 0,
    subscriptions: overrides.subscriptions ?? [],
    available_topics: overrides.available_topics ?? [],
    total_available: overrides.total_available ?? 0,
  }
}

export function createTopicSuggestions(
  overrides: { prefix?: string; suggestions?: string[] } = {}
) {
  return {
    type: 'topic_suggestions' as const,
    session_id: '',
    sequence_id: 0,
    prefix: overrides.prefix ?? '',
    suggestions: overrides.suggestions ?? [],
  }
}

export function createPong(overrides: { timestamp?: string; active_connections?: number } = {}) {
  return {
    type: 'pong' as const,
    session_id: '',
    sequence_id: 0,
    timestamp: overrides.timestamp ?? new Date().toISOString(),
    active_connections: overrides.active_connections ?? 1,
  }
}

export function createError(overrides: { message?: string } = {}) {
  return {
    type: 'error' as const,
    session_id: '',
    sequence_id: 0,
    message: overrides.message ?? 'Unknown error',
  }
}

export function createCandle(
  overrides: {
    timestamp?: string
    open_at?: string
    instrument?: string
    exchange?: string
    timeframe?: string
    volume?: number
    open?: number
    high?: number
    low?: number
    close?: number
    vwap?: number | null
    trades?: number | null
  } = {}
) {
  const now = new Date().toISOString()

  return {
    type: 'candle' as const,
    session_id: '',
    sequence_id: 0,
    timestamp: overrides.timestamp ?? now,
    open_at: overrides.open_at ?? now,
    exchange: overrides.exchange ?? 'kraken',
    instrument: overrides.instrument ?? 'BTC-USD',
    volume: overrides.volume ?? 1000,
    timeframe: overrides.timeframe ?? '1m',
    open: overrides.open ?? 100,
    high: overrides.high ?? 105,
    low: overrides.low ?? 99,
    close: overrides.close ?? 102,
    vwap: overrides.vwap ?? 101,
    trades: overrides.trades ?? 50,
  }
}

export function createTick(
  overrides: {
    timestamp?: string
    instrument?: string
    exchange?: string
    volume?: number
    bid?: number | null
    ask?: number | null
    last?: number | null
  } = {}
) {
  const now = new Date().toISOString()

  return {
    type: 'tick' as const,
    session_id: '',
    sequence_id: 0,
    timestamp: overrides.timestamp ?? now,
    exchange: overrides.exchange ?? 'kraken',
    instrument: overrides.instrument ?? 'BTC-USD',
    volume: overrides.volume ?? 1000,
    bid: overrides.bid ?? 49990,
    ask: overrides.ask ?? 50010,
    last: overrides.last ?? 50000,
  }
}

export function createTrade(
  overrides: {
    timestamp?: string
    instrument?: string
    exchange?: string
    executed_at?: string | null
    price?: number
    volume?: number
    side?: string | null
  } = {}
) {
  const now = new Date().toISOString()

  return {
    type: 'trade' as const,
    session_id: '',
    sequence_id: 0,
    timestamp: overrides.timestamp ?? now,
    exchange: overrides.exchange ?? 'kraken',
    instrument: overrides.instrument ?? 'BTC-USD',
    executed_at: overrides.executed_at ?? null,
    price: overrides.price ?? 50000,
    volume: overrides.volume ?? 1.5,
    side: overrides.side ?? 'buy',
  }
}

export function createSignal(
  overrides: {
    timestamp?: string
    exchange?: string
    instrument?: string
    side?: 'buy' | 'sell'
    strength?: number
    reason?: string
    strategy_name?: string | null
    price?: number | null
    fired_at?: string
  } = {}
) {
  const now = new Date().toISOString()

  return {
    type: 'signal' as const,
    session_id: '',
    sequence_id: 0,
    timestamp: overrides.timestamp ?? now,
    exchange: overrides.exchange ?? 'kraken',
    instrument: overrides.instrument ?? 'BTC-USD',
    side: overrides.side ?? ('buy' as const),
    strength: overrides.strength ?? 0.8,
    reason: overrides.reason ?? 'Test signal',
    strategy_name: overrides.strategy_name ?? 'test-strategy',
    price: overrides.price ?? 50000,
    fired_at: overrides.fired_at ?? now,
  }
}

export function createHeartbeat(
  overrides: {
    timestamp?: string
    meta?: Record<string, unknown>
    component?: string
    sequence?: number
    status?: 'healthy' | 'warning' | 'error'
    lag_ms?: number
  } = {}
) {
  const now = new Date().toISOString()

  return {
    type: 'heartbeat' as const,
    session_id: '',
    sequence_id: 0,
    timestamp: overrides.timestamp ?? now,
    component: overrides.component ?? 'bridge',
    sequence: overrides.sequence ?? 0,
    status: overrides.status ?? ('healthy' as const),
    lag_ms: overrides.lag_ms ?? 0,
    meta: overrides.meta,
  }
}

export function createOrder(
  overrides: {
    timestamp?: string
    client_order_id?: string
    exchange_order_id?: string | null
    instrument?: string
    exchange?: string
    side?: 'buy' | 'sell'
    status?: string
    order_type?: 'market' | 'limit' | 'stop' | 'stop_limit'
    size?: number
    filled_size?: number
    price?: number | null
    average_price?: number | null
    reason?: string | null
    time_in_force?: string | null
    error?: string | null
    created_at?: string
    updated_at?: string | null
  } = {}
) {
  const now = new Date().toISOString()

  return {
    type: 'order' as const,
    session_id: '',
    sequence_id: 0,
    timestamp: overrides.timestamp ?? now,
    client_order_id: overrides.client_order_id ?? 'order-1',
    exchange_order_id: overrides.exchange_order_id ?? null,
    instrument: overrides.instrument ?? 'BTC-USD',
    exchange: overrides.exchange ?? 'kraken',
    side: overrides.side ?? ('buy' as const),
    status: overrides.status ?? 'open',
    order_type: overrides.order_type ?? ('limit' as const),
    size: overrides.size ?? 1,
    filled_size: overrides.filled_size ?? 0,
    price: overrides.price ?? 50000,
    average_price: overrides.average_price ?? null,
    reason: overrides.reason ?? null,
    time_in_force: overrides.time_in_force ?? null,
    error: overrides.error ?? null,
    created_at: overrides.created_at ?? now,
    updated_at: overrides.updated_at ?? null,
  }
}

export function createExecution(
  overrides: {
    timestamp?: string
    trade_id?: string | null
    exchange_order_id?: string | null
    client_order_id?: string
    exchange?: string
    instrument?: string
    side?: 'buy' | 'sell'
    size?: number
    price?: number
    fee?: number
    fee_asset?: string
    status?: 'filled' | 'partial'
    executed_at?: string
  } = {}
) {
  const now = new Date().toISOString()

  return {
    type: 'execution' as const,
    session_id: '',
    sequence_id: 0,
    timestamp: overrides.timestamp ?? now,
    trade_id: overrides.trade_id ?? null,
    exchange_order_id: overrides.exchange_order_id ?? null,
    client_order_id: overrides.client_order_id ?? 'order-1',
    exchange: overrides.exchange ?? 'kraken',
    instrument: overrides.instrument ?? 'BTC-USD',
    side: overrides.side ?? ('buy' as const),
    size: overrides.size ?? 0.5,
    price: overrides.price ?? 50000,
    fee: overrides.fee ?? 0.001,
    fee_asset: overrides.fee_asset ?? 'BTC',
    status: overrides.status ?? ('filled' as const),
    executed_at: overrides.executed_at ?? now,
  }
}
