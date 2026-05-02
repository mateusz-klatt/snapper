import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'

let heartbeatCallback: ((msg: unknown) => void) | null = null
let connectionCallback: ((connected: boolean) => void) | null = null
const mockUnsubscribeConnection = vi.fn()
const mockUnsubscribeHeartbeat = vi.fn()
const mockWsClient = {
  subscribe: vi.fn(),
  unsubscribe: vi.fn(),
  onConnection: vi.fn((cb: (connected: boolean) => void) => {
    connectionCallback = cb

    return mockUnsubscribeConnection
  }),
  onMessage: vi.fn((type: string, cb: (msg: unknown) => void) => {
    if (type === 'heartbeat') {
      heartbeatCallback = cb
    }

    return mockUnsubscribeHeartbeat
  }),
}

vi.mock('../stores/websocket', () => ({
  useWebSocketStore: vi.fn(() => ({
    wsClient: mockWsClient,
  })),
}))
vi.mock('../stores/app', () => ({
  useAppStore: vi.fn((selector: (s: Record<string, unknown>) => unknown) =>
    selector({ isTimeTraveling: false })
  ),
}))

import { useWebSocketStore } from '../stores/websocket'
import { useAppStore } from '../stores/app'
import { useHeartbeats } from './useHeartbeats'

describe('useHeartbeats', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.useFakeTimers()
    heartbeatCallback = null
    connectionCallback = null
    vi.mocked(useWebSocketStore).mockReturnValue({ wsClient: mockWsClient } as ReturnType<
      typeof useWebSocketStore
    >)
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('returns empty record when no wsClient', () => {
    vi.mocked(useWebSocketStore).mockReturnValue({ wsClient: null } as ReturnType<
      typeof useWebSocketStore
    >)
    const { result } = renderHook(() => useHeartbeats(['system.heartbeats.feed.']))

    expect(result.current).toEqual({})
    expect(mockWsClient.subscribe).not.toHaveBeenCalled()
  })

  it('subscribes to provided topics on mount', () => {
    const topics = ['system.heartbeats.executor.', 'system.heartbeats.feed.']

    renderHook(() => useHeartbeats(topics))

    expect(mockWsClient.subscribe).toHaveBeenCalledWith(topics)
    expect(mockWsClient.onConnection).toHaveBeenCalled()
    expect(mockWsClient.onMessage).toHaveBeenCalledWith('heartbeat', expect.any(Function))
  })

  it('updates heartbeat state on message', () => {
    const { result } = renderHook(() => useHeartbeats(['system.heartbeats.feed.']))

    act(() => {
      heartbeatCallback?.({
        component: 'feed.kraken',
        status: 'healthy',
        lag_ms: 42,
      })
    })

    expect(result.current['feed.kraken']).toEqual(
      expect.objectContaining({
        status: 'healthy',
        lag_ms: 42,
        healthy: true,
      })
    )
  })

  it('marks unhealthy heartbeats correctly', () => {
    const { result } = renderHook(() => useHeartbeats(['system.heartbeats.feed.']))

    act(() => {
      heartbeatCallback?.({
        component: 'feed.walutomat',
        status: 'warning',
        lag_ms: 500,
      })
    })

    expect(result.current['feed.walutomat'].healthy).toBe(false)
  })

  it('defaults lag_ms to 0 when missing', () => {
    const { result } = renderHook(() => useHeartbeats(['system.heartbeats.feed.']))

    act(() => {
      heartbeatCallback?.({
        component: 'feed.test',
        status: 'healthy',
      })
    })

    expect(result.current['feed.test'].lag_ms).toBe(0)
  })

  it('prunes stale heartbeats after threshold', () => {
    const { result } = renderHook(() => useHeartbeats(['system.heartbeats.feed.']))

    act(() => {
      heartbeatCallback?.({
        component: 'feed.kraken',
        status: 'healthy',
        lag_ms: 10,
      })
    })

    expect(Object.keys(result.current)).toHaveLength(1)

    act(() => {
      vi.advanceTimersByTime(15_000)
    })

    expect(Object.keys(result.current)).toHaveLength(0)
  })

  it('keeps fresh heartbeats during prune', () => {
    const { result } = renderHook(() => useHeartbeats(['system.heartbeats.feed.']))

    act(() => {
      heartbeatCallback?.({
        component: 'feed.kraken',
        status: 'healthy',
        lag_ms: 10,
      })
    })

    act(() => {
      vi.advanceTimersByTime(6_000)
    })

    act(() => {
      heartbeatCallback?.({
        component: 'feed.walutomat',
        status: 'healthy',
        lag_ms: 5,
      })
    })

    act(() => {
      vi.advanceTimersByTime(10_000)
    })

    expect(result.current['feed.walutomat']).toBeDefined()
    expect(result.current['feed.kraken']).toBeUndefined()
  })

  it('resubscribes on reconnection', () => {
    const topics = ['system.heartbeats.feed.']

    renderHook(() => useHeartbeats(topics))

    expect(mockWsClient.subscribe).toHaveBeenCalledTimes(1)

    act(() => {
      connectionCallback?.(true)
    })

    expect(mockWsClient.subscribe).toHaveBeenCalledTimes(2)
    expect(mockWsClient.subscribe).toHaveBeenLastCalledWith(topics)
  })

  it('does not resubscribe on disconnection', () => {
    renderHook(() => useHeartbeats(['system.heartbeats.feed.']))

    expect(mockWsClient.subscribe).toHaveBeenCalledTimes(1)

    act(() => {
      connectionCallback?.(false)
    })

    expect(mockWsClient.subscribe).toHaveBeenCalledTimes(1)
  })

  it('cleans up subscriptions on unmount', () => {
    const { unmount } = renderHook(() => useHeartbeats(['system.heartbeats.feed.']))

    unmount()

    expect(mockUnsubscribeConnection).toHaveBeenCalled()
    expect(mockUnsubscribeHeartbeat).toHaveBeenCalled()
    expect(mockWsClient.unsubscribe).toHaveBeenCalledWith(['system.heartbeats.feed.'])
  })
  it('does not subscribe when time traveling', () => {
    vi.mocked(useAppStore).mockImplementation(((
      selector: (s: Record<string, unknown>) => unknown
    ) => selector({ isTimeTraveling: true })) as never)
    renderHook(() => useHeartbeats(['system.heartbeats.feed.']))

    expect(mockWsClient.subscribe).not.toHaveBeenCalled()
    vi.mocked(useAppStore).mockImplementation(((
      selector: (s: Record<string, unknown>) => unknown
    ) => selector({ isTimeTraveling: false })) as never)
  })
})
