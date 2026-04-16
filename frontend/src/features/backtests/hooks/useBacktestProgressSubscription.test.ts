import { describe, it, expect, vi, beforeEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useBacktestProgressSubscription } from './useBacktestProgressSubscription'

const mockSubscribe = vi.fn()
const mockUnsubscribe = vi.fn()
let messageHandlers: Map<string, ((msg: unknown) => void)[]>
let connectionHandlers: ((connected: boolean) => void)[]

function createMockWsClient() {
  messageHandlers = new Map()
  connectionHandlers = []

  return {
    subscribe: mockSubscribe,
    unsubscribe: mockUnsubscribe,
    onMessage: vi.fn((type: string, handler: (msg: unknown) => void) => {
      const handlers = messageHandlers.get(type) ?? []

      handlers.push(handler)
      messageHandlers.set(type, handlers)

      return () => {
        const hs = messageHandlers.get(type) ?? []
        const idx = hs.indexOf(handler)

        if (idx >= 0) hs.splice(idx, 1)
      }
    }),
    onConnection: vi.fn((handler: (connected: boolean) => void) => {
      connectionHandlers.push(handler)

      return () => {
        const idx = connectionHandlers.indexOf(handler)

        if (idx >= 0) connectionHandlers.splice(idx, 1)
      }
    }),
  }
}

vi.mock('../../../stores/websocket', () => ({
  useWebSocketStore: vi.fn(() => ({ wsClient: null })),
}))
vi.mock('../../../stores/auth', () => ({
  useAuthStore: vi.fn((selector: (s: never) => unknown) =>
    selector({ user: { active_wallet_public_id: null } } as never)
  ),
}))

import { useWebSocketStore } from '../../../stores/websocket'
import { useAuthStore } from '../../../stores/auth'

describe('useBacktestProgressSubscription', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })
  it('returns null and does not subscribe when wsClient is null', () => {
    vi.mocked(useWebSocketStore).mockReturnValue({ wsClient: null } as never)
    vi.mocked(useAuthStore).mockImplementation((selector: (s: never) => unknown) =>
      selector({ user: { active_wallet_public_id: 'w-1' } } as never)
    )
    const { result } = renderHook(() => useBacktestProgressSubscription('r-1'))

    expect(result.current).toBeNull()
    expect(mockSubscribe).not.toHaveBeenCalled()
  })
  it('does not subscribe when runPublicId is null', () => {
    vi.mocked(useWebSocketStore).mockReturnValue({ wsClient: createMockWsClient() } as never)
    vi.mocked(useAuthStore).mockImplementation((selector: (s: never) => unknown) =>
      selector({ user: { active_wallet_public_id: 'w-1' } } as never)
    )
    renderHook(() => useBacktestProgressSubscription(null))
    expect(mockSubscribe).not.toHaveBeenCalled()
  })
  it('does not subscribe when wallet is null', () => {
    vi.mocked(useWebSocketStore).mockReturnValue({ wsClient: createMockWsClient() } as never)
    vi.mocked(useAuthStore).mockImplementation((selector: (s: never) => unknown) =>
      selector({ user: { active_wallet_public_id: null } } as never)
    )
    renderHook(() => useBacktestProgressSubscription('r-1'))
    expect(mockSubscribe).not.toHaveBeenCalled()
  })
  it('subscribes to backtest.{wallet}.{run}. prefix on mount', () => {
    vi.mocked(useWebSocketStore).mockReturnValue({ wsClient: createMockWsClient() } as never)
    vi.mocked(useAuthStore).mockImplementation((selector: (s: never) => unknown) =>
      selector({ user: { active_wallet_public_id: 'w-1' } } as never)
    )
    renderHook(() => useBacktestProgressSubscription('r-1'))
    expect(mockSubscribe).toHaveBeenCalledWith(['backtest.w-1.r-1.'])
  })
  it('updates snapshot only for matching run_public_id', () => {
    vi.mocked(useWebSocketStore).mockReturnValue({ wsClient: createMockWsClient() } as never)
    vi.mocked(useAuthStore).mockImplementation((selector: (s: never) => unknown) =>
      selector({ user: { active_wallet_public_id: 'w-1' } } as never)
    )
    const { result } = renderHook(() => useBacktestProgressSubscription('r-1'))

    act(() => {
      const handlers = messageHandlers.get('backtest_progress') ?? []

      handlers.forEach(h => h({ type: 'backtest_progress', run_public_id: 'r-other', equity: 1 }))
    })
    expect(result.current).toBeNull()
    act(() => {
      const handlers = messageHandlers.get('backtest_progress') ?? []

      handlers.forEach(h => h({ type: 'backtest_progress', run_public_id: 'r-1', equity: 42 }))
    })
    expect(result.current?.run_public_id).toBe('r-1')
  })
  it('ignores non backtest_progress message types', () => {
    vi.mocked(useWebSocketStore).mockReturnValue({ wsClient: createMockWsClient() } as never)
    vi.mocked(useAuthStore).mockImplementation((selector: (s: never) => unknown) =>
      selector({ user: { active_wallet_public_id: 'w-1' } } as never)
    )
    const { result } = renderHook(() => useBacktestProgressSubscription('r-1'))

    act(() => {
      const handlers = messageHandlers.get('backtest_progress') ?? []

      handlers.forEach(h => h({ type: 'something_else', run_public_id: 'r-1' }))
    })
    expect(result.current).toBeNull()
  })
  it('re-subscribes when connection becomes ready', () => {
    vi.mocked(useWebSocketStore).mockReturnValue({ wsClient: createMockWsClient() } as never)
    vi.mocked(useAuthStore).mockImplementation((selector: (s: never) => unknown) =>
      selector({ user: { active_wallet_public_id: 'w-1' } } as never)
    )
    renderHook(() => useBacktestProgressSubscription('r-1'))
    mockSubscribe.mockClear()
    act(() => {
      connectionHandlers.forEach(h => h(true))
    })
    expect(mockSubscribe).toHaveBeenCalledWith(['backtest.w-1.r-1.'])
    mockSubscribe.mockClear()
    act(() => {
      connectionHandlers.forEach(h => h(false))
    })
    expect(mockSubscribe).not.toHaveBeenCalled()
  })
  it('unsubscribes on unmount', () => {
    vi.mocked(useWebSocketStore).mockReturnValue({ wsClient: createMockWsClient() } as never)
    vi.mocked(useAuthStore).mockImplementation((selector: (s: never) => unknown) =>
      selector({ user: { active_wallet_public_id: 'w-1' } } as never)
    )
    const { unmount } = renderHook(() => useBacktestProgressSubscription('r-1'))

    unmount()
    expect(mockUnsubscribe).toHaveBeenCalledWith(['backtest.w-1.r-1.'])
  })
})
