import { describe, it, expect, vi, beforeEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useMarketSubscription } from './useMarketSubscription'

const mockSubscribe = vi.fn()
const mockUnsubscribe = vi.fn()
let messageHandlers: Map<string, ((msg: unknown) => void)[]>

function createMockWsClient() {
  messageHandlers = new Map()

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
    isConnected: vi.fn(() => true),
  }
}

vi.mock('../stores/websocket', () => ({
  useWebSocketStore: vi.fn(() => ({
    wsClient: null,
    isConnected: false,
  })),
}))
vi.mock('../stores/app', () => ({
  useAppStore: vi.fn((selector: (s: Record<string, unknown>) => unknown) =>
    selector({ isTimeTraveling: false })
  ),
}))

import { useWebSocketStore } from '../stores/websocket'
import { useAppStore } from '../stores/app'

describe('useMarketSubscription', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })
  it('subscribes to candle topic when connected with valid params', () => {
    const mockClient = createMockWsClient()

    vi.mocked(useWebSocketStore).mockReturnValue({
      wsClient: mockClient,
      isConnected: true,
    } as never)
    renderHook(() =>
      useMarketSubscription({
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timeframe: '1m',
      })
    )

    expect(mockSubscribe).toHaveBeenCalledWith(['market.kraken.BTC-USD.candles.1m'])
  })
  it('returns false initially and true after subscription_success', () => {
    const mockClient = createMockWsClient()

    vi.mocked(useWebSocketStore).mockReturnValue({
      wsClient: mockClient,
      isConnected: true,
    } as never)
    const { result } = renderHook(() =>
      useMarketSubscription({
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timeframe: '1m',
      })
    )

    expect(result.current).toBe(false)
    act(() => {
      const handlers = messageHandlers.get('subscription_success') ?? []

      handlers.forEach(h => h({ type: 'subscription_success' }))
    })

    expect(result.current).toBe(true)
  })
  it('does not subscribe when instrument is null', () => {
    vi.mocked(useWebSocketStore).mockReturnValue({
      wsClient: createMockWsClient(),
      isConnected: true,
    } as never)
    renderHook(() =>
      useMarketSubscription({
        instrument: null,
        exchange: 'kraken',
        timeframe: '1m',
      })
    )

    expect(mockSubscribe).not.toHaveBeenCalled()
  })
  it('does not subscribe when exchange is null', () => {
    vi.mocked(useWebSocketStore).mockReturnValue({
      wsClient: createMockWsClient(),
      isConnected: true,
    } as never)
    renderHook(() =>
      useMarketSubscription({
        instrument: 'BTC-USD',
        exchange: null,
        timeframe: '1m',
      })
    )

    expect(mockSubscribe).not.toHaveBeenCalled()
  })
  it('does not subscribe when not connected', () => {
    vi.mocked(useWebSocketStore).mockReturnValue({
      wsClient: createMockWsClient(),
      isConnected: false,
    } as never)
    renderHook(() =>
      useMarketSubscription({
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timeframe: '1m',
      })
    )

    expect(mockSubscribe).not.toHaveBeenCalled()
  })
  it('does not subscribe when wsClient is null', () => {
    vi.mocked(useWebSocketStore).mockReturnValue({
      wsClient: null,
      isConnected: true,
    } as never)
    renderHook(() =>
      useMarketSubscription({
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timeframe: '1m',
      })
    )

    expect(mockSubscribe).not.toHaveBeenCalled()
  })
  it('unsubscribes old topic and subscribes new when instrument changes', () => {
    const mockClient = createMockWsClient()

    vi.mocked(useWebSocketStore).mockReturnValue({
      wsClient: mockClient,
      isConnected: true,
    } as never)
    const { rerender } = renderHook(
      (props: { instrument: string; exchange: string; timeframe: string }) =>
        useMarketSubscription(props),
      {
        initialProps: { instrument: 'BTC-USD', exchange: 'kraken', timeframe: '1m' },
      }
    )

    expect(mockSubscribe).toHaveBeenCalledWith(['market.kraken.BTC-USD.candles.1m'])
    mockSubscribe.mockClear()
    mockUnsubscribe.mockClear()
    rerender({ instrument: 'ETH-USD', exchange: 'kraken', timeframe: '1m' })

    expect(mockUnsubscribe).toHaveBeenCalledWith(['market.kraken.BTC-USD.candles.1m'])
    expect(mockSubscribe).toHaveBeenCalledWith(['market.kraken.ETH-USD.candles.1m'])
  })
  it('unsubscribes on unmount', () => {
    const mockClient = createMockWsClient()

    vi.mocked(useWebSocketStore).mockReturnValue({
      wsClient: mockClient,
      isConnected: true,
    } as never)
    const { unmount } = renderHook(() =>
      useMarketSubscription({
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timeframe: '1m',
      })
    )

    unmount()

    expect(mockUnsubscribe).toHaveBeenCalledWith(['market.kraken.BTC-USD.candles.1m'])
  })
  it('resets subscribed to false when params change', () => {
    const mockClient = createMockWsClient()

    vi.mocked(useWebSocketStore).mockReturnValue({
      wsClient: mockClient,
      isConnected: true,
    } as never)
    const { result, rerender } = renderHook(
      (props: { instrument: string; exchange: string; timeframe: string }) =>
        useMarketSubscription(props),
      {
        initialProps: { instrument: 'BTC-USD', exchange: 'kraken', timeframe: '1m' },
      }
    )

    act(() => {
      const handlers = messageHandlers.get('subscription_success') ?? []

      handlers.forEach(h => h({ type: 'subscription_success' }))
    })

    expect(result.current).toBe(true)
    rerender({ instrument: 'ETH-USD', exchange: 'kraken', timeframe: '1m' })

    expect(result.current).toBe(false)
  })
  it('resubscribes when timeframe changes', () => {
    const mockClient = createMockWsClient()

    vi.mocked(useWebSocketStore).mockReturnValue({
      wsClient: mockClient,
      isConnected: true,
    } as never)
    const { rerender } = renderHook(
      (props: { instrument: string; exchange: string; timeframe: string }) =>
        useMarketSubscription(props),
      {
        initialProps: { instrument: 'BTC-USD', exchange: 'kraken', timeframe: '1m' },
      }
    )

    expect(mockSubscribe).toHaveBeenCalledWith(['market.kraken.BTC-USD.candles.1m'])
    mockSubscribe.mockClear()
    mockUnsubscribe.mockClear()
    rerender({ instrument: 'BTC-USD', exchange: 'kraken', timeframe: '5m' })

    expect(mockUnsubscribe).toHaveBeenCalledWith(['market.kraken.BTC-USD.candles.1m'])
    expect(mockSubscribe).toHaveBeenCalledWith(['market.kraken.BTC-USD.candles.5m'])
  })
  it('cleans up message handler on unmount', () => {
    const mockClient = createMockWsClient()

    vi.mocked(useWebSocketStore).mockReturnValue({
      wsClient: mockClient,
      isConnected: true,
    } as never)
    const { unmount } = renderHook(() =>
      useMarketSubscription({
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timeframe: '1m',
      })
    )

    expect(messageHandlers.get('subscription_success')?.length).toBe(1)
    unmount()

    expect(messageHandlers.get('subscription_success')?.length).toBe(0)
  })
  it('starts buffering on dispatcher when subscribing', () => {
    const mockClient = createMockWsClient()
    const mockDispatcher = {
      startBuffering: vi.fn(),
      stopBuffering: vi.fn(),
      flushBuffer: vi.fn(),
    }

    vi.mocked(useWebSocketStore).mockReturnValue({
      wsClient: mockClient,
      isConnected: true,
    } as never)
    renderHook(() =>
      useMarketSubscription({
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timeframe: '1m',
        dispatcher: mockDispatcher as never,
      })
    )

    expect(mockDispatcher.startBuffering).toHaveBeenCalledWith('BTC-USD', 'kraken', '1m')
  })
  it('stops buffering on dispatcher when unmounting', () => {
    const mockClient = createMockWsClient()
    const mockDispatcher = {
      startBuffering: vi.fn(),
      stopBuffering: vi.fn(),
      flushBuffer: vi.fn(),
    }

    vi.mocked(useWebSocketStore).mockReturnValue({
      wsClient: mockClient,
      isConnected: true,
    } as never)
    const { unmount } = renderHook(() =>
      useMarketSubscription({
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timeframe: '1m',
        dispatcher: mockDispatcher as never,
      })
    )

    unmount()

    expect(mockDispatcher.stopBuffering).toHaveBeenCalledWith('BTC-USD', 'kraken', '1m')
  })
  it('does not subscribe when time traveling', () => {
    vi.mocked(useAppStore).mockImplementation(((
      selector: (s: Record<string, unknown>) => unknown
    ) => selector({ isTimeTraveling: true })) as never)
    vi.mocked(useWebSocketStore).mockReturnValue({
      wsClient: createMockWsClient(),
      isConnected: true,
    } as never)
    renderHook(() =>
      useMarketSubscription({
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timeframe: '1m',
      })
    )

    expect(mockSubscribe).not.toHaveBeenCalled()
    vi.mocked(useAppStore).mockImplementation(((
      selector: (s: Record<string, unknown>) => unknown
    ) => selector({ isTimeTraveling: false })) as never)
  })
})
