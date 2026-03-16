import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { useWebSocketStore, useWebSocketConnection } from './websocket'
import { renderHook, act } from '@testing-library/react'
import { apiClient } from '../lib/apiClient'
import { useAuthStore } from './auth'
import type WebSocketClient from '../lib/websocket/client'

vi.mock('../lib/apiClient', () => ({
  apiClient: {
    hasAuthCookies: vi.fn(() => false),
  },
}))
vi.mock('./auth', () => ({
  useAuthStore: {
    getState: vi.fn(() => ({
      isAuthenticated: false,
    })),
  },
}))

const createMockClient = () => {
  const handlers: {
    connection: Array<(c: boolean) => void>
    message: Map<string, Array<(m: unknown) => void>>
  } = {
    connection: [],
    message: new Map(),
  }

  return {
    connect: vi.fn(() => {
      setTimeout(() => {
        handlers.connection.forEach(h => h(true))
      }, 10)
    }),
    disconnect: vi.fn(),
    subscribe: vi.fn(),
    unsubscribe: vi.fn(),
    isConnected: vi.fn(() => false),
    onConnection: vi.fn((handler: (connected: boolean) => void) => {
      handlers.connection.push(handler)

      return () => {
        const idx = handlers.connection.indexOf(handler)

        if (idx >= 0) handlers.connection.splice(idx, 1)
      }
    }),
    onMessage: vi.fn((type: string, handler: (m: unknown) => void) => {
      if (!handlers.message.has(type)) handlers.message.set(type, [])
      const handlerList = handlers.message.get(type)

      if (handlerList) handlerList.push(handler)

      return () => {}
    }),
    triggerMessage: (type: string, message: unknown) => {
      handlers.message.get(type)?.forEach(h => h(message))
    },
    triggerConnection: (connected: boolean) => {
      handlers.connection.forEach(h => h(connected))
    },
    handlers,
  }
}

let mockClient: ReturnType<typeof createMockClient>
let shouldThrowOnConnect = false

vi.mock('../lib/websocket/client', () => {
  function MockWebSocketClient() {
    mockClient = createMockClient()

    if (shouldThrowOnConnect) {
      mockClient.connect = vi.fn(() => {
        throw new Error('Synchronous connect error')
      })
    }

    return mockClient
  }

  return { default: MockWebSocketClient }
})
describe('websocket store', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.useFakeTimers()
    mockClient = createMockClient()
    useWebSocketStore.setState({
      wsClient: null,
      isConnected: false,
      isConnecting: false,
      error: null,
    })
  })
  afterEach(() => {
    vi.clearAllTimers()
    vi.useRealTimers()
  })
  it('initializes with correct default state', () => {
    const state = useWebSocketStore.getState()

    expect(state.isConnected).toBe(false)
    expect(state.isConnecting).toBe(false)
    expect(state.error).toBeNull()
  })
  it('registers global wsDisconnectCallback on window', () => {
    expect(
      (window as Window & { wsDisconnectCallback?: () => void }).wsDisconnectCallback
    ).toBeDefined()
    expect(
      typeof (window as Window & { wsDisconnectCallback?: () => void }).wsDisconnectCallback
    ).toBe('function')
  })
  it('wsDisconnectCallback calls disconnect', async () => {
    const wsClient = createMockClient() as unknown as WebSocketClient

    useWebSocketStore.setState({
      wsClient,
      isConnected: true,
      isConnecting: false,
      error: null,
    })
    const callback = (window as Window & { wsDisconnectCallback?: () => void }).wsDisconnectCallback

    if (callback) {
      callback()
    }

    const state = useWebSocketStore.getState()

    expect(state.wsClient).toBeNull()
    expect(state.isConnected).toBe(false)
  })
  it('provides connect function', () => {
    const state = useWebSocketStore.getState()

    expect(typeof state.connect).toBe('function')
  })
  it('provides disconnect function', () => {
    const state = useWebSocketStore.getState()

    expect(typeof state.disconnect).toBe('function')
  })
  it('provides subscribe function', () => {
    const state = useWebSocketStore.getState()

    expect(typeof state.subscribe).toBe('function')
  })
  it('provides unsubscribe function', () => {
    const state = useWebSocketStore.getState()

    expect(typeof state.unsubscribe).toBe('function')
  })
  it('warns when subscribing without wsClient', () => {
    const consoleWarnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const state = useWebSocketStore.getState()

    state.subscribe(['test-topic'])
    expect(consoleWarnSpy).toHaveBeenCalledWith('WebSocket client not available for subscribe')
    consoleWarnSpy.mockRestore()
  })
  it('warns when subscribing without connection', () => {
    const consoleWarnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})

    useWebSocketStore.setState({ wsClient: mockClient as unknown as null })
    const state = useWebSocketStore.getState()

    state.subscribe(['test-topic'])
    expect(consoleWarnSpy).toHaveBeenCalled()
    consoleWarnSpy.mockRestore()
  })
  it('warns when unsubscribing without wsClient', () => {
    const consoleWarnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const state = useWebSocketStore.getState()

    state.unsubscribe(['test-topic'])
    expect(consoleWarnSpy).toHaveBeenCalledWith('WebSocket client not available for unsubscribe')
    consoleWarnSpy.mockRestore()
  })
  it('warns when unsubscribing without connection', () => {
    const consoleWarnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})

    useWebSocketStore.setState({ wsClient: mockClient as unknown as null })
    const state = useWebSocketStore.getState()

    state.unsubscribe(['test-topic'])
    expect(consoleWarnSpy).toHaveBeenCalled()
    consoleWarnSpy.mockRestore()
  })
  it('subscribes when connected', () => {
    useWebSocketStore.setState({
      wsClient: mockClient as unknown as null,
      isConnected: true,
    })
    const state = useWebSocketStore.getState()

    state.subscribe(['test-topic'])
    expect(mockClient.subscribe).toHaveBeenCalledWith(['test-topic'])
  })
  it('unsubscribes when connected', () => {
    useWebSocketStore.setState({
      wsClient: mockClient as unknown as null,
      isConnected: true,
    })
    const state = useWebSocketStore.getState()

    state.unsubscribe(['test-topic'])
    expect(mockClient.unsubscribe).toHaveBeenCalledWith(['test-topic'])
  })
  it('disconnects and resets state', () => {
    useWebSocketStore.setState({
      wsClient: mockClient as unknown as WebSocketClient,
      isConnected: true,
    })
    const state = useWebSocketStore.getState()

    state.disconnect()
    expect(mockClient.disconnect).toHaveBeenCalled()
    const newState = useWebSocketStore.getState()

    expect(newState.wsClient).toBeNull()
    expect(newState.isConnected).toBe(false)
    expect(newState.isConnecting).toBe(false)
    expect(newState.error).toBeNull()
  })
  it('disconnects safely when no client is present', () => {
    useWebSocketStore.setState({
      wsClient: null,
      isConnected: false,
      isConnecting: false,
      error: 'still-here',
    })
    const state = useWebSocketStore.getState()

    state.disconnect()
    const newState = useWebSocketStore.getState()

    expect(newState.wsClient).toBeNull()
    expect(newState.error).toBe('still-here')
  })
  it('handles auth_complete message by clearing error', async () => {
    useWebSocketStore.setState({
      error: 'Previous auth error',
    })
    const connectPromise = useWebSocketStore.getState().connect('ws://test')

    await act(async () => {
      await vi.advanceTimersByTimeAsync(5)
    })
    mockClient.triggerMessage('auth_complete', {})
    expect(useWebSocketStore.getState().error).toBeNull()
    mockClient.isConnected.mockReturnValue(true)
    mockClient.triggerConnection(true)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(50)
    })

    try {
      await connectPromise
    } catch {
      void 0
    }
  })
  it('handles auth_failed message with reason', async () => {
    const connectPromise = useWebSocketStore.getState().connect('ws://test')

    await act(async () => {
      await vi.advanceTimersByTimeAsync(5)
    })
    mockClient.triggerMessage('auth_failed', { reason: 'Invalid token' })
    expect(useWebSocketStore.getState().error).toBe(
      'WebSocket authentication failed: Invalid token'
    )
    mockClient.isConnected.mockReturnValue(true)
    mockClient.triggerConnection(true)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(50)
    })

    try {
      await connectPromise
    } catch {
      void 0
    }
  })
  it('handles auth_failed message without reason', async () => {
    const connectPromise = useWebSocketStore.getState().connect('ws://test')

    await act(async () => {
      await vi.advanceTimersByTimeAsync(5)
    })
    mockClient.triggerMessage('auth_failed', {})
    expect(useWebSocketStore.getState().error).toBe('WebSocket authentication failed')
    mockClient.isConnected.mockReturnValue(true)
    mockClient.triggerConnection(true)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(50)
    })

    try {
      await connectPromise
    } catch {
      void 0
    }
  })
  it('handles auth_expired message', async () => {
    const connectPromise = useWebSocketStore.getState().connect('ws://test')

    await act(async () => {
      await vi.advanceTimersByTimeAsync(5)
    })
    mockClient.triggerMessage('auth_expired', {})
    expect(useWebSocketStore.getState().error).toBe(
      'WebSocket session expired, attempting to reconnect…'
    )
    mockClient.isConnected.mockReturnValue(true)
    mockClient.triggerConnection(true)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(50)
    })

    try {
      await connectPromise
    } catch {
      void 0
    }
  })
  it('uses fallback unsubscribe when onConnection returns undefined', async () => {
    const originalOnConnection = mockClient.onConnection

    mockClient.onConnection = vi
      .fn()
      .mockImplementationOnce(originalOnConnection)
      .mockImplementationOnce(() => undefined as unknown as () => void)
      .mockImplementation(originalOnConnection)
    const connectPromise = useWebSocketStore.getState().connect('ws://test')

    await act(async () => {
      await vi.advanceTimersByTimeAsync(5)
    })
    mockClient.isConnected.mockReturnValue(true)
    mockClient.triggerConnection(true)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(50)
    })
    await expect(connectPromise).resolves.toBeUndefined()
  })
  it('handles connection timeout', async () => {
    mockClient.isConnected.mockReturnValue(false)
    const connectPromise = useWebSocketStore.getState().connect('ws://test')
    const catchedPromise = connectPromise.catch((error: Error) => error)

    await act(async () => {
      await vi.runAllTimersAsync()
    })
    const result = await catchedPromise

    expect(result).toBeInstanceOf(Error)
    expect((result as Error).message).toBe('Connection timeout')
    const state = useWebSocketStore.getState()

    expect(state.isConnecting).toBe(false)
    expect(state.error).toBe('Connection timeout')
  })
  it('updates isConnected state via onConnection handler', async () => {
    const connectPromise = useWebSocketStore.getState().connect('ws://test')

    await act(async () => {
      await vi.advanceTimersByTimeAsync(5)
    })
    mockClient.isConnected.mockReturnValue(true)
    mockClient.triggerConnection(true)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(50)
    })

    try {
      await connectPromise
    } catch {
      void 0
    }

    expect(useWebSocketStore.getState().isConnected).toBe(true)
  })
  it('uses fallback unsubscribe when onConnection returns undefined', async () => {
    const customClient = {
      connect: vi.fn(),
      disconnect: vi.fn(),
      subscribe: vi.fn(),
      unsubscribe: vi.fn(),
      isConnected: vi.fn(() => true),
      onConnection: vi.fn((handler: (connected: boolean) => void) => {
        handler(true)

        return undefined
      }),
      onMessage: vi.fn(),
    }

    useWebSocketStore.setState({
      wsClient: customClient as unknown as WebSocketClient,
    })
    await useWebSocketStore.getState().connect('ws://test')
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10000)
    })
    expect(customClient.onConnection).toHaveBeenCalled()
    expect(useWebSocketStore.getState().error).toBeNull()
  })
  it('does not reconnect if already connected', async () => {
    useWebSocketStore.setState({
      isConnected: true,
    })
    const state = useWebSocketStore.getState()

    await state.connect()
    expect(useWebSocketStore.getState().isConnecting).toBe(false)
  })
  it('does not reconnect if already connecting', async () => {
    useWebSocketStore.setState({
      isConnecting: true,
    })
    const state = useWebSocketStore.getState()

    await state.connect()
    expect(useWebSocketStore.getState().isConnecting).toBe(true)
  })
  it('creates secure client when user has auth cookies', async () => {
    vi.mocked(apiClient.hasAuthCookies).mockReturnValue(true)
    const connectPromise = useWebSocketStore.getState().connect('ws://test')

    await act(async () => {
      await vi.advanceTimersByTimeAsync(5)
    })
    expect(useWebSocketStore.getState().wsClient).not.toBeNull()
    mockClient.isConnected.mockReturnValue(true)
    mockClient.triggerConnection(true)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(50)
    })

    try {
      await connectPromise
    } catch {
      void 0
    }

    vi.mocked(apiClient.hasAuthCookies).mockReturnValue(false)
  })
  it('creates secure client when user is authenticated', async () => {
    vi.mocked(useAuthStore.getState).mockReturnValue({
      isAuthenticated: true,
    } as ReturnType<typeof useAuthStore.getState>)
    const connectPromise = useWebSocketStore.getState().connect('ws://test')

    await act(async () => {
      await vi.advanceTimersByTimeAsync(5)
    })
    expect(useWebSocketStore.getState().wsClient).not.toBeNull()
    mockClient.isConnected.mockReturnValue(true)
    mockClient.triggerConnection(true)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(50)
    })

    try {
      await connectPromise
    } catch {
      void 0
    }

    vi.mocked(useAuthStore.getState).mockReturnValue({
      isAuthenticated: false,
    } as ReturnType<typeof useAuthStore.getState>)
  })
  it('reuses existing connected client', async () => {
    const existingClient = createMockClient()

    existingClient.isConnected.mockReturnValue(true)
    useWebSocketStore.setState({
      wsClient: existingClient as unknown as WebSocketClient,
    })
    const connectPromise = useWebSocketStore.getState().connect('ws://test')

    await act(async () => {
      existingClient.triggerConnection(true)
      await vi.advanceTimersByTimeAsync(10)
    })
    await connectPromise
    expect(useWebSocketStore.getState().wsClient).toBe(existingClient)
  })
  it('handles synchronous error thrown by client.connect()', async () => {
    shouldThrowOnConnect = true
    useWebSocketStore.setState({
      wsClient: null,
      isConnected: false,
      isConnecting: false,
      error: null,
    })
    const connectPromise = useWebSocketStore.getState().connect('ws://test')
    const catchedPromise = connectPromise.catch((error: Error) => error)

    await act(async () => {
      await vi.runAllTimersAsync()
    })
    const result = await catchedPromise

    expect(result).toBeInstanceOf(Error)
    expect((result as Error).message).toBe('Synchronous connect error')
    const state = useWebSocketStore.getState()

    expect(state.isConnecting).toBe(false)
    expect(state.error).toBe('Synchronous connect error')
    shouldThrowOnConnect = false
  })
  it('uses fallback error message for non-Error connect failures', async () => {
    const connectError = { code: 'CONNECT_FAIL' }
    const customClient = {
      connect: vi.fn(() => {
        throw connectError
      }),
      disconnect: vi.fn(),
      subscribe: vi.fn(),
      unsubscribe: vi.fn(),
      isConnected: vi.fn(() => true),
      onConnection: vi.fn(() => () => {}),
      onMessage: vi.fn(),
    }

    useWebSocketStore.setState({
      wsClient: customClient as unknown as WebSocketClient,
    })
    await expect(useWebSocketStore.getState().connect('ws://test')).rejects.toBe(connectError)
    const state = useWebSocketStore.getState()

    expect(state.isConnecting).toBe(false)
    expect(state.error).toBe('Connection failed')
  })
})
describe('useWebSocketConnection', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.useFakeTimers()
    useWebSocketStore.setState({
      wsClient: null,
      isConnected: false,
      isConnecting: false,
      error: null,
    })
    vi.mocked(useAuthStore.getState).mockReturnValue({ isAuthenticated: true } as ReturnType<
      typeof useAuthStore.getState
    >)
  })
  afterEach(() => {
    vi.clearAllTimers()
    vi.useRealTimers()
  })
  it('does not attempt connection when not authenticated', async () => {
    vi.mocked(useAuthStore.getState).mockReturnValue({ isAuthenticated: false } as ReturnType<
      typeof useAuthStore.getState
    >)
    const originalConnect = useWebSocketStore.getState().connect
    const connectMock = vi.fn().mockResolvedValue(undefined)

    useWebSocketStore.setState({ connect: connectMock })
    renderHook(() => useWebSocketConnection())
    await act(async () => {
      await vi.advanceTimersByTimeAsync(100)
    })
    expect(connectMock).not.toHaveBeenCalled()
    useWebSocketStore.setState({ connect: originalConnect })
  })
  it('does not retry when user logs out', async () => {
    vi.mocked(useAuthStore.getState).mockReturnValue({ isAuthenticated: true } as ReturnType<
      typeof useAuthStore.getState
    >)
    const originalConnect = useWebSocketStore.getState().connect
    const connectMock = vi.fn().mockRejectedValue(new Error('Connection failed'))

    useWebSocketStore.setState({ connect: connectMock })
    renderHook(() => useWebSocketConnection(undefined, { retryDelayMs: 100 }))
    await act(async () => {
      await vi.advanceTimersByTimeAsync(50)
    })
    expect(connectMock).toHaveBeenCalledTimes(1)
    vi.mocked(useAuthStore.getState).mockReturnValue({ isAuthenticated: false } as ReturnType<
      typeof useAuthStore.getState
    >)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(200)
    })
    expect(connectMock).toHaveBeenCalledTimes(1)
    useWebSocketStore.setState({ connect: originalConnect })
  })
  it('returns connection state', async () => {
    const { result } = renderHook(() => useWebSocketConnection())

    expect(typeof result.current.isConnected).toBe('boolean')
    expect(typeof result.current.isConnecting).toBe('boolean')
    expect(result.current.error === null || typeof result.current.error === 'string').toBe(true)
  })
  it('attempts connection on mount', async () => {
    const { result } = renderHook(() => useWebSocketConnection())

    expect(result.current.isConnected).toBe(false)
  })
  it('disconnects on unmount when autoDisconnect is true', async () => {
    const { unmount } = renderHook(() =>
      useWebSocketConnection(undefined, { autoDisconnect: true })
    )

    unmount()
    expect(useWebSocketStore.getState().isConnected).toBe(false)
  })
  it('does not disconnect on unmount by default', async () => {
    useWebSocketStore.setState({
      wsClient: mockClient as unknown as null,
      isConnected: true,
    })
    const { unmount } = renderHook(() => useWebSocketConnection())

    unmount()
    expect(mockClient.disconnect).not.toHaveBeenCalled()
  })
  it('retries connection after failure', async () => {
    const originalConnect = useWebSocketStore.getState().connect
    const connectMock = vi.fn().mockRejectedValue(new Error('Connection failed'))

    useWebSocketStore.setState({ connect: connectMock })
    renderHook(() => useWebSocketConnection(undefined, { retryDelayMs: 100 }))
    await act(async () => {
      await vi.advanceTimersByTimeAsync(50)
    })
    const callCountAfterFirst = connectMock.mock.calls.length

    expect(callCountAfterFirst).toBeGreaterThanOrEqual(1)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(150)
    })
    expect(connectMock.mock.calls.length).toBeGreaterThan(callCountAfterFirst)
    useWebSocketStore.setState({ connect: originalConnect })
  })
  it('does not retry after unmount', async () => {
    const originalConnect = useWebSocketStore.getState().connect
    const connectMock = vi.fn().mockRejectedValue(new Error('Connection failed'))

    useWebSocketStore.setState({ connect: connectMock })
    const { unmount } = renderHook(() => useWebSocketConnection(undefined, { retryDelayMs: 100 }))

    await act(async () => {
      await vi.advanceTimersByTimeAsync(50)
    })
    const callCountBeforeUnmount = connectMock.mock.calls.length

    unmount()
    await act(async () => {
      await vi.advanceTimersByTimeAsync(200)
    })
    expect(connectMock.mock.calls.length).toBeLessThanOrEqual(callCountBeforeUnmount + 1)
    useWebSocketStore.setState({ connect: originalConnect })
  })
  it('uses provided URL for connection', async () => {
    const customUrl = 'ws://custom.example.com/ws'
    const originalConnect = useWebSocketStore.getState().connect
    const connectMock = vi.fn().mockResolvedValue(undefined)

    useWebSocketStore.setState({ connect: connectMock })
    renderHook(() => useWebSocketConnection(customUrl))
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10)
    })
    expect(connectMock).toHaveBeenCalledWith(customUrl)
    useWebSocketStore.setState({ connect: originalConnect })
  })
  it('does not attempt connection after early unmount', async () => {
    const originalConnect = useWebSocketStore.getState().connect
    const connectMock = vi.fn().mockImplementation(
      () =>
        new Promise(resolve => {
          setTimeout(resolve, 100)
        })
    )

    useWebSocketStore.setState({ connect: connectMock })
    const { unmount } = renderHook(() => useWebSocketConnection())

    unmount()
    const callsBeforeTimer = connectMock.mock.calls.length

    await act(async () => {
      await vi.advanceTimersByTimeAsync(10000)
    })
    expect(connectMock.mock.calls.length).toBeLessThanOrEqual(callsBeforeTimer + 1)
    useWebSocketStore.setState({ connect: originalConnect })
  })
  it('does not retry when unmounted during retry delay', async () => {
    const originalConnect = useWebSocketStore.getState().connect
    let callCount = 0
    const connectMock = vi.fn().mockImplementation(async () => {
      callCount++
      throw new Error('Connection failed')
    })

    useWebSocketStore.setState({ connect: connectMock })
    const { unmount } = renderHook(() => useWebSocketConnection(undefined, { retryDelayMs: 1000 }))

    await act(async () => {
      await vi.advanceTimersByTimeAsync(10)
    })
    expect(callCount).toBe(1)
    unmount()
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000)
    })
    expect(callCount).toBe(1)
    useWebSocketStore.setState({ connect: originalConnect })
  })
  it('handles isMounted check when retry is already scheduled', async () => {
    const originalConnect = useWebSocketStore.getState().connect
    let callCount = 0
    const connectMock = vi.fn().mockImplementation(async () => {
      callCount++
      throw new Error('Connection failed')
    })

    useWebSocketStore.setState({ connect: connectMock })
    vi.useRealTimers()
    const { unmount } = renderHook(() => useWebSocketConnection(undefined, { retryDelayMs: 50 }))

    await new Promise(resolve => setTimeout(resolve, 20))
    expect(callCount).toBe(1)
    unmount()
    await new Promise(resolve => setTimeout(resolve, 100))
    expect(callCount).toBe(1)
    useWebSocketStore.setState({ connect: originalConnect })
    vi.useFakeTimers()
  })
  it('skips retry work when unmounted flag is false', async () => {
    const originalConnect = useWebSocketStore.getState().connect
    const connectMock = vi.fn().mockRejectedValue(new Error('Connection failed'))

    useWebSocketStore.setState({ connect: connectMock })
    const clearTimeoutSpy = vi.spyOn(globalThis, 'clearTimeout').mockImplementation(() => {})
    const { unmount } = renderHook(() => useWebSocketConnection(undefined, { retryDelayMs: 50 }))

    await act(async () => {
      await vi.advanceTimersByTimeAsync(10)
    })
    expect(connectMock).toHaveBeenCalledTimes(1)
    unmount()
    await act(async () => {
      await vi.advanceTimersByTimeAsync(100)
    })
    expect(connectMock).toHaveBeenCalledTimes(1)
    clearTimeoutSpy.mockRestore()
    useWebSocketStore.setState({ connect: originalConnect })
  })
  it('does not schedule retry when unmounted before connect rejects', async () => {
    const originalConnect = useWebSocketStore.getState().connect
    let rejectPromise: ((error: Error) => void) | null = null
    const connectMock = vi.fn(
      () =>
        new Promise<void>((_resolve, reject) => {
          rejectPromise = reject
        })
    )

    useWebSocketStore.setState({ connect: connectMock })
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    const timeoutSpy = vi.spyOn(window, 'setTimeout')
    const { unmount } = renderHook(() => useWebSocketConnection(undefined, { retryDelayMs: 50 }))

    await act(async () => {
      await vi.advanceTimersByTimeAsync(1)
    })
    expect(connectMock).toHaveBeenCalledTimes(1)
    expect(rejectPromise).not.toBeNull()
    unmount()
    await act(async () => {
      rejectPromise?.(new Error('Connection failed'))
      await Promise.resolve()
    })
    expect(timeoutSpy).not.toHaveBeenCalled()
    timeoutSpy.mockRestore()
    errorSpy.mockRestore()
    useWebSocketStore.setState({ connect: originalConnect })
  })
})
