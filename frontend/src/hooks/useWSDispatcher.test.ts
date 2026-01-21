import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { renderHook, waitFor, cleanup } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import React from 'react'
import { useWebSocketStore } from '../stores/websocket'

const { mockDetach, mockDispatcher, mockGetDispatcher, mockResetDispatcher } = vi.hoisted(() => {
  const mockAttach = vi.fn()
  const mockDetach = vi.fn()
  const mockDispatcher = {
    attach: mockAttach,
    detach: mockDetach,
    isAttached: vi.fn(() => false),
    getClient: vi.fn(() => null),
  }

  return {
    mockDetach,
    mockDispatcher,
    mockGetDispatcher: vi.fn(() => mockDispatcher),
    mockResetDispatcher: vi.fn(),
  }
})

vi.mock('../stores/websocket', () => ({
  useWebSocketStore: vi.fn(() => ({
    wsClient: null,
    isConnected: false,
  })),
}))
vi.mock('../stores/wsDispatcher', () => ({
  getDispatcher: mockGetDispatcher,
  resetDispatcher: mockResetDispatcher,
  WSDispatcher: function () {
    return mockDispatcher
  },
}))
import { useWSDispatcher } from './useWSDispatcher'

describe('useWSDispatcher', () => {
  let queryClient: QueryClient

  function wrapper({ children }: { children: React.ReactNode }): React.JSX.Element {
    return React.createElement(QueryClientProvider, { client: queryClient }, children)
  }

  beforeEach(() => {
    vi.clearAllMocks()
    queryClient = new QueryClient({
      defaultOptions: {
        queries: { retry: false },
      },
    })
  })
  afterEach(() => {
    cleanup()
    queryClient.clear()
  })
  it('returns null when not enabled', () => {
    const { result } = renderHook(() => useWSDispatcher({ enabled: false }), { wrapper })

    expect(result.current).toBeNull()
  })
  it('returns dispatcher when enabled', async () => {
    const { result } = renderHook(() => useWSDispatcher({ enabled: true }), { wrapper })

    await waitFor(() => {
      expect(result.current).toBeDefined()
    })
  })
  it('defaults enabled to true', async () => {
    const { result } = renderHook(() => useWSDispatcher(), { wrapper })

    await waitFor(() => {
      expect(result.current).toBeDefined()
    })
  })
  it('creates dispatcher with custom topics', async () => {
    const { result } = renderHook(() => useWSDispatcher({ topics: ['custom.topic'] }), { wrapper })

    await waitFor(() => {
      expect(result.current).toBeDefined()
    })
    expect(mockResetDispatcher).toHaveBeenCalled()
  })
  it('attaches dispatcher when websocket is connected', async () => {
    const wsClient = { id: 'ws-client' }

    vi.mocked(useWebSocketStore).mockReturnValue({
      wsClient,
      isConnected: true,
    } as never)
    renderHook(() => useWSDispatcher(), { wrapper })
    await waitFor(() => {
      expect(mockDispatcher.attach).toHaveBeenCalledWith(wsClient)
    })
    vi.mocked(useWebSocketStore).mockReturnValue({
      wsClient: null,
      isConnected: false,
    } as never)
  })
  it('detaches when disabled after being enabled', async () => {
    const { rerender } = renderHook(({ enabled }) => useWSDispatcher({ enabled }), {
      wrapper,
      initialProps: { enabled: true },
    })

    await waitFor(() => {
      expect(mockGetDispatcher).toHaveBeenCalled()
    })
    const initialDetachCalls = mockDetach.mock.calls.length

    rerender({ enabled: false })
    expect(mockDetach.mock.calls.length).toBeGreaterThan(initialDetachCalls)
  })
  it('detaches on unmount', async () => {
    mockGetDispatcher.mockReturnValue(mockDispatcher as never)
    const { unmount } = renderHook(() => useWSDispatcher(), { wrapper })

    await waitFor(() => {
      expect(mockGetDispatcher).toHaveBeenCalled()
    })
    mockDetach.mockClear()
    unmount()
    await waitFor(() => {
      expect(mockDetach).toHaveBeenCalled()
    })
  })
  it('skips cleanup when dispatcher is null', async () => {
    mockGetDispatcher.mockReturnValueOnce(null as never)
    const { unmount } = renderHook(() => useWSDispatcher(), { wrapper })

    await waitFor(() => {
      expect(mockGetDispatcher).toHaveBeenCalled()
    })
    unmount()
    expect(mockDetach).not.toHaveBeenCalled()
  })
})
