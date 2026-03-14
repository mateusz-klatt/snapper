import { describe, it, expect, vi, beforeEach } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useTradeSubscription } from './useTradeSubscription'

describe('useTradeSubscription', () => {
  let mockDispatcher: {
    startTradeBuffering: ReturnType<typeof vi.fn>
    stopTradeBuffering: ReturnType<typeof vi.fn>
    flushTradeBuffer: ReturnType<typeof vi.fn>
  }

  beforeEach(() => {
    vi.clearAllMocks()
    mockDispatcher = {
      startTradeBuffering: vi.fn(),
      stopTradeBuffering: vi.fn(),
      flushTradeBuffer: vi.fn(),
    }
  })
  it('starts trade buffering when dispatcher is provided and enabled', () => {
    renderHook(() => useTradeSubscription({ dispatcher: mockDispatcher as never, enabled: true }))

    expect(mockDispatcher.startTradeBuffering).toHaveBeenCalledOnce()
  })
  it('does not start buffering when disabled', () => {
    renderHook(() => useTradeSubscription({ dispatcher: mockDispatcher as never, enabled: false }))

    expect(mockDispatcher.startTradeBuffering).not.toHaveBeenCalled()
  })
  it('does not start buffering when dispatcher is null', () => {
    renderHook(() => useTradeSubscription({ dispatcher: null }))

    expect(mockDispatcher.startTradeBuffering).not.toHaveBeenCalled()
  })
  it('stops trade buffering on unmount', () => {
    const { unmount } = renderHook(() =>
      useTradeSubscription({ dispatcher: mockDispatcher as never })
    )

    unmount()

    expect(mockDispatcher.stopTradeBuffering).toHaveBeenCalledOnce()
  })
  it('flush calls flushTradeBuffer on dispatcher', () => {
    const { result } = renderHook(() =>
      useTradeSubscription({ dispatcher: mockDispatcher as never })
    )

    act(() => {
      result.current.flush()
    })

    expect(mockDispatcher.flushTradeBuffer).toHaveBeenCalledOnce()
  })
  it('flush is idempotent - only flushes once', () => {
    const { result } = renderHook(() =>
      useTradeSubscription({ dispatcher: mockDispatcher as never })
    )

    act(() => {
      result.current.flush()
      result.current.flush()
      result.current.flush()
    })

    expect(mockDispatcher.flushTradeBuffer).toHaveBeenCalledOnce()
  })
  it('flush resets after dispatcher changes', () => {
    const dispatcher2 = {
      startTradeBuffering: vi.fn(),
      stopTradeBuffering: vi.fn(),
      flushTradeBuffer: vi.fn(),
    }
    const { result, rerender } = renderHook(
      (props: { dispatcher: typeof mockDispatcher }) =>
        useTradeSubscription({ dispatcher: props.dispatcher as never }),
      { initialProps: { dispatcher: mockDispatcher } }
    )

    act(() => {
      result.current.flush()
    })

    expect(mockDispatcher.flushTradeBuffer).toHaveBeenCalledOnce()
    rerender({ dispatcher: dispatcher2 })

    act(() => {
      result.current.flush()
    })

    expect(dispatcher2.flushTradeBuffer).toHaveBeenCalledOnce()
  })
  it('flush does nothing when dispatcher is null', () => {
    const { result } = renderHook(() => useTradeSubscription({ dispatcher: null }))

    act(() => {
      result.current.flush()
    })

    expect(mockDispatcher.flushTradeBuffer).not.toHaveBeenCalled()
  })
  it('defaults enabled to true', () => {
    renderHook(() => useTradeSubscription({ dispatcher: mockDispatcher as never }))

    expect(mockDispatcher.startTradeBuffering).toHaveBeenCalledOnce()
  })
})
