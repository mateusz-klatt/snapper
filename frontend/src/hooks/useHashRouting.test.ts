import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useTabRouting, useHashSubpath, type ValidTab } from './useHashRouting'

describe('useHashRouting', () => {
  let originalHash: string

  beforeEach(() => {
    originalHash = globalThis.location.hash
    globalThis.location.hash = ''
  })
  afterEach(() => {
    globalThis.location.hash = originalHash
  })
  it('returns default route when no hash present', () => {
    const { result } = renderHook(() => useTabRouting())

    expect(result.current[0]).toBe('overview')
  })
  it('sets initial hash to default route', () => {
    renderHook(() => useTabRouting())
    expect(globalThis.location.hash).toBe('#overview')
  })
  it('returns current hash when valid', () => {
    globalThis.location.hash = '#market'
    const { result } = renderHook(() => useTabRouting())

    expect(result.current[0]).toBe('market')
  })
  it('navigates to new route', () => {
    const { result } = renderHook(() => useTabRouting())

    act(() => {
      result.current[1]('processes')
    })
    expect(result.current[0]).toBe('processes')
    expect(globalThis.location.hash).toBe('#processes')
  })
  it('preserves query string when navigating between routes', () => {
    globalThis.location.hash = '#market?wallet=w-1&operator=o-1'
    const { result } = renderHook(() => useTabRouting())

    act(() => {
      result.current[1]('positions')
    })
    expect(globalThis.location.hash).toBe('#positions?wallet=w-1&operator=o-1')
  })
  it('matches route even when hash includes query string', () => {
    globalThis.location.hash = '#backtests?wallet=019ded78&operator=o-1'
    const { result } = renderHook(() => useTabRouting())

    expect(result.current[0]).toBe('backtests')
  })
  it('handles all valid tabs', () => {
    const validTabs: ValidTab[] = [
      'overview',
      'market',
      'processes',
      'strategies',
      'orders',
      'positions',
      'signals',
      'backtests',
      'health',
      'admin',
      'ai-integration',
      'ai-reviews',
      'settings',
    ]
    const { result } = renderHook(() => useTabRouting())

    validTabs.forEach(tab => {
      act(() => {
        result.current[1](tab)
      })
      expect(result.current[0]).toBe(tab)
      expect(globalThis.location.hash).toBe(`#${tab}`)
    })
  })
  it('falls back to default for invalid hash', () => {
    globalThis.location.hash = '#invalid-route'
    const { result } = renderHook(() => useTabRouting())

    expect(result.current[0]).toBe('overview')
  })
  it('responds to hashchange events', () => {
    const { result } = renderHook(() => useTabRouting())

    act(() => {
      globalThis.location.hash = '#strategies'
      globalThis.dispatchEvent(new HashChangeEvent('hashchange'))
    })
    expect(result.current[0]).toBe('strategies')
  })
  it('responds to external hashchange', () => {
    const { result } = renderHook(() => useTabRouting())

    expect(result.current[0]).toBe('overview')
    act(() => {
      globalThis.location.hash = '#health'
      globalThis.dispatchEvent(new HashChangeEvent('hashchange'))
    })
    expect(result.current[0]).toBe('health')
  })
  it('ignores invalid hashchange events', () => {
    const { result } = renderHook(() => useTabRouting())

    act(() => {
      result.current[1]('orders')
    })
    expect(result.current[0]).toBe('orders')
    act(() => {
      globalThis.location.hash = '#not-valid-route'
      globalThis.dispatchEvent(new HashChangeEvent('hashchange'))
    })
    expect(result.current[0]).toBe('overview')
  })
  it('does not set hash when default already present', () => {
    globalThis.location.hash = '#overview'
    const { result } = renderHook(() => useTabRouting())

    expect(result.current[0]).toBe('overview')
  })
  it('cleans up event listener on unmount', () => {
    const removeEventListenerSpy = vi.spyOn(globalThis, 'removeEventListener')
    const { unmount } = renderHook(() => useTabRouting())

    unmount()
    expect(removeEventListenerSpy).toHaveBeenCalledWith('hashchange', expect.any(Function))
    removeEventListenerSpy.mockRestore()
  })
  it('maintains route state across re-renders', () => {
    const { result, rerender } = renderHook(() => useTabRouting())

    act(() => {
      result.current[1]('admin')
    })
    rerender()
    expect(result.current[0]).toBe('admin')
  })
})

describe('useHashSubpath', () => {
  let originalHash: string

  beforeEach(() => {
    originalHash = globalThis.location.hash
    globalThis.location.hash = ''
  })
  afterEach(() => {
    globalThis.location.hash = originalHash
  })
  it('returns [] when the hash does not match the requested tab', () => {
    globalThis.location.hash = '#orders'
    const { result } = renderHook(() => useHashSubpath('backtests'))

    expect(result.current).toEqual([])
  })
  it('returns [] when the hash is just the tab with no sub-path', () => {
    globalThis.location.hash = '#backtests'
    const { result } = renderHook(() => useHashSubpath('backtests'))

    expect(result.current).toEqual([])
  })
  it('returns the sub-path segments for a matching hash', () => {
    globalThis.location.hash = '#backtests/run-1/equity'
    const { result } = renderHook(() => useHashSubpath('backtests'))

    expect(result.current).toEqual(['run-1', 'equity'])
  })
  it('updates when the hash changes', () => {
    globalThis.location.hash = '#backtests'
    const { result } = renderHook(() => useHashSubpath('backtests'))

    expect(result.current).toEqual([])
    act(() => {
      globalThis.location.hash = '#backtests/run-2'
      globalThis.dispatchEvent(new HashChangeEvent('hashchange'))
    })
    expect(result.current).toEqual(['run-2'])
  })
  it('strips ?query before segmenting (deep route + scope persistence)', () => {
    globalThis.location.hash = '#backtests/01948f94-abcd?wallet=w-1'
    const { result } = renderHook(() => useHashSubpath('backtests'))

    expect(result.current).toEqual(['01948f94-abcd'])
  })
  it('strips ?query when only the tab is present with scope params', () => {
    globalThis.location.hash = '#backtests?wallet=w-1&operator=o-1'
    const { result } = renderHook(() => useHashSubpath('backtests'))

    expect(result.current).toEqual([])
  })
  it('strips ?query across multiple deep segments', () => {
    globalThis.location.hash = '#backtests/run-1/equity?wallet=w-1'
    const { result } = renderHook(() => useHashSubpath('backtests'))

    expect(result.current).toEqual(['run-1', 'equity'])
  })
})
