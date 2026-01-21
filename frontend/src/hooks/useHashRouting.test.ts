import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { renderHook, act } from '@testing-library/react'
import { useTabRouting, type ValidTab } from './useHashRouting'

describe('useHashRouting', () => {
  let originalHash: string

  beforeEach(() => {
    originalHash = window.location.hash
    window.location.hash = ''
  })
  afterEach(() => {
    window.location.hash = originalHash
  })
  it('returns default route when no hash present', () => {
    const { result } = renderHook(() => useTabRouting())

    expect(result.current[0]).toBe('overview')
  })
  it('sets initial hash to default route', () => {
    renderHook(() => useTabRouting())
    expect(window.location.hash).toBe('#overview')
  })
  it('returns current hash when valid', () => {
    window.location.hash = '#market'
    const { result } = renderHook(() => useTabRouting())

    expect(result.current[0]).toBe('market')
  })
  it('navigates to new route', () => {
    const { result } = renderHook(() => useTabRouting())

    act(() => {
      result.current[1]('processes')
    })
    expect(result.current[0]).toBe('processes')
    expect(window.location.hash).toBe('#processes')
  })
  it('handles all valid tabs', () => {
    const validTabs: ValidTab[] = [
      'overview',
      'market',
      'processes',
      'strategies',
      'orders',
      'signals',
      'health',
      'admin',
      'charts',
      'settings',
    ]
    const { result } = renderHook(() => useTabRouting())

    validTabs.forEach(tab => {
      act(() => {
        result.current[1](tab)
      })
      expect(result.current[0]).toBe(tab)
      expect(window.location.hash).toBe(`#${tab}`)
    })
  })
  it('falls back to default for invalid hash', () => {
    window.location.hash = '#invalid-route'
    const { result } = renderHook(() => useTabRouting())

    expect(result.current[0]).toBe('overview')
  })
  it('responds to hashchange events', () => {
    const { result } = renderHook(() => useTabRouting())

    act(() => {
      window.location.hash = '#strategies'
      window.dispatchEvent(new HashChangeEvent('hashchange'))
    })
    expect(result.current[0]).toBe('strategies')
  })
  it('responds to external hashchange', () => {
    const { result } = renderHook(() => useTabRouting())

    expect(result.current[0]).toBe('overview')
    act(() => {
      window.location.hash = '#health'
      window.dispatchEvent(new HashChangeEvent('hashchange'))
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
      window.location.hash = '#not-valid-route'
      window.dispatchEvent(new HashChangeEvent('hashchange'))
    })
    expect(result.current[0]).toBe('overview')
  })
  it('does not set hash when default already present', () => {
    window.location.hash = '#overview'
    const { result } = renderHook(() => useTabRouting())

    expect(result.current[0]).toBe('overview')
  })
  it('cleans up event listener on unmount', () => {
    const removeEventListenerSpy = vi.spyOn(window, 'removeEventListener')
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
