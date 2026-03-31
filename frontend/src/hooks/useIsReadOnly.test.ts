import { describe, it, expect, beforeEach } from 'vitest'
import { renderHook } from '@testing-library/react'
import { useIsReadOnly } from './useIsReadOnly'
import { useAppStore } from '../stores/app'

describe('useIsReadOnly', () => {
  beforeEach(() => {
    useAppStore.setState({ asOf: null, isTimeTraveling: false })
  })

  it('returns false when not time-traveling', () => {
    const { result } = renderHook(() => useIsReadOnly())

    expect(result.current).toBe(false)
  })

  it('returns true when time-traveling', () => {
    useAppStore.setState({ asOf: '2026-03-15T10:00:00Z', isTimeTraveling: true })
    const { result } = renderHook(() => useIsReadOnly())

    expect(result.current).toBe(true)
  })
})
