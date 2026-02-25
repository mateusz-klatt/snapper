import { renderHook, act } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import { useConfirmDialog } from './useConfirmDialog'

describe('useConfirmDialog', () => {
  it('starts closed with empty title and message', () => {
    const { result } = renderHook(() => useConfirmDialog())

    expect(result.current.dialogProps.open).toBe(false)
    expect(result.current.dialogProps.title).toBe('')
    expect(result.current.dialogProps.message).toBe('')
    expect(result.current.dialogProps.variant).toBe('default')
  })

  it('openConfirm opens dialog with correct title and message', () => {
    const { result } = renderHook(() => useConfirmDialog())

    act(() => {
      result.current.openConfirm({
        title: 'Delete item',
        message: 'Are you sure?',
        onConfirm: vi.fn(),
      })
    })

    expect(result.current.dialogProps.open).toBe(true)
    expect(result.current.dialogProps.title).toBe('Delete item')
    expect(result.current.dialogProps.message).toBe('Are you sure?')
  })

  it('openConfirm defaults variant to default', () => {
    const { result } = renderHook(() => useConfirmDialog())

    act(() => {
      result.current.openConfirm({ title: 'T', message: 'M', onConfirm: vi.fn() })
    })

    expect(result.current.dialogProps.variant).toBe('default')
  })

  it('openConfirm accepts danger variant', () => {
    const { result } = renderHook(() => useConfirmDialog())

    act(() => {
      result.current.openConfirm({
        title: 'T',
        message: 'M',
        onConfirm: vi.fn(),
        variant: 'danger',
      })
    })

    expect(result.current.dialogProps.variant).toBe('danger')
  })

  it('onConfirm calls the registered callback', () => {
    const { result } = renderHook(() => useConfirmDialog())
    const onConfirm = vi.fn()

    act(() => {
      result.current.openConfirm({ title: 'T', message: 'M', onConfirm })
    })

    act(() => {
      result.current.dialogProps.onConfirm()
    })

    expect(onConfirm).toHaveBeenCalledOnce()
  })

  it('onConfirm closes the dialog', () => {
    const { result } = renderHook(() => useConfirmDialog())

    act(() => {
      result.current.openConfirm({ title: 'T', message: 'M', onConfirm: vi.fn() })
    })

    act(() => {
      result.current.dialogProps.onConfirm()
    })

    expect(result.current.dialogProps.open).toBe(false)
  })

  it('onCancel closes the dialog', () => {
    const { result } = renderHook(() => useConfirmDialog())

    act(() => {
      result.current.openConfirm({ title: 'T', message: 'M', onConfirm: vi.fn() })
    })

    act(() => {
      result.current.dialogProps.onCancel()
    })

    expect(result.current.dialogProps.open).toBe(false)
  })

  it('onCancel does not call the registered callback', () => {
    const { result } = renderHook(() => useConfirmDialog())
    const onConfirm = vi.fn()

    act(() => {
      result.current.openConfirm({ title: 'T', message: 'M', onConfirm })
    })

    act(() => {
      result.current.dialogProps.onCancel()
    })

    expect(onConfirm).not.toHaveBeenCalled()
  })

  it('openConfirm replaces the previous callback', () => {
    const { result } = renderHook(() => useConfirmDialog())
    const first = vi.fn()
    const second = vi.fn()

    act(() => {
      result.current.openConfirm({ title: 'T', message: 'M', onConfirm: first })
    })

    act(() => {
      result.current.openConfirm({ title: 'T2', message: 'M2', onConfirm: second })
    })

    act(() => {
      result.current.dialogProps.onConfirm()
    })

    expect(second).toHaveBeenCalledOnce()
    expect(first).not.toHaveBeenCalled()
  })
})
