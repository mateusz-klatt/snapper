import { describe, it, expect, beforeEach, vi } from 'vitest'
import { renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ReactNode, createElement } from 'react'
import {
  readUrlScope,
  readLocalStorageScope,
  writeUrlScope,
  writeLocalStorageScope,
  useScopePersistence,
} from './useScopePersistence'

vi.mock('./queries', () => ({
  useWallets: vi.fn(() => ({ data: { payload: [] } })),
  useOperators: vi.fn(() => ({ data: { payload: [] } })),
}))

vi.mock('../lib/apiClient', () => ({
  apiClient: {
    setWalletScope: vi.fn(),
    setOperatorScope: vi.fn(),
    setTimeTravelAsOf: vi.fn(),
  },
}))

vi.mock('../lib/queryClient', () => ({
  queryClient: { invalidateQueries: vi.fn() },
}))

vi.mock('../stores/auth', () => ({
  useAuthStore: {
    getState: () => ({ refreshToken: vi.fn().mockResolvedValue(undefined) }),
  },
}))

import { useWallets, useOperators } from './queries'
import { useAppStore } from '../stores/app'

const wrapper = ({ children }: { children: ReactNode }): ReactNode => {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })

  return createElement(QueryClientProvider, { client: qc }, children)
}

describe('readUrlScope', () => {
  beforeEach(() => {
    globalThis.location.hash = ''
  })

  it('returns null wallet and operator when no hash params', () => {
    globalThis.location.hash = '#market'

    expect(readUrlScope()).toEqual({ wallet: null, operator: null, hasParams: false })
  })

  it('returns null wallet/operator when hash has no query string', () => {
    globalThis.location.hash = ''

    expect(readUrlScope()).toEqual({ wallet: null, operator: null, hasParams: false })
  })

  it('reads wallet param', () => {
    globalThis.location.hash = '#market?wallet=w-1'

    expect(readUrlScope()).toEqual({ wallet: 'w-1', operator: null, hasParams: true })
  })

  it('reads both wallet and operator params', () => {
    globalThis.location.hash = '#market?wallet=w-1&operator=o-1'

    expect(readUrlScope()).toEqual({ wallet: 'w-1', operator: 'o-1', hasParams: true })
  })

  it('treats __all__ sentinel as null wallet (explicit "All wallets")', () => {
    globalThis.location.hash = '#market?wallet=__all__'

    expect(readUrlScope()).toEqual({ wallet: null, operator: null, hasParams: true })
  })

  it('treats __all__ sentinel as null operator', () => {
    globalThis.location.hash = '#market?operator=__all__'

    expect(readUrlScope()).toEqual({ wallet: null, operator: null, hasParams: true })
  })
})

describe('readLocalStorageScope', () => {
  beforeEach(() => {
    globalThis.localStorage.clear()
  })

  it('returns null when storage empty', () => {
    expect(readLocalStorageScope()).toEqual({ wallet: null, operator: null })
  })

  it('reads stored wallet and operator', () => {
    globalThis.localStorage.setItem('snapper-current-wallet', 'w-saved')
    globalThis.localStorage.setItem('snapper-current-operator', 'o-saved')

    expect(readLocalStorageScope()).toEqual({ wallet: 'w-saved', operator: 'o-saved' })
  })

  it('treats __all__ sentinel as null', () => {
    globalThis.localStorage.setItem('snapper-current-wallet', '__all__')
    globalThis.localStorage.setItem('snapper-current-operator', '__all__')

    expect(readLocalStorageScope()).toEqual({ wallet: null, operator: null })
  })

  it('returns null when localStorage throws', () => {
    const original = globalThis.localStorage.getItem

    globalThis.localStorage.getItem = vi.fn(() => {
      throw new Error('quota exceeded')
    })

    expect(readLocalStorageScope()).toEqual({ wallet: null, operator: null })

    globalThis.localStorage.getItem = original
  })
})

describe('writeUrlScope', () => {
  beforeEach(() => {
    globalThis.location.hash = '#market'
  })

  it('appends wallet param when set, no operator', () => {
    writeUrlScope('w-1', null)

    expect(globalThis.location.hash).toBe('#market?wallet=w-1')
  })

  it('appends both wallet and operator', () => {
    writeUrlScope('w-1', 'o-1')

    expect(globalThis.location.hash).toBe('#market?wallet=w-1&operator=o-1')
  })

  it('strips params when both null', () => {
    globalThis.location.hash = '#market?wallet=w-1&operator=o-1'
    writeUrlScope(null, null)

    expect(globalThis.location.hash).toBe('#market')
  })

  it('preserves the route portion when overwriting params', () => {
    globalThis.location.hash = '#backtests?wallet=w-1'
    writeUrlScope('w-2', null)

    expect(globalThis.location.hash).toBe('#backtests?wallet=w-2')
  })

  it('does not call replaceState when hash is unchanged (idempotent)', () => {
    globalThis.location.hash = '#market?wallet=w-1'
    const replaceSpy = vi.spyOn(globalThis.history, 'replaceState')

    writeUrlScope('w-1', null)

    expect(replaceSpy).not.toHaveBeenCalled()

    replaceSpy.mockRestore()
  })

  it('handles bare # hash with no route segment', () => {
    globalThis.location.hash = ''
    writeUrlScope('w-1', null)

    expect(globalThis.location.hash).toBe('#?wallet=w-1')
  })
})

describe('writeLocalStorageScope', () => {
  beforeEach(() => {
    globalThis.localStorage.clear()
  })

  it('persists wallet and operator', () => {
    writeLocalStorageScope('w-1', 'o-1')

    expect(globalThis.localStorage.getItem('snapper-current-wallet')).toBe('w-1')
    expect(globalThis.localStorage.getItem('snapper-current-operator')).toBe('o-1')
  })

  it('removes wallet and operator when null', () => {
    globalThis.localStorage.setItem('snapper-current-wallet', 'w-old')
    globalThis.localStorage.setItem('snapper-current-operator', 'o-old')
    writeLocalStorageScope(null, null)

    expect(globalThis.localStorage.getItem('snapper-current-wallet')).toBeNull()
    expect(globalThis.localStorage.getItem('snapper-current-operator')).toBeNull()
  })

  it('silently ignores localStorage failure', () => {
    const original = globalThis.localStorage.setItem

    globalThis.localStorage.setItem = vi.fn(() => {
      throw new Error('quota')
    })

    expect(() => writeLocalStorageScope('w-1', null)).not.toThrow()

    globalThis.localStorage.setItem = original
  })
})

describe('useScopePersistence', () => {
  beforeEach(() => {
    globalThis.location.hash = '#market'
    globalThis.localStorage.clear()
    vi.clearAllMocks()
    useAppStore.setState({ currentWalletPublicId: null, currentOperatorPublicId: null })
  })

  it('does nothing when wallets and operators API both empty', () => {
    ;(useWallets as ReturnType<typeof vi.fn>).mockReturnValue({ data: { payload: [] } })
    ;(useOperators as ReturnType<typeof vi.fn>).mockReturnValue({ data: { payload: [] } })
    renderHook(() => useScopePersistence(), { wrapper })

    expect(globalThis.location.hash).toBe('#market')
    expect(useAppStore.getState().currentWalletPublicId).toBeNull()
  })

  it('auto-picks first wallet when no URL/storage and wallets available', async () => {
    ;(useWallets as ReturnType<typeof vi.fn>).mockReturnValue({
      data: { payload: [{ public_id: 'w-first' }] },
    })
    renderHook(() => useScopePersistence(), { wrapper })

    await waitFor(() => expect(globalThis.location.hash).toBe('#market?wallet=w-first'))
    expect(globalThis.localStorage.getItem('snapper-current-wallet')).toBe('w-first')
    await waitFor(() => expect(useAppStore.getState().currentWalletPublicId).toBe('w-first'))
  })

  it('uses URL wallet over auto-pick when present', async () => {
    globalThis.location.hash = '#market?wallet=w-from-url'
    ;(useWallets as ReturnType<typeof vi.fn>).mockReturnValue({
      data: { payload: [{ public_id: 'w-first' }, { public_id: 'w-from-url' }] },
    })
    renderHook(() => useScopePersistence(), { wrapper })

    await waitFor(() => expect(useAppStore.getState().currentWalletPublicId).toBe('w-from-url'))
  })

  it('uses localStorage wallet when URL has no params', async () => {
    globalThis.localStorage.setItem('snapper-current-wallet', 'w-from-ls')
    ;(useWallets as ReturnType<typeof vi.fn>).mockReturnValue({
      data: { payload: [{ public_id: 'w-from-ls' }, { public_id: 'w-other' }] },
    })
    renderHook(() => useScopePersistence(), { wrapper })

    await waitFor(() => expect(useAppStore.getState().currentWalletPublicId).toBe('w-from-ls'))
  })

  it('keeps wallet null when URL wallet ID is invalid (URL had explicit param so no auto-pick)', async () => {
    globalThis.location.hash = '#market?wallet=w-deleted'
    ;(useWallets as ReturnType<typeof vi.fn>).mockReturnValue({
      data: { payload: [{ public_id: 'w-real' }] },
    })
    renderHook(() => useScopePersistence(), { wrapper })

    await waitFor(() => expect(globalThis.location.hash).toBe('#market'))
    expect(useAppStore.getState().currentWalletPublicId).toBeNull()
  })

  it('respects __all__ sentinel — explicit no wallet', async () => {
    globalThis.location.hash = '#market?wallet=__all__'
    ;(useWallets as ReturnType<typeof vi.fn>).mockReturnValue({
      data: { payload: [{ public_id: 'w-1' }] },
    })
    renderHook(() => useScopePersistence(), { wrapper })

    await waitFor(() => expect(globalThis.location.hash).toBe('#market'))
    expect(useAppStore.getState().currentWalletPublicId).toBeNull()
  })

  it('sets operator from URL when valid', async () => {
    globalThis.location.hash = '#market?operator=o-1'
    ;(useOperators as ReturnType<typeof vi.fn>).mockReturnValue({
      data: { payload: [{ public_id: 'o-1' }] },
    })
    renderHook(() => useScopePersistence(), { wrapper })

    await waitFor(() => expect(useAppStore.getState().currentOperatorPublicId).toBe('o-1'))
  })

  it('skips setOperator when current store value already matches URL', async () => {
    globalThis.location.hash = '#market?operator=o-1'
    useAppStore.setState({ currentOperatorPublicId: 'o-1' })
    ;(useOperators as ReturnType<typeof vi.fn>).mockReturnValue({
      data: { payload: [{ public_id: 'o-1' }] },
    })
    renderHook(() => useScopePersistence(), { wrapper })

    await waitFor(() => expect(globalThis.location.hash).toBe('#market?operator=o-1'))
    expect(useAppStore.getState().currentOperatorPublicId).toBe('o-1')
  })

  it('skips selectWallet when current store value already matches URL', async () => {
    globalThis.location.hash = '#market?wallet=w-1'
    useAppStore.setState({ currentWalletPublicId: 'w-1' })
    ;(useWallets as ReturnType<typeof vi.fn>).mockReturnValue({
      data: { payload: [{ public_id: 'w-1' }] },
    })
    renderHook(() => useScopePersistence(), { wrapper })

    await waitFor(() => expect(globalThis.location.hash).toBe('#market?wallet=w-1'))
    expect(useAppStore.getState().currentWalletPublicId).toBe('w-1')
  })

  it('writes URL + localStorage on resolution', async () => {
    ;(useWallets as ReturnType<typeof vi.fn>).mockReturnValue({
      data: { payload: [{ public_id: 'w-auto' }] },
    })
    renderHook(() => useScopePersistence(), { wrapper })

    await waitFor(() =>
      expect(globalThis.localStorage.getItem('snapper-current-wallet')).toBe('w-auto')
    )
    expect(globalThis.location.hash).toBe('#market?wallet=w-auto')
  })

  it('syncs URL + localStorage when store changes after init', async () => {
    ;(useWallets as ReturnType<typeof vi.fn>).mockReturnValue({
      data: { payload: [{ public_id: 'w-1' }, { public_id: 'w-2' }] },
    })
    renderHook(() => useScopePersistence(), { wrapper })
    await waitFor(() => expect(globalThis.location.hash).toBe('#market?wallet=w-1'))
    useAppStore.setState({ currentWalletPublicId: 'w-2' })
    await waitFor(() => expect(globalThis.location.hash).toBe('#market?wallet=w-2'))

    expect(globalThis.localStorage.getItem('snapper-current-wallet')).toBe('w-2')
  })

  it('handles empty wallets when only operators API populated (auto-pick is null)', async () => {
    ;(useWallets as ReturnType<typeof vi.fn>).mockReturnValue({ data: { payload: [] } })
    ;(useOperators as ReturnType<typeof vi.fn>).mockReturnValue({
      data: { payload: [{ public_id: 'o-only' }] },
    })
    renderHook(() => useScopePersistence(), { wrapper })

    await waitFor(() => expect(globalThis.location.hash).toBe('#market'))
    expect(useAppStore.getState().currentWalletPublicId).toBeNull()
  })

  it('does not write again when no actual scope change', async () => {
    ;(useWallets as ReturnType<typeof vi.fn>).mockReturnValue({
      data: { payload: [{ public_id: 'w-1' }] },
    })
    const { rerender } = renderHook(() => useScopePersistence(), { wrapper })

    await waitFor(() => expect(globalThis.location.hash).toBe('#market?wallet=w-1'))
    const replaceSpy = vi.spyOn(globalThis.history, 'replaceState')

    rerender()

    expect(replaceSpy).not.toHaveBeenCalled()

    replaceSpy.mockRestore()
  })
})
