import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import React from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { WalletPicker } from './WalletPicker'
import { useAppStore } from '../stores/app'
import { useAuthStore } from '../stores/auth'

let mockWalletsData: unknown = {
  payload: [
    {
      public_id: 'w-1',
      label: 'default',
      is_paper: false,
      type: 'wallet_info',
      session_id: 's',
      sequence_id: 1,
      timestamp: '2026-01-01T00:00:00Z',
    },
    {
      public_id: 'w-2',
      label: 'default',
      is_paper: true,
      type: 'wallet_info',
      session_id: 's',
      sequence_id: 2,
      timestamp: '2026-01-01T00:00:00Z',
    },
  ],
}

vi.mock('../hooks/queries', () => ({
  useWallets: () => ({
    data: mockWalletsData,
    isLoading: false,
  }),
}))

const createWrapper = () => {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })

  return ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={qc}>{children}</QueryClientProvider>
  )
}

describe('WalletPicker', () => {
  beforeEach(() => {
    useAppStore.getState().setCurrentWalletPublicId(null)
    useAuthStore.setState({
      refreshToken: vi.fn().mockResolvedValue(undefined),
    } as Partial<ReturnType<typeof useAuthStore.getState>>)
  })
  it('renders with "All wallets" selected by default', () => {
    render(<WalletPicker />, { wrapper: createWrapper() })

    expect(screen.getByText('All wallets')).toBeDefined()
  })
  it('selects a wallet and updates the store', async () => {
    const user = userEvent.setup()
    const { waitFor } = await import('@testing-library/react')

    render(<WalletPicker />, { wrapper: createWrapper() })
    await user.click(screen.getByRole('combobox'))
    await user.click(screen.getByRole('option', { name: 'default (paper)' }))

    await waitFor(() => {
      expect(useAppStore.getState().currentWalletPublicId).toBe('w-2')
    })
  })
  it('renders gracefully when data is undefined (loading)', () => {
    mockWalletsData = undefined
    render(<WalletPicker />, { wrapper: createWrapper() })

    expect(screen.getByText('All wallets')).toBeDefined()
    mockWalletsData = {
      payload: [
        {
          public_id: 'w-1',
          label: 'default',
          is_paper: false,
          type: 'wallet_info',
          session_id: 's',
          sequence_id: 1,
          timestamp: '2026-01-01T00:00:00Z',
        },
        {
          public_id: 'w-2',
          label: 'default',
          is_paper: true,
          type: 'wallet_info',
          session_id: 's',
          sequence_id: 2,
          timestamp: '2026-01-01T00:00:00Z',
        },
      ],
    }
  })
  it('resets to null when "All wallets" is selected', async () => {
    useAppStore.getState().setCurrentWalletPublicId('w-1')
    const user = userEvent.setup()
    const { waitFor } = await import('@testing-library/react')

    render(<WalletPicker />, { wrapper: createWrapper() })
    await user.click(screen.getByRole('combobox'))
    await user.click(screen.getByRole('option', { name: 'All wallets' }))

    await waitFor(() => {
      expect(useAppStore.getState().currentWalletPublicId).toBeNull()
    })
  })
})
