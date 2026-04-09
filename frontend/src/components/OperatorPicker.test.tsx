import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import React from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { OperatorPicker } from './OperatorPicker'
import { useAppStore } from '../stores/app'

let mockOperatorsData: unknown = {
  payload: [
    {
      public_id: 'op-1',
      label: 'alice',
      type: 'operator_info',
      session_id: 's',
      sequence_id: 1,
      timestamp: '2026-01-01T00:00:00Z',
    },
    {
      public_id: 'op-2',
      label: 'bob',
      type: 'operator_info',
      session_id: 's',
      sequence_id: 2,
      timestamp: '2026-01-01T00:00:00Z',
    },
  ],
}

vi.mock('../hooks/queries', () => ({
  useOperators: () => ({
    data: mockOperatorsData,
    isLoading: false,
  }),
}))

const createWrapper = () => {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })

  return ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={qc}>{children}</QueryClientProvider>
  )
}

describe('OperatorPicker', () => {
  beforeEach(() => {
    useAppStore.getState().setCurrentOperatorPublicId(null)
  })
  it('renders with "All operators" selected by default', () => {
    render(<OperatorPicker />, { wrapper: createWrapper() })

    expect(screen.getByText('All operators')).toBeDefined()
  })
  it('selects an operator and updates the store', async () => {
    const user = userEvent.setup()

    render(<OperatorPicker />, { wrapper: createWrapper() })
    await user.click(screen.getByRole('combobox'))
    await user.click(screen.getByRole('option', { name: 'bob' }))

    expect(useAppStore.getState().currentOperatorPublicId).toBe('op-2')
  })
  it('renders gracefully when data is undefined (loading)', () => {
    mockOperatorsData = undefined
    render(<OperatorPicker />, { wrapper: createWrapper() })

    expect(screen.getByText('All operators')).toBeDefined()
    mockOperatorsData = {
      payload: [
        {
          public_id: 'op-1',
          label: 'alice',
          type: 'operator_info',
          session_id: 's',
          sequence_id: 1,
          timestamp: '2026-01-01T00:00:00Z',
        },
        {
          public_id: 'op-2',
          label: 'bob',
          type: 'operator_info',
          session_id: 's',
          sequence_id: 2,
          timestamp: '2026-01-01T00:00:00Z',
        },
      ],
    }
  })
  it('resets to null when "All operators" is selected', async () => {
    useAppStore.getState().setCurrentOperatorPublicId('op-1')
    const user = userEvent.setup()

    render(<OperatorPicker />, { wrapper: createWrapper() })
    await user.click(screen.getByRole('combobox'))
    await user.click(screen.getByRole('option', { name: 'All operators' }))

    expect(useAppStore.getState().currentOperatorPublicId).toBeNull()
  })
})
