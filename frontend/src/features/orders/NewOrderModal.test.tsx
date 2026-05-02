import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor, fireEvent } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { NewOrderModal } from './NewOrderModal'

vi.mock('../../components/ui/Modal', () => ({
  Modal: (props: { open: boolean; onClose: () => void; children: ReactNode }) =>
    props.open ? <div data-testid='modal'>{props.children}</div> : null,
}))

vi.mock('../../components/ThemeSelect', () => ({
  ThemeSelect: ({
    value,
    onChange,
    options,
  }: {
    value: string
    onChange: (v: string) => void
    options: readonly { value: string; label: string }[]
  }) => (
    <select value={value} onChange={e => onChange(e.target.value)}>
      {options.map(opt => (
        <option key={opt.value} value={opt.value}>
          {opt.label}
        </option>
      ))}
    </select>
  ),
}))

const mockMutateAsync = vi.fn()
const mockCreateOrderState = { isPending: false }

type MockInstrumentRow = {
  symbol: string
  can_trade: boolean
}
const mockHookState: {
  exchanges: { payload: string[] } | undefined
  instruments: { payload: MockInstrumentRow[] } | undefined
  wallets: { public_id: string; label: string; is_paper: boolean }[] | undefined
} = {
  exchanges: { payload: ['kraken', 'walutomat'] },
  instruments: {
    payload: [
      { symbol: 'BTC-USD', can_trade: true },
      { symbol: 'ETH-USD', can_trade: true },
    ],
  },
  wallets: [
    { public_id: 'wallet-1', label: 'default', is_paper: false },
    { public_id: 'wallet-2', label: 'paper', is_paper: true },
  ],
}

vi.mock('../../hooks/queries', () => ({
  useExchanges: () => ({
    data: mockHookState.exchanges,
  }),
  useExchangeInstrumentsDetail: () => ({
    data: mockHookState.instruments,
  }),
  useWallets: () => ({
    data: mockHookState.wallets ? { payload: mockHookState.wallets } : undefined,
  }),
  useCreateOrder: () => ({
    mutateAsync: mockMutateAsync,
    isPending: mockCreateOrderState.isPending,
  }),
}))

function createWrapper() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })

  return ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={qc}>{children}</QueryClientProvider>
  )
}

describe('NewOrderModal', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('does not render when closed', () => {
    render(<NewOrderModal open={false} onClose={vi.fn()} />, {
      wrapper: createWrapper(),
    })
    expect(screen.queryByTestId('modal')).toBeNull()
  })

  it('renders form when open', () => {
    render(<NewOrderModal open={true} onClose={vi.fn()} />, {
      wrapper: createWrapper(),
    })
    expect(screen.getByText('New Manual Order')).toBeTruthy()
    expect(screen.getByText('Review Order')).toBeTruthy()
  })

  it('shows validation error when required fields empty', async () => {
    render(<NewOrderModal open={true} onClose={vi.fn()} />, {
      wrapper: createWrapper(),
    })
    await userEvent.click(screen.getByText('Review Order'))
    expect(screen.getByText('All required fields must be filled')).toBeTruthy()
  })

  it('shows confirmation view after filling form', async () => {
    render(<NewOrderModal open={true} onClose={vi.fn()} />, {
      wrapper: createWrapper(),
    })

    const inputs = screen.getAllByPlaceholderText('0.00')

    fireEvent.change(inputs[0], { target: { value: '0.5' } })
    fireEvent.change(inputs[1], { target: { value: '50000' } })
    await userEvent.click(screen.getByText('Review Order'))

    await waitFor(() => {
      expect(screen.getAllByText('Confirm Order').length).toBeGreaterThan(0)
    })
  })

  it('calls createOrder on confirm', async () => {
    mockMutateAsync.mockResolvedValueOnce({})
    const onClose = vi.fn()

    render(<NewOrderModal open={true} onClose={onClose} />, {
      wrapper: createWrapper(),
    })

    const inputs = screen.getAllByPlaceholderText('0.00')

    fireEvent.change(inputs[0], { target: { value: '0.5' } })
    fireEvent.change(inputs[1], { target: { value: '50000' } })
    await userEvent.click(screen.getByText('Review Order'))

    await waitFor(() => {
      expect(screen.getAllByText('Confirm Order').length).toBeGreaterThan(0)
    })

    await userEvent.click(screen.getByRole('button', { name: 'Confirm Order' }))

    await waitFor(() => {
      expect(mockMutateAsync).toHaveBeenCalledTimes(1)
    })
  })

  it('shows error on submit failure', async () => {
    mockMutateAsync.mockRejectedValueOnce(new Error('API error'))

    render(<NewOrderModal open={true} onClose={vi.fn()} />, {
      wrapper: createWrapper(),
    })

    const inputs = screen.getAllByPlaceholderText('0.00')

    fireEvent.change(inputs[0], { target: { value: '0.5' } })
    fireEvent.change(inputs[1], { target: { value: '50000' } })
    await userEvent.click(screen.getByText('Review Order'))

    await waitFor(() => {
      expect(screen.getAllByText('Confirm Order').length).toBeGreaterThan(0)
    })

    await userEvent.click(screen.getByRole('button', { name: 'Confirm Order' }))

    await waitFor(() => {
      expect(screen.getByText('API error')).toBeTruthy()
    })
  })

  it('shows fallback error message when thrown value is not an Error', async () => {
    mockMutateAsync.mockRejectedValueOnce('string failure')

    render(<NewOrderModal open={true} onClose={vi.fn()} />, {
      wrapper: createWrapper(),
    })

    const inputs = screen.getAllByPlaceholderText('0.00')

    fireEvent.change(inputs[0], { target: { value: '0.5' } })
    fireEvent.change(inputs[1], { target: { value: '50000' } })
    await userEvent.click(screen.getByText('Review Order'))
    await waitFor(() => {
      expect(screen.getAllByText('Confirm Order').length).toBeGreaterThan(0)
    })
    await userEvent.click(screen.getByRole('button', { name: 'Confirm Order' }))
    await waitFor(() => {
      expect(screen.getByText('Order creation failed')).toBeTruthy()
    })
  })

  it('goes back from confirmation', async () => {
    render(<NewOrderModal open={true} onClose={vi.fn()} />, {
      wrapper: createWrapper(),
    })

    const inputs = screen.getAllByPlaceholderText('0.00')

    fireEvent.change(inputs[0], { target: { value: '0.5' } })
    fireEvent.change(inputs[1], { target: { value: '50000' } })
    await userEvent.click(screen.getByText('Review Order'))

    await waitFor(() => {
      expect(screen.getAllByText('Confirm Order').length).toBeGreaterThan(0)
    })

    await userEvent.click(screen.getByText('Back'))

    await waitFor(() => {
      expect(screen.getByText('New Manual Order')).toBeTruthy()
    })
  })

  it('shows stop price field for stop order type', () => {
    render(<NewOrderModal open={true} onClose={vi.fn()} />, {
      wrapper: createWrapper(),
    })
    const selects = screen.getAllByRole('combobox')

    fireEvent.change(selects[3], { target: { value: 'stop' } })
    expect(screen.getByText('Stop Price')).toBeTruthy()
  })

  it('shows error for zero quantity', async () => {
    render(<NewOrderModal open={true} onClose={vi.fn()} />, {
      wrapper: createWrapper(),
    })
    const inputs = screen.getAllByPlaceholderText('0.00')

    fireEvent.change(inputs[0], { target: { value: '0' } })
    fireEvent.change(inputs[1], { target: { value: '50000' } })
    await userEvent.click(screen.getByText('Review Order'))
    expect(screen.getByText('Quantity must be a positive number')).toBeTruthy()
  })

  it('shows error for negative price', async () => {
    render(<NewOrderModal open={true} onClose={vi.fn()} />, {
      wrapper: createWrapper(),
    })
    const inputs = screen.getAllByPlaceholderText('0.00')

    fireEvent.change(inputs[0], { target: { value: '1' } })
    fireEvent.change(inputs[1], { target: { value: '-5' } })
    await userEvent.click(screen.getByText('Review Order'))
    expect(screen.getByText('Price must be a positive number')).toBeTruthy()
  })

  it('shows price required error for limit without price', async () => {
    render(<NewOrderModal open={true} onClose={vi.fn()} />, {
      wrapper: createWrapper(),
    })
    const inputs = screen.getAllByPlaceholderText('0.00')

    fireEvent.change(inputs[0], { target: { value: '1' } })
    fireEvent.change(inputs[1], { target: { value: '' } })
    await userEvent.click(screen.getByText('Review Order'))
    expect(screen.getByText('Price is required for this order type')).toBeTruthy()
  })

  it('shows stop price required error for stop without stop_price', async () => {
    render(<NewOrderModal open={true} onClose={vi.fn()} />, {
      wrapper: createWrapper(),
    })
    const selects = screen.getAllByRole('combobox')

    fireEvent.change(selects[3], { target: { value: 'stop' } })
    const inputs = screen.getAllByPlaceholderText('0.00')

    fireEvent.change(inputs[0], { target: { value: '1' } })
    await userEvent.click(screen.getByText('Review Order'))
    expect(screen.getByText('Stop price is required for this order type')).toBeTruthy()
  })

  it('shows stop price positive error for stop with non-positive stop_price', async () => {
    render(<NewOrderModal open={true} onClose={vi.fn()} />, {
      wrapper: createWrapper(),
    })
    const selects = screen.getAllByRole('combobox')

    fireEvent.change(selects[3], { target: { value: 'stop' } })
    const inputs = screen.getAllByPlaceholderText('0.00')

    fireEvent.change(inputs[0], { target: { value: '1' } })
    fireEvent.change(inputs[1], { target: { value: '0' } })
    await userEvent.click(screen.getByText('Review Order'))
    expect(screen.getByText('Stop price must be a positive number')).toBeTruthy()
  })

  it('renders with empty fallback lists when hooks return undefined data', () => {
    const prev = {
      exchanges: mockHookState.exchanges,
      instruments: mockHookState.instruments,
      wallets: mockHookState.wallets,
    }

    mockHookState.exchanges = undefined
    mockHookState.instruments = undefined
    mockHookState.wallets = undefined

    try {
      render(<NewOrderModal open={true} onClose={vi.fn()} />, {
        wrapper: createWrapper(),
      })
      expect(screen.getByTestId('modal')).toBeTruthy()
    } finally {
      mockHookState.exchanges = prev.exchanges
      mockHookState.instruments = prev.instruments
      mockHookState.wallets = prev.wallets
    }
  })

  it('submits stop_limit order with both price and stop_price', async () => {
    mockMutateAsync.mockResolvedValueOnce({})
    render(<NewOrderModal open={true} onClose={vi.fn()} />, {
      wrapper: createWrapper(),
    })
    const selects = screen.getAllByRole('combobox')

    fireEvent.change(selects[3], { target: { value: 'stop_limit' } })
    const inputs = screen.getAllByPlaceholderText('0.00')

    fireEvent.change(inputs[0], { target: { value: '0.5' } })
    fireEvent.change(inputs[1], { target: { value: '50000' } })
    fireEvent.change(inputs[2], { target: { value: '48000' } })
    await userEvent.click(screen.getByText('Review Order'))
    await waitFor(() => {
      expect(screen.getAllByText('Confirm Order').length).toBeGreaterThan(0)
    })
    await userEvent.click(screen.getByRole('button', { name: 'Confirm Order' }))
    await waitFor(() => {
      expect(mockMutateAsync).toHaveBeenCalledTimes(1)
    })
    const body = mockMutateAsync.mock.calls[0][0] as { payload: Record<string, unknown> }

    expect(body.payload.price).toBe(50000)
    expect(body.payload.stop_price).toBe(48000)
  })

  it('submits market order with null price and null stop_price', async () => {
    mockMutateAsync.mockResolvedValueOnce({})
    render(<NewOrderModal open={true} onClose={vi.fn()} />, {
      wrapper: createWrapper(),
    })
    const selects = screen.getAllByRole('combobox')

    fireEvent.change(selects[3], { target: { value: 'market' } })
    const inputs = screen.getAllByPlaceholderText('0.00')

    fireEvent.change(inputs[0], { target: { value: '0.5' } })
    await userEvent.click(screen.getByText('Review Order'))
    await waitFor(() => {
      expect(screen.getAllByText('Confirm Order').length).toBeGreaterThan(0)
    })
    await userEvent.click(screen.getByRole('button', { name: 'Confirm Order' }))
    await waitFor(() => {
      expect(mockMutateAsync).toHaveBeenCalledTimes(1)
    })
    const body = mockMutateAsync.mock.calls[0][0] as { payload: Record<string, unknown> }

    expect(body.payload.price).toBeNull()
    expect(body.payload.stop_price).toBeNull()
  })

  it('shows sell side styling in confirmation view', async () => {
    render(<NewOrderModal open={true} onClose={vi.fn()} />, {
      wrapper: createWrapper(),
    })
    const selects = screen.getAllByRole('combobox')

    fireEvent.change(selects[2], { target: { value: 'sell' } })
    const inputs = screen.getAllByPlaceholderText('0.00')

    fireEvent.change(inputs[0], { target: { value: '0.5' } })
    fireEvent.change(inputs[1], { target: { value: '50000' } })
    await userEvent.click(screen.getByText('Review Order'))
    await waitFor(() => {
      expect(screen.getAllByText('Confirm Order').length).toBeGreaterThan(0)
    })
    expect(screen.getByText('SELL')).toBeTruthy()
  })

  it('shows submitting label while create order mutation is pending', async () => {
    mockCreateOrderState.isPending = true

    try {
      render(<NewOrderModal open={true} onClose={vi.fn()} />, {
        wrapper: createWrapper(),
      })
      const inputs = screen.getAllByPlaceholderText('0.00')

      fireEvent.change(inputs[0], { target: { value: '0.5' } })
      fireEvent.change(inputs[1], { target: { value: '50000' } })
      await userEvent.click(screen.getByText('Review Order'))
      await waitFor(() => {
        expect(screen.getByText('Submitting...')).toBeTruthy()
      })
    } finally {
      mockCreateOrderState.isPending = false
    }
  })

  it('confirmation shows stop price and mode', async () => {
    render(<NewOrderModal open={true} onClose={vi.fn()} />, {
      wrapper: createWrapper(),
    })
    const selects = screen.getAllByRole('combobox')

    fireEvent.change(selects[3], { target: { value: 'stop' } })
    const inputs = screen.getAllByPlaceholderText('0.00')

    fireEvent.change(inputs[0], { target: { value: '1' } })
    fireEvent.change(inputs[1], { target: { value: '48000' } })
    await userEvent.click(screen.getByText('Review Order'))
    await waitFor(() => {
      expect(screen.getAllByText('Confirm Order').length).toBeGreaterThan(0)
    })
    expect(screen.getByText('Stop Price')).toBeTruthy()
    expect(screen.getByText('$48000')).toBeTruthy()
  })

  it('handles instrument change via select', () => {
    render(<NewOrderModal open={true} onClose={vi.fn()} />, {
      wrapper: createWrapper(),
    })
    const selects = screen.getAllByRole('combobox')

    fireEvent.change(selects[1], { target: { value: 'ETH-USD' } })
  })

  it('resets state on close', async () => {
    const onClose = vi.fn()

    render(<NewOrderModal open={true} onClose={onClose} />, {
      wrapper: createWrapper(),
    })
    await userEvent.click(screen.getByText('Cancel'))
    expect(onClose).toHaveBeenCalledTimes(1)
  })

  it('disables submit and shows inline notice when instrument is market-data-only', async () => {
    const prev = mockHookState.instruments

    mockHookState.instruments = {
      payload: [{ symbol: 'MNQM6-CME', can_trade: false }],
    }

    try {
      render(<NewOrderModal open={true} onClose={vi.fn()} />, {
        wrapper: createWrapper(),
      })
      await waitFor(() => {
        const badges = screen.getAllByTestId('market-data-only-badge')

        expect(badges.length).toBeGreaterThan(0)
      })
      expect(
        screen.getByText(
          /This instrument is observation-only — place orders on an execution-capable instrument\./
        )
      ).toBeTruthy()
      const reviewBtn = screen.getByText('Review Order') as HTMLButtonElement

      expect(reviewBtn.disabled).toBe(true)
    } finally {
      mockHookState.instruments = prev
    }
  })

  it('does not show badge or disable submit for tradable instruments', () => {
    render(<NewOrderModal open={true} onClose={vi.fn()} />, {
      wrapper: createWrapper(),
    })
    expect(screen.queryByTestId('market-data-only-badge')).toBeNull()
    const reviewBtn = screen.getByText('Review Order') as HTMLButtonElement

    expect(reviewBtn.disabled).toBe(false)
  })

  it('shows mapped error when APIError carries instrument_market_data_only code', async () => {
    const { APIError } = await import('../../lib/apiClient')
    const rejection = new APIError('raw reason text', 422, 'Unprocessable Entity', {
      error_code: 'instrument_market_data_only',
      symbol: 'MNQM6-CME',
      exchange: 'kraken_equities',
      reason: 'raw reason text',
    })

    mockMutateAsync.mockRejectedValueOnce(rejection)

    render(<NewOrderModal open={true} onClose={vi.fn()} />, {
      wrapper: createWrapper(),
    })

    const inputs = screen.getAllByPlaceholderText('0.00')

    fireEvent.change(inputs[0], { target: { value: '0.5' } })
    fireEvent.change(inputs[1], { target: { value: '50000' } })
    await userEvent.click(screen.getByText('Review Order'))
    await waitFor(() => {
      expect(screen.getAllByText('Confirm Order').length).toBeGreaterThan(0)
    })
    await userEvent.click(screen.getByRole('button', { name: 'Confirm Order' }))
    await waitFor(() => {
      expect(
        screen.getByText(
          /This instrument is observation-only — place orders on an execution-capable instrument\./
        )
      ).toBeTruthy()
    })
  })

  it('falls back to APIError.message when error_code is unknown', async () => {
    const { APIError } = await import('../../lib/apiClient')
    const rejection = new APIError('raw 500 message', 500, 'Internal Server Error', {
      error_code: 'not_in_map',
      reason: 'raw 500 message',
    })

    mockMutateAsync.mockRejectedValueOnce(rejection)

    render(<NewOrderModal open={true} onClose={vi.fn()} />, {
      wrapper: createWrapper(),
    })

    const inputs = screen.getAllByPlaceholderText('0.00')

    fireEvent.change(inputs[0], { target: { value: '0.5' } })
    fireEvent.change(inputs[1], { target: { value: '50000' } })
    await userEvent.click(screen.getByText('Review Order'))
    await waitFor(() => {
      expect(screen.getAllByText('Confirm Order').length).toBeGreaterThan(0)
    })
    await userEvent.click(screen.getByRole('button', { name: 'Confirm Order' }))
    await waitFor(() => {
      expect(screen.getByText('raw 500 message')).toBeTruthy()
    })
  })
})
