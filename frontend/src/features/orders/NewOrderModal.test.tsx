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

vi.mock('../../hooks/queries', () => ({
  useExchanges: () => ({
    data: { payload: ['kraken', 'zonda'] },
  }),
  useExchangeInstruments: () => ({
    data: { payload: ['BTC-USD', 'ETH-USD'] },
  }),
  useWallets: () => ({
    data: {
      payload: [
        { public_id: 'wallet-1', label: 'default', is_paper: false },
        { public_id: 'wallet-2', label: 'paper', is_paper: true },
      ],
    },
  }),
  useCreateOrder: () => ({
    mutateAsync: mockMutateAsync,
    isPending: false,
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
})
