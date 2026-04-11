import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { Orders } from './Orders'
import type { Order, Execution } from '../../types/entities'

vi.mock('../../lib/csvExport', () => ({
  exportToCSV: vi.fn(),
}))

vi.mock('./NewOrderModal', () => ({
  NewOrderModal: ({ open, onClose }: { open: boolean; onClose: () => void }) =>
    open ? (
      <button type='button' data-testid='new-order-modal-close' onClick={onClose}>
        close
      </button>
    ) : null,
}))

vi.mock('../../components/ThemeSelect', () => ({
  ThemeSelect: ({
    id,
    value,
    onChange,
    options,
    placeholder,
    className,
    disabled,
  }: {
    id?: string
    value: string
    onChange: (v: string) => void
    options: readonly { value: string; label: string }[]
    placeholder?: string
    className?: string
    disabled?: boolean
  }) => (
    <select
      id={id}
      value={value}
      onChange={e => onChange(e.target.value)}
      className={className}
      disabled={disabled}
    >
      {placeholder && <option value=''>{placeholder}</option>}
      {options.map(opt => (
        <option key={opt.value} value={opt.value}>
          {opt.label}
        </option>
      ))}
    </select>
  ),
}))

const cancelMutate = vi.fn()

vi.mock('../../hooks/queries', () => ({
  useOrders: vi.fn(() => ({
    data: [],
    isLoading: false,
  })),
  useExecutions: vi.fn(() => ({
    data: [],
    isLoading: false,
  })),
  useCancelOrder: vi.fn(() => ({
    mutate: cancelMutate,
    isPending: false,
  })),
}))
const createQueryClient = () =>
  new QueryClient({
    defaultOptions: {
      queries: { retry: false },
    },
  })

const renderWithProviders = (ui: ReactNode) => {
  const queryClient = createQueryClient()

  return render(<QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>)
}

describe('Orders', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })
  it('renders orders page', () => {
    renderWithProviders(<Orders />)
    expect(screen.getByText(/Orders & Executions/i)).toBeInTheDocument()
  })
  it('displays orders tab', async () => {
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getAllByText(/Orders/i).length).toBeGreaterThan(0)
    })
  })
  it('displays executions tab', async () => {
    renderWithProviders(<Orders />)
    await waitFor(() => {
      const elements = screen.getAllByText(/Executions/i)

      expect(elements[0]).toBeInTheDocument()
    })
  })
  it('shows empty state when no orders', async () => {
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText(/Orders & Executions/i)).toBeInTheDocument()
    })
  })
  it('handles undefined data from hooks', async () => {
    const { useOrders, useExecutions } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: undefined,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    vi.mocked(useExecutions).mockReturnValue({
      data: undefined,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText(/Orders & Executions/i)).toBeInTheDocument()
    })
  })
  it('displays orders when data is loaded', async () => {
    const mockOrders: Order[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: '1',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        orderType: 'limit',
        size: 1,
        filledSize: 0,
        price: 50000,
        status: 'open',
        createdAt: new Date('2024-01-01T00:00:00Z'),
        updatedAt: new Date('2024-01-01T00:00:00Z'),
      },
    ]
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText(/Orders & Executions/i)).toBeTruthy()
    })
  })
  it('displays loading state for orders', async () => {
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: [],
      isLoading: true,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getAllByTestId('order-card-skeleton').length).toBeGreaterThan(0)
    })
  })
  it('displays executions when data is loaded', async () => {
    const mockExecutions: Execution[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: '1',
        size: 1,
        price: 50000,
        lastSize: 1,
        lastPrice: 50000,
        fee: 25,
        feeAsset: 'USD',
        executedAt: new Date('2024-01-01T00:00:00Z'),
        instrument: 'BTC/USD',
        side: 'buy',
        exchange: 'kraken',
        status: 'filled',
      },
    ]
    const { useExecutions } = await import('../../hooks/queries')

    vi.mocked(useExecutions).mockReturnValue({
      data: mockExecutions,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText(/Orders & Executions/i)).toBeTruthy()
    })
  })
  it('shows status filter dropdown', async () => {
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText('Filter by status:')).toBeTruthy()
    })
  })
  it('shows order count in tab', async () => {
    const mockOrders: Order[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: '1',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        orderType: 'limit',
        size: 1,
        filledSize: 0,
        price: 50000,
        status: 'open',
        createdAt: new Date('2024-01-01T00:00:00Z'),
        updatedAt: new Date('2024-01-01T00:00:00Z'),
      },
    ]
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText(/Orders \(1\)/i)).toBeTruthy()
    })
  })
  it('shows execution count in tab', async () => {
    const mockExecutions: Execution[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: '1',
        size: 1,
        price: 50000,
        lastSize: 1,
        lastPrice: 50000,
        fee: 25,
        feeAsset: 'USD',
        executedAt: new Date('2024-01-01T00:00:00Z'),
        instrument: 'BTC/USD',
        side: 'sell',
        exchange: 'kraken',
        status: 'filled',
      },
    ]
    const { useExecutions } = await import('../../hooks/queries')

    vi.mocked(useExecutions).mockReturnValue({
      data: mockExecutions,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText(/Executions \(1\)/i)).toBeTruthy()
    })
  })
  it('displays loading state for executions', async () => {
    const { useExecutions } = await import('../../hooks/queries')

    vi.mocked(useExecutions).mockReturnValue({
      data: [],
      isLoading: true,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText(/Orders & Executions/i)).toBeTruthy()
    })
  })
  it('shows executions loading state when executions tab is active', async () => {
    const user = userEvent.setup()
    const { useExecutions } = await import('../../hooks/queries')

    vi.mocked(useExecutions).mockReturnValue({
      data: [],
      isLoading: true,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    const executionsTab = screen.getByRole('button', { name: /Executions/i })

    await user.click(executionsTab)
    await waitFor(() => {
      expect(screen.getAllByTestId('order-card-skeleton').length).toBeGreaterThan(0)
    })
  })
  it('shows empty state for executions', async () => {
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText(/Orders & Executions/i)).toBeTruthy()
    })
  })
  it('shows executions empty state when no executions', async () => {
    const user = userEvent.setup()
    const { useOrders, useExecutions } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: [],
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    vi.mocked(useExecutions).mockReturnValue({
      data: [],
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    const executionsTab = screen.getByRole('button', { name: /Executions/i })

    await user.click(executionsTab)
    await waitFor(() => {
      expect(screen.getByText(/No executions found/i)).toBeInTheDocument()
    })
  })
  it('renders status filter options', async () => {
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText('All Orders')).toBeTruthy()
    })
  })
  it('displays order card with buy side', async () => {
    const mockOrders: Order[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: '1',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        orderType: 'limit',
        size: 1.5,
        filledSize: 0,
        price: 50000,
        status: 'open',
        createdAt: new Date('2024-01-01T00:00:00Z'),
        updatedAt: new Date('2024-01-01T00:00:00Z'),
      },
    ]
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText('BTC/USD')).toBeInTheDocument()
      expect(screen.getByText('BUY')).toBeInTheDocument()
      expect(screen.getByText('$50000.00')).toBeInTheDocument()
    })
  })
  it('displays order card with sell side', async () => {
    const mockOrders: Order[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: '2',
        instrument: 'ETH/USD',
        exchange: 'kraken',
        side: 'sell',
        orderType: 'market',
        size: 2,
        filledSize: 0,
        price: null,
        status: 'filled',
        createdAt: new Date('2024-01-01T00:00:00Z'),
        updatedAt: new Date('2024-01-01T00:00:00Z'),
      },
    ]
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText('SELL')).toBeInTheDocument()
      expect(screen.getByText('Market')).toBeInTheDocument()
      expect(screen.getByText('filled')).toBeInTheDocument()
    })
  })
  it('displays order with different statuses', async () => {
    const mockOrders: Order[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: '3',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        orderType: 'limit',
        size: 1,
        filledSize: 0,
        price: 45000,
        status: 'cancelled',
        createdAt: new Date('2024-01-01T00:00:00Z'),
        updatedAt: null,
      },
    ]
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText('cancelled')).toBeInTheDocument()
    })
  })
  it('displays order with new status', async () => {
    const mockOrders: Order[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: '6',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        orderType: 'limit',
        size: 1,
        filledSize: 0,
        price: 45000,
        status: 'new',
        createdAt: new Date('2024-01-01T00:00:00Z'),
        updatedAt: null,
      },
    ]
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText('new')).toBeInTheDocument()
    })
  })
  it('displays rejected order status', async () => {
    const mockOrders: Order[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: '4',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        orderType: 'limit',
        size: 1,
        filledSize: 0,
        price: 45000,
        status: 'rejected',
        createdAt: new Date('2024-01-01T00:00:00Z'),
        updatedAt: null,
      },
    ]
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText('rejected')).toBeInTheDocument()
      expect(screen.getByText('BTC/USD')).toBeInTheDocument()
    })
  })
  it('displays partially_filled order status', async () => {
    const mockOrders: Order[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: '5',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        orderType: 'limit',
        size: 1,
        filledSize: 0,
        price: 45000,
        status: 'partially_filled',
        createdAt: new Date('2024-01-01T00:00:00Z'),
        updatedAt: null,
      },
    ]
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText('partially_filled')).toBeInTheDocument()
    })
  })
  it('switches to executions tab and displays execution card', async () => {
    const user = userEvent.setup()
    const mockExecutions: Execution[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: '10',
        size: 1.5,
        price: 50000,
        lastSize: 1.5,
        lastPrice: 50000,
        fee: 25,
        feeAsset: 'USD',
        executedAt: new Date('2024-01-01T12:00:00Z'),
        instrument: 'BTC/USD',
        side: 'buy',
        exchange: 'kraken',
        status: 'filled',
      },
    ]
    const { useExecutions } = await import('../../hooks/queries')

    vi.mocked(useExecutions).mockReturnValue({
      data: mockExecutions,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    const executionsTab = screen.getByRole('button', { name: /Executions/i })

    await user.click(executionsTab)
    await waitFor(() => {
      expect(screen.getByText('Order #10')).toBeInTheDocument()
      expect(screen.getByText('$50000.00')).toBeInTheDocument()
      expect(screen.getByText('$75000.00')).toBeInTheDocument()
      expect(screen.getByText('$25.00 USD')).toBeInTheDocument()
    })
  })
  it('switches back to orders tab', async () => {
    const user = userEvent.setup()

    renderWithProviders(<Orders />)
    const executionsTab = screen.getByRole('button', { name: /Executions/i })

    await user.click(executionsTab)
    const ordersTab = screen.getByRole('button', { name: /Orders/i })

    await user.click(ordersTab)
    await waitFor(() => {
      expect(screen.getByText('Filter by status:')).toBeInTheDocument()
    })
  })
  it('displays execution card without fees', async () => {
    const user = userEvent.setup()
    const mockExecutions: Execution[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: '20',
        size: 2,
        price: 30000,
        lastSize: 2,
        lastPrice: 30000,
        fee: 0,
        feeAsset: '',
        executedAt: new Date('2024-01-02T12:00:00Z'),
        instrument: 'ETH/USD',
        side: 'sell',
        exchange: 'kraken',
        status: 'filled',
      },
    ]
    const { useExecutions } = await import('../../hooks/queries')

    vi.mocked(useExecutions).mockReturnValue({
      data: mockExecutions,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    const executionsTab = screen.getByRole('button', { name: /Executions/i })

    await user.click(executionsTab)
    await waitFor(() => {
      expect(screen.getByText('Order #20')).toBeInTheDocument()
      expect(screen.queryByText(/\$0\.00/)).not.toBeInTheDocument()
    })
  })
  it('filters orders by status', async () => {
    const user = userEvent.setup()
    const mockOrders: Order[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: '1',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        orderType: 'limit',
        size: 1,
        filledSize: 0,
        price: 50000,
        status: 'open',
        createdAt: new Date('2024-01-01T00:00:00Z'),
        updatedAt: null,
      },
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: '2',
        instrument: 'ETH/USD',
        exchange: 'kraken',
        side: 'sell',
        orderType: 'market',
        size: 2,
        filledSize: 0,
        price: null,
        status: 'filled',
        createdAt: new Date('2024-01-01T00:00:00Z'),
        updatedAt: null,
      },
    ]
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText('BTC/USD')).toBeInTheDocument()
      expect(screen.getByText('ETH/USD')).toBeInTheDocument()
    })
    const statusSelect = screen.getByRole('combobox')

    await user.selectOptions(statusSelect, 'filled')
    await waitFor(() => {
      expect(screen.queryByText('BTC/USD')).not.toBeInTheDocument()
      expect(screen.getByText('ETH/USD')).toBeInTheDocument()
    })
  })
  it('shows filtered empty state for orders', async () => {
    const user = userEvent.setup()
    const { useOrders, useExecutions } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: [],
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    vi.mocked(useExecutions).mockReturnValue({
      data: [],
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    const statusSelect = screen.getByRole('combobox')

    await user.selectOptions(statusSelect, 'filled')
    await waitFor(() => {
      expect(screen.getByText('No filled orders')).toBeInTheDocument()
    })
  })
  it('displays order with rejected status', async () => {
    const mockOrders: Order[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: '1',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        orderType: 'limit',
        size: 1,
        filledSize: 0,
        price: 50000,
        status: 'rejected',
        createdAt: new Date('2024-01-01T00:00:00Z'),
        updatedAt: new Date('2024-01-01T00:00:00Z'),
      },
    ]
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText('BTC/USD')).toBeInTheDocument()
      expect(screen.getByText('rejected')).toBeInTheDocument()
    })
  })
  it('displays order with error status', async () => {
    const mockOrders: Order[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: '1',
        instrument: 'ETH/USD',
        exchange: 'kraken',
        side: 'sell',
        orderType: 'market',
        size: 2,
        filledSize: 0,
        price: null,
        status: 'rejected',
        createdAt: new Date('2024-01-01T00:00:00Z'),
        updatedAt: new Date('2024-01-01T00:00:00Z'),
      },
    ]
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText('ETH/USD')).toBeInTheDocument()
      expect(screen.getByText('rejected')).toBeInTheDocument()
    })
  })
  it('displays order with partially_filled status', async () => {
    const mockOrders: Order[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: '1',
        instrument: 'SOL/USD',
        exchange: 'kraken',
        side: 'buy',
        orderType: 'limit',
        size: 10,
        filledSize: 0,
        price: 100,
        status: 'partially_filled',
        createdAt: new Date('2024-01-01T00:00:00Z'),
        updatedAt: new Date('2024-01-01T00:00:00Z'),
      },
    ]
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText('SOL/USD')).toBeInTheDocument()
      expect(screen.getByText('partially_filled')).toBeInTheDocument()
    })
  })
  it('shows N/A when order created_at is missing', async () => {
    const mockOrders = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: '7',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy' as const,
        orderType: 'limit' as const,
        size: 1,
        filledSize: 0,
        price: 45000,
        status: 'open' as const,
        createdAt: null as unknown as Date,
        updatedAt: null,
      },
    ]
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText('N/A')).toBeInTheDocument()
    })
  })
  it('displays order with unknown status using default styling', async () => {
    const mockOrders: Order[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: '1',
        instrument: 'DOGE/USD',
        exchange: 'kraken',
        side: 'buy',
        orderType: 'limit',
        size: 100,
        filledSize: 0,
        price: 0.1,
        status: 'unknown_status',
        createdAt: new Date('2024-01-01T00:00:00Z'),
        updatedAt: null,
      },
    ]
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText('DOGE/USD')).toBeInTheDocument()
      expect(screen.getByText('unknown_status')).toBeInTheDocument()
    })
  })
  it('shows N/A when order side is null', async () => {
    const mockOrders: Order[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: '8',
        instrument: 'LTC/USD',
        exchange: 'kraken',
        side: null as unknown as Order['side'],
        orderType: 'market',
        size: 5,
        filledSize: 0,
        price: null,
        status: 'open',
        createdAt: new Date('2024-01-01T00:00:00Z'),
        updatedAt: null,
      },
    ]
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText('LTC/USD')).toBeInTheDocument()
      expect(screen.getByText('N/A')).toBeInTheDocument()
    })
  })
  it('shows formatted date when execution executedAt is present', async () => {
    const user = userEvent.setup()
    const executedDate = new Date('2024-01-01T00:00:00Z')
    const mockExecutions: Execution[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: '200',
        size: 1,
        price: 40000,
        lastSize: 1,
        lastPrice: 40000,
        fee: 10,
        feeAsset: 'USD',
        executedAt: executedDate,
        instrument: 'BTC/USD',
        side: 'buy',
        exchange: 'kraken',
        status: 'filled',
      },
    ]
    const { useExecutions } = await import('../../hooks/queries')

    vi.mocked(useExecutions).mockReturnValue({
      data: mockExecutions,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    const executionsTab = screen.getByRole('button', { name: /Executions/i })

    await user.click(executionsTab)
    await waitFor(() => {
      expect(screen.getByText('Order #200')).toBeInTheDocument()
      expect(screen.getByText(executedDate.toLocaleString())).toBeInTheDocument()
    })
  })
  it('exports orders to CSV when export button clicked', async () => {
    const { exportToCSV } = await import('../../lib/csvExport')
    const user = userEvent.setup()
    const mockOrders: Order[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: '1',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        orderType: 'limit',
        size: 1.5,
        filledSize: 0,
        price: 50000,
        status: 'open',
        createdAt: new Date('2024-01-01T00:00:00Z'),
        updatedAt: null,
      },
    ]
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText('BTC/USD')).toBeInTheDocument()
    })
    const exportButton = screen.getByRole('button', { name: /Export CSV/i })

    await user.click(exportButton)
    expect(exportToCSV).toHaveBeenCalledWith(
      'orders.csv',
      [
        'Instrument',
        'Side',
        'Type',
        'Status',
        'Quantity',
        'Price',
        'Leverage',
        'Reduce Only',
        'Created',
      ],
      expect.arrayContaining([expect.arrayContaining(['BTC/USD', 'buy', 'limit', 'open'])])
    )
  })
  it('exports executions to CSV when export button clicked on executions tab', async () => {
    const { exportToCSV } = await import('../../lib/csvExport')
    const user = userEvent.setup()
    const mockExecutions: Execution[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: 'client-10',
        size: 1.5,
        price: 50000,
        lastSize: 1.5,
        lastPrice: 50000,
        fee: 25,
        feeAsset: 'USD',
        executedAt: new Date('2024-01-01T12:00:00Z'),
        instrument: 'BTC/USD',
        side: 'buy',
        exchange: 'kraken',
        status: 'filled',
      },
    ]
    const { useExecutions } = await import('../../hooks/queries')

    vi.mocked(useExecutions).mockReturnValue({
      data: mockExecutions,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    const executionsTab = screen.getByRole('button', { name: /Executions/i })

    await user.click(executionsTab)
    await waitFor(() => {
      expect(screen.getByText('Order #client-10')).toBeInTheDocument()
    })
    const exportButton = screen.getByRole('button', { name: /Export CSV/i })

    await user.click(exportButton)
    expect(exportToCSV).toHaveBeenCalledWith(
      'executions.csv',
      ['Order ID', 'Instrument', 'Side', 'Size', 'Price', 'Total', 'Fee', 'Fee Asset', 'Executed'],
      expect.arrayContaining([expect.arrayContaining(['client-10', 'BTC/USD', 'buy'])])
    )
  })
  it('exports orders with null fields using fallback values', async () => {
    const { exportToCSV } = await import('../../lib/csvExport')
    const user = userEvent.setup()
    const mockOrders: Order[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: '9',
        instrument: 'ETH/USD',
        exchange: 'kraken',
        side: null as unknown as Order['side'],
        orderType: 'market',
        size: 2,
        filledSize: 0,
        price: null,
        status: 'open',
        createdAt: null as unknown as Date,
        updatedAt: null,
      },
    ]
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText('ETH/USD')).toBeInTheDocument()
    })
    const exportButton = screen.getByRole('button', { name: /Export CSV/i })

    await user.click(exportButton)
    expect(exportToCSV).toHaveBeenCalledWith(
      'orders.csv',
      [
        'Instrument',
        'Side',
        'Type',
        'Status',
        'Quantity',
        'Price',
        'Leverage',
        'Reduce Only',
        'Created',
      ],
      [['ETH/USD', '', 'market', 'open', '2.0000', 'Market', '', 'false', '']]
    )
  })
  it('exports executions with zero fee and null feeAsset', async () => {
    const { exportToCSV } = await import('../../lib/csvExport')
    const user = userEvent.setup()
    const executedDate = new Date('2024-01-01T00:00:00Z')
    const mockExecutions: Execution[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: 'client-20',
        size: 1,
        price: 30000,
        lastSize: 1,
        lastPrice: 30000,
        fee: 0,
        feeAsset: null as unknown as string,
        executedAt: executedDate,
        instrument: 'BTC/USD',
        side: 'sell',
        exchange: 'kraken',
        status: 'filled',
      },
    ]
    const { useExecutions } = await import('../../hooks/queries')

    vi.mocked(useExecutions).mockReturnValue({
      data: mockExecutions,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    const executionsTab = screen.getByRole('button', { name: /Executions/i })

    await user.click(executionsTab)
    await waitFor(() => {
      expect(screen.getByText('Order #client-20')).toBeInTheDocument()
    })
    const exportButton = screen.getByRole('button', { name: /Export CSV/i })

    await user.click(exportButton)
    expect(exportToCSV).toHaveBeenCalledWith(
      'executions.csv',
      ['Order ID', 'Instrument', 'Side', 'Size', 'Price', 'Total', 'Fee', 'Fee Asset', 'Executed'],
      [
        [
          'client-20',
          'BTC/USD',
          'sell',
          '1.0000',
          '30000.00',
          '30000.00',
          '0',
          '',
          executedDate.toLocaleString(),
        ],
      ]
    )
  })
  it('renders leverage badge when order.leverage is set', async () => {
    const mockOrders: Order[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: 'lev-1',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'sell',
        orderType: 'limit',
        size: 1,
        filledSize: 0,
        price: 50000,
        status: 'open',
        createdAt: new Date('2024-01-01T00:00:00Z'),
        updatedAt: null,
        leverage: 3,
        reduceOnly: false,
      },
    ]
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByTestId('order-leverage-lev-1')).toHaveTextContent('3x')
    })
    expect(screen.queryByTestId('order-reduce-only-lev-1')).not.toBeInTheDocument()
  })
  it('renders reduce-only badge when order.reduceOnly is true', async () => {
    const mockOrders: Order[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: 'ro-1',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        orderType: 'limit',
        size: 1,
        filledSize: 0,
        price: 50000,
        status: 'open',
        createdAt: new Date('2024-01-01T00:00:00Z'),
        updatedAt: null,
        leverage: 5,
        reduceOnly: true,
      },
    ]
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByTestId('order-leverage-ro-1')).toHaveTextContent('5x')
      expect(screen.getByTestId('order-reduce-only-ro-1')).toHaveTextContent('REDUCE-ONLY')
    })
  })
  it('omits leverage and reduce-only badges for spot orders', async () => {
    const mockOrders: Order[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: 'spot-1',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        orderType: 'limit',
        size: 1,
        filledSize: 0,
        price: 50000,
        status: 'open',
        createdAt: new Date('2024-01-01T00:00:00Z'),
        updatedAt: null,
      },
    ]
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText('BTC/USD')).toBeInTheDocument()
    })
    expect(screen.queryByTestId('order-leverage-spot-1')).not.toBeInTheDocument()
    expect(screen.queryByTestId('order-reduce-only-spot-1')).not.toBeInTheDocument()
  })
  it('exports orders CSV with leverage and reduce_only columns', async () => {
    const { exportToCSV } = await import('../../lib/csvExport')
    const user = userEvent.setup()
    const created = new Date('2024-01-01T00:00:00Z')
    const mockOrders: Order[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: created,
        sessionId: 'test-sid',
        clientOrderId: 'lev-csv',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'sell',
        orderType: 'limit',
        size: 1.5,
        filledSize: 0,
        price: 50000,
        status: 'open',
        createdAt: created,
        updatedAt: null,
        leverage: 4,
        reduceOnly: true,
      },
    ]
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText('BTC/USD')).toBeInTheDocument()
    })
    const exportButton = screen.getByRole('button', { name: /Export CSV/i })

    await user.click(exportButton)
    expect(exportToCSV).toHaveBeenCalledWith(
      'orders.csv',
      [
        'Instrument',
        'Side',
        'Type',
        'Status',
        'Quantity',
        'Price',
        'Leverage',
        'Reduce Only',
        'Created',
      ],
      [
        [
          'BTC/USD',
          'sell',
          'limit',
          'open',
          '1.5000',
          '50000.00',
          '4',
          'true',
          created.toLocaleString(),
        ],
      ]
    )
  })

  it('shows New Order button and opens modal on click', async () => {
    renderWithProviders(<Orders />)
    const button = screen.getByText('New Order')

    expect(button).toBeInTheDocument()
    await userEvent.click(button)
    const closeBtn = await screen.findByTestId('new-order-modal-close')

    await userEvent.click(closeBtn)
    await waitFor(() => {
      expect(screen.queryByTestId('new-order-modal-close')).not.toBeInTheDocument()
    })
  })

  it('renders cancel button on non-terminal order and invokes mutation', async () => {
    const mockOrders: Order[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: 'cid-open',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        orderType: 'limit',
        size: 1,
        filledSize: 0,
        price: 50000,
        status: 'open',
        createdAt: new Date('2024-01-01T00:00:00Z'),
        updatedAt: null,
      },
    ]
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    const cancelBtn = await screen.findByTestId('cancel-order-cid-open')

    await userEvent.click(cancelBtn)
    expect(cancelMutate).toHaveBeenCalledWith('cid-open')
  })

  it('hides cancel button on terminal order statuses', async () => {
    const mockOrders: Order[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: 'cid-filled',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        orderType: 'limit',
        size: 1,
        filledSize: 1,
        price: 50000,
        status: 'filled',
        createdAt: new Date('2024-01-01T00:00:00Z'),
        updatedAt: null,
      },
    ]
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText('filled')).toBeInTheDocument()
    })
    expect(screen.queryByTestId('cancel-order-cid-filled')).not.toBeInTheDocument()
  })

  it('hides cancel button on American-spelled canceled status', async () => {
    const mockOrders: Order[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: 'cid-canceled',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        orderType: 'limit',
        size: 1,
        filledSize: 0,
        price: 50000,
        status: 'canceled',
        createdAt: new Date('2024-01-01T00:00:00Z'),
        updatedAt: null,
      },
    ]
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText('canceled')).toBeInTheDocument()
    })
    expect(screen.queryByTestId('cancel-order-cid-canceled')).not.toBeInTheDocument()
  })

  it('hides cancel button on closed status (venue spelling)', async () => {
    const mockOrders: Order[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: 'cid-closed',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        orderType: 'limit',
        size: 1,
        filledSize: 1,
        price: 50000,
        status: 'closed',
        createdAt: new Date('2024-01-01T00:00:00Z'),
        updatedAt: null,
      },
    ]
    const { useOrders } = await import('../../hooks/queries')

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Orders />)
    expect(screen.queryByTestId('cancel-order-cid-closed')).not.toBeInTheDocument()
  })

  it('cancel button is disabled while the mutation is pending', async () => {
    const { useOrders, useCancelOrder } = await import('../../hooks/queries')
    const mockOrders: Order[] = [
      {
        sequenceId: 0,
        publicId: 'test-pid',
        timestamp: new Date('2024-01-01T00:00:00Z'),
        sessionId: 'test-sid',
        clientOrderId: 'cid-pending',
        instrument: 'BTC/USD',
        exchange: 'kraken',
        side: 'buy',
        orderType: 'limit',
        size: 1,
        filledSize: 0,
        price: 50000,
        status: 'open',
        createdAt: new Date('2024-01-01T00:00:00Z'),
        updatedAt: null,
      },
    ]

    vi.mocked(useOrders).mockReturnValue({
      data: mockOrders,
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    vi.mocked(useCancelOrder).mockReturnValue({
      mutate: cancelMutate,
      isPending: true,
    } as never)
    renderWithProviders(<Orders />)
    const cancelBtn = await screen.findByTestId('cancel-order-cid-pending')

    expect(cancelBtn).toBeDisabled()
    expect(cancelBtn).toHaveTextContent(/Cancelling/i)
  })
})
