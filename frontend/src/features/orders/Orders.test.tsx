import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { Orders } from './Orders'
import type { OrderStatus, Fill } from '../../types/entities'
import { useWebSocketStore } from '../../stores/websocket'

vi.mock('../../hooks/queries', () => ({
  useOrders: vi.fn(() => ({
    data: [],
    isLoading: false,
  })),
  useExecutions: vi.fn(() => ({
    data: [],
    isLoading: false,
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
    useWebSocketStore.setState({ isConnected: false })
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
    const mockOrders: OrderStatus[] = [
      {
        id: 1,
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
    const mockExecutions: Fill[] = [
      {
        id: 1,
        orderId: 1,
        size: 1,
        price: 50000,
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
  it('displays live updates indicator when connected', async () => {
    useWebSocketStore.setState({ isConnected: true })
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText('Live updates via WebSocket')).toBeTruthy()
    })
  })
  it('displays disconnected indicator when not connected', async () => {
    renderWithProviders(<Orders />)
    await waitFor(() => {
      expect(screen.getByText('WebSocket disconnected')).toBeTruthy()
    })
  })
  it('shows order count in tab', async () => {
    const mockOrders: OrderStatus[] = [
      {
        id: 1,
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
    const mockExecutions: Fill[] = [
      {
        id: 1,
        orderId: 1,
        size: 1,
        price: 50000,
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
    const mockOrders: OrderStatus[] = [
      {
        id: 1,
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
    const mockOrders: OrderStatus[] = [
      {
        id: 2,
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
    const mockOrders: OrderStatus[] = [
      {
        id: 3,
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
    const mockOrders: OrderStatus[] = [
      {
        id: 6,
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
    const mockOrders: OrderStatus[] = [
      {
        id: 4,
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
    const mockOrders: OrderStatus[] = [
      {
        id: 5,
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
    const mockExecutions: Fill[] = [
      {
        id: 1,
        orderId: 10,
        size: 1.5,
        price: 50000,
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
    const mockExecutions: Fill[] = [
      {
        id: 2,
        orderId: 20,
        size: 2,
        price: 30000,
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
    const mockOrders: OrderStatus[] = [
      {
        id: 1,
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
        id: 2,
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
    const mockOrders: OrderStatus[] = [
      {
        id: 1,
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
    const mockOrders: OrderStatus[] = [
      {
        id: 1,
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
    const mockOrders: OrderStatus[] = [
      {
        id: 1,
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
        id: 7,
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
    const mockOrders: OrderStatus[] = [
      {
        id: 1,
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
    const mockOrders: OrderStatus[] = [
      {
        id: 8,
        instrument: 'LTC/USD',
        exchange: 'kraken',
        side: null as unknown as OrderStatus['side'],
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
  it('shows N/A when execution executedAt is undefined', async () => {
    const user = userEvent.setup()
    const mockExecutions: Fill[] = [
      {
        id: 100,
        orderId: 200,
        size: 1,
        price: 40000,
        fee: 10,
        feeAsset: 'USD',
        executedAt: undefined,
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
      expect(screen.getByText('N/A')).toBeInTheDocument()
    })
  })
})
