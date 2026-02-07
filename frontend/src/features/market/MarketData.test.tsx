import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { MarketData } from './MarketData'

const mockSetSelectedExchange = vi.fn()
const mockSetSelectedInstrument = vi.fn()
const mockSetSelectedTimeframe = vi.fn()

vi.mock('../../hooks/queries', () => ({
  useCandles: vi.fn(() => ({
    data: [],
    isLoading: false,
    error: null,
    isFetching: false,
  })),
  useExchanges: vi.fn(() => ({
    data: ['kraken', 'binance'],
    isLoading: false,
    error: null,
  })),
  useExchangeInstruments: vi.fn(() => ({
    data: ['EUR-USD', 'GBP-USD', 'BTC-USD'],
    isLoading: false,
    error: null,
  })),
}))
vi.mock('../../stores/market', () => ({
  useMarketStore: vi.fn(() => ({
    selectedExchange: 'kraken',
    selectedInstrument: 'EUR-USD',
    selectedTimeframe: '1h',
    setSelectedExchange: mockSetSelectedExchange,
    setSelectedInstrument: mockSetSelectedInstrument,
    setSelectedTimeframe: mockSetSelectedTimeframe,
  })),
}))
vi.mock('../../stores/app', () => ({
  useAppStore: vi.fn(() => ({
    isConnected: true,
  })),
}))
vi.mock('../../components/LightweightChart', () => ({
  LightweightChart: ({ data }: { data: unknown[] }) => (
    <div data-testid='lightweight-chart'>Chart with {data.length} candles</div>
  ),
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

describe('MarketData', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })
  it('renders market data component', () => {
    renderWithProviders(<MarketData />)
    expect(screen.getByText(/Market Data/i)).toBeInTheDocument()
  })
  it('displays instrument selector', async () => {
    renderWithProviders(<MarketData />)
    await waitFor(() => {
      expect(document.querySelector('select, [role="combobox"]')).toBeInTheDocument()
    })
  })
  it('displays timeframe selector', async () => {
    renderWithProviders(<MarketData />)
    await waitFor(() => {
      const selectors = document.querySelectorAll('select, [role="combobox"]')

      expect(selectors.length).toBeGreaterThan(0)
    })
  })
  it('shows connected status when connected', async () => {
    renderWithProviders(<MarketData />)
    expect(screen.getByText('connected')).toBeInTheDocument()
  })
  it('shows disconnected status when not connected', async () => {
    const { useAppStore } = await import('../../stores/app')

    vi.mocked(useAppStore).mockReturnValueOnce({ isConnected: false })
    renderWithProviders(<MarketData />)
    expect(screen.getByText('disconnected')).toBeInTheDocument()
  })
  it('displays no data message when candles are empty', async () => {
    renderWithProviders(<MarketData />)
    expect(screen.queryByText(/Current Price/)).not.toBeInTheDocument()
  })
  it('shows loading state while fetching candles', async () => {
    const { useCandles } = await import('../../hooks/queries')

    vi.mocked(useCandles).mockReturnValue({
      data: undefined,
      isLoading: true,
      error: null,
      isFetching: true,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<MarketData />)
    expect(screen.queryByText(/Current Price/)).not.toBeInTheDocument()
  })
  it('displays exchange dropdown with options', async () => {
    const user = userEvent.setup()

    renderWithProviders(<MarketData />)
    const trigger = screen.getAllByRole('combobox')[0]

    await user.click(trigger)
    await waitFor(() => {
      expect(screen.getAllByText('kraken').length).toBeGreaterThanOrEqual(2)
    })
  })
  it('displays instrument dropdown with options', async () => {
    const user = userEvent.setup()

    renderWithProviders(<MarketData />)
    const trigger = screen.getAllByRole('combobox')[1]

    await user.click(trigger)
    await waitFor(() => {
      expect(screen.getAllByText('EUR-USD').length).toBeGreaterThanOrEqual(2)
    })
  })
  it('displays timeframe dropdown with options', async () => {
    const user = userEvent.setup()

    renderWithProviders(<MarketData />)
    const triggers = screen.getAllByRole('combobox')

    await user.click(triggers[2])
    await waitFor(() => {
      expect(screen.getAllByText('1 Hour').length).toBeGreaterThanOrEqual(2)
    })
  })
  it('displays refresh button', () => {
    renderWithProviders(<MarketData />)
    expect(screen.getByRole('button', { name: /Refresh/i })).toBeInTheDocument()
  })
  it('calls refetch when refresh is clicked', async () => {
    const user = userEvent.setup()
    const refetch = vi.fn()
    const { useCandles } = await import('../../hooks/queries')

    vi.mocked(useCandles).mockReturnValue({
      data: [],
      isLoading: false,
      error: null,
      isFetching: false,
      refetch,
    } as never)
    renderWithProviders(<MarketData />)
    await user.click(screen.getByRole('button', { name: /Refresh/i }))
    expect(refetch).toHaveBeenCalled()
  })
  it('passes empty strings when no exchange or instrument selected', async () => {
    const { useMarketStore } = await import('../../stores/market')
    const { useCandles } = await import('../../hooks/queries')

    vi.mocked(useMarketStore).mockReturnValueOnce({
      selectedExchange: null,
      selectedInstrument: null,
      selectedTimeframe: '1h',
      setSelectedExchange: mockSetSelectedExchange,
      setSelectedInstrument: mockSetSelectedInstrument,
      setSelectedTimeframe: mockSetSelectedTimeframe,
    })
    renderWithProviders(<MarketData />)
    expect(useCandles).toHaveBeenCalledWith('', '', '1h')
  })
  it('shows unknown error message when error has no message', async () => {
    const { useCandles } = await import('../../hooks/queries')

    vi.mocked(useCandles).mockReturnValue({
      data: [],
      isLoading: false,
      error: new Error(''),
      isFetching: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<MarketData />)
    await waitFor(() => {
      expect(screen.getByText(/Unknown error/i)).toBeInTheDocument()
    })
  })
  it('displays stats when candles data is available', async () => {
    const mockCandles = [
      { timestamp: '2024-01-01T00:00:00Z', open: 1.08, high: 1.085, low: 1.079, close: 1.082 },
      { timestamp: '2024-01-01T01:00:00Z', open: 1.082, high: 1.086, low: 1.081, close: 1.085 },
    ]
    const { useCandles } = await import('../../hooks/queries')

    vi.mocked(useCandles).mockReturnValue({
      data: mockCandles,
      isLoading: false,
      error: null,
      isFetching: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<MarketData />)
    await waitFor(() => {
      expect(screen.getAllByText('Current Price').length).toBeGreaterThan(0)
      expect(screen.getAllByText('24h Change').length).toBeGreaterThan(0)
      expect(screen.getAllByText('24h High').length).toBeGreaterThan(0)
      expect(screen.getAllByText('24h Low').length).toBeGreaterThan(0)
    })
  })
  it('displays chart when candles data is available', async () => {
    const mockCandles = [
      { timestamp: '2024-01-01T00:00:00Z', open: 1.08, high: 1.085, low: 1.079, close: 1.082 },
      { timestamp: '2024-01-01T01:00:00Z', open: 1.082, high: 1.086, low: 1.081, close: 1.085 },
    ]
    const { useCandles } = await import('../../hooks/queries')

    vi.mocked(useCandles).mockReturnValue({
      data: mockCandles,
      isLoading: false,
      error: null,
      isFetching: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<MarketData />)
    await waitFor(() => {
      expect(screen.getByText('Price Chart')).toBeInTheDocument()
    })
  })
  it('handles duplicate timestamps by keeping the last one', async () => {
    const mockCandles = [
      { timestamp: '2024-01-01T00:00:00Z', open: 1.08, high: 1.085, low: 1.079, close: 1.082 },
      { timestamp: '2024-01-01T00:00:00Z', open: 1.0825, high: 1.0855, low: 1.08, close: 1.084 },
      { timestamp: '2024-01-01T01:00:00Z', open: 1.084, high: 1.087, low: 1.083, close: 1.086 },
    ]
    const { useCandles } = await import('../../hooks/queries')

    vi.mocked(useCandles).mockReturnValue({
      data: mockCandles,
      isLoading: false,
      error: null,
      isFetching: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<MarketData />)
    await waitFor(() => {
      expect(screen.getAllByText('Current Price').length).toBeGreaterThan(0)
    })
  })
  it('displays positive change with green color', async () => {
    const mockCandles = [
      { timestamp: '2024-01-01T00:00:00Z', open: 1.08, high: 1.085, low: 1.079, close: 1.08 },
      { timestamp: '2024-01-01T01:00:00Z', open: 1.08, high: 1.086, low: 1.079, close: 1.085 },
    ]
    const { useCandles } = await import('../../hooks/queries')

    vi.mocked(useCandles).mockReturnValue({
      data: mockCandles,
      isLoading: false,
      error: null,
      isFetching: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<MarketData />)
    await waitFor(() => {
      const changeElement = screen.getByText(/\+0\.00500/)

      expect(changeElement).toBeInTheDocument()
      expect(changeElement).toHaveClass('text-green-600')
    })
  })
  it('displays negative change with red color', async () => {
    const mockCandles = [
      { timestamp: '2024-01-01T00:00:00Z', open: 1.08, high: 1.085, low: 1.079, close: 1.085 },
      { timestamp: '2024-01-01T01:00:00Z', open: 1.085, high: 1.086, low: 1.079, close: 1.08 },
    ]
    const { useCandles } = await import('../../hooks/queries')

    vi.mocked(useCandles).mockReturnValue({
      data: mockCandles,
      isLoading: false,
      error: null,
      isFetching: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<MarketData />)
    await waitFor(() => {
      const changeElement = screen.getByText(/-0\.00500/)

      expect(changeElement).toBeInTheDocument()
      expect(changeElement).toHaveClass('text-red-600')
    })
  })
  it('displays error message when error occurs', async () => {
    const { useCandles } = await import('../../hooks/queries')

    vi.mocked(useCandles).mockReturnValue({
      data: undefined,
      isLoading: false,
      error: { message: 'Failed to fetch data' },
      isFetching: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<MarketData />)
    await waitFor(() => {
      expect(screen.getByText(/Error loading chart data/i)).toBeInTheDocument()
    })
  })
  it('displays no data message when selected instrument has no data', async () => {
    const { useCandles } = await import('../../hooks/queries')

    vi.mocked(useCandles).mockReturnValue({
      data: [],
      isLoading: false,
      error: null,
      isFetching: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<MarketData />)
    await waitFor(() => {
      expect(screen.getByText(/No data available for/i)).toBeInTheDocument()
    })
  })
  it('calls setSelectedExchange when exchange is changed', async () => {
    const user = userEvent.setup()

    renderWithProviders(<MarketData />)
    const triggers = screen.getAllByRole('combobox')

    await user.click(triggers[0])
    await waitFor(() => {
      expect(screen.getByText('binance')).toBeInTheDocument()
    })
    await user.click(screen.getByText('binance'))
    expect(mockSetSelectedExchange).toHaveBeenCalledWith('binance')
  })
  it('calls setSelectedInstrument when instrument is changed', async () => {
    const user = userEvent.setup()

    renderWithProviders(<MarketData />)
    const triggers = screen.getAllByRole('combobox')

    await user.click(triggers[1])
    await waitFor(() => {
      expect(screen.getByText('GBP-USD')).toBeInTheDocument()
    })
    await user.click(screen.getByText('GBP-USD'))
    expect(mockSetSelectedInstrument).toHaveBeenCalledWith('GBP-USD')
  })
  it('calls setSelectedTimeframe when timeframe is changed', async () => {
    const user = userEvent.setup()

    renderWithProviders(<MarketData />)
    const triggers = screen.getAllByRole('combobox')

    await user.click(triggers[2])
    await waitFor(() => {
      expect(screen.getByText('15 Minutes')).toBeInTheDocument()
    })
    await user.click(screen.getByText('15 Minutes'))
    expect(mockSetSelectedTimeframe).toHaveBeenCalledWith('15m')
  })
  it('returns empty chartData when isFetching', async () => {
    const mockCandles = [
      { timestamp: '2024-01-01T00:00:00Z', open: 1.08, high: 1.085, low: 1.079, close: 1.082 },
    ]
    const { useCandles } = await import('../../hooks/queries')

    vi.mocked(useCandles).mockReturnValue({
      data: mockCandles,
      isLoading: false,
      error: null,
      isFetching: true,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<MarketData />)
    expect(screen.queryByText('Current Price')).not.toBeInTheDocument()
  })
  it('handles undefined exchanges data', async () => {
    const { useExchanges } = await import('../../hooks/queries')

    vi.mocked(useExchanges).mockReturnValueOnce({
      data: undefined,
      isLoading: false,
      error: null,
    } as never)
    renderWithProviders(<MarketData />)
    expect(screen.getByText(/Market Data/i)).toBeInTheDocument()
  })
  it('handles undefined instruments data', async () => {
    const { useExchangeInstruments } = await import('../../hooks/queries')

    vi.mocked(useExchangeInstruments).mockReturnValueOnce({
      data: undefined,
      isLoading: false,
      error: null,
    } as never)
    renderWithProviders(<MarketData />)
    expect(screen.getByText(/Market Data/i)).toBeInTheDocument()
  })
  it('calculates stats from single candle', async () => {
    const mockCandles = [
      { timestamp: '2024-01-01T00:00:00Z', open: 1.08, high: 1.085, low: 1.079, close: 1.082 },
    ]
    const { useCandles } = await import('../../hooks/queries')

    vi.mocked(useCandles).mockReturnValue({
      data: mockCandles,
      isLoading: false,
      error: null,
      isFetching: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<MarketData />)
    await waitFor(() => {
      expect(screen.getAllByText('Current Price').length).toBeGreaterThan(0)
    })
  })
})
