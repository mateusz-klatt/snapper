import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor, fireEvent } from '@testing-library/react'
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
    data: { payload: ['kraken', 'binance'], count: 2 },
    isLoading: false,
    error: null,
  })),
  useExchangeInstrumentsDetail: vi.fn(() => ({
    data: {
      payload: [
        { symbol: 'EUR-USD', can_trade: true },
        { symbol: 'GBP-USD', can_trade: true },
        { symbol: 'BTC-USD', can_trade: true },
      ],
      count: 3,
    },
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
  useAppStore: vi.fn((selector: (s: Record<string, unknown>) => unknown) =>
    selector({ asOf: null, isTimeTraveling: false })
  ),
}))
vi.mock('../../hooks/useMarketSubscription', () => ({
  useMarketSubscription: vi.fn(() => true),
}))
vi.mock('../../stores/websocket', () => ({
  useWebSocketStore: vi.fn(() => ({
    isConnected: true,
  })),
}))
vi.mock('../../hooks/useWSDispatcher', () => ({
  useWSDispatcher: vi.fn(() => ({
    startBuffering: vi.fn(),
    flushBuffer: vi.fn(),
    stopBuffering: vi.fn(),
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
      expect(screen.getByLabelText('Instrument:')).toBeInTheDocument()
    })
  })
  it('displays timeframe selector', async () => {
    renderWithProviders(<MarketData />)
    await waitFor(() => {
      const selectors = document.querySelectorAll('select, [role="combobox"]')

      expect(selectors.length).toBeGreaterThan(0)
    })
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
    const input = screen.getByLabelText('Instrument:')

    await user.click(input)
    await waitFor(() => {
      expect(screen.getByText('EUR-USD')).toBeInTheDocument()
      expect(screen.getByText('GBP-USD')).toBeInTheDocument()
      expect(screen.getByText('BTC-USD')).toBeInTheDocument()
    })
  })
  it('displays timeframe dropdown with options', async () => {
    const user = userEvent.setup()

    renderWithProviders(<MarketData />)
    const triggers = screen.getAllByRole('combobox')

    await user.click(triggers[1])
    await waitFor(() => {
      expect(screen.getAllByText('1 Hour').length).toBeGreaterThanOrEqual(2)
    })
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
    expect(useCandles).toHaveBeenCalledWith('', '', '1h', 100, true)
  })
  it('enables snapshot when WebSocket is disconnected (fallback)', async () => {
    const { useCandles } = await import('../../hooks/queries')
    const { useMarketSubscription } = await import('../../hooks/useMarketSubscription')
    const { useWebSocketStore } = await import('../../stores/websocket')

    vi.mocked(useMarketSubscription).mockReturnValue(false)
    vi.mocked(useWebSocketStore).mockReturnValue({ isConnected: false } as never)
    renderWithProviders(<MarketData />)

    expect(useCandles).toHaveBeenCalledWith('EUR-USD', 'kraken', '1h', 100, true)
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
      { open_at: '2024-01-01T00:00:00Z', open: 1.08, high: 1.085, low: 1.079, close: 1.082 },
      { open_at: '2024-01-01T01:00:00Z', open: 1.082, high: 1.086, low: 1.081, close: 1.085 },
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
      { open_at: '2024-01-01T00:00:00Z', open: 1.08, high: 1.085, low: 1.079, close: 1.082 },
      { open_at: '2024-01-01T01:00:00Z', open: 1.082, high: 1.086, low: 1.081, close: 1.085 },
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
  it('handles duplicate open_at by keeping the last one', async () => {
    const mockCandles = [
      { open_at: '2024-01-01T00:00:00Z', open: 1.08, high: 1.085, low: 1.079, close: 1.082 },
      { open_at: '2024-01-01T00:00:00Z', open: 1.0825, high: 1.0855, low: 1.08, close: 1.084 },
      { open_at: '2024-01-01T01:00:00Z', open: 1.084, high: 1.087, low: 1.083, close: 1.086 },
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
      { open_at: '2024-01-01T00:00:00Z', open: 1.08, high: 1.085, low: 1.079, close: 1.08 },
      { open_at: '2024-01-01T01:00:00Z', open: 1.08, high: 1.086, low: 1.079, close: 1.085 },
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
      expect(changeElement).toHaveClass('text-gain-600')
    })
  })
  it('displays negative change with red color', async () => {
    const mockCandles = [
      { open_at: '2024-01-01T00:00:00Z', open: 1.08, high: 1.085, low: 1.079, close: 1.085 },
      { open_at: '2024-01-01T01:00:00Z', open: 1.085, high: 1.086, low: 1.079, close: 1.08 },
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
      expect(changeElement).toHaveClass('text-loss-600')
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
    const input = screen.getByLabelText('Instrument:')

    await user.click(input)
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

    await user.click(triggers[1])
    await waitFor(() => {
      expect(screen.getByText('15 Minutes')).toBeInTheDocument()
    })
    await user.click(screen.getByText('15 Minutes'))
    expect(mockSetSelectedTimeframe).toHaveBeenCalledWith('15m')
  })
  it('returns empty chartData when isFetching', async () => {
    const mockCandles = [
      { open_at: '2024-01-01T00:00:00Z', open: 1.08, high: 1.085, low: 1.079, close: 1.082 },
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
    const { useExchangeInstrumentsDetail } = await import('../../hooks/queries')

    vi.mocked(useExchangeInstrumentsDetail).mockReturnValueOnce({
      data: undefined,
      isLoading: false,
      error: null,
    } as never)
    renderWithProviders(<MarketData />)
    expect(screen.getByText(/Market Data/i)).toBeInTheDocument()
  })
  it('calculates stats from single candle', async () => {
    const mockCandles = [
      { open_at: '2024-01-01T00:00:00Z', open: 1.08, high: 1.085, low: 1.079, close: 1.082 },
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
  it('filters instruments on typing', async () => {
    const user = userEvent.setup()

    renderWithProviders(<MarketData />)
    const input = screen.getByLabelText('Instrument:')

    await user.click(input)
    await user.type(input, 'BTC')
    await waitFor(() => {
      expect(screen.getByText('BTC-USD')).toBeInTheDocument()
      expect(screen.queryByText('EUR-USD')).not.toBeInTheDocument()
      expect(screen.queryByText('GBP-USD')).not.toBeInTheDocument()
    })
  })
  it('shows all instruments when input is focused with no search', async () => {
    const user = userEvent.setup()

    renderWithProviders(<MarketData />)
    const input = screen.getByLabelText('Instrument:')

    await user.click(input)
    await waitFor(() => {
      expect(screen.getByText('EUR-USD')).toBeInTheDocument()
      expect(screen.getByText('GBP-USD')).toBeInTheDocument()
      expect(screen.getByText('BTC-USD')).toBeInTheDocument()
    })
  })
  it('closes instrument dropdown on click outside', async () => {
    const user = userEvent.setup()

    renderWithProviders(<MarketData />)
    const input = screen.getByLabelText('Instrument:')

    await user.click(input)
    await waitFor(() => {
      expect(screen.getByText('GBP-USD')).toBeInTheDocument()
    })
    fireEvent.mouseDown(document.body)
    await waitFor(() => {
      expect(screen.queryByText('GBP-USD')).not.toBeInTheDocument()
    })
  })
  it('shows no instruments found when search matches nothing', async () => {
    const user = userEvent.setup()

    renderWithProviders(<MarketData />)
    const input = screen.getByLabelText('Instrument:')

    await user.click(input)
    await user.type(input, 'ZZZZZ')
    await waitFor(() => {
      expect(screen.getByText('No instruments found')).toBeInTheDocument()
    })
  })
  it('shows selected instrument in input when dropdown is closed', async () => {
    renderWithProviders(<MarketData />)
    const input = screen.getByLabelText('Instrument:') as HTMLInputElement

    expect(input.value).toBe('EUR-USD')
  })
  it('disables instrument input when no exchange selected', async () => {
    const { useMarketStore } = await import('../../stores/market')
    const nullExchangeState = {
      selectedExchange: null,
      selectedInstrument: null,
      selectedTimeframe: '1h',
      setSelectedExchange: mockSetSelectedExchange,
      setSelectedInstrument: mockSetSelectedInstrument,
      setSelectedTimeframe: mockSetSelectedTimeframe,
    }

    vi.mocked(useMarketStore).mockReturnValue(nullExchangeState)
    renderWithProviders(<MarketData />)
    const input = screen.getByLabelText('Instrument:')

    expect(input).toBeDisabled()
  })
  it('disables candle fetch when not subscribed and connected in live mode', async () => {
    const { useMarketSubscription } = await import('../../hooks/useMarketSubscription')
    const { useCandles } = await import('../../hooks/queries')
    const { useWebSocketStore } = await import('../../stores/websocket')

    vi.mocked(useMarketSubscription).mockReturnValue(false)
    vi.mocked(useWebSocketStore).mockReturnValue({ isConnected: true } as never)
    renderWithProviders(<MarketData />)

    expect(vi.mocked(useCandles)).toHaveBeenCalledWith(
      expect.any(String),
      expect.any(String),
      expect.any(String),
      expect.any(Number),
      false
    )
  })
  it('enables candle fetch in time travel even without subscription', async () => {
    const { useAppStore } = await import('../../stores/app')
    const { useMarketSubscription } = await import('../../hooks/useMarketSubscription')

    vi.mocked(useAppStore).mockImplementation(((
      selector: (s: Record<string, unknown>) => unknown
    ) => selector({ asOf: '2024-01-01T00:00:00Z', isTimeTraveling: true })) as never)
    vi.mocked(useMarketSubscription).mockReturnValue(false)
    const { useCandles } = await import('../../hooks/queries')

    renderWithProviders(<MarketData />)
    await waitFor(() => {
      expect(vi.mocked(useCandles)).toHaveBeenCalledWith(
        expect.any(String),
        expect.any(String),
        expect.any(String),
        expect.any(Number),
        true
      )
    })
    vi.mocked(useAppStore).mockImplementation(((
      selector: (s: Record<string, unknown>) => unknown
    ) => selector({ asOf: null, isTimeTraveling: false })) as never)
    vi.mocked(useMarketSubscription).mockReturnValue(true)
  })

  it('renders the market-data-only badge next to the title when the selected instrument is observation-only', async () => {
    const queries = await import('../../hooks/queries')
    const marketStore = await import('../../stores/market')

    vi.mocked(marketStore.useMarketStore).mockReturnValue({
      selectedExchange: 'kraken',
      selectedInstrument: 'EUR-USD',
      selectedTimeframe: '1h',
      setSelectedExchange: mockSetSelectedExchange,
      setSelectedInstrument: mockSetSelectedInstrument,
      setSelectedTimeframe: mockSetSelectedTimeframe,
    } as never)
    vi.mocked(queries.useExchangeInstrumentsDetail).mockImplementation((() => ({
      data: {
        payload: [
          { symbol: 'EUR-USD', can_trade: false },
          { symbol: 'GBP-USD', can_trade: true },
        ],
        count: 2,
      },
      isLoading: false,
      error: null,
    })) as never)

    try {
      renderWithProviders(<MarketData />)
      const badges = await screen.findAllByTestId('market-data-only-badge')

      expect(badges.length).toBeGreaterThan(0)
    } finally {
      vi.mocked(queries.useExchangeInstrumentsDetail).mockImplementation((() => ({
        data: {
          payload: [
            { symbol: 'EUR-USD', can_trade: true },
            { symbol: 'GBP-USD', can_trade: true },
            { symbol: 'BTC-USD', can_trade: true },
          ],
          count: 3,
        },
        isLoading: false,
        error: null,
      })) as never)
    }
  })

  it('hides the title badge when no instrument is selected', async () => {
    const { useMarketStore } = await import('../../stores/market')

    vi.mocked(useMarketStore).mockReturnValueOnce({
      selectedExchange: 'kraken',
      selectedInstrument: null,
      selectedTimeframe: '1h',
      setSelectedExchange: mockSetSelectedExchange,
      setSelectedInstrument: mockSetSelectedInstrument,
      setSelectedTimeframe: mockSetSelectedTimeframe,
    } as never)
    renderWithProviders(<MarketData />)
    expect(screen.queryByTestId('market-data-only-badge')).toBeNull()
  })

  it('marks market-data-only rows in the instrument dropdown with a badge', async () => {
    const { useExchangeInstrumentsDetail } = await import('../../hooks/queries')

    vi.mocked(useExchangeInstrumentsDetail).mockImplementation((() => ({
      data: {
        payload: [
          { symbol: 'MNQM6-CME', can_trade: false },
          { symbol: 'BTC-USD', can_trade: true },
        ],
        count: 2,
      },
      isLoading: false,
      error: null,
    })) as never)

    try {
      renderWithProviders(<MarketData />)
      const search = screen.getByPlaceholderText('Search instrument...') as HTMLInputElement

      fireEvent.focus(search)
      const badges = await screen.findAllByTestId('market-data-only-badge')

      expect(badges.length).toBeGreaterThan(0)
    } finally {
      vi.mocked(useExchangeInstrumentsDetail).mockImplementation((() => ({
        data: {
          payload: [
            { symbol: 'EUR-USD', can_trade: true },
            { symbol: 'GBP-USD', can_trade: true },
            { symbol: 'BTC-USD', can_trade: true },
          ],
          count: 3,
        },
        isLoading: false,
        error: null,
      })) as never)
    }
  })
})
