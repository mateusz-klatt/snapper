import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { Signals } from './Signals'
import { useAuth } from '../../stores/auth'
import { useAppStore } from '../../stores/app'

vi.mock('../../stores/auth', () => ({
  useAuth: vi.fn(() => ({
    isAuthenticated: true,
  })),
}))
vi.mock('../../stores/app', () => ({
  useAppStore: vi.fn((selector: (state: { isConnected: boolean }) => boolean) =>
    selector({ isConnected: true })
  ),
}))
vi.mock('../../lib/apiClient', () => ({
  apiClient: {
    getSignals: vi.fn(async () => [
      {
        id: 1,
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timestamp: new Date().toISOString(),
        side: 'buy',
        strength: 0.85,
        reason: 'Strong momentum breakout',
        strategy_name: 'macd',
        price: 42000,
      },
      {
        id: 2,
        instrument: 'ETH-USD',
        exchange: 'kraken',
        timestamp: new Date().toISOString(),
        side: 'sell',
        strength: 0.65,
        reason: 'Overbought RSI',
        strategy_name: 'rsi',
        price: 2800,
      },
    ]),
  },
}))
const createTestQueryClient = () =>
  new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
      },
    },
  })

describe('Signals', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.mocked(useAuth).mockReturnValue({
      isAuthenticated: true,
    } as never)
    vi.mocked(useAppStore).mockImplementation(((
      selector: (state: { isConnected: boolean }) => boolean
    ) => selector({ isConnected: true })) as never)
  })
  it('renders header and stream indicator', () => {
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    expect(screen.getByText('Trading Signals')).toBeInTheDocument()
    expect(screen.getByText('Live Stream Active')).toBeInTheDocument()
  })
  it('shows disconnected indicator when stream is inactive', () => {
    vi.mocked(useAppStore).mockImplementation(((
      selector: (state: { isConnected: boolean }) => boolean
    ) => selector({ isConnected: false })) as never)
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    expect(screen.getByText('Stream Disconnected')).toBeInTheDocument()
  })
  it('displays stats cards with correct labels', () => {
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    expect(screen.getByText('Total Signals')).toBeInTheDocument()
    expect(screen.getByText('Buy Signals')).toBeInTheDocument()
    expect(screen.getByText('Sell Signals')).toBeInTheDocument()
    expect(screen.getByText('Avg Strength')).toBeInTheDocument()
  })
  it('renders strategy filter dropdown', () => {
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    expect(screen.getByText('Filter by strategy:')).toBeInTheDocument()
    const select = screen.getByRole('combobox')

    expect(select).toBeInTheDocument()
  })
  it('displays loading state initially', () => {
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    expect(screen.getAllByTestId('signal-card-skeleton').length).toBeGreaterThan(0)
  })
  it('displays signal cards after loading', async () => {
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    const btcSignal = await screen.findByText('BTC-USD')

    expect(btcSignal).toBeInTheDocument()
    const ethSignal = await screen.findByText('ETH-USD')

    expect(ethSignal).toBeInTheDocument()
  })
  it('displays signal details correctly', async () => {
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('BTC-USD')
    expect(screen.getByText('BUY')).toBeInTheDocument()
    expect(screen.getByText('SELL')).toBeInTheDocument()
    expect(screen.getAllByText('macd').length).toBeGreaterThan(0)
    expect(screen.getAllByText('rsi').length).toBeGreaterThan(0)
    expect(screen.getByText('Strong momentum breakout')).toBeInTheDocument()
    expect(screen.getByText('Overbought RSI')).toBeInTheDocument()
  })
  it('calculates and displays correct stats', async () => {
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('BTC-USD')
    const totalSignals = screen.getByText('2')

    expect(totalSignals).toBeInTheDocument()
    const buySignals = screen.getAllByText('1')

    expect(buySignals.length).toBeGreaterThanOrEqual(2)
    expect(screen.getByText('75%')).toBeInTheDocument()
  })
  it('displays market sentiment based on signal distribution', async () => {
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('BTC-USD')
    expect(screen.getByText('Neutral')).toBeInTheDocument()
  })
  it('displays bullish sentiment when buy signals dominate', async () => {
    const { apiClient } = await import('../../lib/apiClient')

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce([
      {
        id: 10,
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timestamp: new Date().toISOString(),
        side: 'buy',
        strength: 0.9,
        reason: 'Bullish momentum',
        strategy_name: 'macd',
        price: 42000,
      },
      {
        id: 11,
        instrument: 'ETH-USD',
        exchange: 'kraken',
        timestamp: new Date().toISOString(),
        side: 'buy',
        strength: 0.7,
        reason: 'Uptrend',
        strategy_name: 'macd',
        price: 2800,
      },
    ])
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('BTC-USD')
    expect(screen.getByText('Bullish')).toBeInTheDocument()
  })
  it('displays bearish sentiment when sell signals dominate', async () => {
    const { apiClient } = await import('../../lib/apiClient')

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce([
      {
        id: 12,
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timestamp: new Date().toISOString(),
        side: 'sell',
        strength: 0.9,
        reason: 'Bearish momentum',
        strategy_name: 'rsi',
        price: 42000,
      },
      {
        id: 13,
        instrument: 'ETH-USD',
        exchange: 'kraken',
        timestamp: new Date().toISOString(),
        side: 'sell',
        strength: 0.7,
        reason: 'Downtrend',
        strategy_name: 'rsi',
        price: 2800,
      },
    ])
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('BTC-USD')
    expect(screen.getByText('Bearish')).toBeInTheDocument()
  })
  it('displays strength label based on strength value', async () => {
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('BTC-USD')
    expect(screen.getByText('Strong (85%)')).toBeInTheDocument()
    expect(screen.getByText('Medium (65%)')).toBeInTheDocument()
  })
  it('displays weak strength label', async () => {
    const { apiClient } = await import('../../lib/apiClient')

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce([
      {
        id: 3,
        instrument: 'SOL-USD',
        exchange: 'kraken',
        timestamp: new Date().toISOString(),
        side: 'buy',
        strength: 0.45,
        reason: 'Weak signal',
        strategy_name: 'macd',
        price: 100,
      },
    ])
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('SOL-USD')
    expect(screen.getByText('Weak (45%)')).toBeInTheDocument()
  })
  it('displays very weak strength label', async () => {
    const { apiClient } = await import('../../lib/apiClient')

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce([
      {
        id: 4,
        instrument: 'XRP-USD',
        exchange: 'kraken',
        timestamp: new Date().toISOString(),
        side: 'sell',
        strength: 0.25,
        reason: 'Very weak signal',
        strategy_name: 'rsi',
        price: 0.5,
      },
    ])
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('XRP-USD')
    expect(screen.getByText('Very Weak (25%)')).toBeInTheDocument()
  })
  it('displays time as Just now for recent signals', async () => {
    const { apiClient } = await import('../../lib/apiClient')

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce([
      {
        id: 5,
        instrument: 'ADA-USD',
        exchange: 'kraken',
        timestamp: new Date().toISOString(),
        side: 'buy',
        strength: 0.75,
        reason: 'Recent signal',
        strategy_name: 'macd',
        price: 0.3,
      },
    ])
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('ADA-USD')
    expect(screen.getByText('Just now')).toBeInTheDocument()
  })
  it('displays time as minutes ago', async () => {
    const { apiClient } = await import('../../lib/apiClient')
    const fifteenMinutesAgo = new Date(Date.now() - 15 * 60 * 1000).toISOString()

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce([
      {
        id: 6,
        instrument: 'DOT-USD',
        exchange: 'kraken',
        timestamp: fifteenMinutesAgo,
        side: 'sell',
        strength: 0.8,
        reason: 'Minutes ago signal',
        strategy_name: 'rsi',
        price: 5,
      },
    ])
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('DOT-USD')
    expect(screen.getByText('15m ago')).toBeInTheDocument()
  })
  it('displays time as hours ago', async () => {
    const { apiClient } = await import('../../lib/apiClient')
    const threeHoursAgo = new Date(Date.now() - 3 * 60 * 60 * 1000).toISOString()

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce([
      {
        id: 7,
        instrument: 'LINK-USD',
        exchange: 'kraken',
        timestamp: threeHoursAgo,
        side: 'buy',
        strength: 0.9,
        reason: 'Hours ago signal',
        strategy_name: 'macd',
        price: 15,
      },
    ])
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('LINK-USD')
    expect(screen.getByText('3h ago')).toBeInTheDocument()
  })
  it('displays date for old signals', async () => {
    const { apiClient } = await import('../../lib/apiClient')
    const twoDaysAgo = new Date(Date.now() - 2 * 24 * 60 * 60 * 1000).toISOString()

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce([
      {
        id: 8,
        instrument: 'AVAX-USD',
        exchange: 'kraken',
        timestamp: twoDaysAgo,
        side: 'sell',
        strength: 0.7,
        reason: 'Old signal',
        strategy_name: 'rsi',
        price: 30,
      },
    ])
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('AVAX-USD')
    expect(screen.queryByText(/ago/)).not.toBeInTheDocument()
  })
  it('shows empty state when no signals are available', async () => {
    const { apiClient } = await import('../../lib/apiClient')

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce([])
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('No signals found')
    expect(screen.getByText('Signals from active strategies will appear here')).toBeInTheDocument()
  })
  it('shows empty state for filtered strategy with no results', async () => {
    const { apiClient } = await import('../../lib/apiClient')
    const userEventModule = await import('@testing-library/user-event')
    const user = userEventModule.default.setup()

    vi.mocked(apiClient.getSignals)
      .mockResolvedValueOnce([
        {
          id: 1,
          instrument: 'BTC-USD',
          exchange: 'kraken',
          timestamp: new Date().toISOString(),
          side: 'buy',
          strength: 0.85,
          reason: 'MACD signal',
          strategy_name: 'macd',
          price: 42000,
        },
        {
          id: 2,
          instrument: 'ETH-USD',
          exchange: 'kraken',
          timestamp: new Date().toISOString(),
          side: 'sell',
          strength: 0.65,
          reason: 'RSI signal',
          strategy_name: 'rsi',
          price: 2800,
        },
      ])
      .mockResolvedValueOnce([])
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('BTC-USD')
    const select = screen.getByRole('combobox')

    await user.selectOptions(select, 'macd')
    await screen.findByText('No signals found')
    expect(screen.getByText('No signals from macd strategy')).toBeInTheDocument()
  })
  it('filters signals by strategy', async () => {
    const { apiClient } = await import('../../lib/apiClient')
    const userEventModule = await import('@testing-library/user-event')
    const user = userEventModule.default.setup()

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce([
      {
        id: 1,
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timestamp: new Date().toISOString(),
        side: 'buy',
        strength: 0.85,
        reason: 'MACD signal',
        strategy_name: 'macd',
        price: 42000,
      },
      {
        id: 2,
        instrument: 'ETH-USD',
        exchange: 'kraken',
        timestamp: new Date().toISOString(),
        side: 'sell',
        strength: 0.65,
        reason: 'RSI signal',
        strategy_name: 'rsi',
        price: 2800,
      },
    ])
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('BTC-USD')
    const select = screen.getByRole('combobox')

    await user.selectOptions(select, 'macd')
    expect(screen.getByText('BTC-USD')).toBeInTheDocument()
  })
  it('displays N/A for null price', async () => {
    const { apiClient } = await import('../../lib/apiClient')

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce([
      {
        id: 1,
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timestamp: new Date().toISOString(),
        side: 'buy',
        strength: 0.85,
        reason: 'Strong momentum',
        strategy_name: 'macd',
        price: null as unknown as number,
      },
    ])
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('BTC-USD')
    expect(screen.getByText('N/A')).toBeInTheDocument()
  })
  it('does not request signals when not authenticated', async () => {
    const { apiClient } = await import('../../lib/apiClient')

    vi.mocked(useAuth).mockReturnValue({
      isAuthenticated: false,
    } as never)
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    expect(apiClient.getSignals).not.toHaveBeenCalled()
  })
  it('omits strategy badge when strategy name is missing', async () => {
    const { apiClient } = await import('../../lib/apiClient')

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce([
      {
        id: 14,
        instrument: 'BTC-USD',
        exchange: 'kraken',
        timestamp: new Date().toISOString(),
        side: 'buy',
        strength: 0.85,
        reason: 'No strategy label',
        strategy_name: null as unknown as string,
        price: 42000,
      },
    ])
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('BTC-USD')
    expect(screen.queryByText('macd')).not.toBeInTheDocument()
    expect(screen.queryByText('rsi')).not.toBeInTheDocument()
  })
})
