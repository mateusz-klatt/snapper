import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { Signals } from './Signals'
import { useAuth } from '../../stores/auth'

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

vi.mock('../../lib/csvExport', () => ({
  exportToCSV: vi.fn(),
}))
vi.mock('../../stores/auth', () => ({
  useAuth: vi.fn(() => ({
    isAuthenticated: true,
  })),
}))
vi.mock('../../stores/app', () => ({
  useAppStore: vi.fn((selector: (s: Record<string, unknown>) => unknown) =>
    selector({ asOf: null, isTimeTraveling: false })
  ),
}))
vi.mock('../../lib/apiClient', () => ({
  apiClient: {
    getSignals: vi.fn(async () => ({
      type: 'signal_list',
      session_id: '',
      sequence_id: 0,
      payload: [
        {
          type: 'signal',
          instrument: 'BTC-USD',
          exchange: 'kraken',
          timestamp: new Date().toISOString(),
          fired_at: new Date().toISOString(),
          side: 'buy',
          strength: 0.85,
          reason: 'Strong momentum breakout',
          strategy_name: 'macd',
          price: 42000,
        },
        {
          type: 'signal',
          instrument: 'ETH-USD',
          exchange: 'kraken',
          timestamp: new Date().toISOString(),
          fired_at: new Date().toISOString(),
          side: 'sell',
          strength: 0.65,
          reason: 'Overbought RSI',
          strategy_name: 'rsi',
          price: 2800,
        },
        {
          type: 'signal',
          instrument: 'SOL-USD',
          exchange: 'kraken',
          timestamp: new Date().toISOString(),
          side: 'sell',
          strength: 0.5,
          reason: 'Test without fired_at',
          strategy_name: 'test',
          price: 100,
        },
      ],
      count: 2,
    })),
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
  })
  it('renders header', () => {
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    expect(screen.getByText('Signals')).toBeInTheDocument()
  })
  it('displays stats cards with correct labels', async () => {
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('Total')
    expect(screen.getByText('Total')).toBeInTheDocument()
    expect(screen.getByText('Buy')).toBeInTheDocument()
    expect(screen.getByText('Sell')).toBeInTheDocument()
    expect(screen.getByText('Avg Strength')).toBeInTheDocument()
  })
  it('renders strategy filter dropdown', async () => {
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('BTC-USD')
    const select = screen.getByRole('combobox')

    expect(select).toBeInTheDocument()
    expect(screen.getByText('All Strategies')).toBeInTheDocument()
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
    expect(screen.getAllByText('BUY').length).toBeGreaterThan(0)
    expect(screen.getAllByText('SELL').length).toBeGreaterThan(0)
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
    const totalSignals = screen.getByText('3')

    expect(totalSignals).toBeInTheDocument()
    expect(screen.getAllByText('1').length).toBeGreaterThanOrEqual(1)
    expect(screen.getByText('67%')).toBeInTheDocument()
  })
  it('displays balanced stats when signal distribution is even', async () => {
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('BTC-USD')
    expect(screen.getAllByText('1').length).toBeGreaterThanOrEqual(1)
  })
  it('displays all buy stats when buy signals dominate', async () => {
    const { apiClient } = await import('../../lib/apiClient')

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce({
      payload: [
        {
          type: 'signal',
          instrument: 'BTC-USD',
          exchange: 'kraken',
          timestamp: new Date().toISOString(),
          fired_at: new Date().toISOString(),
          side: 'buy',
          strength: 0.9,
          reason: 'Bullish momentum',
          strategy_name: 'macd',
          price: 42000,
        },
        {
          type: 'signal',
          instrument: 'ETH-USD',
          exchange: 'kraken',
          timestamp: new Date().toISOString(),
          fired_at: new Date().toISOString(),
          side: 'buy',
          strength: 0.7,
          reason: 'Uptrend',
          strategy_name: 'macd',
          price: 2800,
        },
      ],
      count: 2,
    } as never)
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('BTC-USD')
    expect(screen.getAllByText('2').length).toBeGreaterThanOrEqual(1)
    expect(screen.getAllByText('BUY').length).toBe(2)
  })
  it('displays all sell stats when sell signals dominate', async () => {
    const { apiClient } = await import('../../lib/apiClient')

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce({
      payload: [
        {
          type: 'signal',
          instrument: 'BTC-USD',
          exchange: 'kraken',
          timestamp: new Date().toISOString(),
          fired_at: new Date().toISOString(),
          side: 'sell',
          strength: 0.9,
          reason: 'Bearish momentum',
          strategy_name: 'rsi',
          price: 42000,
        },
        {
          type: 'signal',
          instrument: 'ETH-USD',
          exchange: 'kraken',
          timestamp: new Date().toISOString(),
          fired_at: new Date().toISOString(),
          side: 'sell',
          strength: 0.7,
          reason: 'Downtrend',
          strategy_name: 'rsi',
          price: 2800,
        },
      ],
      count: 2,
    } as never)
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('BTC-USD')
    expect(screen.getAllByText('2').length).toBeGreaterThanOrEqual(1)
    expect(screen.getAllByText('SELL').length).toBe(2)
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

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce({
      payload: [
        {
          type: 'signal',
          instrument: 'SOL-USD',
          exchange: 'kraken',
          timestamp: new Date().toISOString(),
          fired_at: new Date().toISOString(),
          side: 'buy',
          strength: 0.45,
          reason: 'Weak signal',
          strategy_name: 'macd',
          price: 100,
        },
      ],
      count: 1,
    } as never)
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

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce({
      payload: [
        {
          type: 'signal',
          instrument: 'XRP-USD',
          exchange: 'kraken',
          timestamp: new Date().toISOString(),
          fired_at: new Date().toISOString(),
          side: 'sell',
          strength: 0.25,
          reason: 'Very weak signal',
          strategy_name: 'rsi',
          price: 0.5,
        },
      ],
      count: 1,
    } as never)
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

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce({
      payload: [
        {
          type: 'signal',
          instrument: 'ADA-USD',
          exchange: 'kraken',
          timestamp: new Date().toISOString(),
          fired_at: new Date().toISOString(),
          side: 'buy',
          strength: 0.75,
          reason: 'Recent signal',
          strategy_name: 'macd',
          price: 0.3,
        },
      ],
      count: 1,
    } as never)
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

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce({
      payload: [
        {
          type: 'signal',
          instrument: 'DOT-USD',
          exchange: 'kraken',
          timestamp: fifteenMinutesAgo,
          fired_at: fifteenMinutesAgo,
          side: 'sell',
          strength: 0.8,
          reason: 'Minutes ago signal',
          strategy_name: 'rsi',
          price: 5,
        },
      ],
      count: 1,
    } as never)
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

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce({
      payload: [
        {
          type: 'signal',
          instrument: 'LINK-USD',
          exchange: 'kraken',
          timestamp: threeHoursAgo,
          fired_at: threeHoursAgo,
          side: 'buy',
          strength: 0.9,
          reason: 'Hours ago signal',
          strategy_name: 'macd',
          price: 15,
        },
      ],
      count: 1,
    } as never)
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

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce({
      payload: [
        {
          type: 'signal',
          instrument: 'AVAX-USD',
          exchange: 'kraken',
          timestamp: twoDaysAgo,
          fired_at: twoDaysAgo,
          side: 'sell',
          strength: 0.7,
          reason: 'Old signal',
          strategy_name: 'rsi',
          price: 30,
        },
      ],
      count: 1,
    } as never)
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

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce({ payload: [], count: 0 } as never)
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('No signals found')
    expect(screen.getByText('No trading signals match your current filters.')).toBeInTheDocument()
  })
  it('shows empty state for filtered strategy with no results', async () => {
    const { apiClient } = await import('../../lib/apiClient')
    const userEventModule = await import('@testing-library/user-event')
    const user = userEventModule.default.setup()

    vi.mocked(apiClient.getSignals)
      .mockResolvedValueOnce({
        payload: [
          {
            type: 'signal',
            instrument: 'BTC-USD',
            exchange: 'kraken',
            timestamp: new Date().toISOString(),
            fired_at: new Date().toISOString(),
            side: 'buy',
            strength: 0.85,
            reason: 'MACD signal',
            strategy_name: 'macd',
            price: 42000,
          },
          {
            type: 'signal',
            instrument: 'ETH-USD',
            exchange: 'kraken',
            timestamp: new Date().toISOString(),
            fired_at: new Date().toISOString(),
            side: 'sell',
            strength: 0.65,
            reason: 'RSI signal',
            strategy_name: 'rsi',
            price: 2800,
          },
        ],
        count: 2,
      } as never)
      .mockResolvedValueOnce({ payload: [], count: 0 } as never)
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
    expect(screen.getByText('No trading signals match your current filters.')).toBeInTheDocument()
  })
  it('filters signals by strategy', async () => {
    const { apiClient } = await import('../../lib/apiClient')
    const userEventModule = await import('@testing-library/user-event')
    const user = userEventModule.default.setup()

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce({
      payload: [
        {
          type: 'signal',
          instrument: 'BTC-USD',
          exchange: 'kraken',
          timestamp: new Date().toISOString(),
          fired_at: new Date().toISOString(),
          side: 'buy',
          strength: 0.85,
          reason: 'MACD signal',
          strategy_name: 'macd',
          price: 42000,
        },
        {
          type: 'signal',
          instrument: 'ETH-USD',
          exchange: 'kraken',
          timestamp: new Date().toISOString(),
          fired_at: new Date().toISOString(),
          side: 'sell',
          strength: 0.65,
          reason: 'RSI signal',
          strategy_name: 'rsi',
          price: 2800,
        },
      ],
      count: 2,
    } as never)
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

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce({
      payload: [
        {
          type: 'signal',
          instrument: 'BTC-USD',
          exchange: 'kraken',
          timestamp: new Date().toISOString(),
          fired_at: new Date().toISOString(),
          side: 'buy',
          strength: 0.85,
          reason: 'Strong momentum',
          strategy_name: 'macd',
          price: null as unknown as number,
        },
      ],
      count: 1,
    } as never)
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

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce({
      payload: [
        {
          type: 'signal',
          instrument: 'BTC-USD',
          exchange: 'kraken',
          timestamp: new Date().toISOString(),
          fired_at: new Date().toISOString(),
          side: 'buy',
          strength: 0.85,
          reason: 'No strategy label',
          strategy_name: null as unknown as string,
          price: 42000,
        },
      ],
      count: 1,
    } as never)
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
  it('exports signals to CSV when export button clicked', async () => {
    const { exportToCSV } = await import('../../lib/csvExport')
    const userEventModule = await import('@testing-library/user-event')
    const user = userEventModule.default.setup()
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('BTC-USD')
    const exportButton = screen.getByRole('button', { name: /Export/i })

    await user.click(exportButton)
    expect(exportToCSV).toHaveBeenCalledWith(
      'signals.csv',
      ['instrument', 'exchange', 'side', 'strength', 'strategy', 'price', 'reason', 'fired_at'],
      expect.arrayContaining([expect.arrayContaining(['BTC-USD', 'kraken', 'buy'])])
    )
  })
  it('disables export button when no signals', async () => {
    const { apiClient } = await import('../../lib/apiClient')

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce({ payload: [], count: 0 } as never)
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('No signals found')
    const exportButton = screen.getByRole('button', { name: /Export/i })

    expect(exportButton).toBeDisabled()
  })
  it('renders signal with undefined fired_at as Just now', async () => {
    const { apiClient } = await import('../../lib/apiClient')

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce({
      payload: [
        {
          type: 'signal',
          instrument: 'SOL-USD',
          exchange: 'kraken',
          fired_at: undefined as unknown as string,
          timestamp: new Date().toISOString(),
          side: 'buy',
          strength: 0.5,
          reason: 'test reason',
          strategy_name: 'test',
          price: 100,
        },
      ],
      count: 1,
    } as never)
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('SOL-USD')
    expect(screen.getByText('Just now')).toBeInTheDocument()
  })
  it('exports signals with undefined fired_at', async () => {
    const { apiClient } = await import('../../lib/apiClient')
    const { exportToCSV } = await import('../../lib/csvExport')
    const userEventModule = await import('@testing-library/user-event')
    const user = userEventModule.default.setup()

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce({
      payload: [
        {
          type: 'signal',
          instrument: 'SOL-USD',
          exchange: 'kraken',
          fired_at: undefined as unknown as string,
          timestamp: '2026-01-15T10:30:00Z',
          side: 'sell',
          strength: 0.6,
          reason: 'test',
          strategy_name: null as unknown as string,
          price: null as unknown as number,
        },
      ],
      count: 1,
    } as never)
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('SOL-USD')
    const exportButton = screen.getByRole('button', { name: /Export/i })

    await user.click(exportButton)
    expect(exportToCSV).toHaveBeenCalledWith(
      'signals.csv',
      ['instrument', 'exchange', 'side', 'strength', 'strategy', 'price', 'reason', 'fired_at'],
      [expect.arrayContaining(['SOL-USD', 'kraken', 'sell', '0.6'])]
    )
  })
  it('exports signals with null price and null strategy_name', async () => {
    const { apiClient } = await import('../../lib/apiClient')
    const { exportToCSV } = await import('../../lib/csvExport')
    const userEventModule = await import('@testing-library/user-event')
    const user = userEventModule.default.setup()

    vi.mocked(apiClient.getSignals).mockResolvedValueOnce({
      payload: [
        {
          type: 'signal',
          instrument: 'BTC-USD',
          exchange: 'kraken',
          timestamp: '2024-01-01T00:00:00Z',
          fired_at: '2024-01-01T00:00:00Z',
          side: 'buy',
          strength: 0.85,
          reason: null as unknown as string,
          strategy_name: null as unknown as string,
          price: null as unknown as number,
        },
      ],
      count: 1,
    } as never)
    const queryClient = createTestQueryClient()

    render(
      <QueryClientProvider client={queryClient}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('BTC-USD')
    const exportButton = screen.getByRole('button', { name: /Export/i })

    await user.click(exportButton)
    expect(exportToCSV).toHaveBeenCalledWith(
      'signals.csv',
      ['instrument', 'exchange', 'side', 'strength', 'strategy', 'price', 'reason', 'fired_at'],
      [['BTC-USD', 'kraken', 'buy', '0.85', '', '', null, '2024-01-01T00:00:00.000Z']]
    )
  })
  it('shows relative time based on asOf when time traveling', async () => {
    const { useAppStore } = await import('../../stores/app')
    const { apiClient } = await import('../../lib/apiClient')
    const asOfDate = '2024-06-15T12:00:00Z'
    const firedAt = '2024-06-15T11:30:00Z'

    vi.mocked(useAppStore).mockImplementation(((
      selector: (s: Record<string, unknown>) => unknown
    ) => selector({ asOf: asOfDate, isTimeTraveling: true })) as never)
    vi.mocked(apiClient.getSignals).mockResolvedValueOnce({
      type: 'signal_list',
      session_id: '',
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: asOfDate,
      payload: [
        {
          type: 'signal',
          instrument: 'BTC-USD',
          exchange: 'kraken',
          timestamp: asOfDate,
          fired_at: firedAt,
          side: 'buy',
          strength: 0.85,
          reason: 'Test signal',
          strategy_name: 'macd',
          price: 42000,
        },
      ],
      count: 1,
    } as never)
    const qc = createTestQueryClient()

    render(
      <QueryClientProvider client={qc}>
        <Signals />
      </QueryClientProvider>
    )
    await screen.findByText('30m ago')
    vi.mocked(useAppStore).mockImplementation(((
      selector: (s: Record<string, unknown>) => unknown
    ) => selector({ asOf: null, isTimeTraveling: false })) as never)
  })
})
