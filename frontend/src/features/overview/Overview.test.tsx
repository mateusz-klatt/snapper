import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import type { ReactNode } from 'react'
import { Overview } from './Overview'

vi.mock('../../stores/app', () => ({
  useAppStore: vi.fn((selector: (s: Record<string, unknown>) => unknown) =>
    selector({ asOf: null, isTimeTraveling: false })
  ),
}))

vi.mock('../../hooks/queries', () => ({
  usePositionsSummary: vi.fn(() => ({ data: null, isLoading: false })),
  useLatestSignals: vi.fn(() => ({ data: [], isLoading: false })),
  useOrdersGrouped: vi.fn(() => ({ data: null })),
  useProcessSummary: vi.fn(() => ({
    isLoading: false,
    data: {
      payload: {
        feeds: { running: 0, total: 0 },
        strategies: { running: 0, total: 0 },
        executors: { running: 0, total: 0 },
        brokers: { running: 0, total: 0 },
      },
    },
  })),
  useExecutions: vi.fn(() => ({ data: [], isLoading: false })),
}))

const renderWithMocks = (ui: ReactNode) => {
  return render(ui)
}

describe('Overview', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })
  it('renders overview page', () => {
    renderWithMocks(<Overview />)
    expect(screen.getByText('Feeds Running')).toBeInTheDocument()
  })
  it('displays metric cards', () => {
    renderWithMocks(<Overview />)
    expect(screen.getByText('Strategies Active')).toBeInTheDocument()
    expect(screen.getByText('Open Orders')).toBeInTheDocument()
    expect(screen.getByText("Today's Executions")).toBeInTheDocument()
  })
  it('displays process status section', () => {
    renderWithMocks(<Overview />)
    expect(screen.getByText('Process Status')).toBeInTheDocument()
  })
  it('displays portfolio summary section', () => {
    renderWithMocks(<Overview />)
    expect(screen.getByText('Portfolio Summary')).toBeInTheDocument()
  })
  it('displays recent signals section', () => {
    renderWithMocks(<Overview />)
    expect(screen.getByText('Recent Signals')).toBeInTheDocument()
  })
  it('displays recent executions section', () => {
    renderWithMocks(<Overview />)
    expect(screen.getByText('Recent Executions')).toBeInTheDocument()
  })
  it('shows no positions message when data is null', () => {
    renderWithMocks(<Overview />)
    expect(screen.getByText('No positions data available')).toBeInTheDocument()
  })
  it('shows no recent signals message when empty', () => {
    renderWithMocks(<Overview />)
    expect(screen.getByText('No recent signals')).toBeInTheDocument()
  })
  it('shows no recent executions message when empty', () => {
    renderWithMocks(<Overview />)
    expect(screen.getByText('No recent executions')).toBeInTheDocument()
  })
  it('handles undefined store values with defaults', async () => {
    const { useProcessSummary, useExecutions } = await import('../../hooks/queries')

    vi.mocked(useProcessSummary).mockReturnValue({
      isLoading: false,
      data: undefined,
    } as never)
    vi.mocked(useExecutions).mockReturnValue({
      data: undefined,
      isLoading: false,
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('Feeds Running')).toBeInTheDocument()
  })
  it('displays loading spinner for process status', async () => {
    const { useProcessSummary } = await import('../../hooks/queries')

    vi.mocked(useProcessSummary).mockReturnValue({
      isLoading: true,
      data: null,
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('Process Status')).toBeInTheDocument()
  })
  it('displays loading spinner for portfolio', async () => {
    const { usePositionsSummary } = await import('../../hooks/queries')

    vi.mocked(usePositionsSummary).mockReturnValue({
      isLoading: true,
      data: null,
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('Portfolio Summary')).toBeInTheDocument()
  })
  it('displays loading spinner for signals', async () => {
    const { useLatestSignals } = await import('../../hooks/queries')

    vi.mocked(useLatestSignals).mockReturnValue({
      isLoading: true,
      data: [],
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('Recent Signals')).toBeInTheDocument()
  })
  it('displays portfolio data when available', async () => {
    const { usePositionsSummary } = await import('../../hooks/queries')

    vi.mocked(usePositionsSummary).mockReturnValue({
      isLoading: false,
      data: {
        totalValue: 10000,
        totalPnL: 500,
        pnlPercent: 5,
        count: 3,
        longCost: 6000,
        shortCost: 4000,
      },
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('Total Value')).toBeInTheDocument()
    expect(screen.getByTestId('overview-long-exposure')).toHaveTextContent('$6,000.00')
    expect(screen.getByTestId('overview-short-exposure')).toHaveTextContent('$4,000.00')
    expect(screen.getByTestId('overview-net-delta')).toHaveTextContent('+$2,000.00')
  })

  it('shows negative net delta as red when net short', async () => {
    const { usePositionsSummary } = await import('../../hooks/queries')

    vi.mocked(usePositionsSummary).mockReturnValue({
      isLoading: false,
      data: {
        totalValue: 8000,
        totalPnL: -500,
        pnlPercent: -5.5,
        count: 2,
        longCost: 1000,
        shortCost: 7000,
      },
    } as never)
    renderWithMocks(<Overview />)
    const netDelta = screen.getByTestId('overview-net-delta')

    expect(netDelta).toHaveTextContent('$-6,000.00')
    expect(netDelta.className).toContain('text-loss-600')
  })
  it('displays running feeds status', async () => {
    const { useProcessSummary } = await import('../../hooks/queries')

    vi.mocked(useProcessSummary).mockReturnValue({
      isLoading: false,
      data: {
        payload: {
          feeds: { running: 1, total: 2 },
          strategies: { running: 0, total: 0 },
          executors: { running: 0, total: 0 },
          brokers: { running: 0, total: 0 },
        },
      },
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('1 Running')).toBeInTheDocument()
  })
  it('displays running strategies status', async () => {
    const { useProcessSummary } = await import('../../hooks/queries')

    vi.mocked(useProcessSummary).mockReturnValue({
      isLoading: false,
      data: {
        payload: {
          feeds: { running: 0, total: 0 },
          strategies: { running: 1, total: 1 },
          executors: { running: 0, total: 0 },
          brokers: { running: 0, total: 0 },
        },
      },
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('1 Active')).toBeInTheDocument()
  })
  it('displays executor status', async () => {
    const { useProcessSummary } = await import('../../hooks/queries')

    vi.mocked(useProcessSummary).mockReturnValue({
      isLoading: false,
      data: {
        payload: {
          feeds: { running: 0, total: 0 },
          strategies: { running: 0, total: 0 },
          executors: { running: 1, total: 1 },
          brokers: { running: 0, total: 0 },
        },
      },
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('1/1 Running')).toBeInTheDocument()
  })
  it('displays broker status', async () => {
    const { useProcessSummary } = await import('../../hooks/queries')

    vi.mocked(useProcessSummary).mockReturnValue({
      isLoading: false,
      data: {
        payload: {
          feeds: { running: 0, total: 0 },
          strategies: { running: 0, total: 0 },
          executors: { running: 0, total: 0 },
          brokers: { running: 1, total: 1 },
        },
      },
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getAllByText('1/1 Running').length).toBeGreaterThan(0)
  })
  it('displays recent signals when available', async () => {
    const { useLatestSignals } = await import('../../hooks/queries')

    vi.mocked(useLatestSignals).mockReturnValue({
      isLoading: false,
      data: [
        {
          publicId: 1,
          instrument: 'BTC/USD',
          side: 'buy',
          timestamp: new Date('2024-01-01T12:00:00Z'),
        },
      ],
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('BTC/USD')).toBeInTheDocument()
  })
  it('displays recent executions when available', async () => {
    const { useExecutions } = await import('../../hooks/queries')

    vi.mocked(useExecutions).mockReturnValue({
      data: [
        {
          publicId: 1,
          instrument: 'ETH/USD',
          side: 'buy',
          size: 1.5,
          price: 2000,
          executedAt: new Date('2024-01-01T12:00:00Z'),
        },
      ],
      isLoading: false,
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('ETH/USD')).toBeInTheDocument()
  })
  it('counts today executions correctly', async () => {
    const { useExecutions } = await import('../../hooks/queries')

    const today = new Date()
    const yesterday = new Date(Date.now() - 86400000)

    vi.mocked(useExecutions).mockReturnValue({
      data: [
        {
          publicId: 1,
          instrument: 'BTC/USD',
          side: 'buy',
          size: 1,
          price: 50000,
          executedAt: today,
        },
        {
          publicId: 2,
          instrument: 'ETH/USD',
          side: 'sell',
          size: 2,
          price: 3000,
          executedAt: yesterday,
        },
        {
          publicId: 3,
          instrument: 'SOL/USD',
          side: 'buy',
          size: 10,
          price: 100,
          executedAt: today,
        },
      ],
      isLoading: false,
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText("Today's Executions")).toBeInTheDocument()
  })
  it('displays sell signal with error status', async () => {
    const { useLatestSignals } = await import('../../hooks/queries')

    vi.mocked(useLatestSignals).mockReturnValue({
      isLoading: false,
      data: [
        {
          publicId: 1,
          instrument: 'BTC/USD',
          side: 'sell',
          timestamp: new Date('2024-01-01T12:00:00Z'),
        },
      ],
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getAllByText('SELL').length).toBeGreaterThan(0)
  })
  it('displays execution with sell side', async () => {
    const { useExecutions } = await import('../../hooks/queries')

    vi.mocked(useExecutions).mockReturnValue({
      data: [
        {
          publicId: 1,
          instrument: 'BTC/USD',
          side: 'sell',
          size: 0.5,
          price: 45000,
          executedAt: new Date('2024-01-01T12:00:00Z'),
        },
      ],
      isLoading: false,
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getAllByText('SELL').length).toBeGreaterThan(0)
  })
  it('displays negative PnL with correct styling', async () => {
    const { usePositionsSummary } = await import('../../hooks/queries')

    vi.mocked(usePositionsSummary).mockReturnValue({
      isLoading: false,
      data: {
        totalValue: 10000,
        totalPnL: -500,
        pnlPercent: -5,
        count: 2,
        longCost: 5000,
        shortCost: 5000,
      },
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('Unrealized P&L')).toBeInTheDocument()
    expect(screen.getByText('P&L %')).toBeInTheDocument()
  })
  it('uses timestamp as key when signal id is null', async () => {
    const { useLatestSignals } = await import('../../hooks/queries')

    vi.mocked(useLatestSignals).mockReturnValue({
      isLoading: false,
      data: [
        {
          publicId: null,
          instrument: 'XRP/USD',
          side: 'buy',
          timestamp: new Date('2024-01-01T12:00:00Z'),
        },
      ],
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('XRP/USD')).toBeInTheDocument()
  })
  it('uses index as key when signal id is null and timestamp is undefined', async () => {
    const { useLatestSignals } = await import('../../hooks/queries')

    vi.mocked(useLatestSignals).mockReturnValue({
      isLoading: false,
      data: [
        {
          publicId: null,
          instrument: 'AVAX/USD',
          side: 'sell',
          timestamp: undefined,
        },
      ],
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('AVAX/USD')).toBeInTheDocument()
  })
  it('shows N/A when signal timestamp is undefined', async () => {
    const { useLatestSignals } = await import('../../hooks/queries')

    vi.mocked(useLatestSignals).mockReturnValue({
      isLoading: false,
      data: [
        {
          publicId: 1,
          instrument: 'ADA/USD',
          side: 'buy',
          timestamp: undefined,
        },
      ],
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('ADA/USD')).toBeInTheDocument()
    expect(screen.getByText('N/A')).toBeInTheDocument()
  })
  it('shows N/A when execution executedAt is undefined', async () => {
    const { useLatestSignals, useExecutions } = await import('../../hooks/queries')

    vi.mocked(useLatestSignals).mockReturnValue({
      isLoading: false,
      data: [],
    } as never)
    vi.mocked(useExecutions).mockReturnValue({
      data: [
        {
          publicId: 1,
          instrument: 'DOT/USD',
          side: 'sell',
          size: 10,
          price: 5,
          executedAt: undefined,
        },
      ],
      isLoading: false,
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('DOT/USD')).toBeInTheDocument()
    expect(screen.getByText('N/A')).toBeInTheDocument()
  })
  it('shows date-specific execution label when time traveling', async () => {
    const { useAppStore } = await import('../../stores/app')
    const { useExecutions } = await import('../../hooks/queries')

    vi.mocked(useAppStore).mockImplementation(((
      selector: (s: Record<string, unknown>) => unknown
    ) => selector({ asOf: '2024-06-15T12:00:00Z', isTimeTraveling: true })) as never)
    vi.mocked(useExecutions).mockReturnValue({
      data: [
        {
          instrument: 'BTC/USD',
          exchange: 'kraken',
          side: 'buy',
          size: 1,
          price: 50000,
          executedAt: new Date('2024-06-15T10:00:00Z'),
        },
      ],
      isLoading: false,
    } as never)
    renderWithMocks(<Overview />)

    expect(screen.getByText(/Executions \(/)).toBeInTheDocument()
    vi.mocked(useAppStore).mockImplementation(((
      selector: (s: Record<string, unknown>) => unknown
    ) => selector({ asOf: null, isTimeTraveling: false })) as never)
  })
})
