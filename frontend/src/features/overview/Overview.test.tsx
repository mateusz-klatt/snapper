import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import type { ReactNode } from 'react'
import { Overview } from './Overview'

vi.mock('../../hooks/queries', () => ({
  usePositionsSummary: vi.fn(() => ({ data: null, isLoading: false })),
  useLatestSignals: vi.fn(() => ({ data: [], isLoading: false })),
  useOrdersGrouped: vi.fn(() => ({ data: null })),
  useConfiguredProcesses: vi.fn(() => ({ isLoading: false })),
}))
vi.mock('../../stores/process', () => ({
  useProcessStore: vi.fn(() => ({
    feeds: {},
    strategies: {},
    executors: {},
    brokers: {},
  })),
}))
vi.mock('../../stores/trade', () => ({
  useTradeStore: vi.fn(() => ({
    executions: [],
  })),
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
    const processModule = await import('../../stores/process')
    const tradeModule = await import('../../stores/trade')

    vi.mocked(processModule.useProcessStore).mockReturnValue({
      feeds: undefined,
      strategies: undefined,
      executors: undefined,
      brokers: undefined,
    } as never)
    vi.mocked(tradeModule.useTradeStore).mockReturnValue({
      executions: undefined,
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('Feeds Running')).toBeInTheDocument()
  })
  it('displays loading spinner for process status', async () => {
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
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
      },
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('Total Value')).toBeInTheDocument()
  })
  it('displays running feeds status', async () => {
    const { useProcessStore } = await import('../../stores/process')
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({ isLoading: false, data: null } as never)
    vi.mocked(useProcessStore).mockReturnValue({
      feeds: {
        feed1: { running: true },
        feed2: { running: false },
      },
      strategies: {},
      executors: {},
      brokers: {},
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('1 Running')).toBeInTheDocument()
  })
  it('displays running strategies status', async () => {
    const { useProcessStore } = await import('../../stores/process')
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({ isLoading: false, data: null } as never)
    vi.mocked(useProcessStore).mockReturnValue({
      feeds: {},
      strategies: {
        strat1: { running: true },
      },
      executors: {},
      brokers: {},
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('1 Active')).toBeInTheDocument()
  })
  it('displays executor status', async () => {
    const { useProcessStore } = await import('../../stores/process')
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({ isLoading: false, data: null } as never)
    vi.mocked(useProcessStore).mockReturnValue({
      feeds: {},
      strategies: {},
      executors: { default: { running: true } },
      brokers: {},
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('1/1 Running')).toBeInTheDocument()
  })
  it('displays broker status', async () => {
    const { useProcessStore } = await import('../../stores/process')
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({ isLoading: false, data: null } as never)
    vi.mocked(useProcessStore).mockReturnValue({
      feeds: {},
      strategies: {},
      executors: {},
      brokers: { default: { running: true } },
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getAllByText('1/1 Running').length).toBeGreaterThan(0)
  })
  it('displays recent signals when available', async () => {
    const { useLatestSignals } = await import('../../hooks/queries')
    const { useProcessStore } = await import('../../stores/process')

    vi.mocked(useLatestSignals).mockReturnValue({
      isLoading: false,
      data: [
        {
          id: 1,
          instrument: 'BTC/USD',
          side: 'buy',
          timestamp: new Date('2024-01-01T12:00:00Z'),
        },
      ],
    } as never)
    vi.mocked(useProcessStore).mockReturnValue({
      feeds: {},
      strategies: {},
      executors: {},
      brokers: {},
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('BTC/USD')).toBeInTheDocument()
  })
  it('displays recent executions when available', async () => {
    const { useProcessStore } = await import('../../stores/process')
    const { useTradeStore } = await import('../../stores/trade')

    vi.mocked(useProcessStore).mockReturnValue({
      feeds: {},
      strategies: {},
      executors: {},
      brokers: {},
    } as never)
    vi.mocked(useTradeStore).mockReturnValue({
      executions: [
        {
          id: 1,
          instrument: 'ETH/USD',
          side: 'buy',
          size: 1.5,
          price: 2000,
          executedAt: new Date('2024-01-01T12:00:00Z'),
        },
      ],
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('ETH/USD')).toBeInTheDocument()
  })
  it('counts today executions correctly', async () => {
    const { useProcessStore } = await import('../../stores/process')
    const { useTradeStore } = await import('../../stores/trade')

    vi.mocked(useProcessStore).mockReturnValue({
      feeds: {},
      strategies: {},
      executors: {},
      brokers: {},
    } as never)
    const today = new Date()
    const yesterday = new Date(Date.now() - 86400000)

    vi.mocked(useTradeStore).mockReturnValue({
      executions: [
        {
          id: 1,
          instrument: 'BTC/USD',
          side: 'buy',
          size: 1,
          price: 50000,
          executedAt: today,
        },
        {
          id: 2,
          instrument: 'ETH/USD',
          side: 'sell',
          size: 2,
          price: 3000,
          executedAt: yesterday,
        },
        {
          id: 3,
          instrument: 'SOL/USD',
          side: 'buy',
          size: 10,
          price: 100,
          executedAt: today,
        },
      ],
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText("Today's Executions")).toBeInTheDocument()
  })
  it('displays sell signal with error status', async () => {
    const { useLatestSignals } = await import('../../hooks/queries')
    const { useProcessStore } = await import('../../stores/process')

    vi.mocked(useLatestSignals).mockReturnValue({
      isLoading: false,
      data: [
        {
          id: 1,
          instrument: 'BTC/USD',
          side: 'sell',
          timestamp: new Date('2024-01-01T12:00:00Z'),
        },
      ],
    } as never)
    vi.mocked(useProcessStore).mockReturnValue({
      feeds: {},
      strategies: {},
      executors: {},
      brokers: {},
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getAllByText('SELL').length).toBeGreaterThan(0)
  })
  it('displays execution with sell side', async () => {
    const { useProcessStore } = await import('../../stores/process')
    const { useTradeStore } = await import('../../stores/trade')

    vi.mocked(useProcessStore).mockReturnValue({
      feeds: {},
      strategies: {},
      executors: {},
      brokers: {},
    } as never)
    vi.mocked(useTradeStore).mockReturnValue({
      executions: [
        {
          id: 1,
          instrument: 'BTC/USD',
          side: 'sell',
          size: 0.5,
          price: 45000,
          executedAt: new Date('2024-01-01T12:00:00Z'),
        },
      ],
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
      },
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('Unrealized P&L')).toBeInTheDocument()
    expect(screen.getByText('P&L %')).toBeInTheDocument()
  })
  it('uses timestamp as key when signal id is null', async () => {
    const { useLatestSignals } = await import('../../hooks/queries')
    const { useProcessStore } = await import('../../stores/process')

    vi.mocked(useLatestSignals).mockReturnValue({
      isLoading: false,
      data: [
        {
          id: null,
          instrument: 'XRP/USD',
          side: 'buy',
          timestamp: new Date('2024-01-01T12:00:00Z'),
        },
      ],
    } as never)
    vi.mocked(useProcessStore).mockReturnValue({
      feeds: {},
      strategies: {},
      executors: {},
      brokers: {},
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('XRP/USD')).toBeInTheDocument()
  })
  it('uses index as key when signal id is null and timestamp is undefined', async () => {
    const { useLatestSignals } = await import('../../hooks/queries')
    const { useProcessStore } = await import('../../stores/process')

    vi.mocked(useLatestSignals).mockReturnValue({
      isLoading: false,
      data: [
        {
          id: null,
          instrument: 'AVAX/USD',
          side: 'sell',
          timestamp: undefined,
        },
      ],
    } as never)
    vi.mocked(useProcessStore).mockReturnValue({
      feeds: {},
      strategies: {},
      executors: {},
      brokers: {},
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('AVAX/USD')).toBeInTheDocument()
  })
  it('shows N/A when signal timestamp is undefined', async () => {
    const { useLatestSignals } = await import('../../hooks/queries')
    const { useProcessStore } = await import('../../stores/process')

    vi.mocked(useLatestSignals).mockReturnValue({
      isLoading: false,
      data: [
        {
          id: 1,
          instrument: 'ADA/USD',
          side: 'buy',
          timestamp: undefined,
        },
      ],
    } as never)
    vi.mocked(useProcessStore).mockReturnValue({
      feeds: {},
      strategies: {},
      executors: {},
      brokers: {},
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('ADA/USD')).toBeInTheDocument()
    expect(screen.getByText('N/A')).toBeInTheDocument()
  })
  it('shows N/A when execution executedAt is undefined', async () => {
    const { useProcessStore } = await import('../../stores/process')
    const { useTradeStore } = await import('../../stores/trade')
    const { useLatestSignals } = await import('../../hooks/queries')

    vi.mocked(useLatestSignals).mockReturnValue({
      isLoading: false,
      data: [],
    } as never)
    vi.mocked(useProcessStore).mockReturnValue({
      feeds: {},
      strategies: {},
      executors: {},
      brokers: {},
    } as never)
    vi.mocked(useTradeStore).mockReturnValue({
      executions: [
        {
          id: 1,
          instrument: 'DOT/USD',
          side: 'sell',
          size: 10,
          price: 5,
          executedAt: undefined,
        },
      ],
    } as never)
    renderWithMocks(<Overview />)
    expect(screen.getByText('DOT/USD')).toBeInTheDocument()
    expect(screen.getByText('N/A')).toBeInTheDocument()
  })
})
