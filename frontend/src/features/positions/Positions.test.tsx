import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { Positions } from './Positions'
import type { Position } from '../../types/entities'

vi.mock('../../hooks/queries', () => ({
  usePositions: vi.fn(() => ({
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

const makePosition = (overrides: Partial<Position> = {}): Position => ({
  sequenceId: 1,
  publicId: 'pos-1',
  timestamp: new Date('2026-04-06T12:00:00Z'),
  sessionId: 'sess-1',
  instrument: 'BTC-USD',
  exchange: 'kraken',
  quantity: 1.5,
  averagePrice: 50000,
  unrealizedPnl: 1000,
  realizedPnl: 250,
  ...overrides,
})

describe('Positions', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('renders the page title', () => {
    renderWithProviders(<Positions />)
    expect(screen.getByText('Positions')).toBeInTheDocument()
  })

  it('renders empty state when no positions are returned', async () => {
    const { usePositions } = await import('../../hooks/queries')

    vi.mocked(usePositions).mockReturnValue({
      data: [],
      isLoading: false,
    } as never)
    renderWithProviders(<Positions />)
    expect(screen.getByText('No open positions')).toBeInTheDocument()
  })

  it('renders skeletons while loading', async () => {
    const { usePositions } = await import('../../hooks/queries')

    vi.mocked(usePositions).mockReturnValue({
      data: undefined,
      isLoading: true,
    } as never)
    const { container } = renderWithProviders(<Positions />)

    expect(container.querySelectorAll('[data-testid="position-"]')).toHaveLength(0)
    expect(screen.getByText('Positions')).toBeInTheDocument()
  })

  it('renders a LONG position with green badge and absolute quantity', async () => {
    const long = makePosition({
      instrument: 'BTC-USD',
      quantity: 2.5,
      averagePrice: 50000,
      unrealizedPnl: 1000,
      realizedPnl: 0,
    })
    const { usePositions } = await import('../../hooks/queries')

    vi.mocked(usePositions).mockReturnValue({
      data: [long],
      isLoading: false,
    } as never)
    renderWithProviders(<Positions />)
    expect(screen.getByTestId('position-side-BTC-USD-kraken-live')).toHaveTextContent('LONG')
    expect(screen.getByText('2.5000')).toBeInTheDocument()
    expect(screen.getByTestId('position-unrealized-BTC-USD-kraken-live')).toHaveTextContent(
      '+$1000.00'
    )
  })

  it('renders a SHORT position with red badge and absolute quantity', async () => {
    const short = makePosition({
      instrument: 'ETH-USD',
      quantity: -3,
      averagePrice: 2000,
      unrealizedPnl: -150,
      realizedPnl: 50,
    })
    const { usePositions } = await import('../../hooks/queries')

    vi.mocked(usePositions).mockReturnValue({
      data: [short],
      isLoading: false,
    } as never)
    renderWithProviders(<Positions />)
    const sideBadge = screen.getByTestId('position-side-ETH-USD-kraken-live')

    expect(sideBadge).toHaveTextContent('SHORT')
    expect(sideBadge.className).toContain('text-loss-400')
    expect(screen.getByText('3.0000')).toBeInTheDocument()
    expect(screen.getByTestId('position-unrealized-ETH-USD-kraken-live')).toHaveTextContent(
      '-$150.00'
    )
    expect(screen.getByTestId('position-realized-ETH-USD-kraken-live')).toHaveTextContent('+$50.00')
  })

  it('renders a FLAT position with neutral badge and zero P&L formatting', async () => {
    const flat = makePosition({
      instrument: 'SOL-USD',
      quantity: 0,
      averagePrice: 100,
      unrealizedPnl: 0,
      realizedPnl: 0,
    })
    const { usePositions } = await import('../../hooks/queries')

    vi.mocked(usePositions).mockReturnValue({
      data: [flat],
      isLoading: false,
    } as never)
    renderWithProviders(<Positions />)
    const sideBadge = screen.getByTestId('position-side-SOL-USD-kraken-live')

    expect(sideBadge).toHaveTextContent('FLAT')
    expect(sideBadge.className).toContain('text-muted-400')
    expect(screen.getByTestId('position-unrealized-SOL-USD-kraken-live')).toHaveTextContent('$0.00')
  })

  it('renders multiple positions side by side', async () => {
    const long = makePosition({
      instrument: 'BTC-USD',
      quantity: 1,
      unrealizedPnl: 100,
    })
    const short = makePosition({
      instrument: 'ETH-USD',
      quantity: -2,
      unrealizedPnl: -50,
      publicId: 'pos-2',
    })
    const { usePositions } = await import('../../hooks/queries')

    vi.mocked(usePositions).mockReturnValue({
      data: [long, short],
      isLoading: false,
    } as never)
    renderWithProviders(<Positions />)
    expect(screen.getByTestId('position-side-BTC-USD-kraken-live')).toHaveTextContent('LONG')
    expect(screen.getByTestId('position-side-ETH-USD-kraken-live')).toHaveTextContent('SHORT')
  })

  it('disambiguates same instrument across exchanges and modes', async () => {
    const krakenLive = makePosition({
      instrument: 'BTC-USD',
      exchange: 'kraken',
      mode: 'live',
      quantity: 1,
      publicId: 'pos-k-live',
    })
    const krakenPaper = makePosition({
      instrument: 'BTC-USD',
      exchange: 'kraken',
      mode: 'paper',
      quantity: -1,
      publicId: 'pos-k-paper',
    })
    const { usePositions } = await import('../../hooks/queries')

    vi.mocked(usePositions).mockReturnValue({
      data: [krakenLive, krakenPaper],
      isLoading: false,
    } as never)
    renderWithProviders(<Positions />)
    expect(screen.getByTestId('position-side-BTC-USD-kraken-live')).toHaveTextContent('LONG')
    expect(screen.getByTestId('position-side-BTC-USD-kraken-paper')).toHaveTextContent('SHORT')
  })
})
