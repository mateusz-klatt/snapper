import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { AppRoutes } from './AppRoutes'

vi.mock('./auth/ProtectedRoute', () => ({
  default: ({ children }: { children: ReactNode }) => <>{children}</>,
}))
vi.mock('../features/overview/Overview', () => ({
  Overview: () => <div data-testid='overview'>Overview Component</div>,
}))
vi.mock('../features/market/MarketData', () => ({
  MarketData: () => <div data-testid='market-data'>Market Data Component</div>,
}))
vi.mock('../features/processes/Processes', () => ({
  Processes: () => <div data-testid='processes'>Processes Component</div>,
}))
vi.mock('../features/strategies/Strategies', () => ({
  Strategies: () => <div data-testid='strategies'>Strategies Component</div>,
}))
vi.mock('../features/orders/Orders', () => ({
  Orders: () => <div data-testid='orders'>Orders Component</div>,
}))
vi.mock('../features/positions/Positions', () => ({
  Positions: () => <div data-testid='positions'>Positions Component</div>,
}))
vi.mock('../features/signals/Signals', () => ({
  Signals: () => <div data-testid='signals'>Signals Component</div>,
}))
vi.mock('../features/backtests/Backtests', () => ({
  Backtests: () => <div data-testid='backtests'>Backtests Component</div>,
}))
vi.mock('../features/backtests/BacktestDetailPage', () => ({
  BacktestDetailPage: ({ runPublicId }: { runPublicId: string }) => (
    <div data-testid='backtest-detail'>{runPublicId}</div>
  ),
}))
vi.mock('../features/backtests/ComparePage', () => ({
  ComparePage: ({ comparisonPublicId }: { comparisonPublicId: string }) => (
    <div data-testid='compare-page'>{comparisonPublicId}</div>
  ),
}))
vi.mock('../features/health/Health', () => ({
  Health: () => <div data-testid='health'>Health Component</div>,
}))
vi.mock('../features/admin/Admin', () => ({
  Admin: () => <div data-testid='admin'>Admin Component</div>,
}))
vi.mock('../features/ai-integration/AIIntegration', () => ({
  AIIntegration: () => <div data-testid='ai-integration'>AI Integration Component</div>,
}))
vi.mock('../features/ai-reviews/AiReviewInbox', () => ({
  AiReviewInbox: () => <div data-testid='ai-reviews'>AI Reviews Component</div>,
}))
vi.mock('../features/settings/Settings', () => ({
  Settings: () => <div data-testid='settings'>Settings Component</div>,
}))
const createQueryClient = (): QueryClient =>
  new QueryClient({
    defaultOptions: {
      queries: { retry: false },
    },
  })

const renderWithProviders = (ui: ReactNode): ReturnType<typeof render> => {
  const queryClient = createQueryClient()

  return render(<QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>)
}

describe('AppRoutes', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })
  it('renders Overview component for overview tab', async () => {
    renderWithProviders(<AppRoutes activeTab='overview' />)
    await waitFor(() => {
      expect(screen.getByTestId('overview')).toBeTruthy()
    })
  })
  it('renders Market Data component for market tab', async () => {
    renderWithProviders(<AppRoutes activeTab='market' />)
    await waitFor(() => {
      expect(screen.getByTestId('market-data')).toBeTruthy()
    })
  })
  it('renders Processes component for processes tab', async () => {
    renderWithProviders(<AppRoutes activeTab='processes' />)
    await waitFor(() => {
      expect(screen.getByTestId('processes')).toBeTruthy()
    })
  })
  it('renders Strategies component for strategies tab', async () => {
    renderWithProviders(<AppRoutes activeTab='strategies' />)
    await waitFor(() => {
      expect(screen.getByTestId('strategies')).toBeTruthy()
    })
  })
  it('renders Orders component for orders tab', async () => {
    renderWithProviders(<AppRoutes activeTab='orders' />)
    await waitFor(() => {
      expect(screen.getByTestId('orders')).toBeTruthy()
    })
  })
  it('renders Positions component for positions tab', async () => {
    renderWithProviders(<AppRoutes activeTab='positions' />)
    await waitFor(() => {
      expect(screen.getByTestId('positions')).toBeTruthy()
    })
  })
  it('renders Signals component for signals tab', async () => {
    renderWithProviders(<AppRoutes activeTab='signals' />)
    await waitFor(() => {
      expect(screen.getByTestId('signals')).toBeTruthy()
    })
  })
  it('renders Backtests component for backtests tab', async () => {
    globalThis.location.hash = '#backtests'
    renderWithProviders(<AppRoutes activeTab='backtests' />)
    await waitFor(() => {
      expect(screen.getByTestId('backtests')).toBeTruthy()
    })
  })
  it('renders BacktestDetailPage when hash has a sub-path', async () => {
    globalThis.location.hash = '#backtests/run-xyz'
    renderWithProviders(<AppRoutes activeTab='backtests' />)
    await waitFor(() => {
      expect(screen.getByTestId('backtest-detail')).toBeTruthy()
    })
    expect(screen.getByTestId('backtest-detail').textContent).toBe('run-xyz')
  })
  it('renders ComparePage when hash matches #backtests/compare/{id}', async () => {
    globalThis.location.hash = '#backtests/compare/cmp-1'
    renderWithProviders(<AppRoutes activeTab='backtests' />)
    await waitFor(() => {
      expect(screen.getByTestId('compare-page')).toBeTruthy()
    })
    expect(screen.getByTestId('compare-page').textContent).toBe('cmp-1')
  })
  it('falls back to Backtests list for malformed #backtests/compare (no id)', async () => {
    globalThis.location.hash = '#backtests/compare'
    renderWithProviders(<AppRoutes activeTab='backtests' />)
    await waitFor(() => {
      expect(screen.getByTestId('backtests')).toBeTruthy()
    })
    expect(screen.queryByTestId('backtest-detail')).toBeNull()
    expect(screen.queryByTestId('compare-page')).toBeNull()
  })
  it('renders Health component for health tab', async () => {
    renderWithProviders(<AppRoutes activeTab='health' />)
    await waitFor(() => {
      expect(screen.getByTestId('health')).toBeTruthy()
    })
  })
  it('renders Admin component for admin tab', async () => {
    renderWithProviders(<AppRoutes activeTab='admin' />)
    await waitFor(() => {
      expect(screen.getByTestId('admin')).toBeTruthy()
    })
  })
  it('renders AIIntegration component for ai-integration tab', async () => {
    renderWithProviders(<AppRoutes activeTab='ai-integration' />)
    await waitFor(() => {
      expect(screen.getByTestId('ai-integration')).toBeTruthy()
    })
  })
  it('renders AiReviewInbox component for ai-reviews tab', async () => {
    renderWithProviders(<AppRoutes activeTab='ai-reviews' />)
    await waitFor(() => {
      expect(screen.getByTestId('ai-reviews')).toBeTruthy()
    })
  })
  it('renders Settings component for settings tab', async () => {
    renderWithProviders(<AppRoutes activeTab='settings' />)
    await waitFor(() => {
      expect(screen.getByTestId('settings')).toBeTruthy()
    })
  })
  it('renders Overview as default for unknown tab', async () => {
    renderWithProviders(<AppRoutes activeTab='unknown-tab' />)
    await waitFor(() => {
      expect(screen.getByTestId('overview')).toBeTruthy()
    })
  })
  it('renders Overview as default for empty tab', async () => {
    renderWithProviders(<AppRoutes activeTab='' />)
    await waitFor(() => {
      expect(screen.getByTestId('overview')).toBeTruthy()
    })
  })
})
