import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, waitFor, screen } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import App from './App'

vi.mock('./stores/app', () => ({
  useAppStore: vi.fn(() => ({
    isConnected: false,
    connectionLag: 0,
    subscribedTopics: [],
    setConnected: vi.fn(),
    setConnectionLag: vi.fn(),
    updateLastUpdate: vi.fn(),
  })),
}))
vi.mock('./stores/process', () => ({
  useProcessStore: vi.fn(() => ({
    feeds: {},
    strategies: {},
    executor: null,
    broker: null,
    updateFeedStatus: vi.fn(),
    updateExecutorStatus: vi.fn(),
    updateBrokerStatus: vi.fn(),
  })),
}))
vi.mock('./stores/market', () => ({
  useMarketStore: vi.fn(() => ({
    selectedInstrument: null,
    updateLastPrice: vi.fn(),
  })),
}))
vi.mock('./stores/trade', () => ({
  useTradeStore: vi.fn(() => ({
    orders: [],
    executions: [],
    positions: [],
    signals: [],
    addOrder: vi.fn(),
    addExecution: vi.fn(),
    addSignal: vi.fn(),
  })),
}))
vi.mock('./stores/auth', () => ({
  useAuth: vi.fn(() => ({
    user: null,
    isAuthenticated: false,
    canAccess: vi.fn(() => true),
  })),
}))
vi.mock('./stores/websocket', () => ({
  useWebSocketConnection: vi.fn(() => ({
    isConnected: false,
    isConnecting: false,
    error: null,
  })),
  useWebSocketStore: vi.fn(() => ({
    wsClient: null,
    subscribe: vi.fn(),
    unsubscribe: vi.fn(),
  })),
}))
vi.mock('./hooks/useHashRouting', () => ({
  useTabRouting: vi.fn(() => ['overview', vi.fn()]),
}))
vi.mock('./hooks/useWSDispatcher', () => ({
  useWSDispatcher: vi.fn(() => null),
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

describe('App', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })
  it('renders app', async () => {
    const { container } = renderWithProviders(<App />)

    await waitFor(() => {
      expect(container).toBeInTheDocument()
    })
  })
  it('renders connection bar', async () => {
    const { container } = renderWithProviders(<App />)

    await waitFor(() => {
      expect(container).toBeInTheDocument()
    })
  })
  it('renders header with title', async () => {
    renderWithProviders(<App />)
    await waitFor(() => {
      expect(screen.getByText('Snapper Trading Console')).toBeInTheDocument()
    })
  })
  it('renders navigation tabs', async () => {
    renderWithProviders(<App />)
    await waitFor(() => {
      expect(screen.getByText('Overview')).toBeInTheDocument()
    })
  })
  it('navigates to tab when clicked', async () => {
    const { useTabRouting } = await import('./hooks/useHashRouting')
    const mockNavigate = vi.fn()

    vi.mocked(useTabRouting).mockReturnValue(['overview', mockNavigate])
    renderWithProviders(<App />)
    await waitFor(() => {
      expect(screen.getByText('Processes')).toBeInTheDocument()
    })
    const processesTab = screen.getByText('Processes')

    processesTab.click()
    expect(mockNavigate).toHaveBeenCalledWith('processes')
  })
})
