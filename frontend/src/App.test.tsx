import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, waitFor, screen, fireEvent } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import App from './App'
import { useAppStore } from './stores/app'

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
vi.mock('./components/auth/UserProfile', () => ({
  default: () => <div data-testid='user-profile'>User Profile</div>,
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
    globalThis.localStorage.clear()
    vi.mocked(useAppStore).mockReturnValue({
      isConnected: false,
      connectionLag: 0,
      subscribedTopics: [],
      setConnected: vi.fn(),
      setConnectionLag: vi.fn(),
      updateLastUpdate: vi.fn(),
    })
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
      expect(screen.getByText('Trading Console')).toBeInTheDocument()
    })
  })
  it('renders navigation tabs', async () => {
    renderWithProviders(<App />)
    await waitFor(() => {
      expect(screen.getAllByText('Overview').length).toBeGreaterThan(0)
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
  it('renders connected status when app shell is connected', async () => {
    vi.mocked(useAppStore).mockReturnValue({
      isConnected: true,
      connectionLag: 10,
      subscribedTopics: ['a', 'b', 'c', 'd'],
      setConnected: vi.fn(),
      setConnectionLag: vi.fn(),
      updateLastUpdate: vi.fn(),
    })
    renderWithProviders(<App />)

    await waitFor(() => {
      expect(screen.getByText('Connected')).toBeInTheDocument()
    })
  })
  it('shows unknown lag when lag value is negative', async () => {
    vi.mocked(useAppStore).mockReturnValue({
      isConnected: true,
      connectionLag: -1,
      subscribedTopics: ['a'],
      setConnected: vi.fn(),
      setConnectionLag: vi.fn(),
      updateLastUpdate: vi.fn(),
    })
    renderWithProviders(<App />)

    await waitFor(() => {
      expect(screen.getByText('Lag: Unknown')).toBeInTheDocument()
    })
  })
  it('opens sidebar when hamburger menu is clicked', async () => {
    renderWithProviders(<App />)
    await waitFor(() => {
      expect(screen.getByLabelText('Open sidebar')).toBeInTheDocument()
    })
    const sidebar = document.querySelector('aside')

    expect(sidebar).toBeDefined()
    expect(sidebar?.className).toContain('-translate-x-full')
    fireEvent.click(screen.getByLabelText('Open sidebar'))
    expect(sidebar?.className).toContain('translate-x-0')
    expect(sidebar?.className).not.toContain('-translate-x-full')
  })
  it('closes sidebar when overlay backdrop is clicked', async () => {
    renderWithProviders(<App />)
    await waitFor(() => {
      expect(screen.getByLabelText('Open sidebar')).toBeInTheDocument()
    })
    fireEvent.click(screen.getByLabelText('Open sidebar'))
    const backdrop = screen
      .getAllByLabelText('Close sidebar')
      .find(el => el.className.includes('fixed inset-0'))

    expect(backdrop).toBeDefined()
    fireEvent.click(backdrop as HTMLElement)
    const sidebar = document.querySelector('aside')

    expect(sidebar?.className).toContain('-translate-x-full')
  })
  it('closes sidebar when Escape key is pressed on overlay', async () => {
    renderWithProviders(<App />)
    await waitFor(() => {
      expect(screen.getByLabelText('Open sidebar')).toBeInTheDocument()
    })
    fireEvent.click(screen.getByLabelText('Open sidebar'))
    const backdrop = screen
      .getAllByLabelText('Close sidebar')
      .find(el => el.className.includes('fixed inset-0'))

    expect(backdrop).toBeDefined()
    fireEvent.keyDown(backdrop as HTMLElement, { key: 'Escape' })
    const sidebar = document.querySelector('aside')

    expect(sidebar?.className).toContain('-translate-x-full')
  })
  it('closes sidebar when close button inside sidebar is clicked', async () => {
    renderWithProviders(<App />)
    await waitFor(() => {
      expect(screen.getByLabelText('Open sidebar')).toBeInTheDocument()
    })
    fireEvent.click(screen.getByLabelText('Open sidebar'))
    const closeBtn = screen
      .getAllByLabelText('Close sidebar')
      .find(el => el.tagName === 'BUTTON' && !el.className.includes('fixed inset-0'))

    expect(closeBtn).toBeDefined()
    fireEvent.click(closeBtn as HTMLElement)
    const sidebar = document.querySelector('aside')

    expect(sidebar?.className).toContain('-translate-x-full')
  })
  it('closes sidebar when a navigation tab is clicked', async () => {
    const { useTabRouting } = await import('./hooks/useHashRouting')
    const mockNavigate = vi.fn()

    vi.mocked(useTabRouting).mockReturnValue(['overview', mockNavigate])
    renderWithProviders(<App />)
    await waitFor(() => {
      expect(screen.getByLabelText('Open sidebar')).toBeInTheDocument()
    })
    fireEvent.click(screen.getByLabelText('Open sidebar'))
    fireEvent.click(screen.getByText('Processes'))
    expect(mockNavigate).toHaveBeenCalledWith('processes')
  })
  it('does not close sidebar on non-Escape key press on overlay', async () => {
    renderWithProviders(<App />)
    await waitFor(() => {
      expect(screen.getByLabelText('Open sidebar')).toBeInTheDocument()
    })
    fireEvent.click(screen.getByLabelText('Open sidebar'))
    const backdrop = screen
      .getAllByLabelText('Close sidebar')
      .find(el => el.className.includes('fixed inset-0'))

    expect(backdrop).toBeDefined()
    fireEvent.keyDown(backdrop as HTMLElement, { key: 'Enter' })
    const sidebar = document.querySelector('aside')

    expect(sidebar?.className).toContain('translate-x-0')
    expect(sidebar?.className).not.toContain('-translate-x-full')
  })
})
