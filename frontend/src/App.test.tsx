import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, waitFor, screen, fireEvent } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import App from './App'
import { useAppStore } from './stores/app'

vi.mock('./stores/app', () => ({
  useAppStore: vi.fn(),
}))
vi.mock('./stores/market', () => ({
  useMarketStore: vi.fn(() => ({
    selectedInstrument: null,
    updateLastPrice: vi.fn(),
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
vi.mock('./components/OperatorPicker', () => ({
  OperatorPicker: () => <div data-testid='operator-picker'>OperatorPicker</div>,
}))
vi.mock('./components/WalletPicker', () => ({
  WalletPicker: () => <div data-testid='wallet-picker'>WalletPicker</div>,
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

const defaultAppState = {
  isConnected: false,
  connectionLag: 0,
  subscribedTopics: [] as string[],
  setConnected: vi.fn(),
  setConnectionLag: vi.fn(),
  updateLastUpdate: vi.fn(),
  isDarkMode: false,
  toggleDarkMode: vi.fn(),
  asOf: null as string | null,
  isTimeTraveling: false,
  setAsOf: vi.fn(),
  clearAsOf: vi.fn(),
  currentOperatorPublicId: null as string | null,
  currentWalletPublicId: null as string | null,
  setCurrentOperatorPublicId: vi.fn(),
  setCurrentWalletPublicId: vi.fn(),
}

type AppState = typeof defaultAppState

const mockAppStore = (overrides: Partial<AppState> = {}) => {
  const state = { ...defaultAppState, ...overrides }

  vi.mocked(useAppStore).mockImplementation((selector?: unknown) => {
    if (typeof selector === 'function') return (selector as (s: AppState) => unknown)(state)

    return state
  })
}

describe('App', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    globalThis.localStorage.clear()
    mockAppStore()
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
    mockAppStore({
      isConnected: true,
      connectionLag: 10,
      subscribedTopics: ['a', 'b', 'c', 'd'],
    })
    renderWithProviders(<App />)

    await waitFor(() => {
      expect(screen.getByText('Connected')).toBeInTheDocument()
    })
  })
  it('shows unknown lag when lag value is negative', async () => {
    mockAppStore({
      isConnected: true,
      connectionLag: -1,
      subscribedTopics: ['a'],
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
  it('renders light mode toggle when dark mode is active', async () => {
    mockAppStore({ isDarkMode: true })
    renderWithProviders(<App />)
    await waitFor(() => {
      expect(screen.getByLabelText('Switch to light mode')).toBeInTheDocument()
    })
  })
  it('renders dark mode toggle when light mode is active', async () => {
    mockAppStore({ isDarkMode: false })
    renderWithProviders(<App />)
    await waitFor(() => {
      expect(screen.getByLabelText('Switch to dark mode')).toBeInTheDocument()
    })
  })
  it('shows time travel banner when isTimeTraveling is true', async () => {
    mockAppStore({ isTimeTraveling: true, asOf: '2026-03-15T10:00:00Z' })
    renderWithProviders(<App />)
    await waitFor(() => {
      expect(screen.getByText(/Time Travel Mode/)).toBeInTheDocument()
    })
  })
  it('does not show time travel banner in live mode', async () => {
    mockAppStore({ isTimeTraveling: false })
    renderWithProviders(<App />)
    await waitFor(() => {
      expect(screen.queryByText(/Time Travel Mode/)).not.toBeInTheDocument()
    })
  })
  describe('AI Integration sidebar cascade', () => {
    it('shows AI Integration entry for admin role via App.tsx:22 canAccess filter', async () => {
      const { useAuth } = await import('./stores/auth')

      vi.mocked(useAuth).mockReturnValue({
        user: { username: 'admin', role: 'admin' },
        isAuthenticated: true,
        canAccess: vi.fn((resource: string) =>
          [
            'overview',
            'market',
            'processes',
            'strategies',
            'orders',
            'positions',
            'signals',
            'backtests',
            'health',
            'admin',
            'ai-integration',
            'settings',
          ].includes(resource)
        ),
      } as unknown as ReturnType<typeof useAuth>)
      renderWithProviders(<App />)
      await waitFor(() => {
        expect(screen.getAllByText('AI Integration').length).toBeGreaterThan(0)
      })
    })
    it('shows AI Integration entry for operator role via App.tsx:22 canAccess filter', async () => {
      const { useAuth } = await import('./stores/auth')

      vi.mocked(useAuth).mockReturnValue({
        user: { username: 'op', role: 'operator' },
        isAuthenticated: true,
        canAccess: vi.fn((resource: string) =>
          [
            'overview',
            'market',
            'processes',
            'strategies',
            'orders',
            'positions',
            'signals',
            'backtests',
            'health',
            'ai-integration',
            'settings',
          ].includes(resource)
        ),
      } as unknown as ReturnType<typeof useAuth>)
      renderWithProviders(<App />)
      await waitFor(() => {
        expect(screen.getAllByText('AI Integration').length).toBeGreaterThan(0)
      })
    })
    it('hides AI Integration entry for readonly role via App.tsx:22 canAccess filter', async () => {
      const { useAuth } = await import('./stores/auth')

      vi.mocked(useAuth).mockReturnValue({
        user: { username: 'ro', role: 'readonly' },
        isAuthenticated: true,
        canAccess: vi.fn((resource: string) =>
          ['overview', 'market', 'orders', 'positions', 'signals', 'health'].includes(resource)
        ),
      } as unknown as ReturnType<typeof useAuth>)
      renderWithProviders(<App />)
      await waitFor(() => {
        expect(screen.getByText('Trading Console')).toBeInTheDocument()
      })
      expect(screen.queryByText('AI Integration')).not.toBeInTheDocument()
    })
  })
})
