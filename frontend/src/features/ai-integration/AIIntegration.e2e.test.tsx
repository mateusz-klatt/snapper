import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, act, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { AIIntegration } from './AIIntegration'
import type { DelegateCreatedResponse, DelegateListResponse, DelegateRead } from '../../types/api'

const mockApiClient = vi.hoisted(() => ({
  getFeatureFlags: vi.fn(),
  listAiDelegates: vi.fn(),
  createAiDelegate: vi.fn(),
  getAiDelegate: vi.fn(),
  updateAiDelegateCaps: vi.fn(),
  deactivateAiDelegate: vi.fn(),
}))

vi.mock('../../lib/apiClient', () => ({
  apiClient: mockApiClient,
  APIError: class APIError extends Error {},
}))

vi.mock('../../stores/auth', () => ({
  useAuth: vi.fn(() => ({ isAuthenticated: true })),
}))

vi.mock('../../stores/app', () => ({
  useAppStore: vi.fn((selector?: unknown) => {
    const state = {
      asOf: null,
      isTimeTraveling: false,
      currentOperatorPublicId: null,
      currentWalletPublicId: null,
    }

    if (typeof selector === 'function') {
      return (selector as (s: typeof state) => unknown)(state)
    }

    return state
  }),
}))

vi.mock('../../hooks/useIsReadOnly', () => ({
  useIsReadOnly: vi.fn(() => false),
}))

vi.mock('../../components/LiveOnlyNotice', () => ({
  LiveOnlyNotice: () => <div data-testid='live-only-notice' />,
}))

vi.mock('react-hot-toast', () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}))

const createdDelegate: DelegateRead = {
  public_id: '019db000-0000-7000-8000-000000000abc',
  username: 'ai-alpha-prop',
  label: 'alpha-prop',
  created_by_user_public_id: '019db000-0000-7000-8000-0000000000aa',
  created_at: '2026-04-21T12:00:00Z',
  is_active: true,
  caps: {
    max_open_orders: 10,
    max_daily_notional_usd: 5000,
    max_cancels_per_minute: null,
    max_order_quantity_per_instrument: null,
  },
}

const emptyList: DelegateListResponse = {
  type: 'delegate_list',
  sequence_id: 1,
  public_id: 'env-1',
  timestamp: '2026-04-21T12:00:00Z',
  session_id: 's',
  payload: [],
  count: 0,
}

const createdResponse: DelegateCreatedResponse = {
  type: 'delegate_created_response',
  sequence_id: 2,
  public_id: 'env-2',
  timestamp: '2026-04-21T12:00:00Z',
  session_id: 's',
  payload: {
    delegate: createdDelegate,
    access_token: 'access-token-xyz',
    refresh_token: 'refresh-token-xyz',
    expires_in: 900,
  },
}

function renderApp(): ReturnType<typeof render> {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  })
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  )

  return render(<AIIntegration />, { wrapper })
}

describe('AIIntegration — end-to-end create flow', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockApiClient.getFeatureFlags.mockResolvedValue({
      type: 'feature_flags_response',
      sequence_id: 1,
      public_id: 'p',
      timestamp: '2026-04-21T12:00:00Z',
      session_id: 's',
      payload: { ai_integration_enabled: true },
    })
    mockApiClient.listAiDelegates.mockResolvedValue(emptyList)
  })

  it('admin flow: create delegate → copy config snippet → close → list refetched', async () => {
    const user = userEvent.setup()

    mockApiClient.createAiDelegate.mockResolvedValueOnce(createdResponse)

    renderApp()

    await waitFor(() => {
      expect(screen.getByRole('heading', { name: 'AI Integration' })).toBeInTheDocument()
    })
    await waitFor(() => {
      expect(screen.getByText('No AI delegates yet')).toBeInTheDocument()
    })
    expect(mockApiClient.listAiDelegates).toHaveBeenCalled()

    await act(async () => {
      await user.click(screen.getByRole('button', { name: /Create delegate/ }))
    })
    expect(screen.getByRole('heading', { name: 'Create AI delegate' })).toBeInTheDocument()

    await act(async () => {
      await user.type(screen.getByLabelText('Label'), 'alpha-prop')
    })
    await act(async () => {
      await user.click(screen.getByRole('button', { name: 'Next' }))
    })
    expect(screen.getByRole('heading', { name: 'Scope' })).toBeInTheDocument()

    await act(async () => {
      await user.click(screen.getByRole('button', { name: 'Next' }))
    })
    expect(screen.getByRole('heading', { name: /Review & create/ })).toBeInTheDocument()

    const submitButtons = screen.getAllByRole('button', { name: /Create delegate/ })
    const wizardSubmit = submitButtons[submitButtons.length - 1]

    await act(async () => {
      await user.click(wizardSubmit)
    })

    expect(mockApiClient.createAiDelegate).toHaveBeenCalledWith({
      label: 'alpha-prop',
      caps: {
        max_order_quantity_per_instrument: null,
        max_open_orders: null,
        max_daily_notional_usd: null,
        max_cancels_per_minute: null,
      },
      operator_public_id: null,
    })

    await waitFor(() => {
      expect(screen.getByText(/Save these credentials now/)).toBeInTheDocument()
    })

    const snippet = screen.getByLabelText(/\.mcp-config\.json/) as HTMLTextAreaElement

    expect(snippet.value).toContain('SNAPPER_ACCESS_TOKEN')
    expect(snippet.value).toContain('access-token-xyz')
    expect(snippet.value).toContain('SNAPPER_REFRESH_TOKEN')
    expect(snippet.value).toContain('refresh-token-xyz')
    expect(snippet.value).toContain('/api/mcp')

    const clipboardSpy = vi.spyOn(navigator.clipboard, 'writeText')

    await act(async () => {
      await user.click(screen.getByRole('button', { name: /Copy config snippet to clipboard/ }))
    })
    expect(clipboardSpy).toHaveBeenCalledWith(expect.stringContaining('access-token-xyz'))

    mockApiClient.listAiDelegates.mockResolvedValueOnce({
      ...emptyList,
      payload: [createdDelegate],
      count: 1,
    })

    await act(async () => {
      await user.click(screen.getByRole('button', { name: /I have saved these/ }))
    })

    await waitFor(() => {
      expect(screen.queryByRole('heading', { name: 'Create AI delegate' })).not.toBeInTheDocument()
    })

    expect(screen.queryByLabelText(/\.mcp-config\.json/)).not.toBeInTheDocument()

    await waitFor(() => {
      expect(mockApiClient.listAiDelegates.mock.calls.length).toBeGreaterThanOrEqual(2)
    })
  })

  it('disabled feature flag prevents delegate fetch', async () => {
    mockApiClient.getFeatureFlags.mockResolvedValueOnce({
      type: 'feature_flags_response',
      sequence_id: 1,
      public_id: 'p',
      timestamp: '2026-04-21T12:00:00Z',
      session_id: 's',
      payload: { ai_integration_enabled: false },
    })
    renderApp()
    await waitFor(() => {
      expect(screen.getByText('AI Integration is disabled')).toBeInTheDocument()
    })
    expect(mockApiClient.listAiDelegates).not.toHaveBeenCalled()
  })
})
