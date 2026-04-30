import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { AIIntegration } from './AIIntegration'
import type { DelegateListResponse, DelegateRead } from '../../types/api'

const mockUseFeatureFlags = vi.fn()
const mockUseAiDelegates = vi.fn()

type AiDelegateQueryShape = {
  data: { payload: DelegateRead } | undefined
  isLoading: boolean
}
const mockUseAiDelegate = vi.fn<() => AiDelegateQueryShape>(() => ({
  data: undefined,
  isLoading: false,
}))
const mockUseIsReadOnly = vi.fn()

vi.mock('../../hooks/queries', () => ({
  useFeatureFlags: () => mockUseFeatureFlags(),
  useAiDelegates: () => mockUseAiDelegates(),
  useAiDelegate: (_id: string | null) => mockUseAiDelegate(),
  useUpdateAiDelegateCaps: vi.fn(() => ({ mutateAsync: vi.fn(), isPending: false })),
  useDeactivateAiDelegate: vi.fn(() => ({ mutateAsync: vi.fn() })),
  useCreateAiDelegate: vi.fn(() => ({ mutateAsync: vi.fn(), reset: vi.fn(), isPending: false })),
}))

vi.mock('../../hooks/useIsReadOnly', () => ({
  useIsReadOnly: () => mockUseIsReadOnly(),
}))

vi.mock('../../components/LiveOnlyNotice', () => ({
  LiveOnlyNotice: () => <div data-testid='live-only-notice' />,
}))

vi.mock('react-hot-toast', () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}))

const renderWithProviders = (ui: ReactNode): ReturnType<typeof render> => {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })

  return render(<QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>)
}

const mkDelegate = (overrides: Partial<DelegateRead> = {}): DelegateRead => ({
  public_id: '019db000-0000-7000-8000-000000000001',
  username: 'ai-alpha',
  label: 'Alpha',
  created_by_user_public_id: 'u-1',
  created_at: '2026-04-21T00:00:00Z',
  is_active: true,
  caps: {
    max_open_orders: 10,
    max_daily_notional_usd: 5000,
    max_cancels_per_minute: null,
    max_order_quantity_per_instrument: null,
  },
  ...overrides,
})

const mkListResponse = (payload: DelegateRead[]): DelegateListResponse => ({
  type: 'delegate_list',
  sequence_id: 1,
  public_id: 'env-1',
  timestamp: '2026-04-21T00:00:00Z',
  session_id: 's',
  payload,
  count: payload.length,
})

describe('AIIntegration', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockUseIsReadOnly.mockReturnValue(false)
    mockUseAiDelegates.mockReturnValue({ data: undefined, isLoading: false })
  })

  it('renders_loading_shell_while_feature_flag_pending', () => {
    mockUseFeatureFlags.mockReturnValue({ isEnabled: false, isLoading: true })
    renderWithProviders(<AIIntegration />)
    expect(screen.getByText(/Loading AI integration/)).toBeInTheDocument()
    expect(mockUseAiDelegates).not.toHaveBeenCalled()
  })

  it('renders_disabled_panel_when_feature_flag_false', () => {
    mockUseFeatureFlags.mockReturnValue({ isEnabled: false, isLoading: false })
    renderWithProviders(<AIIntegration />)
    expect(screen.getByText('AI Integration is disabled')).toBeInTheDocument()
    expect(mockUseAiDelegates).not.toHaveBeenCalled()
  })

  it('renders_disabled_panel_on_fetch_error', () => {
    mockUseFeatureFlags.mockReturnValue({ isEnabled: false, isLoading: false, isError: true })
    renderWithProviders(<AIIntegration />)
    expect(screen.getByText('AI Integration is disabled')).toBeInTheDocument()
    expect(mockUseAiDelegates).not.toHaveBeenCalled()
  })

  it('enabled_shell_calls_useAiDelegates_when_feature_flag_true', () => {
    mockUseFeatureFlags.mockReturnValue({ isEnabled: true, isLoading: false })
    mockUseAiDelegates.mockReturnValue({ data: mkListResponse([]), isLoading: false })
    renderWithProviders(<AIIntegration />)
    expect(screen.getByRole('heading', { name: 'AI Integration' })).toBeInTheDocument()
    expect(mockUseAiDelegates).toHaveBeenCalled()
  })

  it('renders_loading_state_while_delegates_pending', () => {
    mockUseFeatureFlags.mockReturnValue({ isEnabled: true, isLoading: false })
    mockUseAiDelegates.mockReturnValue({ data: undefined, isLoading: true })
    renderWithProviders(<AIIntegration />)
    expect(screen.getByText(/Loading delegates/)).toBeInTheDocument()
  })

  it('renders_empty_state_when_no_delegates', () => {
    mockUseFeatureFlags.mockReturnValue({ isEnabled: true, isLoading: false })
    mockUseAiDelegates.mockReturnValue({ data: mkListResponse([]), isLoading: false })
    renderWithProviders(<AIIntegration />)
    expect(screen.getByText('No AI delegates yet')).toBeInTheDocument()
  })

  it('renders_list_rows_when_delegates_present', () => {
    mockUseFeatureFlags.mockReturnValue({ isEnabled: true, isLoading: false })
    mockUseAiDelegates.mockReturnValue({
      data: mkListResponse([mkDelegate({ label: 'Alpha', is_active: true })]),
      isLoading: false,
    })
    renderWithProviders(<AIIntegration />)
    expect(screen.getByText('Alpha')).toBeInTheDocument()
    expect(screen.getByText('ai-alpha')).toBeInTheDocument()
    expect(screen.getByText('Active')).toBeInTheDocument()
  })

  it('marks_revoked_delegate_rows', () => {
    mockUseFeatureFlags.mockReturnValue({ isEnabled: true, isLoading: false })
    mockUseAiDelegates.mockReturnValue({
      data: mkListResponse([mkDelegate({ is_active: false })]),
      isLoading: false,
    })
    renderWithProviders(<AIIntegration />)
    expect(screen.getByText('Revoked')).toBeInTheDocument()
  })

  it('shows_default_label_when_caps_are_null', () => {
    mockUseFeatureFlags.mockReturnValue({ isEnabled: true, isLoading: false })
    mockUseAiDelegates.mockReturnValue({
      data: mkListResponse([
        mkDelegate({
          caps: {
            max_open_orders: null,
            max_daily_notional_usd: null,
            max_cancels_per_minute: null,
            max_order_quantity_per_instrument: null,
          },
        }),
      ]),
      isLoading: false,
    })
    renderWithProviders(<AIIntegration />)
    const defaultCells = screen.getAllByText('default')

    expect(defaultCells.length).toBeGreaterThanOrEqual(2)
  })

  it('create_button_disabled_when_read_only', () => {
    mockUseFeatureFlags.mockReturnValue({ isEnabled: true, isLoading: false })
    mockUseIsReadOnly.mockReturnValue(true)
    mockUseAiDelegates.mockReturnValue({ data: mkListResponse([]), isLoading: false })
    renderWithProviders(<AIIntegration />)
    expect(screen.getByRole('button', { name: /Create delegate/ })).toBeDisabled()
  })

  it('opens_wizard_modal_when_create_clicked', () => {
    mockUseFeatureFlags.mockReturnValue({ isEnabled: true, isLoading: false })
    mockUseAiDelegates.mockReturnValue({ data: mkListResponse([]), isLoading: false })
    renderWithProviders(<AIIntegration />)
    fireEvent.click(screen.getByRole('button', { name: /Create delegate/ }))
    expect(screen.getByRole('heading', { name: 'Create AI delegate' })).toBeInTheDocument()
  })

  it('closes_wizard_modal_via_x_button', () => {
    mockUseFeatureFlags.mockReturnValue({ isEnabled: true, isLoading: false })
    mockUseAiDelegates.mockReturnValue({ data: mkListResponse([]), isLoading: false })
    renderWithProviders(<AIIntegration />)
    fireEvent.click(screen.getByRole('button', { name: /Create delegate/ }))
    expect(screen.getByRole('heading', { name: 'Create AI delegate' })).toBeInTheDocument()
    // Click the close-modal backdrop which wires up to onClose → setWizardOpen(false)
    const closeBackdrop = screen.getByRole('button', { name: 'Close modal' })

    fireEvent.click(closeBackdrop)
    expect(screen.queryByRole('heading', { name: 'Create AI delegate' })).not.toBeInTheDocument()
  })

  it('navigates_to_detail_view_when_row_details_clicked', () => {
    const delegate = mkDelegate()

    mockUseFeatureFlags.mockReturnValue({ isEnabled: true, isLoading: false })
    mockUseAiDelegates.mockReturnValue({
      data: mkListResponse([delegate]),
      isLoading: false,
    })
    mockUseAiDelegate.mockReturnValue({
      data: { payload: delegate },
      isLoading: false,
    })
    renderWithProviders(<AIIntegration />)
    fireEvent.click(screen.getByRole('button', { name: /View delegate Alpha/ }))
    expect(screen.getByRole('button', { name: /Back to list/ })).toBeInTheDocument()
  })

  it('returns_to_list_when_detail_back_clicked', () => {
    const delegate = mkDelegate()

    mockUseFeatureFlags.mockReturnValue({ isEnabled: true, isLoading: false })
    mockUseAiDelegates.mockReturnValue({
      data: mkListResponse([delegate]),
      isLoading: false,
    })
    mockUseAiDelegate.mockReturnValue({
      data: { payload: delegate },
      isLoading: false,
    })
    renderWithProviders(<AIIntegration />)
    fireEvent.click(screen.getByRole('button', { name: /View delegate Alpha/ }))
    fireEvent.click(screen.getByRole('button', { name: /Back to list/ }))
    expect(screen.getByRole('heading', { name: 'AI Integration' })).toBeInTheDocument()
  })
})
