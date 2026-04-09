import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, act } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import ScopeGrantManagement from './ScopeGrantManagement'

vi.mock('../../../stores/auth', () => ({
  useAuthStore: vi.fn(() => ({
    hasPermission: vi.fn((p: string) => p === 'manage:scope_grants'),
  })),
}))
vi.mock('./ScopeGrantList', () => ({
  default: ({
    onCreateGrant,
    onHandover,
  }: {
    onCreateGrant: () => void
    onHandover: (g: unknown) => void
  }) => (
    <div data-testid='scope-grant-list'>
      <button onClick={onCreateGrant}>Create</button>
      <button
        onClick={() =>
          onHandover({
            type: 'scope_grant_info',
            public_id: 'sg-1',
            operator_public_id: 'op-1',
            wallet_public_id: 'w-1',
            scope_kind: 'underlying',
            underlying_public_id: 'BTC',
            instrument_public_id: null,
            note: null,
            known_to: '9999-12-31T23:59:59.999999Z',
            session_id: 's',
            sequence_id: 1,
            timestamp: '2026-01-01T00:00:00Z',
            granted_by_user_public_id: 'u-1',
          })
        }
      >
        Handover
      </button>
    </div>
  ),
}))
vi.mock('./ScopeGrantForm', () => ({
  default: ({ open, onClose }: { open: boolean; onClose: () => void }) =>
    open ? (
      <div data-testid='scope-grant-form'>
        <button onClick={onClose}>Close Form</button>
      </div>
    ) : null,
}))
vi.mock('./HandoverDialog', () => ({
  default: ({ open, onClose }: { open: boolean; onClose: () => void }) =>
    open ? (
      <div data-testid='handover-dialog'>
        <button onClick={onClose}>Close Handover</button>
      </div>
    ) : null,
}))

const { useAuthStore } = await import('../../../stores/auth')

function renderWithQuery(ui: React.ReactElement) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })

  return render(<QueryClientProvider client={qc}>{ui}</QueryClientProvider>)
}

describe('ScopeGrantManagement', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })
  it('renders list component when user has permission', () => {
    renderWithQuery(<ScopeGrantManagement />)
    expect(screen.getByTestId('scope-grant-list')).toBeDefined()
  })
  it('shows access denied when user lacks permission', () => {
    vi.mocked(useAuthStore).mockReturnValue({
      hasPermission: vi.fn(() => false),
    } as unknown as ReturnType<typeof useAuthStore>)
    renderWithQuery(<ScopeGrantManagement />)
    expect(screen.getByText('Access Denied')).toBeDefined()
    vi.mocked(useAuthStore).mockReturnValue({
      hasPermission: vi.fn((p: string) => p === 'manage:scope_grants'),
    } as unknown as ReturnType<typeof useAuthStore>)
  })
  it('opens create form when Create button clicked', () => {
    renderWithQuery(<ScopeGrantManagement />)
    act(() => {
      screen.getByText('Create').click()
    })
    expect(screen.getByTestId('scope-grant-form')).toBeDefined()
  })
  it('opens handover dialog when Handover button clicked', () => {
    renderWithQuery(<ScopeGrantManagement />)
    act(() => {
      screen.getByText('Handover').click()
    })
    expect(screen.getByTestId('handover-dialog')).toBeDefined()
  })
  it('closes create form when Close Form clicked', () => {
    renderWithQuery(<ScopeGrantManagement />)
    act(() => {
      screen.getByText('Create').click()
    })
    expect(screen.getByTestId('scope-grant-form')).toBeDefined()
    act(() => {
      screen.getByText('Close Form').click()
    })
    expect(screen.queryByTestId('scope-grant-form')).toBeNull()
  })
  it('closes handover dialog when Close Handover clicked', () => {
    renderWithQuery(<ScopeGrantManagement />)
    act(() => {
      screen.getByText('Handover').click()
    })
    expect(screen.getByTestId('handover-dialog')).toBeDefined()
    act(() => {
      screen.getByText('Close Handover').click()
    })
    expect(screen.queryByTestId('handover-dialog')).toBeNull()
  })
  it('passes readOnly prop', () => {
    renderWithQuery(<ScopeGrantManagement readOnly />)
    expect(screen.getByTestId('scope-grant-list')).toBeDefined()
  })
})
