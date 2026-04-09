import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, act } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import CredentialManagement from './CredentialManagement'

vi.mock('../../../stores/auth', () => ({
  useAuthStore: vi.fn(() => ({
    hasPermission: vi.fn((p: string) => p === 'manage:wallet_credentials'),
  })),
}))
vi.mock('./CredentialList', () => ({
  default: ({
    onCreateCredential,
    onRotate,
  }: {
    onCreateCredential: () => void
    onRotate: (c: unknown) => void
  }) => (
    <div data-testid='credential-list'>
      <button onClick={onCreateCredential}>Create</button>
      <button
        onClick={() =>
          onRotate({
            type: 'credential_summary',
            public_id: 'cred-1',
            wallet_public_id: 'w-1',
            exchange: 'kraken',
            credential_type: 'api_key_secret',
            label: 'main',
            session_id: 's',
            sequence_id: 1,
            timestamp: '2026-01-01T00:00:00Z',
          })
        }
      >
        Rotate
      </button>
    </div>
  ),
}))
vi.mock('./CredentialForm', () => ({
  default: ({ open, onClose }: { open: boolean; onClose: () => void }) =>
    open ? (
      <div data-testid='credential-form'>
        <button onClick={onClose}>Close Form</button>
      </div>
    ) : null,
}))
vi.mock('./RotateDialog', () => ({
  default: ({ open, onClose }: { open: boolean; onClose: () => void }) =>
    open ? (
      <div data-testid='rotate-dialog'>
        <button onClick={onClose}>Close Rotate</button>
      </div>
    ) : null,
}))

const { useAuthStore } = await import('../../../stores/auth')

function renderWithQuery(ui: React.ReactElement) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })

  return render(<QueryClientProvider client={qc}>{ui}</QueryClientProvider>)
}

describe('CredentialManagement', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })
  it('renders list component when user has permission', () => {
    renderWithQuery(<CredentialManagement />)
    expect(screen.getByTestId('credential-list')).toBeDefined()
  })
  it('shows access denied when user lacks permission', () => {
    vi.mocked(useAuthStore).mockReturnValue({
      hasPermission: vi.fn(() => false),
    } as unknown as ReturnType<typeof useAuthStore>)
    renderWithQuery(<CredentialManagement />)
    expect(screen.getByText('Access Denied')).toBeDefined()
    vi.mocked(useAuthStore).mockReturnValue({
      hasPermission: vi.fn((p: string) => p === 'manage:wallet_credentials'),
    } as unknown as ReturnType<typeof useAuthStore>)
  })
  it('opens create form when Create button clicked', () => {
    renderWithQuery(<CredentialManagement />)
    act(() => {
      screen.getByText('Create').click()
    })
    expect(screen.getByTestId('credential-form')).toBeDefined()
  })
  it('opens rotate dialog when Rotate button clicked', () => {
    renderWithQuery(<CredentialManagement />)
    act(() => {
      screen.getByText('Rotate').click()
    })
    expect(screen.getByTestId('rotate-dialog')).toBeDefined()
  })
  it('closes create form when Close Form clicked', () => {
    renderWithQuery(<CredentialManagement />)
    act(() => {
      screen.getByText('Create').click()
    })
    expect(screen.getByTestId('credential-form')).toBeDefined()
    act(() => {
      screen.getByText('Close Form').click()
    })
    expect(screen.queryByTestId('credential-form')).toBeNull()
  })
  it('closes rotate dialog when Close Rotate clicked', () => {
    renderWithQuery(<CredentialManagement />)
    act(() => {
      screen.getByText('Rotate').click()
    })
    expect(screen.getByTestId('rotate-dialog')).toBeDefined()
    act(() => {
      screen.getByText('Close Rotate').click()
    })
    expect(screen.queryByTestId('rotate-dialog')).toBeNull()
  })
  it('passes readOnly prop', () => {
    renderWithQuery(<CredentialManagement readOnly />)
    expect(screen.getByTestId('credential-list')).toBeDefined()
  })
})
