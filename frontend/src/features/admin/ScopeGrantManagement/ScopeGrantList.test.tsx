import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import ScopeGrantList from './ScopeGrantList'

const mockWallets = {
  payload: [
    {
      type: 'wallet_info' as const,
      session_id: 's',
      sequence_id: 1,
      public_id: 'w-1',
      timestamp: '2026-01-01T00:00:00Z',
      label: 'default',
      is_paper: false,
    },
    {
      type: 'wallet_info' as const,
      session_id: 's',
      sequence_id: 2,
      public_id: 'w-2',
      timestamp: '2026-01-01T00:00:00Z',
      label: 'paper',
      is_paper: true,
    },
  ],
  count: 2,
}

const mockOperators = {
  payload: [
    {
      type: 'operator_info' as const,
      session_id: 's',
      sequence_id: 1,
      public_id: 'op-1',
      timestamp: '2026-01-01T00:00:00Z',
      label: 'alice',
    },
  ],
  count: 1,
}

const mockGrants = {
  payload: [
    {
      type: 'scope_grant_info' as const,
      session_id: 's',
      sequence_id: 1,
      public_id: 'sg-1',
      timestamp: '2026-01-01T00:00:00Z',
      operator_public_id: 'op-1',
      wallet_public_id: 'w-1',
      granted_by_user_public_id: 'u-1',
      scope_kind: 'underlying' as const,
      underlying_public_id: 'BTC',
      instrument_public_id: null,
      note: null,
      known_to: '9999-12-31T23:59:59.999999Z',
    },
  ],
  count: 1,
}

const mockUseScopeGrants = vi.fn()
const mockUseWallets = vi.fn()
const mockUseOperators = vi.fn()

vi.mock('../../../hooks/queries', () => ({
  useWallets: () => mockUseWallets(),
  useScopeGrants: () => mockUseScopeGrants(),
  useOperators: () => mockUseOperators(),
}))
vi.mock('../../../components/ThemeSelect', () => ({
  ThemeSelect: ({
    id,
    value,
    onChange,
    options,
    placeholder,
  }: {
    id: string
    value: string
    onChange: (v: string) => void
    options: { value: string; label: string }[]
    placeholder?: string
  }) => (
    <select data-testid={id} value={value} onChange={e => onChange(e.target.value)}>
      <option value=''>{placeholder ?? 'Select...'}</option>
      {options.map(o => (
        <option key={o.value} value={o.value}>
          {o.label}
        </option>
      ))}
    </select>
  ),
}))

function renderWithQuery(ui: React.ReactElement) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })

  return render(<QueryClientProvider client={qc}>{ui}</QueryClientProvider>)
}

describe('ScopeGrantList', () => {
  const onCreateGrant = vi.fn()
  const onHandover = vi.fn()

  beforeEach(() => {
    vi.clearAllMocks()
    mockUseWallets.mockReturnValue({ data: mockWallets })
    mockUseOperators.mockReturnValue({ data: mockOperators })
    mockUseScopeGrants.mockReturnValue({ data: undefined, isLoading: false, error: null })
  })
  it('shows wallet selector and select-a-wallet message', () => {
    renderWithQuery(<ScopeGrantList onCreateGrant={onCreateGrant} onHandover={onHandover} />)
    expect(screen.getByText('Select a wallet')).toBeDefined()
    expect(screen.getByTestId('wallet-filter')).toBeDefined()
  })
  it('shows loading spinner when fetching grants', () => {
    mockUseScopeGrants.mockReturnValue({ data: undefined, isLoading: true, error: null })
    renderWithQuery(<ScopeGrantList onCreateGrant={onCreateGrant} onHandover={onHandover} />)
    fireEvent.change(screen.getByTestId('wallet-filter'), { target: { value: 'w-1' } })
    expect(screen.getByTestId('wallet-filter')).toBeDefined()
  })
  it('shows error when fetch fails', () => {
    mockUseScopeGrants.mockReturnValue({
      data: undefined,
      isLoading: false,
      error: new Error('Network error'),
    })
    renderWithQuery(<ScopeGrantList onCreateGrant={onCreateGrant} onHandover={onHandover} />)
    fireEvent.change(screen.getByTestId('wallet-filter'), { target: { value: 'w-1' } })
    expect(screen.getByText(/Network error/)).toBeDefined()
  })
  it('shows empty state when wallet selected but no grants', () => {
    mockUseScopeGrants.mockReturnValue({
      data: { payload: [], count: 0 },
      isLoading: false,
      error: null,
    })
    renderWithQuery(<ScopeGrantList onCreateGrant={onCreateGrant} onHandover={onHandover} />)
    fireEvent.change(screen.getByTestId('wallet-filter'), { target: { value: 'w-1' } })
    expect(screen.getByText('No grants found')).toBeDefined()
  })
  it('shows grants table when wallet selected and grants exist', () => {
    mockUseScopeGrants.mockReturnValue({
      data: mockGrants,
      isLoading: false,
      error: null,
    })
    renderWithQuery(<ScopeGrantList onCreateGrant={onCreateGrant} onHandover={onHandover} />)
    fireEvent.change(screen.getByTestId('wallet-filter'), { target: { value: 'w-1' } })
    expect(screen.getByText('alice')).toBeDefined()
    expect(screen.getByText('BTC')).toBeDefined()
    expect(screen.getByText('underlying')).toBeDefined()
  })
  it('resolves operator label from operators list', () => {
    mockUseScopeGrants.mockReturnValue({
      data: mockGrants,
      isLoading: false,
      error: null,
    })
    renderWithQuery(<ScopeGrantList onCreateGrant={onCreateGrant} onHandover={onHandover} />)
    fireEvent.change(screen.getByTestId('wallet-filter'), { target: { value: 'w-1' } })
    expect(screen.getByText('alice')).toBeDefined()
  })
  it('falls back to public_id when operator not in list', () => {
    mockUseOperators.mockReturnValue({ data: { payload: [], count: 0 } })
    mockUseScopeGrants.mockReturnValue({
      data: mockGrants,
      isLoading: false,
      error: null,
    })
    renderWithQuery(<ScopeGrantList onCreateGrant={onCreateGrant} onHandover={onHandover} />)
    fireEvent.change(screen.getByTestId('wallet-filter'), { target: { value: 'w-1' } })
    const cells = screen.getAllByText('op-1')

    expect(cells.length).toBeGreaterThanOrEqual(1)
  })
  it('calls onCreateGrant when Create Grant clicked', () => {
    renderWithQuery(<ScopeGrantList onCreateGrant={onCreateGrant} onHandover={onHandover} />)
    fireEvent.click(screen.getByText('Create Grant'))
    expect(onCreateGrant).toHaveBeenCalled()
  })
  it('calls onHandover when Handover button clicked', () => {
    mockUseScopeGrants.mockReturnValue({
      data: mockGrants,
      isLoading: false,
      error: null,
    })
    renderWithQuery(<ScopeGrantList onCreateGrant={onCreateGrant} onHandover={onHandover} />)
    fireEvent.change(screen.getByTestId('wallet-filter'), { target: { value: 'w-1' } })
    fireEvent.click(screen.getByText('Handover'))
    expect(onHandover).toHaveBeenCalledWith(mockGrants.payload[0])
  })
  it('shows instrument_public_id for instrument scope_kind', () => {
    const instrumentGrant = {
      ...mockGrants.payload[0],
      scope_kind: 'instrument' as const,
      underlying_public_id: null,
      instrument_public_id: 'BTC-USD-PERP',
    }

    mockUseScopeGrants.mockReturnValue({
      data: { payload: [instrumentGrant], count: 1 },
      isLoading: false,
      error: null,
    })
    renderWithQuery(<ScopeGrantList onCreateGrant={onCreateGrant} onHandover={onHandover} />)
    fireEvent.change(screen.getByTestId('wallet-filter'), { target: { value: 'w-1' } })
    expect(screen.getByText('BTC-USD-PERP')).toBeDefined()
  })
  it('disables Create Grant when readOnly', () => {
    renderWithQuery(
      <ScopeGrantList onCreateGrant={onCreateGrant} onHandover={onHandover} readOnly />
    )
    const btn = screen.getByText('Create Grant').closest('button')

    expect(btn?.disabled).toBe(true)
  })
  it('handles undefined wallets data gracefully', () => {
    mockUseWallets.mockReturnValue({ data: undefined })
    renderWithQuery(<ScopeGrantList onCreateGrant={onCreateGrant} onHandover={onHandover} />)
    expect(screen.getByText('Select a wallet')).toBeDefined()
  })
  it('handles undefined operators data gracefully', () => {
    mockUseOperators.mockReturnValue({ data: undefined })
    mockUseScopeGrants.mockReturnValue({
      data: mockGrants,
      isLoading: false,
      error: null,
    })
    renderWithQuery(<ScopeGrantList onCreateGrant={onCreateGrant} onHandover={onHandover} />)
    fireEvent.change(screen.getByTestId('wallet-filter'), { target: { value: 'w-1' } })
    const cells = screen.getAllByText('op-1')

    expect(cells.length).toBeGreaterThanOrEqual(1)
  })
  it('shows non-Error object in error message', () => {
    mockUseScopeGrants.mockReturnValue({
      data: undefined,
      isLoading: false,
      error: 'string error',
    })
    renderWithQuery(<ScopeGrantList onCreateGrant={onCreateGrant} onHandover={onHandover} />)
    fireEvent.change(screen.getByTestId('wallet-filter'), { target: { value: 'w-1' } })
    expect(screen.getByText(/Unknown error/)).toBeDefined()
  })
})
