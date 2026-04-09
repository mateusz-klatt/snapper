import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import ScopeGrantForm from './ScopeGrantForm'

const mockCreateMutation = {
  mutate: vi.fn(),
  isPending: false,
}

const mockUseOperators = vi.fn()
const mockUseWallets = vi.fn()

vi.mock('../../../hooks/queries', () => ({
  useOperators: () => mockUseOperators(),
  useWallets: () => mockUseWallets(),
  useCreateScopeGrant: () => mockCreateMutation,
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
vi.mock('../../../components/ui/Modal', () => ({
  Modal: ({
    open,
    onClose,
    title,
    children,
  }: {
    open: boolean
    onClose: () => void
    title: string
    children: React.ReactNode
  }) =>
    open ? (
      <div data-testid='modal'>
        <h3>{title}</h3>
        <button onClick={onClose} data-testid='modal-close'>
          Close
        </button>
        {children}
      </div>
    ) : null,
}))

function renderWithQuery(ui: React.ReactElement) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })

  return render(<QueryClientProvider client={qc}>{ui}</QueryClientProvider>)
}

describe('ScopeGrantForm', () => {
  const onClose = vi.fn()

  beforeEach(() => {
    vi.clearAllMocks()
    mockCreateMutation.mutate = vi.fn()
    mockCreateMutation.isPending = false
    mockUseOperators.mockReturnValue({
      data: {
        payload: [
          {
            type: 'operator_info',
            session_id: 's',
            sequence_id: 1,
            public_id: 'op-1',
            timestamp: '2026-01-01T00:00:00Z',
            label: 'alice',
          },
        ],
      },
    })
    mockUseWallets.mockReturnValue({
      data: {
        payload: [
          {
            type: 'wallet_info',
            session_id: 's',
            sequence_id: 1,
            public_id: 'w-1',
            timestamp: '2026-01-01T00:00:00Z',
            label: 'default',
            is_paper: false,
          },
        ],
      },
    })
  })
  it('does not render when closed', () => {
    renderWithQuery(<ScopeGrantForm open={false} onClose={onClose} />)
    expect(screen.queryByTestId('modal')).toBeNull()
  })
  it('renders form when open', () => {
    renderWithQuery(<ScopeGrantForm open onClose={onClose} />)
    expect(screen.getByText('Create Scope Grant')).toBeDefined()
    expect(screen.getByTestId('sg-operator')).toBeDefined()
    expect(screen.getByTestId('sg-wallet')).toBeDefined()
    expect(screen.getByTestId('sg-scope-kind')).toBeDefined()
  })
  it('shows validation errors when submitting empty form', async () => {
    renderWithQuery(<ScopeGrantForm open onClose={onClose} />)
    const form = screen.getByText('Create Grant').closest('form') as HTMLFormElement

    fireEvent.submit(form)
    await waitFor(() => {
      expect(screen.getByText('Operator is required')).toBeDefined()
      expect(screen.getByText('Wallet is required')).toBeDefined()
      expect(screen.getByText('Target ID is required')).toBeDefined()
    })
  })
  it('calls createScopeGrant with underlying scope', async () => {
    renderWithQuery(<ScopeGrantForm open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('sg-operator'), { target: { value: 'op-1' } })
    fireEvent.change(screen.getByTestId('sg-wallet'), { target: { value: 'w-1' } })
    fireEvent.change(screen.getByLabelText(/Underlying Public ID/), {
      target: { value: 'BTC' },
    })
    const form = screen.getByText('Create Grant').closest('form') as HTMLFormElement

    fireEvent.submit(form)
    await waitFor(() => {
      expect(mockCreateMutation.mutate).toHaveBeenCalledWith(
        {
          operator_public_id: 'op-1',
          wallet_public_id: 'w-1',
          scope_kind: 'underlying',
          underlying_public_id: 'BTC',
          instrument_public_id: undefined,
          note: undefined,
        },
        expect.objectContaining({ onSuccess: expect.any(Function), onError: expect.any(Function) })
      )
    })
  })
  it('switches label when scope kind changes to instrument', () => {
    renderWithQuery(<ScopeGrantForm open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('sg-scope-kind'), { target: { value: 'instrument' } })
    expect(screen.getByLabelText(/Instrument Public ID/)).toBeDefined()
  })
  it('calls createScopeGrant with instrument scope', async () => {
    renderWithQuery(<ScopeGrantForm open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('sg-operator'), { target: { value: 'op-1' } })
    fireEvent.change(screen.getByTestId('sg-wallet'), { target: { value: 'w-1' } })
    fireEvent.change(screen.getByTestId('sg-scope-kind'), { target: { value: 'instrument' } })
    fireEvent.change(screen.getByLabelText(/Instrument Public ID/), {
      target: { value: 'BTC-USD-PERP' },
    })
    const form = screen.getByText('Create Grant').closest('form') as HTMLFormElement

    fireEvent.submit(form)
    await waitFor(() => {
      expect(mockCreateMutation.mutate).toHaveBeenCalledWith(
        expect.objectContaining({
          scope_kind: 'instrument',
          instrument_public_id: 'BTC-USD-PERP',
          underlying_public_id: undefined,
        }),
        expect.anything()
      )
    })
  })
  it('includes note when provided', async () => {
    renderWithQuery(<ScopeGrantForm open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('sg-operator'), { target: { value: 'op-1' } })
    fireEvent.change(screen.getByTestId('sg-wallet'), { target: { value: 'w-1' } })
    fireEvent.change(screen.getByLabelText(/Underlying Public ID/), {
      target: { value: 'ETH' },
    })
    fireEvent.change(screen.getByPlaceholderText('Optional note'), {
      target: { value: 'test note' },
    })
    const form = screen.getByText('Create Grant').closest('form') as HTMLFormElement

    fireEvent.submit(form)
    await waitFor(() => {
      expect(mockCreateMutation.mutate).toHaveBeenCalledWith(
        expect.objectContaining({ note: 'test note' }),
        expect.anything()
      )
    })
  })
  it('clears target error when typing', () => {
    renderWithQuery(<ScopeGrantForm open onClose={onClose} />)
    const form = screen.getByText('Create Grant').closest('form') as HTMLFormElement

    fireEvent.submit(form)
    expect(screen.getByText('Target ID is required')).toBeDefined()
    fireEvent.change(screen.getByLabelText(/Underlying Public ID/), {
      target: { value: 'X' },
    })
    expect(screen.queryByText('Target ID is required')).toBeNull()
  })
  it('calls onClose and resets form on Cancel', () => {
    renderWithQuery(<ScopeGrantForm open onClose={onClose} />)
    fireEvent.click(screen.getByText('Cancel'))
    expect(onClose).toHaveBeenCalled()
  })
  it('handles 409 conflict error specially', async () => {
    mockCreateMutation.mutate = vi.fn((_data, opts) => {
      opts.onError(new Error('HTTP 409: Conflict'))
    })
    renderWithQuery(<ScopeGrantForm open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('sg-operator'), { target: { value: 'op-1' } })
    fireEvent.change(screen.getByTestId('sg-wallet'), { target: { value: 'w-1' } })
    fireEvent.change(screen.getByLabelText(/Underlying Public ID/), {
      target: { value: 'BTC' },
    })
    const form = screen.getByText('Create Grant').closest('form') as HTMLFormElement

    fireEvent.submit(form)
    await waitFor(() => {
      expect(mockCreateMutation.mutate).toHaveBeenCalled()
    })
  })
  it('handles generic error', async () => {
    mockCreateMutation.mutate = vi.fn((_data, opts) => {
      opts.onError(new Error('Server error'))
    })
    renderWithQuery(<ScopeGrantForm open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('sg-operator'), { target: { value: 'op-1' } })
    fireEvent.change(screen.getByTestId('sg-wallet'), { target: { value: 'w-1' } })
    fireEvent.change(screen.getByLabelText(/Underlying Public ID/), {
      target: { value: 'BTC' },
    })
    const form = screen.getByText('Create Grant').closest('form') as HTMLFormElement

    fireEvent.submit(form)
    await waitFor(() => {
      expect(mockCreateMutation.mutate).toHaveBeenCalled()
    })
  })
  it('handles success callback', async () => {
    mockCreateMutation.mutate = vi.fn((_data, opts) => {
      opts.onSuccess()
    })
    renderWithQuery(<ScopeGrantForm open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('sg-operator'), { target: { value: 'op-1' } })
    fireEvent.change(screen.getByTestId('sg-wallet'), { target: { value: 'w-1' } })
    fireEvent.change(screen.getByLabelText(/Underlying Public ID/), {
      target: { value: 'BTC' },
    })
    const form = screen.getByText('Create Grant').closest('form') as HTMLFormElement

    fireEvent.submit(form)
    await waitFor(() => {
      expect(onClose).toHaveBeenCalled()
    })
  })
  it('handles undefined wallets/operators data gracefully', () => {
    mockUseOperators.mockReturnValue({ data: undefined })
    mockUseWallets.mockReturnValue({ data: undefined })
    renderWithQuery(<ScopeGrantForm open onClose={onClose} />)
    expect(screen.getByText('Create Scope Grant')).toBeDefined()
  })
  it('shows paper annotation for paper wallets', () => {
    mockUseWallets.mockReturnValue({
      data: {
        payload: [
          {
            type: 'wallet_info',
            session_id: 's',
            sequence_id: 1,
            public_id: 'w-paper',
            timestamp: '2026-01-01T00:00:00Z',
            label: 'paper-test',
            is_paper: true,
          },
        ],
      },
    })
    renderWithQuery(<ScopeGrantForm open onClose={onClose} />)
    const walletSelect = screen.getByTestId('sg-wallet')
    const options = walletSelect.querySelectorAll('option')
    const labels = Array.from(options).map(o => o.textContent)

    expect(labels).toContain('paper-test (paper)')
  })
  it('handles error with empty message using fallback text', async () => {
    mockCreateMutation.mutate = vi.fn((_data, opts) => {
      const emptyErr = new Error('non-empty')

      emptyErr.message = ''
      opts.onError(emptyErr)
    })
    renderWithQuery(<ScopeGrantForm open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('sg-operator'), { target: { value: 'op-1' } })
    fireEvent.change(screen.getByTestId('sg-wallet'), { target: { value: 'w-1' } })
    fireEvent.change(screen.getByLabelText(/Underlying Public ID/), {
      target: { value: 'BTC' },
    })
    const form = screen.getByText('Create Grant').closest('form') as HTMLFormElement

    fireEvent.submit(form)
    await waitFor(() => {
      expect(mockCreateMutation.mutate).toHaveBeenCalled()
    })
  })
})
