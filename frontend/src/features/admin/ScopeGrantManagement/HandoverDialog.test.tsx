import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import HandoverDialog from './HandoverDialog'
import type { ScopeGrantInfo } from '../../../types/api'

const mockHandoverMutation = {
  mutate: vi.fn(),
  isPending: false,
}

const mockUseOperators = vi.fn()

vi.mock('../../../hooks/queries', () => ({
  useOperators: () => mockUseOperators(),
  useHandoverScopeGrant: () => mockHandoverMutation,
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

const testGrant: ScopeGrantInfo = {
  type: 'scope_grant_info',
  session_id: 's',
  sequence_id: 1,
  public_id: 'sg-1',
  timestamp: '2026-01-01T00:00:00Z',
  operator_public_id: 'op-1',
  wallet_public_id: 'w-1',
  granted_by_user_public_id: 'u-1',
  scope_kind: 'underlying',
  underlying_public_id: 'BTC',
  instrument_public_id: null,
  note: null,
  known_to: '9999-12-31T23:59:59.999999Z',
}

function renderWithQuery(ui: React.ReactElement) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })

  return render(<QueryClientProvider client={qc}>{ui}</QueryClientProvider>)
}

describe('HandoverDialog', () => {
  const onClose = vi.fn()

  beforeEach(() => {
    vi.clearAllMocks()
    mockHandoverMutation.mutate = vi.fn()
    mockHandoverMutation.isPending = false
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
          {
            type: 'operator_info',
            session_id: 's',
            sequence_id: 2,
            public_id: 'op-2',
            timestamp: '2026-01-01T00:00:00Z',
            label: 'bob',
          },
        ],
      },
    })
  })
  it('does not render when closed', () => {
    renderWithQuery(<HandoverDialog grant={null} open={false} onClose={onClose} />)
    expect(screen.queryByTestId('modal')).toBeNull()
  })
  it('shows grant details when open', () => {
    renderWithQuery(<HandoverDialog grant={testGrant} open onClose={onClose} />)
    expect(screen.getByText('Handover Scope Grant')).toBeDefined()
    expect(screen.getByText('alice')).toBeDefined()
    expect(screen.getByText('underlying: BTC')).toBeDefined()
    expect(screen.getByText('w-1')).toBeDefined()
  })
  it('filters out current operator from destination list', () => {
    renderWithQuery(<HandoverDialog grant={testGrant} open onClose={onClose} />)
    const select = screen.getByTestId('handover-operator')
    const options = select.querySelectorAll('option')
    const values = Array.from(options).map(o => o.value)

    expect(values).not.toContain('op-1')
    expect(values).toContain('op-2')
  })
  it('disables Handover button when no destination selected', () => {
    renderWithQuery(<HandoverDialog grant={testGrant} open onClose={onClose} />)
    const btn = screen.getByText('Handover').closest('button')

    expect(btn?.disabled).toBe(true)
  })
  it('calls handover mutation with correct params', async () => {
    renderWithQuery(<HandoverDialog grant={testGrant} open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('handover-operator'), { target: { value: 'op-2' } })
    fireEvent.change(screen.getByPlaceholderText('e.g. Shift change'), {
      target: { value: 'shift change' },
    })
    fireEvent.click(screen.getByText('Handover'))
    await waitFor(() => {
      expect(mockHandoverMutation.mutate).toHaveBeenCalledWith(
        {
          from_grant_public_id: 'sg-1',
          to_operator_public_id: 'op-2',
          reason: 'shift change',
        },
        expect.objectContaining({
          onSuccess: expect.any(Function),
          onError: expect.any(Function),
        })
      )
    })
  })
  it('calls onClose on Cancel', () => {
    renderWithQuery(<HandoverDialog grant={testGrant} open onClose={onClose} />)
    fireEvent.click(screen.getByText('Cancel'))
    expect(onClose).toHaveBeenCalled()
  })
  it('handles success callback', async () => {
    mockHandoverMutation.mutate = vi.fn((_data, opts) => {
      opts.onSuccess()
    })
    renderWithQuery(<HandoverDialog grant={testGrant} open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('handover-operator'), { target: { value: 'op-2' } })
    fireEvent.click(screen.getByText('Handover'))
    await waitFor(() => {
      expect(onClose).toHaveBeenCalled()
    })
  })
  it('handles 409 conflict error', async () => {
    mockHandoverMutation.mutate = vi.fn((_data, opts) => {
      opts.onError(new Error('HTTP 409: Conflict'))
    })
    renderWithQuery(<HandoverDialog grant={testGrant} open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('handover-operator'), { target: { value: 'op-2' } })
    fireEvent.click(screen.getByText('Handover'))
    await waitFor(() => {
      expect(mockHandoverMutation.mutate).toHaveBeenCalled()
    })
  })
  it('handles generic error', async () => {
    mockHandoverMutation.mutate = vi.fn((_data, opts) => {
      opts.onError(new Error('Server error'))
    })
    renderWithQuery(<HandoverDialog grant={testGrant} open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('handover-operator'), { target: { value: 'op-2' } })
    fireEvent.click(screen.getByText('Handover'))
    await waitFor(() => {
      expect(mockHandoverMutation.mutate).toHaveBeenCalled()
    })
  })
  it('handles error with empty message using fallback text', async () => {
    mockHandoverMutation.mutate = vi.fn((_data, opts) => {
      const emptyErr = new Error('non-empty')

      emptyErr.message = ''
      opts.onError(emptyErr)
    })
    renderWithQuery(<HandoverDialog grant={testGrant} open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('handover-operator'), { target: { value: 'op-2' } })
    fireEvent.click(screen.getByText('Handover'))
    await waitFor(() => {
      expect(mockHandoverMutation.mutate).toHaveBeenCalled()
    })
  })
  it('does not call handover when grant is null', async () => {
    renderWithQuery(<HandoverDialog grant={null} open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('handover-operator'), { target: { value: 'op-2' } })
    fireEvent.click(screen.getByText('Handover'))
    expect(mockHandoverMutation.mutate).not.toHaveBeenCalled()
  })
  it('shows instrument scope target', () => {
    const instrumentGrant: ScopeGrantInfo = {
      ...testGrant,
      scope_kind: 'instrument',
      underlying_public_id: null,
      instrument_public_id: 'BTC-USD-PERP',
    }

    renderWithQuery(<HandoverDialog grant={instrumentGrant} open onClose={onClose} />)
    expect(screen.getByText('instrument: BTC-USD-PERP')).toBeDefined()
  })
  it('handles undefined operators data gracefully', () => {
    mockUseOperators.mockReturnValue({ data: undefined })
    renderWithQuery(<HandoverDialog grant={testGrant} open onClose={onClose} />)
    expect(screen.getByText('Handover Scope Grant')).toBeDefined()
    expect(screen.getByText('op-1')).toBeDefined()
  })
  it('falls back to public_id when operator not found in list', () => {
    const unknownOpGrant: ScopeGrantInfo = {
      ...testGrant,
      operator_public_id: 'op-unknown',
    }

    renderWithQuery(<HandoverDialog grant={unknownOpGrant} open onClose={onClose} />)
    expect(screen.getByText('op-unknown')).toBeDefined()
  })
  it('disables Handover button when readOnly', () => {
    renderWithQuery(<HandoverDialog grant={testGrant} open onClose={onClose} readOnly />)
    fireEvent.change(screen.getByTestId('handover-operator'), { target: { value: 'op-2' } })
    const btn = screen.getByText('Handover').closest('button')

    expect(btn?.disabled).toBe(true)
  })
  it('omits reason when empty', async () => {
    renderWithQuery(<HandoverDialog grant={testGrant} open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('handover-operator'), { target: { value: 'op-2' } })
    fireEvent.click(screen.getByText('Handover'))
    await waitFor(() => {
      expect(mockHandoverMutation.mutate).toHaveBeenCalledWith(
        expect.objectContaining({ reason: undefined }),
        expect.anything()
      )
    })
  })
})
