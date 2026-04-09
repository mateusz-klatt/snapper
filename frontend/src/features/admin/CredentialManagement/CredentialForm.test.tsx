import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import CredentialForm from './CredentialForm'

const mockCreateMutation = {
  mutate: vi.fn(),
  isPending: false,
}

const mockUseWallets = vi.fn()

vi.mock('../../../hooks/queries', () => ({
  useWallets: () => mockUseWallets(),
  useCreateCredential: () => mockCreateMutation,
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

describe('CredentialForm', () => {
  const onClose = vi.fn()

  beforeEach(() => {
    vi.clearAllMocks()
    mockCreateMutation.mutate = vi.fn()
    mockCreateMutation.isPending = false
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
          {
            type: 'wallet_info',
            session_id: 's',
            sequence_id: 2,
            public_id: 'w-paper',
            timestamp: '2026-01-01T00:00:00Z',
            label: 'paper-test',
            is_paper: true,
          },
        ],
      },
    })
  })
  it('does not render when closed', () => {
    renderWithQuery(<CredentialForm open={false} onClose={onClose} />)
    expect(screen.queryByTestId('modal')).toBeNull()
  })
  it('renders form when open', () => {
    renderWithQuery(<CredentialForm open onClose={onClose} />)
    expect(screen.getByTestId('cred-wallet')).toBeDefined()
    expect(screen.getByTestId('cred-type')).toBeDefined()
    expect(screen.getByLabelText('Exchange')).toBeDefined()
  })
  it('shows validation errors when submitting empty form', async () => {
    renderWithQuery(<CredentialForm open onClose={onClose} />)
    const form = screen
      .getByText('Add Credential', { selector: 'button' })
      .closest('form') as HTMLFormElement

    fireEvent.submit(form)
    await waitFor(() => {
      expect(screen.getByText('Wallet is required')).toBeDefined()
      expect(screen.getByText('Exchange is required')).toBeDefined()
      expect(screen.getByText('API Key is required')).toBeDefined()
      expect(screen.getByText('API Secret is required')).toBeDefined()
    })
  })
  it('creates credential with api_key_secret type', async () => {
    renderWithQuery(<CredentialForm open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('cred-wallet'), { target: { value: 'w-1' } })
    fireEvent.change(screen.getByLabelText('Exchange'), { target: { value: 'kraken' } })
    fireEvent.change(screen.getByLabelText('API Key'), { target: { value: 'my-key' } })
    fireEvent.change(screen.getByLabelText('API Secret'), { target: { value: 'my-secret' } })
    const form = screen
      .getByText('Add Credential', { selector: 'button' })
      .closest('form') as HTMLFormElement

    fireEvent.submit(form)
    await waitFor(() => {
      expect(mockCreateMutation.mutate).toHaveBeenCalledWith(
        {
          walletPublicId: 'w-1',
          data: {
            exchange: 'kraken',
            credential_type: 'api_key_secret',
            credential_payload: { api_key: 'my-key', api_secret: 'my-secret' },
            label: undefined,
          },
        },
        expect.objectContaining({ onSuccess: expect.any(Function), onError: expect.any(Function) })
      )
    })
  })
  it('switches fields when credential type changes to rsa_pem', () => {
    renderWithQuery(<CredentialForm open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('cred-type'), { target: { value: 'rsa_pem' } })
    expect(screen.getByLabelText('API Key')).toBeDefined()
    expect(screen.getByLabelText('Private Key (PEM)')).toBeDefined()
  })
  it('shows textarea for private_key_pem field', () => {
    renderWithQuery(<CredentialForm open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('cred-type'), { target: { value: 'rsa_pem' } })
    const pemField = screen.getByLabelText('Private Key (PEM)')

    expect(pemField.tagName).toBe('TEXTAREA')
  })
  it('handles typing in PEM textarea without error', () => {
    renderWithQuery(<CredentialForm open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('cred-type'), { target: { value: 'rsa_pem' } })
    fireEvent.change(screen.getByLabelText('Private Key (PEM)'), {
      target: { value: '-----BEGIN' },
    })
    expect(screen.queryByText('Private Key (PEM) is required')).toBeNull()
  })
  it('clears textarea field error when typing in PEM field', () => {
    renderWithQuery(<CredentialForm open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('cred-type'), { target: { value: 'rsa_pem' } })
    const form = screen
      .getByText('Add Credential', { selector: 'button' })
      .closest('form') as HTMLFormElement

    fireEvent.submit(form)
    expect(screen.getByText('Private Key (PEM) is required')).toBeDefined()
    fireEvent.change(screen.getByLabelText('Private Key (PEM)'), {
      target: { value: '-----BEGIN' },
    })
    expect(screen.queryByText('Private Key (PEM) is required')).toBeNull()
  })
  it('switches fields when credential type changes to paper', () => {
    renderWithQuery(<CredentialForm open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('cred-type'), { target: { value: 'paper' } })
    expect(screen.getByLabelText('Initial Balance')).toBeDefined()
  })
  it('switches fields when credential type changes to oauth', () => {
    renderWithQuery(<CredentialForm open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('cred-type'), { target: { value: 'oauth' } })
    expect(screen.getByLabelText('Client ID')).toBeDefined()
    expect(screen.getByLabelText('Client Secret')).toBeDefined()
    expect(screen.getByLabelText('Refresh Token')).toBeDefined()
  })
  it('masks refresh_token as password field', () => {
    renderWithQuery(<CredentialForm open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('cred-type'), { target: { value: 'oauth' } })
    const tokenField = screen.getByLabelText('Refresh Token') as HTMLInputElement

    expect(tokenField.type).toBe('password')
  })
  it('includes label when provided', async () => {
    renderWithQuery(<CredentialForm open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('cred-wallet'), { target: { value: 'w-1' } })
    fireEvent.change(screen.getByLabelText('Exchange'), { target: { value: 'kraken' } })
    fireEvent.change(screen.getByLabelText('API Key'), { target: { value: 'k' } })
    fireEvent.change(screen.getByLabelText('API Secret'), { target: { value: 's' } })
    fireEvent.change(screen.getByLabelText('Label (optional)'), {
      target: { value: 'main key' },
    })
    const form = screen
      .getByText('Add Credential', { selector: 'button' })
      .closest('form') as HTMLFormElement

    fireEvent.submit(form)
    await waitFor(() => {
      expect(mockCreateMutation.mutate).toHaveBeenCalledWith(
        expect.objectContaining({
          data: expect.objectContaining({ label: 'main key' }),
        }),
        expect.anything()
      )
    })
  })
  it('clears exchange error when typing', () => {
    renderWithQuery(<CredentialForm open onClose={onClose} />)
    const form = screen
      .getByText('Add Credential', { selector: 'button' })
      .closest('form') as HTMLFormElement

    fireEvent.submit(form)
    expect(screen.getByText('Exchange is required')).toBeDefined()
    fireEvent.change(screen.getByLabelText('Exchange'), { target: { value: 'x' } })
    expect(screen.queryByText('Exchange is required')).toBeNull()
  })
  it('clears field error when typing in credential field', () => {
    renderWithQuery(<CredentialForm open onClose={onClose} />)
    const form = screen
      .getByText('Add Credential', { selector: 'button' })
      .closest('form') as HTMLFormElement

    fireEvent.submit(form)
    expect(screen.getByText('API Key is required')).toBeDefined()
    fireEvent.change(screen.getByLabelText('API Key'), { target: { value: 'x' } })
    expect(screen.queryByText('API Key is required')).toBeNull()
  })
  it('calls onClose and resets form on Cancel', () => {
    renderWithQuery(<CredentialForm open onClose={onClose} />)
    fireEvent.click(screen.getByText('Cancel'))
    expect(onClose).toHaveBeenCalled()
  })
  it('handles 409 conflict error', async () => {
    mockCreateMutation.mutate = vi.fn((_data, opts) => {
      opts.onError(new Error('HTTP 409: Conflict'))
    })
    renderWithQuery(<CredentialForm open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('cred-wallet'), { target: { value: 'w-1' } })
    fireEvent.change(screen.getByLabelText('Exchange'), { target: { value: 'kraken' } })
    fireEvent.change(screen.getByLabelText('API Key'), { target: { value: 'k' } })
    fireEvent.change(screen.getByLabelText('API Secret'), { target: { value: 's' } })
    const form = screen
      .getByText('Add Credential', { selector: 'button' })
      .closest('form') as HTMLFormElement

    fireEvent.submit(form)
    await waitFor(() => {
      expect(mockCreateMutation.mutate).toHaveBeenCalled()
    })
  })
  it('handles generic error', async () => {
    mockCreateMutation.mutate = vi.fn((_data, opts) => {
      opts.onError(new Error('Server error'))
    })
    renderWithQuery(<CredentialForm open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('cred-wallet'), { target: { value: 'w-1' } })
    fireEvent.change(screen.getByLabelText('Exchange'), { target: { value: 'kraken' } })
    fireEvent.change(screen.getByLabelText('API Key'), { target: { value: 'k' } })
    fireEvent.change(screen.getByLabelText('API Secret'), { target: { value: 's' } })
    const form = screen
      .getByText('Add Credential', { selector: 'button' })
      .closest('form') as HTMLFormElement

    fireEvent.submit(form)
    await waitFor(() => {
      expect(mockCreateMutation.mutate).toHaveBeenCalled()
    })
  })
  it('handles error with empty message', async () => {
    mockCreateMutation.mutate = vi.fn((_data, opts) => {
      const emptyErr = new Error('non-empty')

      emptyErr.message = ''
      opts.onError(emptyErr)
    })
    renderWithQuery(<CredentialForm open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('cred-wallet'), { target: { value: 'w-1' } })
    fireEvent.change(screen.getByLabelText('Exchange'), { target: { value: 'kraken' } })
    fireEvent.change(screen.getByLabelText('API Key'), { target: { value: 'k' } })
    fireEvent.change(screen.getByLabelText('API Secret'), { target: { value: 's' } })
    const form = screen
      .getByText('Add Credential', { selector: 'button' })
      .closest('form') as HTMLFormElement

    fireEvent.submit(form)
    await waitFor(() => {
      expect(mockCreateMutation.mutate).toHaveBeenCalled()
    })
  })
  it('handles success callback', async () => {
    mockCreateMutation.mutate = vi.fn((_data, opts) => {
      opts.onSuccess()
    })
    renderWithQuery(<CredentialForm open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('cred-wallet'), { target: { value: 'w-1' } })
    fireEvent.change(screen.getByLabelText('Exchange'), { target: { value: 'kraken' } })
    fireEvent.change(screen.getByLabelText('API Key'), { target: { value: 'k' } })
    fireEvent.change(screen.getByLabelText('API Secret'), { target: { value: 's' } })
    const form = screen
      .getByText('Add Credential', { selector: 'button' })
      .closest('form') as HTMLFormElement

    fireEvent.submit(form)
    await waitFor(() => {
      expect(onClose).toHaveBeenCalled()
    })
  })
  it('handles unknown credential type with no required fields', () => {
    renderWithQuery(<CredentialForm open onClose={onClose} />)
    fireEvent.change(screen.getByTestId('cred-type'), { target: { value: 'unknown_type' } })
    expect(screen.queryByLabelText('API Key')).toBeNull()
    expect(screen.queryByLabelText('API Secret')).toBeNull()
  })
  it('handles undefined wallets data gracefully', () => {
    mockUseWallets.mockReturnValue({ data: undefined })
    renderWithQuery(<CredentialForm open onClose={onClose} />)
    expect(screen.getByTestId('modal')).toBeDefined()
  })
  it('shows paper annotation for paper wallets', () => {
    renderWithQuery(<CredentialForm open onClose={onClose} />)
    const walletSelect = screen.getByTestId('cred-wallet')
    const options = walletSelect.querySelectorAll('option')
    const labels = Array.from(options).map(o => o.textContent)

    expect(labels).toContain('paper-test (paper)')
  })
})
