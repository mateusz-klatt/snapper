import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import RotateDialog from './RotateDialog'
import type { CredentialSummary } from '../../../types/api'

const mockRotateMutation = {
  mutate: vi.fn(),
  isPending: false,
}

vi.mock('../../../hooks/queries', () => ({
  useRotateCredential: () => mockRotateMutation,
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

const testCredential: CredentialSummary = {
  type: 'credential_summary',
  session_id: 's',
  sequence_id: 1,
  public_id: 'cred-1',
  timestamp: '2026-01-01T00:00:00Z',
  wallet_public_id: 'w-1',
  exchange: 'kraken',
  credential_type: 'api_key_secret',
  label: 'main-key',
}

function renderWithQuery(ui: React.ReactElement) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })

  return render(<QueryClientProvider client={qc}>{ui}</QueryClientProvider>)
}

describe('RotateDialog', () => {
  const onClose = vi.fn()

  beforeEach(() => {
    vi.clearAllMocks()
    mockRotateMutation.mutate = vi.fn()
    mockRotateMutation.isPending = false
  })
  it('does not render when closed', () => {
    renderWithQuery(<RotateDialog credential={null} open={false} onClose={onClose} />)
    expect(screen.queryByTestId('modal')).toBeNull()
  })
  it('shows credential details when open', () => {
    renderWithQuery(<RotateDialog credential={testCredential} open onClose={onClose} />)
    expect(screen.getByText('Rotate Credential')).toBeDefined()
    expect(screen.getByText('kraken')).toBeDefined()
    expect(screen.getByText('api_key_secret')).toBeDefined()
    expect(screen.getByText('main-key')).toBeDefined()
  })
  it('shows dash when label is null', () => {
    const noLabel: CredentialSummary = { ...testCredential, label: null }

    renderWithQuery(<RotateDialog credential={noLabel} open onClose={onClose} />)
    expect(screen.getByText('-')).toBeDefined()
  })
  it('shows correct fields for api_key_secret type', () => {
    renderWithQuery(<RotateDialog credential={testCredential} open onClose={onClose} />)
    expect(screen.getByLabelText('API Key')).toBeDefined()
    expect(screen.getByLabelText('API Secret')).toBeDefined()
  })
  it('shows correct fields for rsa_pem type', () => {
    const rsaCred: CredentialSummary = { ...testCredential, credential_type: 'rsa_pem' }

    renderWithQuery(<RotateDialog credential={rsaCred} open onClose={onClose} />)
    expect(screen.getByLabelText('API Key')).toBeDefined()
    expect(screen.getByLabelText('Private Key (PEM)')).toBeDefined()
    const pemField = screen.getByLabelText('Private Key (PEM)')

    expect(pemField.tagName).toBe('TEXTAREA')
  })
  it('shows correct fields for paper type', () => {
    const paperCred: CredentialSummary = { ...testCredential, credential_type: 'paper' }

    renderWithQuery(<RotateDialog credential={paperCred} open onClose={onClose} />)
    expect(screen.getByLabelText('Initial Balance')).toBeDefined()
  })
  it('shows correct fields for oauth type', () => {
    const oauthCred: CredentialSummary = { ...testCredential, credential_type: 'oauth' }

    renderWithQuery(<RotateDialog credential={oauthCred} open onClose={onClose} />)
    expect(screen.getByLabelText('Client ID')).toBeDefined()
    expect(screen.getByLabelText('Client Secret')).toBeDefined()
    expect(screen.getByLabelText('Refresh Token')).toBeDefined()
  })
  it('masks refresh_token as password field', () => {
    const oauthCred: CredentialSummary = { ...testCredential, credential_type: 'oauth' }

    renderWithQuery(<RotateDialog credential={oauthCred} open onClose={onClose} />)
    const tokenField = screen.getByLabelText('Refresh Token') as HTMLInputElement

    expect(tokenField.type).toBe('password')
  })
  it('shows validation errors when rotating with empty fields', () => {
    renderWithQuery(<RotateDialog credential={testCredential} open onClose={onClose} />)
    fireEvent.click(screen.getByText('Rotate'))
    expect(screen.getByText('API Key is required')).toBeDefined()
    expect(screen.getByText('API Secret is required')).toBeDefined()
    expect(mockRotateMutation.mutate).not.toHaveBeenCalled()
  })
  it('calls rotateMutation with correct params', async () => {
    renderWithQuery(<RotateDialog credential={testCredential} open onClose={onClose} />)
    fireEvent.change(screen.getByLabelText('API Key'), { target: { value: 'new-key' } })
    fireEvent.change(screen.getByLabelText('API Secret'), { target: { value: 'new-secret' } })
    fireEvent.click(screen.getByText('Rotate'))
    await waitFor(() => {
      expect(mockRotateMutation.mutate).toHaveBeenCalledWith(
        {
          walletPublicId: 'w-1',
          credentialPublicId: 'cred-1',
          data: {
            credential_payload: { api_key: 'new-key', api_secret: 'new-secret' },
            label: undefined,
          },
        },
        expect.objectContaining({
          onSuccess: expect.any(Function),
          onError: expect.any(Function),
        })
      )
    })
  })
  it('includes label when provided', async () => {
    renderWithQuery(<RotateDialog credential={testCredential} open onClose={onClose} />)
    fireEvent.change(screen.getByLabelText('API Key'), { target: { value: 'k' } })
    fireEvent.change(screen.getByLabelText('API Secret'), { target: { value: 's' } })
    fireEvent.change(screen.getByLabelText('New Label (optional)'), {
      target: { value: 'rotated' },
    })
    fireEvent.click(screen.getByText('Rotate'))
    await waitFor(() => {
      expect(mockRotateMutation.mutate).toHaveBeenCalledWith(
        expect.objectContaining({
          data: expect.objectContaining({ label: 'rotated' }),
        }),
        expect.anything()
      )
    })
  })
  it('clears field error when typing', () => {
    renderWithQuery(<RotateDialog credential={testCredential} open onClose={onClose} />)
    fireEvent.click(screen.getByText('Rotate'))
    expect(screen.getByText('API Key is required')).toBeDefined()
    fireEvent.change(screen.getByLabelText('API Key'), { target: { value: 'x' } })
    expect(screen.queryByText('API Key is required')).toBeNull()
  })
  it('calls onClose on Cancel', () => {
    renderWithQuery(<RotateDialog credential={testCredential} open onClose={onClose} />)
    fireEvent.click(screen.getByText('Cancel'))
    expect(onClose).toHaveBeenCalled()
  })
  it('handles success callback', async () => {
    mockRotateMutation.mutate = vi.fn((_data, opts) => {
      opts.onSuccess()
    })
    renderWithQuery(<RotateDialog credential={testCredential} open onClose={onClose} />)
    fireEvent.change(screen.getByLabelText('API Key'), { target: { value: 'k' } })
    fireEvent.change(screen.getByLabelText('API Secret'), { target: { value: 's' } })
    fireEvent.click(screen.getByText('Rotate'))
    await waitFor(() => {
      expect(onClose).toHaveBeenCalled()
    })
  })
  it('handles error', async () => {
    mockRotateMutation.mutate = vi.fn((_data, opts) => {
      opts.onError(new Error('Server error'))
    })
    renderWithQuery(<RotateDialog credential={testCredential} open onClose={onClose} />)
    fireEvent.change(screen.getByLabelText('API Key'), { target: { value: 'k' } })
    fireEvent.change(screen.getByLabelText('API Secret'), { target: { value: 's' } })
    fireEvent.click(screen.getByText('Rotate'))
    await waitFor(() => {
      expect(mockRotateMutation.mutate).toHaveBeenCalled()
    })
  })
  it('handles error with empty message', async () => {
    mockRotateMutation.mutate = vi.fn((_data, opts) => {
      const emptyErr = new Error('non-empty')

      emptyErr.message = ''
      opts.onError(emptyErr)
    })
    renderWithQuery(<RotateDialog credential={testCredential} open onClose={onClose} />)
    fireEvent.change(screen.getByLabelText('API Key'), { target: { value: 'k' } })
    fireEvent.change(screen.getByLabelText('API Secret'), { target: { value: 's' } })
    fireEvent.click(screen.getByText('Rotate'))
    await waitFor(() => {
      expect(mockRotateMutation.mutate).toHaveBeenCalled()
    })
  })
  it('does not call rotate when credential is null', () => {
    renderWithQuery(<RotateDialog credential={null} open onClose={onClose} />)
    fireEvent.click(screen.getByText('Rotate'))
    expect(mockRotateMutation.mutate).not.toHaveBeenCalled()
  })
  it('handles unknown credential type with no required fields', () => {
    const unknownCred: CredentialSummary = {
      ...testCredential,
      credential_type: 'unknown_type',
    }

    renderWithQuery(<RotateDialog credential={unknownCred} open onClose={onClose} />)
    expect(screen.queryByLabelText('API Key')).toBeNull()
  })
  it('handles typing in PEM textarea without error', () => {
    const rsaCred: CredentialSummary = { ...testCredential, credential_type: 'rsa_pem' }

    renderWithQuery(<RotateDialog credential={rsaCred} open onClose={onClose} />)
    fireEvent.change(screen.getByLabelText('Private Key (PEM)'), {
      target: { value: '-----BEGIN' },
    })
    expect(screen.queryByText('Private Key (PEM) is required')).toBeNull()
  })
  it('disables Rotate button when readOnly', () => {
    renderWithQuery(<RotateDialog credential={testCredential} open onClose={onClose} readOnly />)
    const btn = screen.getByText('Rotate').closest('button')

    expect(btn?.disabled).toBe(true)
  })
  it('clears textarea field error when typing in rsa_pem field', () => {
    const rsaCred: CredentialSummary = { ...testCredential, credential_type: 'rsa_pem' }

    renderWithQuery(<RotateDialog credential={rsaCred} open onClose={onClose} />)
    fireEvent.click(screen.getByText('Rotate'))
    expect(screen.getByText('Private Key (PEM) is required')).toBeDefined()
    fireEvent.change(screen.getByLabelText('Private Key (PEM)'), {
      target: { value: '-----BEGIN' },
    })
    expect(screen.queryByText('Private Key (PEM) is required')).toBeNull()
  })
})
