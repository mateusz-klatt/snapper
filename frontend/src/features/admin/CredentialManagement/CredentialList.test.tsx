import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import CredentialList from './CredentialList'

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

const mockCredentials = {
  payload: [
    {
      type: 'credential_summary' as const,
      session_id: 's',
      sequence_id: 1,
      public_id: 'cred-1',
      timestamp: '2026-01-01T00:00:00Z',
      wallet_public_id: 'w-1',
      exchange: 'kraken',
      credential_type: 'api_key_secret',
      label: 'main-key',
    },
  ],
  count: 1,
}

const mockUseCredentials = vi.fn()
const mockUseWallets = vi.fn()

vi.mock('../../../hooks/queries', () => ({
  useWallets: () => mockUseWallets(),
  useCredentials: () => mockUseCredentials(),
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

describe('CredentialList', () => {
  const onCreateCredential = vi.fn()
  const onRotate = vi.fn()

  beforeEach(() => {
    vi.clearAllMocks()
    mockUseWallets.mockReturnValue({ data: mockWallets })
    mockUseCredentials.mockReturnValue({ data: undefined, isLoading: false, error: null })
  })
  it('shows wallet selector and select-a-wallet message', () => {
    renderWithQuery(<CredentialList onCreateCredential={onCreateCredential} onRotate={onRotate} />)
    expect(screen.getByText('Select a wallet')).toBeDefined()
    expect(screen.getByTestId('cred-wallet-filter')).toBeDefined()
  })
  it('shows loading spinner when fetching credentials', () => {
    mockUseCredentials.mockReturnValue({ data: undefined, isLoading: true, error: null })
    renderWithQuery(<CredentialList onCreateCredential={onCreateCredential} onRotate={onRotate} />)
    fireEvent.change(screen.getByTestId('cred-wallet-filter'), { target: { value: 'w-1' } })
    expect(screen.getByTestId('cred-wallet-filter')).toBeDefined()
  })
  it('shows error when fetch fails', () => {
    mockUseCredentials.mockReturnValue({
      data: undefined,
      isLoading: false,
      error: new Error('Network error'),
    })
    renderWithQuery(<CredentialList onCreateCredential={onCreateCredential} onRotate={onRotate} />)
    fireEvent.change(screen.getByTestId('cred-wallet-filter'), { target: { value: 'w-1' } })
    expect(screen.getByText(/Network error/)).toBeDefined()
  })
  it('shows non-Error object in error message', () => {
    mockUseCredentials.mockReturnValue({
      data: undefined,
      isLoading: false,
      error: 'string error',
    })
    renderWithQuery(<CredentialList onCreateCredential={onCreateCredential} onRotate={onRotate} />)
    fireEvent.change(screen.getByTestId('cred-wallet-filter'), { target: { value: 'w-1' } })
    expect(screen.getByText(/Unknown error/)).toBeDefined()
  })
  it('shows empty state when wallet selected but no credentials', () => {
    mockUseCredentials.mockReturnValue({
      data: { payload: [], count: 0 },
      isLoading: false,
      error: null,
    })
    renderWithQuery(<CredentialList onCreateCredential={onCreateCredential} onRotate={onRotate} />)
    fireEvent.change(screen.getByTestId('cred-wallet-filter'), { target: { value: 'w-1' } })
    expect(screen.getByText('No credentials found')).toBeDefined()
  })
  it('shows credentials table when wallet selected and credentials exist', () => {
    mockUseCredentials.mockReturnValue({
      data: mockCredentials,
      isLoading: false,
      error: null,
    })
    renderWithQuery(<CredentialList onCreateCredential={onCreateCredential} onRotate={onRotate} />)
    fireEvent.change(screen.getByTestId('cred-wallet-filter'), { target: { value: 'w-1' } })
    expect(screen.getByText('kraken')).toBeDefined()
    expect(screen.getByText('API Key + Secret')).toBeDefined()
    expect(screen.getByText('main-key')).toBeDefined()
  })
  it('shows credential type labels correctly', () => {
    const multiCred = {
      payload: [
        { ...mockCredentials.payload[0], credential_type: 'rsa_pem', public_id: 'c-rsa' },
        { ...mockCredentials.payload[0], credential_type: 'oauth', public_id: 'c-oauth' },
        { ...mockCredentials.payload[0], credential_type: 'paper', public_id: 'c-paper' },
        { ...mockCredentials.payload[0], credential_type: 'unknown_type', public_id: 'c-unk' },
      ],
      count: 4,
    }

    mockUseCredentials.mockReturnValue({ data: multiCred, isLoading: false, error: null })
    renderWithQuery(<CredentialList onCreateCredential={onCreateCredential} onRotate={onRotate} />)
    fireEvent.change(screen.getByTestId('cred-wallet-filter'), { target: { value: 'w-1' } })
    expect(screen.getByText('RSA PEM')).toBeDefined()
    expect(screen.getByText('OAuth')).toBeDefined()
    expect(screen.getByText('Paper')).toBeDefined()
    expect(screen.getByText('unknown_type')).toBeDefined()
  })
  it('shows dash when label is null', () => {
    const noLabelCred = {
      payload: [{ ...mockCredentials.payload[0], label: null }],
      count: 1,
    }

    mockUseCredentials.mockReturnValue({ data: noLabelCred, isLoading: false, error: null })
    renderWithQuery(<CredentialList onCreateCredential={onCreateCredential} onRotate={onRotate} />)
    fireEvent.change(screen.getByTestId('cred-wallet-filter'), { target: { value: 'w-1' } })
    expect(screen.getByText('-')).toBeDefined()
  })
  it('calls onCreateCredential when Add Credential clicked', () => {
    renderWithQuery(<CredentialList onCreateCredential={onCreateCredential} onRotate={onRotate} />)
    fireEvent.click(screen.getByText('Add Credential'))
    expect(onCreateCredential).toHaveBeenCalled()
  })
  it('calls onRotate when Rotate button clicked', () => {
    mockUseCredentials.mockReturnValue({
      data: mockCredentials,
      isLoading: false,
      error: null,
    })
    renderWithQuery(<CredentialList onCreateCredential={onCreateCredential} onRotate={onRotate} />)
    fireEvent.change(screen.getByTestId('cred-wallet-filter'), { target: { value: 'w-1' } })
    fireEvent.click(screen.getByText('Rotate'))
    expect(onRotate).toHaveBeenCalledWith(mockCredentials.payload[0])
  })
  it('disables Add Credential when readOnly', () => {
    renderWithQuery(
      <CredentialList onCreateCredential={onCreateCredential} onRotate={onRotate} readOnly />
    )
    const btn = screen.getByText('Add Credential').closest('button')

    expect(btn?.disabled).toBe(true)
  })
  it('handles undefined wallets data gracefully', () => {
    mockUseWallets.mockReturnValue({ data: undefined })
    renderWithQuery(<CredentialList onCreateCredential={onCreateCredential} onRotate={onRotate} />)
    expect(screen.getByText('Select a wallet')).toBeDefined()
  })
  it('handles undefined credentials data gracefully', () => {
    mockUseCredentials.mockReturnValue({ data: undefined, isLoading: false, error: null })
    renderWithQuery(<CredentialList onCreateCredential={onCreateCredential} onRotate={onRotate} />)
    fireEvent.change(screen.getByTestId('cred-wallet-filter'), { target: { value: 'w-1' } })
    expect(screen.getByText('No credentials found')).toBeDefined()
  })
  it('shows paper annotation for paper wallets', () => {
    renderWithQuery(<CredentialList onCreateCredential={onCreateCredential} onRotate={onRotate} />)
    const select = screen.getByTestId('cred-wallet-filter')
    const options = select.querySelectorAll('option')
    const labels = Array.from(options).map(o => o.textContent)

    expect(labels).toContain('paper (paper)')
  })
})
