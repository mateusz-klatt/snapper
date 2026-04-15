import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { AttachTrailingStopModal } from './AttachTrailingStopModal'
import { validateTrailingStopParams } from './validation'

const mockMutate = vi.fn()
const mockReset = vi.fn()

vi.mock('../../hooks/queries', () => ({
  useCreateTrailingStop: vi.fn(() => ({
    mutate: mockMutate,
    isPending: false,
    reset: mockReset,
  })),
}))

const createQueryClient = () =>
  new QueryClient({
    defaultOptions: {
      queries: { retry: false },
    },
  })

const renderWithProviders = (ui: ReactNode) => {
  const queryClient = createQueryClient()

  return render(<QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>)
}

const defaultProps = {
  open: true,
  onClose: vi.fn(),
  positionCyclePublicId: 'cycle-1',
  instrument: 'BTC-USD',
  side: 'LONG' as const,
  averagePrice: 50000,
}

beforeEach(() => {
  vi.clearAllMocks()
})

describe('validateTrailingStopParams', () => {
  it('returns null for valid params', () => {
    expect(validateTrailingStopParams(5, 0)).toBeNull()
    expect(validateTrailingStopParams(2.5, 3)).toBeNull()
    expect(validateTrailingStopParams(0.1, null)).toBeNull()
  })

  it('rejects null trailing_pct', () => {
    expect(validateTrailingStopParams(null, null)).toBe('Trailing percentage is required')
  })

  it('rejects non-finite trailing_pct', () => {
    expect(validateTrailingStopParams(NaN, null)).toBe('Invalid trailing percentage')
    expect(validateTrailingStopParams(Infinity, null)).toBe('Invalid trailing percentage')
  })

  it('rejects out-of-range trailing_pct', () => {
    expect(validateTrailingStopParams(0, null)).toBe(
      'Trailing percentage must be between 0 and 100'
    )
    expect(validateTrailingStopParams(100, null)).toBe(
      'Trailing percentage must be between 0 and 100'
    )
    expect(validateTrailingStopParams(-1, null)).toBe(
      'Trailing percentage must be between 0 and 100'
    )
  })

  it('rejects invalid min_lock_pct', () => {
    expect(validateTrailingStopParams(5, NaN)).toBe('Invalid min lock percentage')
    expect(validateTrailingStopParams(5, -1)).toBe('Min lock percentage must be between 0 and 100')
    expect(validateTrailingStopParams(5, 100)).toBe('Min lock percentage must be between 0 and 100')
  })
})

describe('AttachTrailingStopModal', () => {
  it('renders modal with position info', () => {
    renderWithProviders(<AttachTrailingStopModal {...defaultProps} />)

    expect(screen.getByText('Attach Trailing Stop — BTC-USD')).toBeInTheDocument()
    expect(screen.getByText('LONG BTC-USD')).toBeInTheDocument()
    expect(screen.getByText('$50000.00')).toBeInTheDocument()
  })

  it('shows validation error for empty trailing pct', () => {
    renderWithProviders(<AttachTrailingStopModal {...defaultProps} />)

    fireEvent.click(screen.getByTestId('trailing-stop-submit'))

    expect(screen.getByTestId('trailing-stop-error')).toHaveTextContent(
      'Trailing percentage is required'
    )
  })

  it('shows validation error for zero trailing pct', () => {
    renderWithProviders(<AttachTrailingStopModal {...defaultProps} />)

    fireEvent.change(screen.getByTestId('trailing-pct-input'), { target: { value: '0' } })
    fireEvent.click(screen.getByTestId('trailing-stop-submit'))

    expect(screen.getByTestId('trailing-stop-error')).toHaveTextContent(
      'Trailing percentage must be between 0 and 100'
    )
  })

  it('advances to confirm step with valid params', () => {
    renderWithProviders(<AttachTrailingStopModal {...defaultProps} />)

    fireEvent.change(screen.getByTestId('trailing-pct-input'), { target: { value: '5' } })
    fireEvent.click(screen.getByTestId('trailing-stop-submit'))

    expect(screen.getByText('Confirm Trailing Stop')).toBeInTheDocument()
    expect(screen.getByText('5.0%')).toBeInTheDocument()
  })

  it('shows min lock warning on confirm step', () => {
    renderWithProviders(<AttachTrailingStopModal {...defaultProps} />)

    fireEvent.change(screen.getByTestId('trailing-pct-input'), { target: { value: '5' } })
    fireEvent.change(screen.getByTestId('min-lock-pct-input'), { target: { value: '3' } })
    fireEvent.click(screen.getByTestId('trailing-stop-submit'))

    expect(screen.getByText('3.0%')).toBeInTheDocument()
    expect(
      screen.getByText(/Trailing stop will not activate until price moves 3% in your favor/)
    ).toBeInTheDocument()
  })

  it('calls mutate with correct payload on confirm', () => {
    renderWithProviders(<AttachTrailingStopModal {...defaultProps} />)

    fireEvent.change(screen.getByTestId('trailing-pct-input'), { target: { value: '5' } })
    fireEvent.click(screen.getByTestId('trailing-stop-submit'))
    fireEvent.click(screen.getByTestId('trailing-stop-confirm'))

    expect(mockMutate).toHaveBeenCalledWith(
      {
        position_cycle_public_id: 'cycle-1',
        trailing_pct: 5,
        min_lock_pct: 0,
      },
      expect.objectContaining({
        onSuccess: expect.any(Function),
        onError: expect.any(Function),
      })
    )
  })

  it('calls onClose on cancel', () => {
    renderWithProviders(<AttachTrailingStopModal {...defaultProps} />)

    fireEvent.click(screen.getByText('Cancel'))

    expect(defaultProps.onClose).toHaveBeenCalled()
  })

  it('shows error when onError callback fires', () => {
    mockMutate.mockImplementation((_body: unknown, opts: { onError: (e: Error) => void }) => {
      opts.onError(new Error('Server error'))
    })
    renderWithProviders(<AttachTrailingStopModal {...defaultProps} />)

    fireEvent.change(screen.getByTestId('trailing-pct-input'), { target: { value: '5' } })
    fireEvent.click(screen.getByTestId('trailing-stop-submit'))
    fireEvent.click(screen.getByTestId('trailing-stop-confirm'))

    expect(screen.getByTestId('trailing-stop-error')).toHaveTextContent('Server error')
  })

  it('calls onClose when onSuccess callback fires', () => {
    mockMutate.mockImplementation((_body: unknown, opts: { onSuccess: () => void }) => {
      opts.onSuccess()
    })
    renderWithProviders(<AttachTrailingStopModal {...defaultProps} />)

    fireEvent.change(screen.getByTestId('trailing-pct-input'), { target: { value: '5' } })
    fireEvent.click(screen.getByTestId('trailing-stop-submit'))
    fireEvent.click(screen.getByTestId('trailing-stop-confirm'))

    expect(defaultProps.onClose).toHaveBeenCalled()
  })

  it('shows Creating text when pending', async () => {
    const { useCreateTrailingStop } = vi.mocked(await import('../../hooks/queries'))

    useCreateTrailingStop.mockReturnValue({
      mutate: mockMutate,
      isPending: true,
      reset: mockReset,
    } as never)
    renderWithProviders(<AttachTrailingStopModal {...defaultProps} />)

    fireEvent.change(screen.getByTestId('trailing-pct-input'), { target: { value: '5' } })
    fireEvent.click(screen.getByTestId('trailing-stop-submit'))

    expect(screen.getByTestId('trailing-stop-confirm')).toHaveTextContent('Creating...')

    useCreateTrailingStop.mockReturnValue({
      mutate: mockMutate,
      isPending: false,
      reset: mockReset,
    } as never)
  })

  it('returns to input step when back is clicked on confirm', () => {
    renderWithProviders(<AttachTrailingStopModal {...defaultProps} />)

    fireEvent.change(screen.getByTestId('trailing-pct-input'), { target: { value: '5' } })
    fireEvent.click(screen.getByTestId('trailing-stop-submit'))
    fireEvent.click(screen.getByText('Back'))

    expect(screen.getByTestId('trailing-pct-input')).toBeInTheDocument()
  })
})
