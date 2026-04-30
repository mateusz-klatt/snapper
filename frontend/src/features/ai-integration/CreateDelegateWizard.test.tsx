import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, act, fireEvent } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { CreateDelegateWizard } from './CreateDelegateWizard'
import type { DelegateCreatedPayload } from '../../types/api'

const mockResetFn = vi.fn()
const mockMutation = {
  mutateAsync: vi.fn(),
  reset: mockResetFn,
  isPending: false,
}
const mockUseIsReadOnly = vi.fn()

vi.mock('../../hooks/queries', () => ({
  useCreateAiDelegate: () => mockMutation,
}))

vi.mock('../../hooks/useIsReadOnly', () => ({
  useIsReadOnly: () => mockUseIsReadOnly(),
}))

vi.mock('react-hot-toast', () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}))

const mockPayload: DelegateCreatedPayload = {
  delegate: {
    public_id: 'd-1',
    username: 'ai-alpha',
    label: 'Alpha',
    created_by_user_public_id: 'u-1',
    created_at: '2026-04-21T00:00:00Z',
    is_active: true,
    caps: {
      max_open_orders: null,
      max_daily_notional_usd: null,
      max_cancels_per_minute: null,
      max_order_quantity_per_instrument: null,
    },
  },
  access_token: 'access-xyz',
  expires_in: 900,
}

const envelope = { payload: mockPayload }

async function fillLabelAndAdvance(label: string): Promise<void> {
  const user = userEvent.setup()
  const labelInput = screen.getByLabelText('Label')

  await act(async () => {
    await user.type(labelInput, label)
  })
  await act(async () => {
    await user.click(screen.getByRole('button', { name: 'Next' }))
  })
}

describe('CreateDelegateWizard', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockUseIsReadOnly.mockReturnValue(false)
    mockMutation.isPending = false
    mockMutation.mutateAsync.mockReset()
    mockResetFn.mockReset()
  })

  it('opens with identity step; Next disabled until label provided', () => {
    render(<CreateDelegateWizard open onClose={vi.fn()} />)
    expect(screen.getByText('1. Identity')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Next' })).toBeDisabled()
  })

  it('advances identity → scope-and-caps → review → submit → done', async () => {
    const user = userEvent.setup()

    mockMutation.mutateAsync.mockResolvedValueOnce(envelope)
    render(<CreateDelegateWizard open onClose={vi.fn()} />)
    await fillLabelAndAdvance('alpha-desk')
    expect(screen.getByRole('heading', { name: 'Scope' })).toBeInTheDocument()
    await act(async () => {
      await user.click(screen.getByRole('button', { name: 'Next' }))
    })
    expect(screen.getByRole('heading', { name: /Review & create/ })).toBeInTheDocument()
    await act(async () => {
      await user.click(screen.getByRole('button', { name: /Create delegate/ }))
    })
    expect(mockMutation.mutateAsync).toHaveBeenCalledWith({
      label: 'alpha-desk',
      caps: {
        max_order_quantity_per_instrument: null,
        max_open_orders: null,
        max_daily_notional_usd: null,
        max_cancels_per_minute: null,
      },
      operator_public_id: null,
    })
    expect(screen.getByText(/Save these credentials now/)).toBeInTheDocument()
  })

  it('submits caps as parsed numbers and operator_public_id when filled', async () => {
    const user = userEvent.setup()

    mockMutation.mutateAsync.mockResolvedValueOnce(envelope)
    render(<CreateDelegateWizard open onClose={vi.fn()} />)
    await fillLabelAndAdvance('alpha')
    await act(async () => {
      await user.type(screen.getByLabelText(/Operator public_id/), 'op-1')
      await user.type(screen.getByLabelText('Max open orders'), '5')
      await user.type(screen.getByLabelText('Max daily notional USD'), '1000')
      await user.type(screen.getByLabelText('Max cancels per minute'), '20')
    })
    await act(async () => {
      await user.click(screen.getByRole('button', { name: 'Next' }))
    })
    await act(async () => {
      await user.click(screen.getByRole('button', { name: /Create delegate/ }))
    })
    expect(mockMutation.mutateAsync).toHaveBeenCalledWith({
      label: 'alpha',
      caps: {
        max_order_quantity_per_instrument: null,
        max_open_orders: 5,
        max_daily_notional_usd: 1000,
        max_cancels_per_minute: 20,
      },
      operator_public_id: 'op-1',
    })
  })

  it('parses per-instrument JSON caps', async () => {
    const user = userEvent.setup()

    mockMutation.mutateAsync.mockResolvedValueOnce(envelope)
    render(<CreateDelegateWizard open onClose={vi.fn()} />)
    await fillLabelAndAdvance('alpha')
    await act(async () => {
      await user.type(screen.getByLabelText(/Per-instrument max quantity/), '{{"inst":5}')
    })
    await act(async () => {
      await user.click(screen.getByRole('button', { name: 'Next' }))
    })
    await act(async () => {
      await user.click(screen.getByRole('button', { name: /Create delegate/ }))
    })
    expect(mockMutation.mutateAsync).toHaveBeenCalledWith(
      expect.objectContaining({
        caps: expect.objectContaining({
          max_order_quantity_per_instrument: { inst: 5 },
        }),
      })
    )
  })

  it('shows caps error for invalid JSON and stays on scope step', async () => {
    const user = userEvent.setup()

    render(<CreateDelegateWizard open onClose={vi.fn()} />)
    await fillLabelAndAdvance('alpha')
    await act(async () => {
      await user.type(screen.getByLabelText(/Per-instrument max quantity/), 'not-json')
    })
    await act(async () => {
      await user.click(screen.getByRole('button', { name: 'Next' }))
    })
    expect(screen.getByRole('alert')).toHaveTextContent(/valid JSON/)
    expect(screen.queryByRole('heading', { name: /Review & create/ })).not.toBeInTheDocument()
  })

  it('shows caps error when JSON is array (not object)', async () => {
    const user = userEvent.setup()

    render(<CreateDelegateWizard open onClose={vi.fn()} />)
    await fillLabelAndAdvance('alpha')
    const jsonField = screen.getByLabelText(/Per-instrument max quantity/)

    await act(async () => {
      fireEvent.change(jsonField, { target: { value: '[1,2,3]' } })
    })
    await act(async () => {
      await user.click(screen.getByRole('button', { name: 'Next' }))
    })
    expect(screen.getByRole('alert')).toHaveTextContent(/JSON object or empty/)
  })

  it('shows caps error for negative integer in number field', async () => {
    const user = userEvent.setup()

    render(<CreateDelegateWizard open onClose={vi.fn()} />)
    await fillLabelAndAdvance('alpha')
    const openInput = screen.getByLabelText('Max open orders') as HTMLInputElement

    await act(async () => {
      await user.type(openInput, '-3')
    })
    await act(async () => {
      await user.click(screen.getByRole('button', { name: 'Next' }))
    })
    expect(screen.getByRole('alert')).toHaveTextContent(/non-negative integers/)
  })

  it('returns to scope step with caps error when JSON edit is corrupted between review and submit', async () => {
    const user = userEvent.setup()

    render(<CreateDelegateWizard open onClose={vi.fn()} />)
    await fillLabelAndAdvance('alpha')
    await act(async () => {
      await user.click(screen.getByRole('button', { name: 'Next' }))
    })
    expect(screen.getByRole('heading', { name: /Review & create/ })).toBeInTheDocument()
    await act(async () => {
      await user.click(screen.getByRole('button', { name: 'Back' }))
    })
    const jsonField = screen.getByLabelText(/Per-instrument max quantity/) as HTMLInputElement

    await act(async () => {
      await user.type(jsonField, 'broken')
    })
    await act(async () => {
      await user.click(screen.getByRole('button', { name: 'Next' }))
    })
    expect(screen.getByRole('alert')).toHaveTextContent(/valid JSON/)
  })

  it('submit button disabled + loading while mutation isPending', async () => {
    const user = userEvent.setup()

    render(<CreateDelegateWizard open onClose={vi.fn()} />)
    await fillLabelAndAdvance('alpha')
    mockMutation.isPending = true
    await act(async () => {
      await user.click(screen.getByRole('button', { name: 'Next' }))
    })
    // loading={true} replaces button text with "Loading..." per Button component contract
    expect(screen.getByRole('button', { name: /Loading/ })).toBeDisabled()
  })

  it('toasts and stays on review step when mutation fails', async () => {
    const user = userEvent.setup()
    const { toast } = await import('react-hot-toast')

    mockMutation.mutateAsync.mockRejectedValueOnce(new Error('boom'))
    render(<CreateDelegateWizard open onClose={vi.fn()} />)
    await fillLabelAndAdvance('alpha')
    await act(async () => {
      await user.click(screen.getByRole('button', { name: 'Next' }))
    })
    await act(async () => {
      await user.click(screen.getByRole('button', { name: /Create delegate/ }))
    })
    expect(toast.error).toHaveBeenCalledWith('boom')
    expect(screen.queryByText(/Save these credentials now/)).not.toBeInTheDocument()
  })

  it('toasts generic message when thrown error is not Error instance', async () => {
    const user = userEvent.setup()
    const { toast } = await import('react-hot-toast')

    mockMutation.mutateAsync.mockRejectedValueOnce('boom-string')
    render(<CreateDelegateWizard open onClose={vi.fn()} />)
    await fillLabelAndAdvance('alpha')
    await act(async () => {
      await user.click(screen.getByRole('button', { name: 'Next' }))
    })
    await act(async () => {
      await user.click(screen.getByRole('button', { name: /Create delegate/ }))
    })
    expect(toast.error).toHaveBeenCalledWith('Failed to create delegate')
  })

  it('Cancel on identity invokes onClose and calls mutation.reset', async () => {
    const user = userEvent.setup()
    const onClose = vi.fn()

    render(<CreateDelegateWizard open onClose={onClose} />)
    await act(async () => {
      await user.click(screen.getByRole('button', { name: 'Cancel' }))
    })
    expect(onClose).toHaveBeenCalled()
    expect(mockResetFn).toHaveBeenCalled()
  })

  it('Back from scope returns to identity preserving label', async () => {
    const user = userEvent.setup()

    render(<CreateDelegateWizard open onClose={vi.fn()} />)
    await fillLabelAndAdvance('persisted')
    await act(async () => {
      await user.click(screen.getByRole('button', { name: 'Back' }))
    })
    expect((screen.getByLabelText('Label') as HTMLInputElement).value).toBe('persisted')
  })

  it('Back from review returns to scope-and-caps', async () => {
    const user = userEvent.setup()

    render(<CreateDelegateWizard open onClose={vi.fn()} />)
    await fillLabelAndAdvance('alpha')
    await act(async () => {
      await user.click(screen.getByRole('button', { name: 'Next' }))
    })
    await act(async () => {
      await user.click(screen.getByRole('button', { name: 'Back' }))
    })
    expect(screen.getByRole('heading', { name: 'Scope' })).toBeInTheDocument()
  })

  it('Done step Close button calls onClose and resets mutation', async () => {
    const user = userEvent.setup()
    const onClose = vi.fn()

    mockMutation.mutateAsync.mockResolvedValueOnce(envelope)
    render(<CreateDelegateWizard open onClose={onClose} />)
    await fillLabelAndAdvance('alpha')
    await act(async () => {
      await user.click(screen.getByRole('button', { name: 'Next' }))
    })
    await act(async () => {
      await user.click(screen.getByRole('button', { name: /Create delegate/ }))
    })
    await act(async () => {
      await user.click(screen.getByRole('button', { name: /I have saved these/ }))
    })
    expect(onClose).toHaveBeenCalled()
    expect(mockResetFn).toHaveBeenCalled()
  })

  it('resets wizard state when open flips false', () => {
    const { rerender } = render(<CreateDelegateWizard open onClose={vi.fn()} />)

    expect(screen.getByText('1. Identity')).toBeInTheDocument()
    rerender(<CreateDelegateWizard open={false} onClose={vi.fn()} />)
    // wizard still reducer-controlled; reset call is internal
    rerender(<CreateDelegateWizard open onClose={vi.fn()} />)
    expect((screen.getByLabelText('Label') as HTMLInputElement).value).toBe('')
  })

  it('calls mutation.reset on unmount (post-unmount race guard)', () => {
    const { unmount } = render(<CreateDelegateWizard open onClose={vi.fn()} />)

    mockResetFn.mockClear()
    unmount()
    expect(mockResetFn).toHaveBeenCalled()
  })

  it('post-unmount error path does not toast', async () => {
    const user = userEvent.setup()
    const { toast } = await import('react-hot-toast')
    let rejectFn: ((err: Error) => void) | null = null
    const pendingPromise = new Promise<typeof envelope>((_res, rej) => {
      rejectFn = rej
    })

    mockMutation.mutateAsync.mockReturnValueOnce(pendingPromise)
    const { unmount } = render(<CreateDelegateWizard open onClose={vi.fn()} />)

    await fillLabelAndAdvance('alpha')
    await act(async () => {
      await user.click(screen.getByRole('button', { name: 'Next' }))
    })
    await act(async () => {
      await user.click(screen.getByRole('button', { name: /Create delegate/ }))
    })
    unmount()
    await act(async () => {
      if (rejectFn !== null) rejectFn(new Error('boom'))
      await pendingPromise.catch(() => {})
    })
    // Wizard was unmounted before rejection — toast should not fire
    expect(toast.error).not.toHaveBeenCalled()
  })

  it('post-unmount submit resolves but does not surface Done step', async () => {
    const user = userEvent.setup()
    const onClose = vi.fn()
    let resolveFn: ((payload: typeof envelope) => void) | null = null
    const pendingPromise = new Promise<typeof envelope>(res => {
      resolveFn = res
    })

    mockMutation.mutateAsync.mockReturnValueOnce(pendingPromise)
    const { unmount } = render(<CreateDelegateWizard open onClose={onClose} />)

    await fillLabelAndAdvance('alpha')
    await act(async () => {
      await user.click(screen.getByRole('button', { name: 'Next' }))
    })
    await act(async () => {
      await user.click(screen.getByRole('button', { name: /Create delegate/ }))
    })
    unmount()
    await act(async () => {
      if (resolveFn !== null) resolveFn(envelope)
      await pendingPromise
    })
    expect(mockResetFn).toHaveBeenCalled()
    // Wizard is gone, Done step cannot render
    expect(screen.queryByText(/Save these credentials now/)).not.toBeInTheDocument()
  })

  it('disables Modal close paths while mutation.isPending by passing a noop', async () => {
    mockMutation.isPending = true
    const onClose = vi.fn()

    render(<CreateDelegateWizard open onClose={onClose} />)
    // Modal renders an aria-label="Close modal" backdrop; clicking it should NOT invoke parent onClose
    const backdrop = screen.getByRole('button', { name: 'Close modal' })

    await act(async () => {
      backdrop.click()
    })
    expect(onClose).not.toHaveBeenCalled()
  })

  it('Next disabled on identity when read-only', async () => {
    mockUseIsReadOnly.mockReturnValue(true)
    const user = userEvent.setup()

    render(<CreateDelegateWizard open onClose={vi.fn()} />)
    const labelInput = screen.getByLabelText('Label')

    await act(async () => {
      await user.type(labelInput, 'alpha')
    })
    expect(screen.getByRole('button', { name: 'Next' })).toBeDisabled()
  })

  it('Next button on scope-and-caps disabled when read-only', async () => {
    const user = userEvent.setup()
    // First render (identity step): readonly=false so user can advance
    // Second render onward: readonly=true so scope-and-caps Next is blocked
    let renderCount = 0

    mockUseIsReadOnly.mockImplementation(() => {
      renderCount += 1

      return renderCount > 2
    })
    render(<CreateDelegateWizard open onClose={vi.fn()} />)
    const labelInput = screen.getByLabelText('Label')

    await act(async () => {
      await user.type(labelInput, 'alpha')
    })
    await act(async () => {
      await user.click(screen.getByRole('button', { name: 'Next' }))
    })
    // On scope-and-caps step the read-only is now true; Next is disabled
    expect(screen.getByRole('button', { name: 'Next' })).toBeDisabled()
  })
})
