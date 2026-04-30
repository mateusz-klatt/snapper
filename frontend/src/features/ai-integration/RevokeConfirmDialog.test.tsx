import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, act } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { RevokeConfirmDialog } from './RevokeConfirmDialog'
import type { DelegateRead } from '../../types/api'

const mockDeactivate = { mutateAsync: vi.fn() }

vi.mock('../../hooks/queries', () => ({
  useDeactivateAiDelegate: () => mockDeactivate,
}))

vi.mock('react-hot-toast', () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
  },
}))

const delegate: DelegateRead = {
  public_id: 'd-1',
  username: 'ai-alpha',
  label: 'Alpha',
  created_by_user_public_id: 'u-1',
  created_at: '2026-04-21T00:00:00Z',
  is_active: true,
  caps: {
    max_open_orders: 10,
    max_daily_notional_usd: 1000,
    max_cancels_per_minute: null,
    max_order_quantity_per_instrument: null,
  },
}

describe('RevokeConfirmDialog', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('renders title with delegate label and danger-variant confirm button', () => {
    render(<RevokeConfirmDialog delegate={delegate} open onClose={vi.fn()} />)
    expect(screen.getByText(/Revoke delegate "Alpha"\?/)).toBeInTheDocument()
    const confirmButtons = screen.getAllByRole('button', { name: 'Revoke' })
    const confirmButton = confirmButtons.find(el => el.className.includes('bg-loss-600'))

    expect(confirmButton).toBeDefined()
  })

  it('invokes mutation with delegate public_id on confirm + toasts + closes', async () => {
    const user = userEvent.setup()
    const onClose = vi.fn()
    const { toast } = await import('react-hot-toast')

    mockDeactivate.mutateAsync.mockResolvedValueOnce({ payload: { is_active: false } })
    render(<RevokeConfirmDialog delegate={delegate} open onClose={onClose} />)
    const confirmBtn = screen
      .getAllByRole('button', { name: 'Revoke' })
      .find(el => el.className.includes('bg-loss-600')) as HTMLElement

    await act(async () => {
      await user.click(confirmBtn)
    })
    expect(mockDeactivate.mutateAsync).toHaveBeenCalledWith('d-1')
    expect(toast.success).toHaveBeenCalledWith('Delegate revoked')
    expect(onClose).toHaveBeenCalled()
  })

  it('toasts error on mutation failure but does not close the dialog', async () => {
    const user = userEvent.setup()
    const onClose = vi.fn()
    const { toast } = await import('react-hot-toast')

    mockDeactivate.mutateAsync.mockRejectedValueOnce(new Error('database is locked'))
    render(<RevokeConfirmDialog delegate={delegate} open onClose={onClose} />)
    const confirmBtn = screen
      .getAllByRole('button', { name: 'Revoke' })
      .find(el => el.className.includes('bg-loss-600')) as HTMLElement

    await act(async () => {
      await user.click(confirmBtn)
    })
    expect(toast.error).toHaveBeenCalledWith('database is locked')
    expect(onClose).not.toHaveBeenCalled()
  })

  it('toasts generic message when error is not an Error instance', async () => {
    const user = userEvent.setup()
    const { toast } = await import('react-hot-toast')

    mockDeactivate.mutateAsync.mockRejectedValueOnce('non-error-value')
    render(<RevokeConfirmDialog delegate={delegate} open onClose={vi.fn()} />)
    const confirmBtn = screen
      .getAllByRole('button', { name: 'Revoke' })
      .find(el => el.className.includes('bg-loss-600')) as HTMLElement

    await act(async () => {
      await user.click(confirmBtn)
    })
    expect(toast.error).toHaveBeenCalledWith('Failed to revoke delegate')
  })

  it('invokes onClose when cancel button is clicked', async () => {
    const user = userEvent.setup()
    const onClose = vi.fn()

    render(<RevokeConfirmDialog delegate={delegate} open onClose={onClose} />)
    await user.click(screen.getByRole('button', { name: 'Cancel' }))
    expect(onClose).toHaveBeenCalled()
    expect(mockDeactivate.mutateAsync).not.toHaveBeenCalled()
  })

  it('post-unmount success does not fire toast.success or onClose', async () => {
    const user = userEvent.setup()
    const onClose = vi.fn()
    const { toast } = await import('react-hot-toast')
    let resolveFn: ((value: unknown) => void) | null = null
    const pending = new Promise(res => {
      resolveFn = res
    })

    mockDeactivate.mutateAsync.mockReturnValueOnce(pending)
    const { unmount } = render(<RevokeConfirmDialog delegate={delegate} open onClose={onClose} />)
    const confirmBtn = screen
      .getAllByRole('button', { name: 'Revoke' })
      .find(el => el.className.includes('bg-loss-600')) as HTMLElement

    await act(async () => {
      await user.click(confirmBtn)
    })
    unmount()
    await act(async () => {
      if (resolveFn !== null) resolveFn({ payload: { is_active: false } })
      await pending
    })
    expect(toast.success).not.toHaveBeenCalled()
    expect(onClose).not.toHaveBeenCalled()
  })

  it('post-unmount error does not fire toast.error', async () => {
    const user = userEvent.setup()
    const { toast } = await import('react-hot-toast')
    let rejectFn: ((err: Error) => void) | null = null
    const pending = new Promise((_res, rej) => {
      rejectFn = rej
    })

    mockDeactivate.mutateAsync.mockReturnValueOnce(pending)
    const { unmount } = render(<RevokeConfirmDialog delegate={delegate} open onClose={vi.fn()} />)
    const confirmBtn = screen
      .getAllByRole('button', { name: 'Revoke' })
      .find(el => el.className.includes('bg-loss-600')) as HTMLElement

    await act(async () => {
      await user.click(confirmBtn)
    })
    unmount()
    await act(async () => {
      if (rejectFn !== null) rejectFn(new Error('boom'))
      await pending.catch(() => {})
    })
    expect(toast.error).not.toHaveBeenCalled()
  })
})
