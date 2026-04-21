import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, act, fireEvent } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { DelegateDetailView } from './DelegateDetailView'
import type { DelegateRead } from '../../types/api'

const mockUseAiDelegate = vi.fn()
const mockUseIsReadOnly = vi.fn()
const mockUpdateMutation = { mutateAsync: vi.fn(), isPending: false }
const mockDeactivateMutation = { mutateAsync: vi.fn() }

vi.mock('../../hooks/queries', () => ({
  useAiDelegate: (publicId: string | null) => mockUseAiDelegate(publicId),
  useUpdateAiDelegateCaps: () => mockUpdateMutation,
  useDeactivateAiDelegate: () => mockDeactivateMutation,
}))

vi.mock('../../hooks/useIsReadOnly', () => ({
  useIsReadOnly: () => mockUseIsReadOnly(),
}))

vi.mock('react-hot-toast', () => ({
  toast: { success: vi.fn(), error: vi.fn() },
}))

const buildDelegate = (overrides: Partial<DelegateRead> = {}): DelegateRead => ({
  public_id: '019db037-6586-722b-bd0c-87f1806e41f2',
  username: 'ai-alpha',
  label: 'Alpha',
  created_by_user_public_id: '019db005-0fea-7350-9a9a-5e1485a0bb6f',
  created_at: '2026-04-21T13:25:13Z',
  is_active: true,
  caps: {
    max_open_orders: 5,
    max_daily_notional_usd: 10000,
    max_cancels_per_minute: null,
    max_order_quantity_per_instrument: null,
  },
  ...overrides,
})

describe('DelegateDetailView', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockUseIsReadOnly.mockReturnValue(false)
    mockUpdateMutation.isPending = false
  })

  it('renders loading state while detail query is pending', () => {
    mockUseAiDelegate.mockReturnValue({ data: undefined, isLoading: true })
    render(<DelegateDetailView publicId='d-1' onBack={vi.fn()} />)
    expect(screen.getByText(/Loading delegate/)).toBeInTheDocument()
  })

  it('renders not-found fallback when delegate is undefined after load', () => {
    mockUseAiDelegate.mockReturnValue({ data: undefined, isLoading: false })
    render(<DelegateDetailView publicId='d-1' onBack={vi.fn()} />)
    expect(screen.getByText(/not found or unavailable/)).toBeInTheDocument()
  })

  it('not-found back button triggers onBack', async () => {
    const onBack = vi.fn()

    mockUseAiDelegate.mockReturnValue({ data: undefined, isLoading: false })
    render(<DelegateDetailView publicId='d-1' onBack={onBack} />)
    fireEvent.click(screen.getByRole('button', { name: /Back/ }))
    expect(onBack).toHaveBeenCalled()
  })

  it('renders metadata and caps for active delegate', () => {
    const delegate = buildDelegate()

    mockUseAiDelegate.mockReturnValue({
      data: { payload: delegate },
      isLoading: false,
    })
    render(<DelegateDetailView publicId={delegate.public_id} onBack={vi.fn()} />)
    expect(screen.getByRole('heading', { name: 'Alpha' })).toBeInTheDocument()
    expect(screen.getByText('ai-alpha')).toBeInTheDocument()
    expect(screen.getByText('Active')).toBeInTheDocument()
    expect(screen.getByText('5')).toBeInTheDocument()
    expect(screen.getByText('10000')).toBeInTheDocument()
  })

  it('renders per-instrument caps JSON when present', () => {
    const delegate = buildDelegate({
      caps: {
        max_open_orders: null,
        max_daily_notional_usd: null,
        max_cancels_per_minute: null,
        max_order_quantity_per_instrument: { 'inst-a': 5 },
      },
    })

    mockUseAiDelegate.mockReturnValue({ data: { payload: delegate }, isLoading: false })
    render(<DelegateDetailView publicId={delegate.public_id} onBack={vi.fn()} />)
    expect(screen.getByText(/"inst-a":\s*5/)).toBeInTheDocument()
  })

  it('disables Update caps button when read-only', () => {
    mockUseIsReadOnly.mockReturnValue(true)
    const delegate = buildDelegate()

    mockUseAiDelegate.mockReturnValue({ data: { payload: delegate }, isLoading: false })
    render(<DelegateDetailView publicId={delegate.public_id} onBack={vi.fn()} />)
    expect(screen.getByRole('button', { name: /Update caps/ })).toBeDisabled()
    expect(screen.getByRole('button', { name: /Revoke/ })).toBeDisabled()
  })

  it('hides Revoke + Update buttons when delegate is already inactive', () => {
    const delegate = buildDelegate({ is_active: false })

    mockUseAiDelegate.mockReturnValue({ data: { payload: delegate }, isLoading: false })
    render(<DelegateDetailView publicId={delegate.public_id} onBack={vi.fn()} />)
    expect(screen.queryByRole('button', { name: /Revoke/ })).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: /Update caps/ })).toBeDisabled()
  })

  it('enters edit mode on Update caps click and pre-fills current values', async () => {
    const user = userEvent.setup()
    const delegate = buildDelegate()

    mockUseAiDelegate.mockReturnValue({ data: { payload: delegate }, isLoading: false })
    render(<DelegateDetailView publicId={delegate.public_id} onBack={vi.fn()} />)
    await user.click(screen.getByRole('button', { name: /Update caps/ }))
    const openInput = screen.getByLabelText('Max open orders') as HTMLInputElement

    expect(openInput.value).toBe('5')
  })

  it('submits caps update via PATCH mutation', async () => {
    const user = userEvent.setup()
    const delegate = buildDelegate()

    mockUseAiDelegate.mockReturnValue({ data: { payload: delegate }, isLoading: false })
    mockUpdateMutation.mutateAsync.mockResolvedValueOnce({ payload: delegate })
    render(<DelegateDetailView publicId={delegate.public_id} onBack={vi.fn()} />)
    await user.click(screen.getByRole('button', { name: /Update caps/ }))
    const openInput = screen.getByLabelText('Max open orders') as HTMLInputElement

    await act(async () => {
      await user.clear(openInput)
      await user.type(openInput, '25')
    })
    await act(async () => {
      await user.click(screen.getByRole('button', { name: /Save/ }))
    })
    expect(mockUpdateMutation.mutateAsync).toHaveBeenCalledWith({
      publicId: delegate.public_id,
      body: {
        caps: {
          max_open_orders: 25,
          max_daily_notional_usd: 10000,
          max_cancels_per_minute: null,
          max_order_quantity_per_instrument: null,
        },
      },
    })
  })

  it('toasts error when caps editor contains negative number', async () => {
    const user = userEvent.setup()
    const { toast } = await import('react-hot-toast')
    const delegate = buildDelegate()

    mockUseAiDelegate.mockReturnValue({ data: { payload: delegate }, isLoading: false })
    render(<DelegateDetailView publicId={delegate.public_id} onBack={vi.fn()} />)
    await user.click(screen.getByRole('button', { name: /Update caps/ }))
    const openInput = screen.getByLabelText('Max open orders') as HTMLInputElement

    await act(async () => {
      await user.clear(openInput)
      await user.type(openInput, '-5')
    })
    await act(async () => {
      await user.click(screen.getByRole('button', { name: /Save/ }))
    })
    expect(toast.error).toHaveBeenCalledWith('Caps must be non-negative integers or empty.')
    expect(mockUpdateMutation.mutateAsync).not.toHaveBeenCalled()
  })

  it('toasts error when caps update mutation fails', async () => {
    const user = userEvent.setup()
    const { toast } = await import('react-hot-toast')
    const delegate = buildDelegate()

    mockUseAiDelegate.mockReturnValue({ data: { payload: delegate }, isLoading: false })
    mockUpdateMutation.mutateAsync.mockRejectedValueOnce(new Error('boom'))
    render(<DelegateDetailView publicId={delegate.public_id} onBack={vi.fn()} />)
    await user.click(screen.getByRole('button', { name: /Update caps/ }))
    await act(async () => {
      await user.click(screen.getByRole('button', { name: /Save/ }))
    })
    expect(toast.error).toHaveBeenCalledWith('boom')
  })

  it('toasts generic error when thrown value is not an Error instance', async () => {
    const user = userEvent.setup()
    const { toast } = await import('react-hot-toast')
    const delegate = buildDelegate()

    mockUseAiDelegate.mockReturnValue({ data: { payload: delegate }, isLoading: false })
    mockUpdateMutation.mutateAsync.mockRejectedValueOnce('not-an-error')
    render(<DelegateDetailView publicId={delegate.public_id} onBack={vi.fn()} />)
    await user.click(screen.getByRole('button', { name: /Update caps/ }))
    await act(async () => {
      await user.click(screen.getByRole('button', { name: /Save/ }))
    })
    expect(toast.error).toHaveBeenCalledWith('Failed to update caps')
  })

  it('Cancel exits edit mode without submitting', async () => {
    const user = userEvent.setup()
    const delegate = buildDelegate()

    mockUseAiDelegate.mockReturnValue({ data: { payload: delegate }, isLoading: false })
    render(<DelegateDetailView publicId={delegate.public_id} onBack={vi.fn()} />)
    await user.click(screen.getByRole('button', { name: /Update caps/ }))
    expect(screen.getByLabelText('Max open orders')).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: 'Cancel' }))
    expect(screen.queryByLabelText('Max open orders')).not.toBeInTheDocument()
    expect(mockUpdateMutation.mutateAsync).not.toHaveBeenCalled()
  })

  it('opens RevokeConfirmDialog on Revoke click', async () => {
    const user = userEvent.setup()
    const delegate = buildDelegate()

    mockUseAiDelegate.mockReturnValue({ data: { payload: delegate }, isLoading: false })
    render(<DelegateDetailView publicId={delegate.public_id} onBack={vi.fn()} />)
    await user.click(screen.getByRole('button', { name: /Revoke/ }))
    expect(screen.getByText(/Revoke delegate "Alpha"\?/)).toBeInTheDocument()
  })

  it('Back to list triggers onBack', async () => {
    const user = userEvent.setup()
    const onBack = vi.fn()
    const delegate = buildDelegate()

    mockUseAiDelegate.mockReturnValue({ data: { payload: delegate }, isLoading: false })
    render(<DelegateDetailView publicId={delegate.public_id} onBack={onBack} />)
    await user.click(screen.getByRole('button', { name: /Back to list/ }))
    expect(onBack).toHaveBeenCalled()
  })

  it('edits max_daily_notional_usd and max_cancels_per_minute fields', async () => {
    const user = userEvent.setup()
    const delegate = buildDelegate()

    mockUseAiDelegate.mockReturnValue({ data: { payload: delegate }, isLoading: false })
    mockUpdateMutation.mutateAsync.mockResolvedValueOnce({ payload: delegate })
    render(<DelegateDetailView publicId={delegate.public_id} onBack={vi.fn()} />)
    await user.click(screen.getByRole('button', { name: /Update caps/ }))
    const dailyInput = screen.getByLabelText('Max daily notional USD') as HTMLInputElement
    const cancelsInput = screen.getByLabelText('Max cancels per minute') as HTMLInputElement

    await act(async () => {
      await user.clear(dailyInput)
      await user.type(dailyInput, '5000')
      await user.clear(cancelsInput)
      await user.type(cancelsInput, '3')
    })
    await act(async () => {
      await user.click(screen.getByRole('button', { name: /Save/ }))
    })
    expect(mockUpdateMutation.mutateAsync).toHaveBeenCalledWith({
      publicId: delegate.public_id,
      body: {
        caps: {
          max_open_orders: 5,
          max_daily_notional_usd: 5000,
          max_cancels_per_minute: 3,
          max_order_quantity_per_instrument: null,
        },
      },
    })
  })

  it('closes RevokeConfirmDialog via its Cancel path', async () => {
    const user = userEvent.setup()
    const delegate = buildDelegate()

    mockUseAiDelegate.mockReturnValue({ data: { payload: delegate }, isLoading: false })
    render(<DelegateDetailView publicId={delegate.public_id} onBack={vi.fn()} />)
    await user.click(screen.getByRole('button', { name: /Revoke/ }))
    expect(screen.getByText(/Revoke delegate "Alpha"\?/)).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: 'Cancel' }))
    expect(screen.queryByText(/Revoke delegate "Alpha"\?/)).not.toBeInTheDocument()
  })

  it('pre-fills editor with empty strings when caps are null', async () => {
    const user = userEvent.setup()
    const delegate = buildDelegate({
      caps: {
        max_open_orders: null,
        max_daily_notional_usd: null,
        max_cancels_per_minute: null,
        max_order_quantity_per_instrument: null,
      },
    })

    mockUseAiDelegate.mockReturnValue({ data: { payload: delegate }, isLoading: false })
    render(<DelegateDetailView publicId={delegate.public_id} onBack={vi.fn()} />)
    await user.click(screen.getByRole('button', { name: /Update caps/ }))
    expect((screen.getByLabelText('Max open orders') as HTMLInputElement).value).toBe('')
    expect((screen.getByLabelText('Max daily notional USD') as HTMLInputElement).value).toBe('')
    expect((screen.getByLabelText('Max cancels per minute') as HTMLInputElement).value).toBe('')
  })
})
