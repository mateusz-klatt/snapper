import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { ConfirmDialog } from './ConfirmDialog'

vi.mock('../../hooks/useIsReadOnly', () => ({
  useIsReadOnly: vi.fn(() => false),
}))
import { useIsReadOnly } from '../../hooks/useIsReadOnly'

describe('ConfirmDialog', () => {
  const defaultProps = {
    open: true,
    title: 'Confirm Action',
    message: 'Are you sure you want to proceed?',
    onConfirm: vi.fn(),
    onCancel: vi.fn(),
  }

  it('does not render when open is false', () => {
    render(<ConfirmDialog {...defaultProps} open={false} />)
    expect(screen.queryByText('Are you sure you want to proceed?')).not.toBeInTheDocument()
  })
  it('renders when open is true', () => {
    render(<ConfirmDialog {...defaultProps} />)
    expect(screen.getByText('Confirm Action')).toBeInTheDocument()
    expect(screen.getByText('Are you sure you want to proceed?')).toBeInTheDocument()
  })
  it('renders default button texts', () => {
    render(<ConfirmDialog {...defaultProps} />)
    expect(screen.getByText('Confirm')).toBeInTheDocument()
    expect(screen.getByText('Cancel')).toBeInTheDocument()
  })
  it('renders custom button texts when provided', () => {
    render(<ConfirmDialog {...defaultProps} confirmText='Delete' cancelText='Keep' />)
    expect(screen.getByText('Delete')).toBeInTheDocument()
    expect(screen.getByText('Keep')).toBeInTheDocument()
  })
  it('calls onConfirm when confirm button is clicked', () => {
    const onConfirm = vi.fn()

    render(<ConfirmDialog {...defaultProps} onConfirm={onConfirm} />)
    const confirmButton = screen.getByText('Confirm')

    fireEvent.click(confirmButton)
    expect(onConfirm).toHaveBeenCalledOnce()
  })
  it('calls onCancel when cancel button is clicked', () => {
    const onCancel = vi.fn()

    render(<ConfirmDialog {...defaultProps} onCancel={onCancel} />)
    const cancelButton = screen.getByText('Cancel')

    fireEvent.click(cancelButton)
    expect(onCancel).toHaveBeenCalledOnce()
  })
  it('calls onCancel when modal is closed via backdrop', () => {
    const onCancel = vi.fn()

    render(<ConfirmDialog {...defaultProps} onCancel={onCancel} />)
    const backdrop = screen.getByRole('button', { name: 'Close modal' })

    expect(backdrop).toBeInTheDocument()

    if (backdrop) {
      fireEvent.click(backdrop)
    }

    expect(onCancel).toHaveBeenCalledOnce()
  })
  it('applies default variant styling', () => {
    render(<ConfirmDialog {...defaultProps} variant='default' />)
    const confirmButton = screen.getByText('Confirm')

    expect(confirmButton.className).toContain('bg-primary-600')
  })
  it('applies danger variant styling', () => {
    render(<ConfirmDialog {...defaultProps} variant='danger' />)
    const confirmButton = screen.getByText('Confirm')

    expect(confirmButton.className).toContain('bg-loss-600')
  })
  it('applies danger variant to custom confirm button text', () => {
    render(
      <ConfirmDialog {...defaultProps} variant='danger' confirmText='Delete' cancelText='Keep' />
    )
    const confirmButton = screen.getByText('Delete')

    expect(confirmButton.className).toContain('bg-loss-600')
  })
  it('disables confirm button in read-only mode', () => {
    vi.mocked(useIsReadOnly).mockReturnValue(true)
    const onConfirm = vi.fn()

    render(<ConfirmDialog {...defaultProps} onConfirm={onConfirm} />)
    const confirmButton = screen.getByText('Confirm')

    expect(confirmButton).toBeDisabled()
    expect(onConfirm).not.toHaveBeenCalled()
    vi.mocked(useIsReadOnly).mockReturnValue(false)
  })
})
