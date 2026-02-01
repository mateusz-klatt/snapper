import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { Modal } from './Modal'

describe('Modal', () => {
  it('does not render when open is false', () => {
    render(
      <Modal open={false} onClose={vi.fn()}>
        <div>Modal content</div>
      </Modal>
    )
    expect(screen.queryByText('Modal content')).not.toBeInTheDocument()
  })
  it('renders when open is true', () => {
    render(
      <Modal open={true} onClose={vi.fn()}>
        <div>Modal content</div>
      </Modal>
    )
    expect(screen.getByText('Modal content')).toBeInTheDocument()
  })
  it('renders title when provided', () => {
    render(
      <Modal open={true} onClose={vi.fn()} title='Test Modal'>
        <div>Modal content</div>
      </Modal>
    )
    expect(screen.getByText('Test Modal')).toBeInTheDocument()
  })
  it('does not render title when not provided', () => {
    render(
      <Modal open={true} onClose={vi.fn()}>
        <div>Modal content</div>
      </Modal>
    )
    expect(screen.queryByRole('heading')).not.toBeInTheDocument()
  })
  it('calls onClose when backdrop is clicked', () => {
    const onClose = vi.fn()

    render(
      <Modal open={true} onClose={onClose}>
        <div>Modal content</div>
      </Modal>
    )
    const backdrop = document.querySelector('.bg-black.bg-opacity-50')

    expect(backdrop).toBeInTheDocument()

    if (backdrop) {
      fireEvent.click(backdrop)
    }

    expect(onClose).toHaveBeenCalledOnce()
  })
  it('calls onClose when close button is clicked', () => {
    const onClose = vi.fn()

    render(
      <Modal open={true} onClose={onClose} title='Test Modal'>
        <div>Modal content</div>
      </Modal>
    )
    const closeButton = screen.getByRole('button', { name: 'Close' })

    fireEvent.click(closeButton)
    expect(onClose).toHaveBeenCalledOnce()
  })
  it('calls onClose when ESC key is pressed', () => {
    const onClose = vi.fn()

    render(
      <Modal open={true} onClose={onClose}>
        <div>Modal content</div>
      </Modal>
    )
    fireEvent.keyDown(document, { key: 'Escape' })
    expect(onClose).toHaveBeenCalledOnce()
  })
  it('does not close on ESC when modal is closed', () => {
    const onClose = vi.fn()

    render(
      <Modal open={false} onClose={onClose}>
        <div>Modal content</div>
      </Modal>
    )
    fireEvent.keyDown(document, { key: 'Escape' })
    expect(onClose).not.toHaveBeenCalled()
  })
  it('does not call onClose when modal content is clicked', () => {
    const onClose = vi.fn()

    render(
      <Modal open={true} onClose={onClose}>
        <div>Modal content</div>
      </Modal>
    )
    const content = screen.getByText('Modal content')

    fireEvent.click(content)
    expect(onClose).not.toHaveBeenCalled()
  })
  it('applies correct size class for sm size', () => {
    render(
      <Modal open={true} onClose={vi.fn()} size='sm'>
        <div>Modal content</div>
      </Modal>
    )
    const modal = document.querySelector('.max-w-md')

    expect(modal).toBeInTheDocument()
  })
  it('applies correct size class for md size (default)', () => {
    render(
      <Modal open={true} onClose={vi.fn()}>
        <div>Modal content</div>
      </Modal>
    )
    const modal = document.querySelector('.max-w-lg')

    expect(modal).toBeInTheDocument()
  })
  it('applies correct size class for lg size', () => {
    render(
      <Modal open={true} onClose={vi.fn()} size='lg'>
        <div>Modal content</div>
      </Modal>
    )
    const modal = document.querySelector('.max-w-2xl')

    expect(modal).toBeInTheDocument()
  })
  it('applies correct size class for xl size', () => {
    render(
      <Modal open={true} onClose={vi.fn()} size='xl'>
        <div>Modal content</div>
      </Modal>
    )
    const modal = document.querySelector('.max-w-4xl')

    expect(modal).toBeInTheDocument()
  })
  it('sets body overflow to hidden when open', () => {
    document.body.style.overflow = ''
    const { rerender } = render(
      <Modal open={false} onClose={vi.fn()}>
        <div>Modal content</div>
      </Modal>
    )

    expect(document.body.style.overflow).toBe('')
    rerender(
      <Modal open={true} onClose={vi.fn()}>
        <div>Modal content</div>
      </Modal>
    )
    expect(document.body.style.overflow).toBe('hidden')
  })
  it('restores body overflow when closed', () => {
    const { rerender } = render(
      <Modal open={true} onClose={vi.fn()}>
        <div>Modal content</div>
      </Modal>
    )

    expect(document.body.style.overflow).toBe('hidden')
    rerender(
      <Modal open={false} onClose={vi.fn()}>
        <div>Modal content</div>
      </Modal>
    )
    expect(document.body.style.overflow).toBe('unset')
  })
})
