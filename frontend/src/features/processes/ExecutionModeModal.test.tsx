import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import type { ReactNode } from 'react'
import { ExecutionModeModal } from './ExecutionModeModal'

const renderWithMocks = (ui: ReactNode) => {
  return render(ui)
}

describe('ExecutionModeModal', () => {
  const mockOnClose = vi.fn()
  const mockOnStart = vi.fn()

  beforeEach(() => {
    vi.clearAllMocks()
  })
  it('renders modal when open', () => {
    renderWithMocks(
      <ExecutionModeModal
        open={true}
        onClose={mockOnClose}
        onStart={mockOnStart}
        componentName='Test Component'
        description='Test description'
      />
    )
    const buttons = screen.getAllByText('Start Test Component')

    expect(buttons[0]).toBeInTheDocument()
  })
  it('does not render when closed', () => {
    renderWithMocks(
      <ExecutionModeModal
        open={false}
        onClose={mockOnClose}
        onStart={mockOnStart}
        componentName='Test Component'
        description='Test description'
      />
    )
    expect(screen.queryByText('Start Test Component')).not.toBeInTheDocument()
  })
  it('calls onStart with thread mode by default', async () => {
    const user = userEvent.setup()

    renderWithMocks(
      <ExecutionModeModal
        open={true}
        onClose={mockOnClose}
        onStart={mockOnStart}
        componentName='Test Component'
        description='Test description'
      />
    )
    const startButton = screen.getAllByRole('button', { name: /Start Test Component/i })[0]

    await user.click(startButton)
    expect(mockOnStart).toHaveBeenCalledWith({
      executionMode: 'thread',
    })
    expect(mockOnClose).toHaveBeenCalled()
  })
  it('allows selecting process mode', async () => {
    const user = userEvent.setup()

    renderWithMocks(
      <ExecutionModeModal
        open={true}
        onClose={mockOnClose}
        onStart={mockOnStart}
        componentName='Test Component'
        description='Test description'
      />
    )
    const processRadio = screen.getByRole('radio', { name: /Process Mode/i })

    await user.click(processRadio)
    const startButton = screen.getAllByRole('button', { name: /Start Test Component/i })[0]

    await user.click(startButton)
    expect(mockOnStart).toHaveBeenCalledWith({
      executionMode: 'process',
    })
  })
  it('allows switching back to thread mode', async () => {
    const user = userEvent.setup()

    renderWithMocks(
      <ExecutionModeModal
        open={true}
        onClose={mockOnClose}
        onStart={mockOnStart}
        componentName='Test Component'
        description='Test description'
      />
    )
    const processRadio = screen.getByRole('radio', { name: /Process Mode/i })
    const threadRadio = screen.getByRole('radio', { name: /Thread Mode/i })

    await user.click(processRadio)
    expect(processRadio).toBeChecked()
    await user.click(threadRadio)
    expect(threadRadio).toBeChecked()
  })
  it('calls onClose when cancel is clicked', async () => {
    const user = userEvent.setup()

    renderWithMocks(
      <ExecutionModeModal
        open={true}
        onClose={mockOnClose}
        onStart={mockOnStart}
        componentName='Test Component'
        description='Test description'
      />
    )
    const cancelButton = screen.getByRole('button', { name: /Cancel/i })

    await user.click(cancelButton)
    expect(mockOnClose).toHaveBeenCalled()
  })
  it('resets state when modal reopens', async () => {
    const user = userEvent.setup()
    const { rerender } = renderWithMocks(
      <ExecutionModeModal
        open={true}
        onClose={mockOnClose}
        onStart={mockOnStart}
        componentName='Test Component'
        description='Test description'
      />
    )
    const processRadio = screen.getByRole('radio', { name: /Process Mode/i })

    await user.click(processRadio)
    rerender(
      <ExecutionModeModal
        open={false}
        onClose={mockOnClose}
        onStart={mockOnStart}
        componentName='Test Component'
        description='Test description'
      />
    )
    rerender(
      <ExecutionModeModal
        open={true}
        onClose={mockOnClose}
        onStart={mockOnStart}
        componentName='Test Component'
        description='Test description'
      />
    )
    const threadRadio = screen.getByRole('radio', { name: /Thread Mode/i })

    expect(threadRadio).toBeChecked()
  })
})
