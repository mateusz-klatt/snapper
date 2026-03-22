import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import type { ReactNode } from 'react'
import { ProcessControlCard } from './ProcessControlCard'

const renderWithMocks = (ui: ReactNode) => {
  return render(ui)
}

describe('ProcessControlCard', () => {
  const mockOnStart = vi.fn()
  const mockOnStop = vi.fn()
  const mockOnRestart = vi.fn()

  it('renders card with title and description', () => {
    renderWithMocks(
      <ProcessControlCard
        title='Test Process'
        description='Test description'
        status='stopped'
        onStart={mockOnStart}
        onStop={mockOnStop}
      />
    )
    expect(screen.getByText('Test Process')).toBeInTheDocument()
    expect(screen.getByText('Test description')).toBeInTheDocument()
  })
  it('omits description element when description is empty', () => {
    renderWithMocks(
      <ProcessControlCard
        title='Test Process'
        description=''
        status='stopped'
        onStart={mockOnStart}
        onStop={mockOnStop}
      />
    )
    expect(screen.queryByRole('paragraph')).not.toBeInTheDocument()
  })
  it('shows start button when stopped', () => {
    renderWithMocks(
      <ProcessControlCard
        title='Test Process'
        description='Test description'
        status='stopped'
        onStart={mockOnStart}
        onStop={mockOnStop}
      />
    )
    expect(screen.getByText('Start')).toBeInTheDocument()
  })
  it('shows stop button when running', () => {
    renderWithMocks(
      <ProcessControlCard
        title='Test Process'
        description='Test description'
        status='running'
        onStart={mockOnStart}
        onStop={mockOnStop}
      />
    )
    expect(screen.getByText('Stop')).toBeInTheDocument()
  })
  it('calls onStart when start button clicked', async () => {
    const user = userEvent.setup()

    renderWithMocks(
      <ProcessControlCard
        title='Test Process'
        description='Test description'
        status='stopped'
        onStart={mockOnStart}
        onStop={mockOnStop}
      />
    )
    await user.click(screen.getByText('Start'))
    expect(mockOnStart).toHaveBeenCalled()
  })
  it('calls onStop when stop button clicked', async () => {
    const user = userEvent.setup()

    renderWithMocks(
      <ProcessControlCard
        title='Test Process'
        description='Test description'
        status='running'
        onStart={mockOnStart}
        onStop={mockOnStop}
      />
    )
    await user.click(screen.getByText('Stop'))
    expect(mockOnStop).toHaveBeenCalled()
  })
  it('displays status badge', () => {
    renderWithMocks(
      <ProcessControlCard
        title='Test Process'
        description='Test description'
        status='running'
        onStart={mockOnStart}
        onStop={mockOnStop}
      />
    )
    expect(screen.getByText('running')).toBeInTheDocument()
  })
  it('shows restart button when running', () => {
    renderWithMocks(
      <ProcessControlCard
        title='Test Process'
        description='Test description'
        status='running'
        onStart={mockOnStart}
        onStop={mockOnStop}
      />
    )
    expect(screen.getByText('Restart')).toBeInTheDocument()
  })
  it('displays loading state when starting', () => {
    renderWithMocks(
      <ProcessControlCard
        title='Test Process'
        description='Test description'
        status='stopped'
        isStarting={true}
        onStart={mockOnStart}
        onStop={mockOnStop}
      />
    )
    expect(screen.getByText(/starting/i)).toBeInTheDocument()
  })
  it('displays loading state when stopping', () => {
    renderWithMocks(
      <ProcessControlCard
        title='Test Process'
        description='Test description'
        status='running'
        isStopping={true}
        onStart={mockOnStart}
        onStop={mockOnStop}
      />
    )
    expect(screen.getByText(/stopping/i)).toBeInTheDocument()
  })
  it('displays status badge when provided', () => {
    renderWithMocks(
      <ProcessControlCard
        title='Test Process'
        description='Test description'
        status='running'
        statusBadge='v1.0.0'
        onStart={mockOnStart}
        onStop={mockOnStop}
      />
    )
    expect(screen.getByText('v1.0.0')).toBeInTheDocument()
  })
  it('displays details when provided', () => {
    renderWithMocks(
      <ProcessControlCard
        title='Test Process'
        description='Test description'
        status='running'
        details={{ memory: '100MB', cpu: '5%' }}
        onStart={mockOnStart}
        onStop={mockOnStop}
      />
    )
    expect(screen.getByText(/memory:/i)).toBeInTheDocument()
    expect(screen.getByText(/cpu:/i)).toBeInTheDocument()
  })
  it('stringifies object values in details', () => {
    renderWithMocks(
      <ProcessControlCard
        title='Test Process'
        description='Test description'
        status='running'
        details={{ meta: { version: '1.0.0' } }}
        onStart={mockOnStart}
        onStop={mockOnStop}
      />
    )
    expect(screen.getByText(/meta:/i)).toBeInTheDocument()
    expect(screen.getByText(/\{"version":"1.0.0"\}/)).toBeInTheDocument()
  })
  it('displays heartbeat as healthy with lag', () => {
    renderWithMocks(
      <ProcessControlCard
        title='Test Process'
        description='Test description'
        status='running'
        heartbeat={{ status: 'healthy', lag_ms: 50, timestamp: Date.now(), healthy: true }}
        onStart={mockOnStart}
        onStop={mockOnStop}
      />
    )
    expect(screen.getByText(/healthy/i)).toBeInTheDocument()
    expect(screen.getByText(/50ms/)).toBeInTheDocument()
  })
  it('displays heartbeat as error when unhealthy', () => {
    renderWithMocks(
      <ProcessControlCard
        title='Test Process'
        description='Test description'
        status='running'
        heartbeat={{ status: 'error', lag_ms: undefined, timestamp: Date.now(), healthy: false }}
        onStart={mockOnStart}
        onStop={mockOnStop}
      />
    )
    expect(screen.getByText(/error/i)).toBeInTheDocument()
  })
  it('displays heartbeat as unknown when status is unknown', () => {
    renderWithMocks(
      <ProcessControlCard
        title='Test Process'
        description='Test description'
        status='running'
        heartbeat={{
          status: 'unknown' as 'healthy',
          lag_ms: undefined,
          timestamp: 0,
          healthy: false,
        }}
        onStart={mockOnStart}
        onStop={mockOnStop}
      />
    )
    expect(screen.getByText(/unknown/i)).toBeInTheDocument()
  })
  it('omits heartbeat section when heartbeat is not provided', () => {
    renderWithMocks(
      <ProcessControlCard
        title='Test Process'
        description='Test description'
        status='running'
        onStart={mockOnStart}
        onStop={mockOnStop}
      />
    )
    expect(screen.queryByText(/waiting/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/healthy/i)).not.toBeInTheDocument()
  })
  it('displays error status correctly', () => {
    renderWithMocks(
      <ProcessControlCard
        title='Test Process'
        description='Test description'
        status='error'
        onStart={mockOnStart}
        onStop={mockOnStop}
      />
    )
    expect(screen.getByText('error')).toBeInTheDocument()
  })
  it('calls onRestart when restart clicked', async () => {
    const user = userEvent.setup()

    renderWithMocks(
      <ProcessControlCard
        title='Test Process'
        description='Test description'
        status='running'
        onStart={mockOnStart}
        onStop={mockOnStop}
        onRestart={mockOnRestart}
      />
    )
    await user.click(screen.getByText('Restart'))
    expect(mockOnRestart).toHaveBeenCalled()
  })
  it('uses default noop when onRestart is not provided and restart is clicked', async () => {
    const user = userEvent.setup()

    renderWithMocks(
      <ProcessControlCard
        title='Test Process'
        description='Test description'
        status='running'
        onStart={mockOnStart}
        onStop={mockOnStop}
      />
    )
    await user.click(screen.getByText('Restart'))
  })
})
