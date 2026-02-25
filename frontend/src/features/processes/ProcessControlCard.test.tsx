import { describe, it, expect, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
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
  it('displays last heartbeat when provided', () => {
    renderWithMocks(
      <ProcessControlCard
        title='Test Process'
        description='Test description'
        status='running'
        lastHeartbeat='2024-01-01T12:00:00Z'
        onStart={mockOnStart}
        onStop={mockOnStop}
      />
    )
    expect(screen.getByText(/Last heartbeat:/)).toBeInTheDocument()
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
  it('displays process list when showList is true', () => {
    const listItems = [
      { id: '1', name: 'Process 1', status: 'running' as const },
      { id: '2', name: 'Process 2', status: 'stopped' as const },
    ]

    renderWithMocks(
      <ProcessControlCard
        title='Test Process'
        description='Test description'
        status='running'
        showList={true}
        listItems={listItems}
        onStart={mockOnStart}
        onStop={mockOnStop}
      />
    )
    expect(screen.getByText('Process 1')).toBeInTheDocument()
    expect(screen.getByText('Process 2')).toBeInTheDocument()
  })
  it('shows stop button for running list item with onStop handler', async () => {
    const user = userEvent.setup()
    const onStopItem = vi.fn()
    const listItems = [
      { id: '1', name: 'Process 1', status: 'running' as const, onStop: onStopItem },
    ]

    renderWithMocks(
      <ProcessControlCard
        title='Test Process'
        description='Test description'
        status='running'
        showList={true}
        listItems={listItems}
        onStart={mockOnStart}
        onStop={mockOnStop}
      />
    )
    const listItem = screen.getByText('Process 1').closest('div')?.parentElement

    expect(listItem).toBeTruthy()
    const stopButton = within(listItem as HTMLElement).getByRole('button', { name: /Stop/i })

    await user.click(stopButton)
    expect(onStopItem).toHaveBeenCalled()
  })
  it('displays heartbeat data when provided', () => {
    const heartbeatData = {
      component1: { status: 'healthy', lag_ms: 50, timestamp: Date.now(), healthy: true },
      component2: { status: 'error', lag_ms: undefined, timestamp: Date.now(), healthy: false },
    }

    renderWithMocks(
      <ProcessControlCard
        title='Test Process'
        description='Test description'
        status='running'
        heartbeatData={heartbeatData}
        onStart={mockOnStart}
        onStop={mockOnStop}
      />
    )
    expect(screen.getByText(/component1/i)).toBeInTheDocument()
    expect(screen.getByText(/component2/i)).toBeInTheDocument()
    expect(screen.getByText(/50ms/)).toBeInTheDocument()
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
  it('displays custom heartbeat label', () => {
    const heartbeatData = {
      feed: { status: 'healthy', lag_ms: 10, timestamp: Date.now(), healthy: true },
    }

    renderWithMocks(
      <ProcessControlCard
        title='Test Process'
        description='Test description'
        status='running'
        heartbeatData={heartbeatData}
        heartbeatLabel='Feeds'
        onStart={mockOnStart}
        onStop={mockOnStop}
      />
    )
    expect(screen.getByText('Feeds:')).toBeInTheDocument()
  })
})
