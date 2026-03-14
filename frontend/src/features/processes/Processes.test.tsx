import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor, fireEvent, act } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { Processes } from './Processes'
import type { ConfiguredProcess, AvailableProcess, ProcessRun } from '../../types/api'
import type { HeartbeatData } from '../../types/ws'

function createHeartbeat(
  component: string,
  status: 'healthy' | 'warning' | 'error',
  lagMs: number = 0,
  sequence: number = 1
): HeartbeatData {
  return {
    type: 'heartbeat',
    component,
    status,
    lag_ms: lagMs,
    sequence,
  }
}

let heartbeatCallback: ((msg: unknown) => void) | null = null
let connectionCallback: ((connected: boolean) => void) | null = null
const mockWsClient = {
  subscribe: vi.fn(),
  unsubscribe: vi.fn(),
  onConnection: vi.fn((cb: (connected: boolean) => void) => {
    connectionCallback = cb

    return vi.fn()
  }),
  onMessage: vi.fn((type: string, cb: (msg: unknown) => void) => {
    if (type === 'heartbeat') {
      heartbeatCallback = cb
    }

    return vi.fn()
  }),
}
const mockStartProcessMutate = vi.fn()
const mockStopProcessMutate = vi.fn()
const mockCreateProcessConfig = vi.fn()

vi.mock('../../hooks/queries', () => ({
  useConfiguredProcesses: vi.fn(() => ({
    data: null,
    isLoading: false,
  })),
  useAvailableProcesses: vi.fn(() => ({
    data: null,
    isLoading: false,
  })),
  useProcessRuns: vi.fn(() => ({
    data: null,
    isLoading: false,
  })),
  useStartProcessByName: vi.fn(() => ({
    mutate: mockStartProcessMutate,
    isPending: false,
  })),
  useStopProcessByName: vi.fn(() => ({
    mutate: mockStopProcessMutate,
    isPending: false,
  })),
  useCreateProcessConfig: vi.fn(() => ({ mutate: mockCreateProcessConfig })),
}))
vi.mock('../../stores/websocket', () => ({
  useWebSocketStore: vi.fn(() => ({
    wsClient: mockWsClient,
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

describe('Processes', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    heartbeatCallback = null
    connectionCallback = null
  })
  afterEach(() => {
    vi.clearAllTimers()
    vi.useRealTimers()
  })
  it('renders processes page', () => {
    renderWithProviders(<Processes />)
    expect(screen.getByText('Process Control')).toBeTruthy()
  })
  it('displays loading state', async () => {
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: null,
      isLoading: true,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByTestId('processes-skeleton')).toBeTruthy()
    })
  })
  it('displays configured processes section', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'executor_kraken',
        enabled: true,
        running: true,
        mode: 'thread',
        class_path: 'snapper.executor',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Kraken Executor',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Long-Running Processes')).toBeTruthy()
    })
  })
  it('subscribes to heartbeat topics', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'executor_kraken',
        enabled: true,
        running: true,
        mode: 'thread',
        class_path: 'snapper.executor',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Test',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(mockWsClient.subscribe).toHaveBeenCalled()
    })
  })
  it('handles empty process list', async () => {
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: [], count: 0 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Process Control')).toBeTruthy()
    })
  })
  it('displays available processes registry', async () => {
    const { useAvailableProcesses } = await import('../../hooks/queries')

    vi.mocked(useAvailableProcesses).mockReturnValue({
      data: {
        processes: [
          {
            name: 'executor',
            description: 'Trading Executor',
            role: 'executor',
            category: 'trading',
            parameters: [],
          },
        ],
      },
      isLoading: false,
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Process Control')).toBeTruthy()
    })
  })
  it('opens execution mode modal on start', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'executor_kraken',
        enabled: true,
        running: false,
        mode: 'thread',
        class_path: 'snapper.executor',
        method: 'main',
        args: [],
        kwargs: {},
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Process Control')).toBeTruthy()
    })
  })
  it('stops a running process', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'executor_kraken',
        enabled: true,
        running: true,
        mode: 'thread',
        class_path: 'snapper.executor',
        method: 'main',
        args: [],
        kwargs: {},
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Process Control')).toBeTruthy()
    })
  })
  it('displays process with heartbeat status', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'executor_kraken',
        enabled: true,
        running: true,
        mode: 'thread',
        class_path: 'snapper.executor',
        method: 'main',
        args: [],
        kwargs: {},
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Process Control')).toBeTruthy()
    })
  })
  it('groups processes by role', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'executor_kraken',
        enabled: true,
        running: true,
        mode: 'thread',
        class_path: 'snapper.executor',
        method: 'main',
        args: [],
        kwargs: {},
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
      {
        name: 'feed_kraken',
        enabled: true,
        running: true,
        mode: 'thread',
        class_path: 'snapper.feed',
        method: 'main',
        args: [],
        kwargs: {},
        lifecycle: 'long_running',
        role: 'task',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 2 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Process Control')).toBeTruthy()
    })
  })
  it('displays process runs history', async () => {
    const { useProcessRuns } = await import('../../hooks/queries')

    vi.mocked(useProcessRuns).mockReturnValue({
      data: {
        runs: [
          {
            run_id: '1',
            process_name: 'executor_kraken',
            status: 'succeeded',
            role: 'core',
            lifecycle: 'daemon',
            started_at: '2024-01-01T00:00:00Z',
            completed_at: '2024-01-01T01:00:00Z',
          },
        ],
      },
      isLoading: false,
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Process Control')).toBeTruthy()
    })
  })
  it('keeps latest run when earlier run appears later in list', async () => {
    const { useConfiguredProcesses, useProcessRuns } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: {
        processes: [
          {
            name: 'task_process',
            enabled: true,
            running: false,
            mode: 'thread',
            class_path: 'snapper.task',
            method: 'main',
            args: [],
            kwargs: {},
            lifecycle: 'one_shot',
            role: 'task',
            tags: [],
            is_one_shot: true,
          },
        ],
      },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    vi.mocked(useProcessRuns).mockReturnValue({
      data: {
        runs: [
          {
            run_id: 'latest',
            process_name: 'task_process',
            status: 'failed',
            role: 'task',
            lifecycle: 'one_shot',
            started_at: '2024-01-02T00:00:00Z',
            completed_at: '2024-01-02T01:00:00Z',
          },
          {
            run_id: 'earlier',
            process_name: 'task_process',
            status: 'succeeded',
            role: 'task',
            lifecycle: 'one_shot',
            started_at: '2024-01-01T00:00:00Z',
            completed_at: '2024-01-01T01:00:00Z',
          },
        ],
      },
      isLoading: false,
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText(/last run:/i)).toBeInTheDocument()
      expect(screen.getByText(/failed/i)).toBeInTheDocument()
    })
  })
  it('renders last run with null timestamp when missing', async () => {
    const { useConfiguredProcesses, useProcessRuns } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: {
        processes: [
          {
            name: 'task_process',
            enabled: true,
            running: false,
            mode: 'thread',
            class_path: 'snapper.task',
            method: 'main',
            args: [],
            kwargs: {},
            lifecycle: 'one_shot',
            role: 'task',
            tags: [],
            is_one_shot: true,
          },
        ],
      },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    vi.mocked(useProcessRuns).mockReturnValue({
      data: {
        runs: [
          {
            run_id: 'missing-time',
            process_name: 'task_process',
            status: 'succeeded',
            role: 'task',
            lifecycle: 'one_shot',
            started_at: null,
            completed_at: null,
          },
        ],
      },
      isLoading: false,
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText(/last run:/i)).toBeInTheDocument()
      expect(screen.getByText(/succeeded \(null\)/i)).toBeInTheDocument()
    })
  })
  it('subscribes to heartbeat topics', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'executor_kraken',
        enabled: true,
        running: true,
        mode: 'thread',
        class_path: 'snapper.executor',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Kraken Executor',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(mockWsClient.subscribe).toHaveBeenCalled()
    })
  })
  it('handles heartbeat messages', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'executor_kraken',
        enabled: true,
        running: true,
        mode: 'thread',
        class_path: 'snapper.executor',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Kraken Executor',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(mockWsClient.onMessage).toHaveBeenCalledWith('heartbeat', expect.any(Function))
    })

    if (heartbeatCallback) {
      heartbeatCallback(createHeartbeat('executor_kraken', 'healthy', 10))
    }
  })
  it('defaults heartbeat lag and healthy when missing', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'executor_kraken',
        enabled: true,
        running: true,
        mode: 'thread',
        class_path: 'snapper.executor',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Kraken Executor',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(mockWsClient.onMessage).toHaveBeenCalledWith('heartbeat', expect.any(Function))
    })
    act(() => {
      heartbeatCallback?.(createHeartbeat('executor_kraken', 'healthy'))
    })
    await waitFor(() => {
      expect(screen.getByText('(0ms)')).toBeTruthy()
    })
    act(() => {
      heartbeatCallback?.(createHeartbeat('executor_kraken', 'error'))
    })
    expect(screen.getByText('error')).toBeTruthy()
  })
  it('filters long-running processes correctly', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'executor_kraken',
        enabled: true,
        running: true,
        mode: 'thread',
        class_path: 'snapper.executor',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Kraken Executor',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
      {
        name: 'strategy_test',
        enabled: true,
        running: false,
        mode: 'thread',
        class_path: 'snapper.strategy',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Test Strategy',
        lifecycle: 'long_running',
        role: 'strategy',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Process Control')).toBeTruthy()
    })
    expect(screen.queryByText('Test Strategy')).toBeNull()
  })
  it('filters one-shot task processes', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'data_sync_task',
        enabled: true,
        running: false,
        mode: 'thread',
        class_path: 'snapper.tasks.sync',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Data sync task',
        lifecycle: 'one_shot',
        role: 'core',
        tags: [],
        is_one_shot: true,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Process Control')).toBeTruthy()
    })
  })
  it('shows auto-start and manual badges for non-one-shot tasks', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'auto_task',
        enabled: true,
        running: false,
        mode: 'thread',
        class_path: 'snapper.tasks.auto',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Auto task',
        lifecycle: 'one_shot',
        role: 'task',
        tags: [],
        is_one_shot: false,
      },
      {
        name: 'manual_task',
        enabled: false,
        running: false,
        mode: 'thread',
        class_path: 'snapper.tasks.manual',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Manual task',
        lifecycle: 'one_shot',
        role: 'task',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 2 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Task Processes')).toBeTruthy()
    })
    expect(screen.getByText('auto-start')).toBeTruthy()
    expect(screen.getByText('manual')).toBeTruthy()
  })
  it('handles connection callback for websocket', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'executor_kraken',
        enabled: true,
        running: true,
        mode: 'thread',
        class_path: 'snapper.executor',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Kraken Executor',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(mockWsClient.onConnection).toHaveBeenCalled()
    })

    if (connectionCallback) {
      connectionCallback(true)
      expect(mockWsClient.subscribe).toHaveBeenCalled()
    }
  })
  it('does not resubscribe when connection callback is false', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'executor_kraken',
        enabled: true,
        running: true,
        mode: 'thread',
        class_path: 'snapper.executor',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Kraken Executor',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(mockWsClient.onConnection).toHaveBeenCalled()
    })
    const initialCalls = mockWsClient.subscribe.mock.calls.length

    act(() => {
      connectionCallback?.(false)
    })
    expect(mockWsClient.subscribe).toHaveBeenCalledTimes(initialCalls)
  })
  it('displays feed publisher processes with heartbeat', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'kraken_feed_publisher',
        enabled: true,
        running: true,
        mode: 'thread',
        class_path: 'snapper.feed_publisher',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Kraken Feed Publisher',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Process Control')).toBeTruthy()
    })
  })
  it('renders executor process with heartbeat data mapping', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'executor_kraken',
        enabled: true,
        running: true,
        mode: 'thread',
        class_path: 'snapper.executor',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Kraken Executor',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const mockAvailableProcesses: AvailableProcess[] = [
      {
        name: 'executor_kraken',
        class_path: 'snapper.executor',
        method: 'main',
        description: 'Kraken Trading Executor',
        lifecycle: 'long_running',
        role: 'core',
        tags: ['kraken', 'trading'],
        parameters_schema: null,
      },
    ]
    const { useConfiguredProcesses, useAvailableProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    vi.mocked(useAvailableProcesses).mockReturnValue({
      data: { processes: mockAvailableProcesses },
      isLoading: false,
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Kraken Trading Executor')).toBeTruthy()
    })
  })
  it('opens execution mode modal and starts long-running process', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'executor_kraken',
        enabled: false,
        running: false,
        mode: 'thread',
        class_path: 'snapper.executor',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Kraken Executor',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Long-Running Processes')).toBeTruthy()
    })
    const startButtons = screen.getAllByRole('button', { name: /start/i })

    expect(startButtons.length).toBeGreaterThan(0)
    fireEvent.click(startButtons[0])
    await waitFor(() => {
      expect(screen.getByText('Execution Mode:')).toBeTruthy()
    })
    const modalButtons = screen.getAllByRole('button')
    const modalStartButton = modalButtons.find(
      btn =>
        btn.textContent?.toLowerCase().includes('start') && btn.classList.contains('bg-primary-600')
    )

    expect(modalStartButton).toBeTruthy()

    if (modalStartButton) {
      fireEvent.click(modalStartButton)
    }

    expect(mockStartProcessMutate).toHaveBeenCalled()
  })
  it('starts long-running process from list', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'executor_kraken',
        enabled: false,
        running: false,
        mode: 'thread',
        class_path: 'snapper.executor',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Kraken Executor',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses, useAvailableProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    vi.mocked(useAvailableProcesses).mockReturnValue({
      data: { processes: [] },
      isLoading: false,
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Kraken Executor')).toBeTruthy()
    })
    const executorCard = screen.getByText('Kraken Executor').closest('.rounded-2xl')
    const startButton = executorCard?.querySelector('button')

    expect(startButton).toBeTruthy()

    if (startButton) {
      fireEvent.click(startButton)
    }

    await waitFor(() => {
      expect(screen.getByText('Execution Mode:')).toBeTruthy()
    })
    fireEvent.click(screen.getByRole('button', { name: /start executor_kraken/i }))
    expect(mockStartProcessMutate).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'executor_kraken',
      })
    )
  })
  it('opens confirm dialog and stops long-running process', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'executor_kraken',
        enabled: true,
        running: true,
        mode: 'thread',
        class_path: 'snapper.executor',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Kraken Executor',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Long-Running Processes')).toBeTruthy()
    })
    const stopButtons = screen.getAllByRole('button', { name: /stop/i })

    expect(stopButtons.length).toBeGreaterThan(0)
    fireEvent.click(stopButtons[0])
    await waitFor(() => {
      expect(screen.getByRole('button', { name: /confirm/i })).toBeTruthy()
    })
    fireEvent.click(screen.getByRole('button', { name: /confirm/i }))
    expect(mockStopProcessMutate).toHaveBeenCalled()
  })
  it('displays task processes with details including tags and parameters_schema', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'data_sync_task',
        enabled: true,
        running: false,
        mode: 'process',
        class_path: 'snapper.tasks.sync',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Data sync task',
        lifecycle: 'one_shot',
        role: 'task',
        tags: ['sync', 'data'],
        is_one_shot: true,
        parameters_schema: { type: 'object', properties: { source: { type: 'string' } } },
        active_run_id: 'run-123',
      },
    ]
    const mockAvailableProcesses: AvailableProcess[] = [
      {
        name: 'data_sync_task',
        class_path: 'snapper.tasks.sync',
        method: 'main',
        description: 'Synchronize data from external sources',
        lifecycle: 'one_shot',
        role: 'task',
        tags: ['sync', 'data'],
        parameters_schema: { type: 'object', properties: { source: { type: 'string' } } },
      },
    ]
    const mockRuns: ProcessRun[] = [
      {
        run_id: 'run-old',
        process_name: 'data_sync_task',
        status: 'succeeded',
        role: 'task',
        lifecycle: 'one_shot',
        started_at: '2024-01-01T00:00:00Z',
        completed_at: '2024-01-01T00:05:00Z',
      },
      {
        run_id: 'run-123',
        process_name: 'data_sync_task',
        status: 'running',
        role: 'task',
        lifecycle: 'one_shot',
        started_at: '2024-01-02T00:00:00Z',
      },
    ]
    const { useConfiguredProcesses, useAvailableProcesses, useProcessRuns } =
      await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    vi.mocked(useAvailableProcesses).mockReturnValue({
      data: { processes: mockAvailableProcesses },
      isLoading: false,
    } as never)
    vi.mocked(useProcessRuns).mockReturnValue({
      data: { runs: mockRuns },
      isLoading: false,
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Task Processes')).toBeTruthy()
    })
    expect(screen.getByText('Synchronize data from external sources')).toBeTruthy()
  })
  it('starts task process with execution mode modal and confirm dialog', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'data_sync_task',
        enabled: false,
        running: false,
        mode: 'process',
        class_path: 'snapper.tasks.sync',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Data sync task',
        lifecycle: 'one_shot',
        role: 'task',
        tags: [],
        is_one_shot: true,
      },
    ]
    const { useConfiguredProcesses, useAvailableProcesses } = await import('../../hooks/queries')

    vi.mocked(useAvailableProcesses).mockReturnValue({
      data: null,
      isLoading: false,
    } as never)

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Task Processes')).toBeTruthy()
    })
    const taskTitle = screen.getByText('Data sync task')
    const taskCard = taskTitle.closest('.rounded-2xl')
    const startButton = taskCard?.querySelector('button')

    expect(startButton).toBeTruthy()

    if (startButton) {
      fireEvent.click(startButton)
    }

    await waitFor(() => {
      expect(screen.getByText('Execution Mode:')).toBeTruthy()
    })
    const modalButtons = screen.getAllByRole('button')
    const modalStartButton = modalButtons.find(
      btn =>
        btn.textContent?.toLowerCase().includes('start') &&
        btn.textContent?.includes('data_sync_task')
    )

    expect(modalStartButton).toBeTruthy()

    if (modalStartButton) {
      fireEvent.click(modalStartButton)
    }

    expect(mockStartProcessMutate).toHaveBeenCalled()
  })
  it('stops task process with confirm dialog', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'data_sync_task',
        enabled: true,
        running: true,
        mode: 'process',
        class_path: 'snapper.tasks.sync',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Data sync task',
        lifecycle: 'one_shot',
        role: 'task',
        tags: [],
        is_one_shot: true,
      },
    ]
    const { useConfiguredProcesses, useAvailableProcesses } = await import('../../hooks/queries')

    vi.mocked(useAvailableProcesses).mockReturnValue({
      data: null,
      isLoading: false,
    } as never)

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Task Processes')).toBeTruthy()
    })
    const taskTitle = screen.getByText('Data sync task')
    const taskCard = taskTitle.closest('.rounded-2xl')
    const stopButton = taskCard?.querySelector('button')

    expect(stopButton).toBeTruthy()

    if (stopButton) {
      fireEvent.click(stopButton)
    }

    await waitFor(() => {
      expect(screen.getByRole('button', { name: /confirm/i })).toBeTruthy()
    })
    fireEvent.click(screen.getByRole('button', { name: /confirm/i }))
    expect(mockStopProcessMutate).toHaveBeenCalled()
  })
  it('cancels confirm dialog', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'executor_kraken',
        enabled: true,
        running: true,
        mode: 'thread',
        class_path: 'snapper.executor',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Kraken Executor',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Long-Running Processes')).toBeTruthy()
    })
    const stopButtons = screen.getAllByRole('button', { name: /stop/i })

    fireEvent.click(stopButtons[0])
    await waitFor(() => {
      expect(screen.getByRole('button', { name: /cancel/i })).toBeTruthy()
    })
    fireEvent.click(screen.getByRole('button', { name: /cancel/i }))
    expect(mockStopProcessMutate).not.toHaveBeenCalled()
  })
  it('closes execution mode modal', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'executor_kraken',
        enabled: false,
        running: false,
        mode: 'thread',
        class_path: 'snapper.executor',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Kraken Executor',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Long-Running Processes')).toBeTruthy()
    })
    const startButtons = screen.getAllByRole('button', { name: /start/i })

    fireEvent.click(startButtons[0])
    await waitFor(() => {
      expect(screen.getByText('Execution Mode:')).toBeTruthy()
    })
    const cancelButton = screen.getByRole('button', { name: /cancel/i })

    fireEvent.click(cancelButton)
    await waitFor(() => {
      expect(screen.queryByText('Execution Mode:')).toBeNull()
    })
  })
  it('cleans up stale heartbeats after timeout', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'executor_kraken',
        enabled: true,
        running: true,
        mode: 'thread',
        class_path: 'snapper.executor',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Kraken Executor',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Process Control')).toBeTruthy()
    })
    expect(mockWsClient.onMessage).toHaveBeenCalledWith('heartbeat', expect.any(Function))
    expect(heartbeatCallback).toBeDefined()
    act(() => {
      heartbeatCallback?.(createHeartbeat('executor_kraken', 'healthy', 10))
    })
  })
  it('removes stale heartbeats after threshold', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2024-01-01T00:00:00Z'))

    try {
      const mockProcesses: ConfiguredProcess[] = [
        {
          name: 'executor_kraken',
          enabled: true,
          running: true,
          mode: 'thread',
          class_path: 'snapper.executor',
          method: 'main',
          args: [],
          kwargs: {},
          note: 'Kraken Executor',
          lifecycle: 'long_running',
          role: 'core',
          tags: [],
          is_one_shot: false,
        },
      ]
      const { useConfiguredProcesses } = await import('../../hooks/queries')

      vi.mocked(useConfiguredProcesses).mockReturnValue({
        data: { processes: mockProcesses, count: 1 },
        isLoading: false,
        refetch: vi.fn(),
      } as never)
      renderWithProviders(<Processes />)
      expect(screen.getByText('Process Control')).toBeTruthy()
      expect(screen.getByText(/waiting/i)).toBeTruthy()
      act(() => {
        heartbeatCallback?.(createHeartbeat('executor_kraken', 'healthy', 10))
      })
      expect(screen.queryByText(/waiting/i)).toBeNull()
      act(() => {
        vi.advanceTimersByTime(15000)
      })
      expect(screen.getByText(/waiting/i)).toBeTruthy()
    } finally {
      vi.useRealTimers()
    }
  })
  it('starts ZMQ Broker process', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'zmq_broker',
        enabled: true,
        running: false,
        mode: 'thread',
        class_path: 'snapper.zmq_broker',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'ZMQ Broker',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('ZMQ Broker')).toBeTruthy()
    })
    const zmqCard = screen.getByText('ZMQ Broker').closest('.rounded-2xl')
    const startButton = zmqCard?.querySelector('button')

    expect(startButton).toBeTruthy()
    expect(startButton?.textContent).toContain('Start')

    if (startButton) {
      fireEvent.click(startButton)
    }

    await waitFor(() => {
      expect(screen.getByText('Execution Mode:')).toBeTruthy()
    })
    const modalButtons = screen.getAllByRole('button')
    const modalStartButton = modalButtons.find(
      btn =>
        btn.textContent?.toLowerCase().includes('start') && btn.classList.contains('bg-primary-600')
    )

    expect(modalStartButton).toBeTruthy()

    if (modalStartButton) {
      fireEvent.click(modalStartButton)
    }

    expect(mockStartProcessMutate).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'zmq_broker',
      })
    )
  })
  it('stops ZMQ Broker process with warning', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'zmq_broker',
        enabled: true,
        running: true,
        mode: 'thread',
        class_path: 'snapper.zmq_broker',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'ZMQ Broker',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('ZMQ Broker')).toBeTruthy()
    })
    const zmqCard = screen.getByText('ZMQ Broker').closest('.rounded-2xl')
    const stopButton = zmqCard?.querySelector('button')

    expect(stopButton).toBeTruthy()
    expect(stopButton?.textContent).toContain('Stop')

    if (stopButton) {
      fireEvent.click(stopButton)
    }

    await waitFor(() => {
      expect(screen.getByText(/This will stop the zmq_broker process/i)).toBeTruthy()
    })
    fireEvent.click(screen.getByRole('button', { name: /confirm/i }))
    expect(mockStopProcessMutate).toHaveBeenCalledWith({ name: 'zmq_broker' })
  })
  it('formats timestamp correctly', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'data_sync_task',
        enabled: true,
        running: false,
        mode: 'process',
        class_path: 'snapper.tasks.sync',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Data sync task',
        lifecycle: 'one_shot',
        role: 'task',
        tags: [],
        is_one_shot: true,
      },
    ]
    const mockRuns: ProcessRun[] = [
      {
        run_id: 'run-123',
        process_name: 'data_sync_task',
        status: 'succeeded',
        role: 'task',
        lifecycle: 'one_shot',
        started_at: '2024-06-15T10:30:00Z',
        completed_at: '2024-06-15T10:35:00Z',
      },
    ]
    const { useConfiguredProcesses, useProcessRuns } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    vi.mocked(useProcessRuns).mockReturnValue({
      data: { runs: mockRuns },
      isLoading: false,
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Task Processes')).toBeTruthy()
    })
  })
  it('handles null timestamp gracefully', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'data_sync_task',
        enabled: true,
        running: false,
        mode: 'process',
        class_path: 'snapper.tasks.sync',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Data sync task',
        lifecycle: 'one_shot',
        role: 'task',
        tags: [],
        is_one_shot: true,
      },
    ]
    const mockRuns: ProcessRun[] = [
      {
        run_id: 'run-123',
        process_name: 'data_sync_task',
        status: 'running',
        role: 'task',
        lifecycle: 'one_shot',
        started_at: 'invalid-date',
      },
    ]
    const { useConfiguredProcesses, useProcessRuns } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    vi.mocked(useProcessRuns).mockReturnValue({
      data: { runs: mockRuns },
      isLoading: false,
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Task Processes')).toBeTruthy()
    })
  })
  it('handles task process without tags from registry', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'simple_task',
        enabled: false,
        running: false,
        mode: 'thread',
        class_path: 'snapper.tasks.simple',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Simple task',
        lifecycle: 'one_shot',
        role: 'task',
        tags: ['local-tag'],
        is_one_shot: true,
      },
    ]
    const { useConfiguredProcesses, useAvailableProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    vi.mocked(useAvailableProcesses).mockReturnValue({
      data: { processes: [] },
      isLoading: false,
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Task Processes')).toBeTruthy()
    })
    expect(screen.getByText('Simple task')).toBeTruthy()
  })
  it('handles wsClient being null', async () => {
    const { useWebSocketStore } = await import('../../stores/websocket')

    vi.mocked(useWebSocketStore).mockReturnValue({
      wsClient: null,
    } as never)
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'executor_kraken',
        enabled: true,
        running: true,
        mode: 'thread',
        class_path: 'snapper.executor',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Kraken Executor',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Process Control')).toBeTruthy()
    })
    expect(mockWsClient.subscribe).not.toHaveBeenCalled()
    vi.mocked(useWebSocketStore).mockReturnValue({
      wsClient: mockWsClient,
    } as never)
  })
  it('cleans up stale heartbeats when they exceed threshold', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'executor_kraken',
        enabled: true,
        running: true,
        mode: 'thread',
        class_path: 'snapper.executor',
        method: 'main',
        args: [],
        kwargs: {},
        note: 'Kraken Executor',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    const { unmount } = renderWithProviders(<Processes />)

    await waitFor(() => {
      expect(screen.getByText('Process Control')).toBeTruthy()
    })
    act(() => {
      heartbeatCallback?.(createHeartbeat('executor_kraken', 'healthy', 10))
    })
    unmount()
  })
  it('shows starting state only for ZMQ Broker when isPending', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'zmq_broker',
        enabled: false,
        running: false,
        mode: 'thread',
        class_path: 'snapper.zmq_broker',
        method: 'main',
        args: [],
        kwargs: {},
        note: '',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses, useStartProcessByName } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    vi.mocked(useStartProcessByName).mockReturnValue({
      mutate: mockStartProcessMutate,
      isPending: true,
      variables: { name: 'zmq_broker' },
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('zmq_broker')).toBeTruthy()
    })
    expect(screen.getByText(/Starting/i)).toBeTruthy()
  })
  it('shows starting state only for targeted long-running process when isPending', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'executor_kraken',
        enabled: false,
        running: false,
        mode: 'thread',
        class_path: 'snapper.executor',
        method: 'main',
        args: [],
        kwargs: {},
        note: '',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses, useStartProcessByName } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    vi.mocked(useStartProcessByName).mockReturnValue({
      mutate: mockStartProcessMutate,
      isPending: true,
      variables: { name: 'executor_kraken' },
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Long-Running Processes')).toBeTruthy()
    })
    expect(screen.getByText(/Starting/i)).toBeTruthy()
  })
  it('shows starting state only for targeted task process when isPending', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'sync_task',
        enabled: false,
        running: false,
        mode: 'thread',
        class_path: 'snapper.sync_task',
        method: 'main',
        args: [],
        kwargs: {},
        note: '',
        lifecycle: 'one_shot',
        role: 'task',
        tags: [],
        is_one_shot: true,
      },
    ]
    const { useConfiguredProcesses, useStartProcessByName } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    vi.mocked(useStartProcessByName).mockReturnValue({
      mutate: mockStartProcessMutate,
      isPending: true,
      variables: { name: 'sync_task' },
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Task Processes')).toBeTruthy()
    })
    expect(screen.getByText(/Starting/i)).toBeTruthy()
  })
  it('shows stopping state only for targeted long-running process when isPending', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'executor_kraken',
        enabled: true,
        running: true,
        mode: 'thread',
        class_path: 'snapper.executor',
        method: 'main',
        args: [],
        kwargs: {},
        note: '',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
      {
        name: 'executor_zonda',
        enabled: true,
        running: true,
        mode: 'thread',
        class_path: 'snapper.executor',
        method: 'main',
        args: [],
        kwargs: {},
        note: '',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses, useStopProcessByName } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 2 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    vi.mocked(useStopProcessByName).mockReturnValue({
      mutate: mockStopProcessMutate,
      isPending: true,
      variables: { name: 'executor_kraken' },
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Long-Running Processes')).toBeTruthy()
    })
    expect(screen.getByText(/Stopping/i)).toBeTruthy()
  })
  it('shows stopping state only for targeted task process when isPending', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'sync_task',
        enabled: false,
        running: true,
        mode: 'thread',
        class_path: 'snapper.sync_task',
        method: 'main',
        args: [],
        kwargs: {},
        note: '',
        lifecycle: 'one_shot',
        role: 'task',
        tags: [],
        is_one_shot: true,
      },
    ]
    const { useConfiguredProcesses, useStopProcessByName } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    vi.mocked(useStopProcessByName).mockReturnValue({
      mutate: mockStopProcessMutate,
      isPending: true,
      variables: { name: 'sync_task' },
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Task Processes')).toBeTruthy()
    })
    expect(screen.getByText(/Stopping/i)).toBeTruthy()
  })
  it('restarts ZMQ Broker: shows confirm dialog and calls stop with onSuccess', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'zmq_broker',
        enabled: true,
        running: true,
        mode: 'thread',
        class_path: 'snapper.zmq_broker',
        method: 'main',
        args: [],
        kwargs: {},
        note: '',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('zmq_broker')).toBeTruthy()
    })
    const zmqCard = screen.getByText('zmq_broker').closest('.rounded-2xl')
    const restartButton = Array.from(zmqCard?.querySelectorAll('button') ?? []).find(
      (btn: Element) => btn.textContent === 'Restart'
    )

    expect(restartButton).toBeTruthy()

    if (restartButton) {
      fireEvent.click(restartButton)
    }

    await waitFor(() => {
      expect(screen.getByText(/Restart zmq_broker/i)).toBeTruthy()
    })
    fireEvent.click(screen.getByRole('button', { name: /confirm/i }))
    expect(mockStopProcessMutate).toHaveBeenCalledWith(
      { name: 'zmq_broker' },
      expect.objectContaining({ onSuccess: expect.any(Function) })
    )
  })
  it('restarts ZMQ Broker: onSuccess opens execution mode modal and starts process', async () => {
    let capturedOnSuccess: (() => void) | undefined

    mockStopProcessMutate.mockImplementation(
      (_args: unknown, options?: { onSuccess?: () => void }) => {
        capturedOnSuccess = options?.onSuccess
      }
    )
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'zmq_broker',
        enabled: true,
        running: true,
        mode: 'thread',
        class_path: 'snapper.zmq_broker',
        method: 'main',
        args: [],
        kwargs: {},
        note: '',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('zmq_broker')).toBeTruthy()
    })
    const zmqCard = screen.getByText('zmq_broker').closest('.rounded-2xl')
    const restartButton = Array.from(zmqCard?.querySelectorAll('button') ?? []).find(
      (btn: Element) => btn.textContent === 'Restart'
    )

    if (restartButton) {
      fireEvent.click(restartButton)
    }

    fireEvent.click(screen.getByRole('button', { name: /confirm/i }))
    expect(capturedOnSuccess).toBeDefined()
    act(() => {
      capturedOnSuccess?.()
    })
    await waitFor(() => {
      expect(screen.getByText('Execution Mode:')).toBeTruthy()
    })
    const modalButtons = screen.getAllByRole('button')
    const modalStartButton = modalButtons.find(
      (btn: HTMLElement) =>
        btn.textContent?.toLowerCase().includes('start') && btn.classList.contains('bg-primary-600')
    )

    expect(modalStartButton).toBeTruthy()

    if (modalStartButton) {
      fireEvent.click(modalStartButton)
    }

    expect(mockStartProcessMutate).toHaveBeenCalledWith(
      expect.objectContaining({ name: 'zmq_broker' })
    )
  })
  it('restarts long-running process: shows confirm dialog and calls stop with onSuccess', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'my_service',
        enabled: false,
        running: true,
        mode: 'thread',
        class_path: 'snapper.my_service',
        method: 'main',
        args: [],
        kwargs: {},
        note: '',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Long-Running Processes')).toBeTruthy()
    })
    const restartButtons = screen.getAllByRole('button', { name: /restart/i })

    expect(restartButtons.length).toBeGreaterThan(0)
    fireEvent.click(restartButtons[0])
    await waitFor(() => {
      expect(screen.getByText(/Restart my_service/i)).toBeTruthy()
    })
    fireEvent.click(screen.getByRole('button', { name: /confirm/i }))
    expect(mockStopProcessMutate).toHaveBeenCalledWith(
      { name: 'my_service' },
      expect.objectContaining({ onSuccess: expect.any(Function) })
    )
  })
  it('restarts long-running process: onSuccess opens execution mode modal and starts process', async () => {
    let capturedOnSuccess: (() => void) | undefined

    mockStopProcessMutate.mockImplementation(
      (_args: unknown, options?: { onSuccess?: () => void }) => {
        capturedOnSuccess = options?.onSuccess
      }
    )
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'my_service',
        enabled: false,
        running: true,
        mode: 'thread',
        class_path: 'snapper.my_service',
        method: 'main',
        args: [],
        kwargs: {},
        note: '',
        lifecycle: 'long_running',
        role: 'core',
        tags: [],
        is_one_shot: false,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Long-Running Processes')).toBeTruthy()
    })
    const restartButtons = screen.getAllByRole('button', { name: /restart/i })

    fireEvent.click(restartButtons[0])
    fireEvent.click(screen.getByRole('button', { name: /confirm/i }))
    expect(capturedOnSuccess).toBeDefined()
    act(() => {
      capturedOnSuccess?.()
    })
    await waitFor(() => {
      expect(screen.getByText('Execution Mode:')).toBeTruthy()
    })
    const modalButtons = screen.getAllByRole('button')
    const modalStartButton = modalButtons.find(
      (btn: HTMLElement) =>
        btn.textContent?.toLowerCase().includes('start') && btn.classList.contains('bg-primary-600')
    )

    expect(modalStartButton).toBeTruthy()

    if (modalStartButton) {
      fireEvent.click(modalStartButton)
    }

    expect(mockStartProcessMutate).toHaveBeenCalledWith(
      expect.objectContaining({ name: 'my_service' })
    )
  })
  it('restarts task process: shows confirm dialog and calls stop with onSuccess', async () => {
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'sync_task',
        enabled: false,
        running: true,
        mode: 'thread',
        class_path: 'snapper.sync_task',
        method: 'main',
        args: [],
        kwargs: {},
        note: '',
        lifecycle: 'one_shot',
        role: 'task',
        tags: [],
        is_one_shot: true,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Task Processes')).toBeTruthy()
    })
    const restartButtons = screen.getAllByRole('button', { name: /restart/i })

    expect(restartButtons.length).toBeGreaterThan(0)
    fireEvent.click(restartButtons[0])
    await waitFor(() => {
      expect(screen.getByText(/Restart sync_task/i)).toBeTruthy()
    })
    fireEvent.click(screen.getByRole('button', { name: /confirm/i }))
    expect(mockStopProcessMutate).toHaveBeenCalledWith(
      { name: 'sync_task' },
      expect.objectContaining({ onSuccess: expect.any(Function) })
    )
  })
  it('restarts task process: onSuccess opens execution mode modal', async () => {
    let capturedOnSuccess: (() => void) | undefined

    mockStopProcessMutate.mockImplementation(
      (_args: unknown, options?: { onSuccess?: () => void }) => {
        capturedOnSuccess = options?.onSuccess
      }
    )
    const mockProcesses: ConfiguredProcess[] = [
      {
        name: 'sync_task',
        enabled: false,
        running: true,
        mode: 'thread',
        class_path: 'snapper.sync_task',
        method: 'main',
        args: [],
        kwargs: {},
        note: '',
        lifecycle: 'one_shot',
        role: 'task',
        tags: [],
        is_one_shot: true,
      },
    ]
    const { useConfiguredProcesses } = await import('../../hooks/queries')

    vi.mocked(useConfiguredProcesses).mockReturnValue({
      data: { processes: mockProcesses, count: 1 },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Processes />)
    await waitFor(() => {
      expect(screen.getByText('Task Processes')).toBeTruthy()
    })
    const restartButtons = screen.getAllByRole('button', { name: /restart/i })

    fireEvent.click(restartButtons[0])
    fireEvent.click(screen.getByRole('button', { name: /confirm/i }))
    expect(capturedOnSuccess).toBeDefined()
    act(() => {
      capturedOnSuccess?.()
    })
    await waitFor(() => {
      expect(screen.getByText('Execution Mode:')).toBeTruthy()
    })
  })
})
