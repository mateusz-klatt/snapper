import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { Strategies } from './Strategies'
import type { HeartbeatData } from '../../types/ws'
import { makeStrategyProcess, makeListEnvelope, stamp } from '../../test/factories'

function createHeartbeat(
  component: string,
  status: 'healthy' | 'warning' | 'error',
  lagMs: number = 0,
  sequence: number = 1,
  meta?: Record<string, unknown>
): HeartbeatData {
  return stamp('heartbeat', {
    component,
    status,
    lag_ms: lagMs,
    sequence,
    meta,
  }) as HeartbeatData
}

const mockStartProcess = vi.fn()
const mockStopProcess = vi.fn()
const mockCreateProcessConfig = vi.fn()
let storedHeartbeatCallback: ((msg: unknown) => void) | null = null
let storedConnectionCallback: ((connected: boolean) => void) | null = null
const mockWsClient = {
  subscribe: vi.fn(),
  unsubscribe: vi.fn(),
  onConnection: vi.fn((callback: (connected: boolean) => void) => {
    storedConnectionCallback = callback

    return vi.fn()
  }),
  onMessage: vi.fn((topic: string, callback: (msg: unknown) => void) => {
    if (topic === 'heartbeat') {
      storedHeartbeatCallback = callback
    }

    return vi.fn()
  }),
}

vi.mock('../../hooks/queries', () => ({
  useStrategies: vi.fn(() => ({
    data: null,
    isLoading: false,
  })),
  useAvailableProcesses: vi.fn(() => ({
    data: null,
    isLoading: false,
  })),
  useProcessSchema: vi.fn(() => ({
    data: null,
    isLoading: false,
  })),
  useStartProcessByName: vi.fn(() => mockStartProcess),
  useStopProcessByName: vi.fn(() => mockStopProcess),
  useCreateProcessConfig: vi.fn(() => ({
    mutateAsync: mockCreateProcessConfig,
    isPending: false,
  })),
}))
vi.mock('../../stores/websocket', () => ({
  useWebSocketStore: vi.fn(() => ({
    wsClient: mockWsClient,
  })),
}))
vi.mock('../../stores/app', () => ({
  useAppStore: vi.fn((selector: (s: Record<string, unknown>) => unknown) =>
    selector({ asOf: null, isTimeTraveling: false })
  ),
}))
vi.mock('../../stores/auth', () => ({
  useAuth: vi.fn(() => ({
    hasPermission: () => true,
  })),
}))
vi.mock('react-hot-toast', () => ({
  default: {
    success: vi.fn(),
    error: vi.fn(),
  },
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

describe('Strategies', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    storedHeartbeatCallback = null
    storedConnectionCallback = null
  })
  it('renders strategies page', () => {
    renderWithProviders(<Strategies />)
    expect(screen.getByText('Strategy Management')).toBeTruthy()
    expect(screen.getAllByText('Register Strategy').length).toBeGreaterThanOrEqual(1)
  })
  it('shows saving state when create process config is pending', async () => {
    const { useCreateProcessConfig } = await import('../../hooks/queries')

    vi.mocked(useCreateProcessConfig).mockReturnValueOnce({
      mutateAsync: mockCreateProcessConfig,
      isPending: true,
    } as never)
    renderWithProviders(<Strategies />)
    expect(screen.getByText(/Saving/i)).toBeTruthy()
  })
  it('displays empty state when no strategies configured', async () => {
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', []),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('No strategies configured')).toBeTruthy()
    })
  })
  it('displays configured strategies', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_macd_btc' })]
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('MACD BTC')).toBeTruthy()
    })
  })
  it('subscribes to heartbeat topics for strategies', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_macd_btc', running: true })]
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(mockWsClient.subscribe).toHaveBeenCalledWith(['system.heartbeats.strategy.macd_btc'])
    })
  })
  it('filters only strategy role processes', async () => {
    const mockProcesses = [makeStrategyProcess({ name: 'strategy_macd_btc' })]
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockProcesses),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('Strategy Management')).toBeTruthy()
    })
  })
  it('shows register strategy button', async () => {
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('Register Strategy')).toBeTruthy()
    })
  })
  it('displays available strategy templates', async () => {
    const { useAvailableProcesses } = await import('../../hooks/queries')

    vi.mocked(useAvailableProcesses).mockReturnValue({
      data: {
        payload: [
          {
            name: 'strategy_macd',
            class_path: 'snapper.strategy_macd',
            method: 'main',
            description: 'MACD Template',
            lifecycle: 'long_running',
            role: 'strategy',
            tags: [],
          },
        ],
      },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('Strategy Management')).toBeTruthy()
    })
  })
  it('handles websocket connection callback', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_test', running: true })]
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(mockWsClient.onConnection).toHaveBeenCalled()
    })
  })
  it('resubscribes to heartbeats on reconnect', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_test', running: true })]
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(mockWsClient.subscribe).toHaveBeenCalledWith(['system.heartbeats.strategy.test'])
    })
    storedConnectionCallback?.(true)
    expect(mockWsClient.subscribe).toHaveBeenCalledWith(['system.heartbeats.strategy.test'])
  })
  it('does not resubscribe when connection callback is false', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_test', running: true })]
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(mockWsClient.subscribe).toHaveBeenCalledWith(['system.heartbeats.strategy.test'])
    })
    const subscribeCalls = mockWsClient.subscribe.mock.calls.length

    storedConnectionCallback?.(false)
    expect(mockWsClient.subscribe).toHaveBeenCalledTimes(subscribeCalls)
  })
  it('unsubscribes heartbeat topics on unmount', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_test', running: true })]
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    const { unmount } = renderWithProviders(<Strategies />)

    await waitFor(() => {
      expect(mockWsClient.subscribe).toHaveBeenCalledWith(['system.heartbeats.strategy.test'])
    })
    unmount()
    expect(mockWsClient.unsubscribe).toHaveBeenCalledWith(['system.heartbeats.strategy.test'])
  })
  it('handles heartbeat messages', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_test', running: true })]
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(mockWsClient.onMessage).toHaveBeenCalledWith('heartbeat', expect.any(Function))
    })
  })
  it('renders configured strategies section header', async () => {
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('Configured Strategies')).toBeTruthy()
    })
  })
  it('shows autostart description', async () => {
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText(/Register new strategy processes directly from the UI/i)).toBeTruthy()
    })
  })
  it('shows loading skeleton while loading', async () => {
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: null,
      isLoading: true,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Strategies />)
    expect(screen.queryByText('Strategy Management')).toBeNull()
  })
  it('handles start strategy with success', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_test' })]
    const { useStrategies, useStartProcessByName } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    const mockMutate = vi.fn().mockImplementation((_data, options) => {
      options?.onSuccess?.()
    })

    vi.mocked(useStartProcessByName).mockReturnValue({
      mutate: mockMutate,
      isPending: false,
    } as never)
    const user = (await import('@testing-library/user-event')).default.setup()

    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('TEST')).toBeTruthy()
    })
    const startButtons = screen.getAllByRole('button', { name: /start/i })

    await user.click(startButtons[0])
    await waitFor(() => {
      expect(screen.getByText('Confirm')).toBeTruthy()
    })
    await user.click(screen.getByText('Confirm'))
    expect(mockMutate).toHaveBeenCalledWith(
      { name: 'strategy_test', mode: 'thread' },
      expect.any(Object)
    )
  })
  it('defaults to thread mode when strategy mode is missing', async () => {
    const mockStrategies = [
      makeStrategyProcess({ name: 'strategy_test', mode: '' as 'thread' | 'process' }),
    ]
    const { useStrategies, useStartProcessByName } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    const mockMutate = vi.fn()

    vi.mocked(useStartProcessByName).mockReturnValue({
      mutate: mockMutate,
      isPending: false,
    } as never)
    const user = (await import('@testing-library/user-event')).default.setup()

    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('TEST')).toBeTruthy()
    })
    const startButton = screen.getByLabelText('Start TEST strategy')

    await user.click(startButton)
    await waitFor(() => {
      expect(screen.getByText('Confirm')).toBeTruthy()
    })
    await user.click(screen.getByText('Confirm'))
    expect(mockMutate).toHaveBeenCalledWith(
      { name: 'strategy_test', mode: 'thread' },
      expect.any(Object)
    )
  })
  it('shows starting state while start mutation is pending', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_test' })]
    const { useStrategies, useStartProcessByName, useStopProcessByName } =
      await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    let startPending = false
    const mockStartMutate = vi.fn()

    vi.mocked(useStartProcessByName).mockImplementation(
      () =>
        ({
          mutate: mockStartMutate,
          isPending: startPending,
        }) as never
    )
    vi.mocked(useStopProcessByName).mockReturnValue({
      mutate: vi.fn(),
      isPending: false,
    } as never)
    const user = (await import('@testing-library/user-event')).default.setup()
    const queryClient = createQueryClient()
    const { rerender } = render(
      <QueryClientProvider client={queryClient}>
        <Strategies />
      </QueryClientProvider>
    )

    await waitFor(() => {
      expect(screen.getByText('TEST')).toBeTruthy()
    })
    const startButton = screen.getByLabelText('Start TEST strategy')

    await user.click(startButton)
    await waitFor(() => {
      expect(screen.getByText('Confirm')).toBeTruthy()
    })
    await user.click(screen.getByText('Confirm'))
    startPending = true
    rerender(
      <QueryClientProvider client={queryClient}>
        <Strategies />
      </QueryClientProvider>
    )
    expect(screen.getByLabelText('Status: starting')).toBeTruthy()
  })
  it('handles start strategy error - already running', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_test' })]
    const { useStrategies, useStartProcessByName } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    const mockMutate = vi.fn().mockImplementation((_data, options) => {
      options?.onError?.(new Error('Strategy is already running'))
    })

    vi.mocked(useStartProcessByName).mockReturnValue({
      mutate: mockMutate,
      isPending: false,
    } as never)
    const user = (await import('@testing-library/user-event')).default.setup()

    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('TEST')).toBeTruthy()
    })
    const startButtons = screen.getAllByRole('button', { name: /start/i })

    await user.click(startButtons[0])
    await waitFor(() => {
      expect(screen.getByText('Confirm')).toBeTruthy()
    })
    await user.click(screen.getByText('Confirm'))
    expect(mockMutate).toHaveBeenCalled()
  })
  it('handles start strategy error - not found', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_test' })]
    const { useStrategies, useStartProcessByName } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    const mockMutate = vi.fn().mockImplementation((_data, options) => {
      options?.onError?.(new Error('Process not found'))
    })

    vi.mocked(useStartProcessByName).mockReturnValue({
      mutate: mockMutate,
      isPending: false,
    } as never)
    const user = (await import('@testing-library/user-event')).default.setup()

    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('TEST')).toBeTruthy()
    })
    const startButtons = screen.getAllByRole('button', { name: /start/i })

    await user.click(startButtons[0])
    await waitFor(() => {
      expect(screen.getByText('Confirm')).toBeTruthy()
    })
    await user.click(screen.getByText('Confirm'))
    expect(mockMutate).toHaveBeenCalled()
  })
  it('handles start strategy error - network error', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_test' })]
    const { useStrategies, useStartProcessByName } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    const mockMutate = vi.fn().mockImplementation((_data, options) => {
      options?.onError?.(new Error('network timeout'))
    })

    vi.mocked(useStartProcessByName).mockReturnValue({
      mutate: mockMutate,
      isPending: false,
    } as never)
    const user = (await import('@testing-library/user-event')).default.setup()

    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('TEST')).toBeTruthy()
    })
    const startButtons = screen.getAllByRole('button', { name: /start/i })

    await user.click(startButtons[0])
    await waitFor(() => {
      expect(screen.getByText('Confirm')).toBeTruthy()
    })
    await user.click(screen.getByText('Confirm'))
    expect(mockMutate).toHaveBeenCalled()
  })
  it('handles stop strategy with success', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_test', running: true })]
    const { useStrategies, useStopProcessByName } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    const mockMutate = vi.fn().mockImplementation((_data, options) => {
      options?.onSuccess?.()
    })

    vi.mocked(useStopProcessByName).mockReturnValue({
      mutate: mockMutate,
      isPending: false,
    } as never)
    const user = (await import('@testing-library/user-event')).default.setup()

    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('TEST')).toBeTruthy()
    })
    const stopButtons = screen.getAllByRole('button', { name: /stop/i })

    await user.click(stopButtons[0])
    await waitFor(() => {
      expect(screen.getByText('Confirm')).toBeTruthy()
    })
    await user.click(screen.getByText('Confirm'))
    expect(mockMutate).toHaveBeenCalledWith({ name: 'strategy_test' }, expect.any(Object))
  })
  it('disables stop button while stop mutation is pending for active strategy', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_test' })]
    const { useStrategies, useStartProcessByName, useStopProcessByName } =
      await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    const mockStartMutate = vi.fn()

    vi.mocked(useStartProcessByName).mockReturnValue({
      mutate: mockStartMutate,
      isPending: false,
    } as never)
    let stopPending = false

    vi.mocked(useStopProcessByName).mockImplementation(
      () =>
        ({
          mutate: vi.fn(),
          isPending: stopPending,
        }) as never
    )
    const user = (await import('@testing-library/user-event')).default.setup()
    const queryClient = createQueryClient()
    const { rerender } = render(
      <QueryClientProvider client={queryClient}>
        <Strategies />
      </QueryClientProvider>
    )

    await waitFor(() => {
      expect(screen.getByText('TEST')).toBeTruthy()
    })
    const startButton = screen.getByLabelText('Start TEST strategy')

    await user.click(startButton)
    await waitFor(() => {
      expect(screen.getByText('Confirm')).toBeTruthy()
    })
    await user.click(screen.getByText('Confirm'))
    stopPending = true
    rerender(
      <QueryClientProvider client={queryClient}>
        <Strategies />
      </QueryClientProvider>
    )
    expect(screen.getByLabelText('Stop TEST strategy')).toBeDisabled()
  })
  it('handles stop strategy error - not running', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_test', running: true })]
    const { useStrategies, useStopProcessByName } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    const mockMutate = vi.fn().mockImplementation((_data, options) => {
      options?.onError?.(new Error('Strategy is not running'))
    })

    vi.mocked(useStopProcessByName).mockReturnValue({
      mutate: mockMutate,
      isPending: false,
    } as never)
    const user = (await import('@testing-library/user-event')).default.setup()

    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('TEST')).toBeTruthy()
    })
    const stopButtons = screen.getAllByRole('button', { name: /stop/i })

    await user.click(stopButtons[0])
    await waitFor(() => {
      expect(screen.getByText('Confirm')).toBeTruthy()
    })
    await user.click(screen.getByText('Confirm'))
    expect(mockMutate).toHaveBeenCalled()
  })
  it('handles stop strategy error - network error', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_test', running: true })]
    const { useStrategies, useStopProcessByName } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    const mockMutate = vi.fn().mockImplementation((_data, options) => {
      options?.onError?.(new Error('Network timeout'))
    })

    vi.mocked(useStopProcessByName).mockReturnValue({
      mutate: mockMutate,
      isPending: false,
    } as never)
    const user = (await import('@testing-library/user-event')).default.setup()

    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('TEST')).toBeTruthy()
    })
    const stopButtons = screen.getAllByRole('button', { name: /stop/i })

    await user.click(stopButtons[0])
    await waitFor(() => {
      expect(screen.getByText('Confirm')).toBeTruthy()
    })
    await user.click(screen.getByText('Confirm'))
    expect(mockMutate).toHaveBeenCalled()
  })
  it('handles opening and closing strategy launch modal', async () => {
    const user = (await import('@testing-library/user-event')).default.setup()

    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('Register Strategy')).toBeTruthy()
    })
    const registerButton = screen.getByText('Register Strategy')

    await user.click(registerButton)
    await waitFor(() => {
      expect(screen.getByText('Register Strategy Process')).toBeTruthy()
    })
    const cancelButton = screen.getByRole('button', { name: /Cancel/i })

    await user.click(cancelButton)
    await waitFor(() => {
      expect(screen.queryByText('Register Strategy Process')).toBeFalsy()
    })
  })
  it('handles strategy launch with startImmediately', async () => {
    const {
      useAvailableProcesses,
      useCreateProcessConfig,
      useStartProcessByName,
      useProcessSchema,
    } = await import('../../hooks/queries')

    vi.mocked(useAvailableProcesses).mockReturnValue({
      data: {
        payload: [
          {
            name: 'strategy_macd',
            class_path: 'snapper.strategy_macd',
            method: 'main',
            description: 'MACD Template',
            lifecycle: 'long_running',
            role: 'strategy',
            tags: [],
          },
        ],
      },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    vi.mocked(useProcessSchema).mockReturnValue({
      data: {
        payload: {
          default_args: [],
          default_parameters: { name: 'macd_default' },
          default_mode: 'thread',
        },
      },
      isLoading: false,
      error: null,
    } as never)
    const mockMutateAsync = vi.fn().mockResolvedValue({})

    vi.mocked(useCreateProcessConfig).mockReturnValue({
      mutateAsync: mockMutateAsync,
      isPending: false,
    } as never)
    const mockStartMutateAsync = vi.fn().mockResolvedValue({})

    vi.mocked(useStartProcessByName).mockReturnValue({
      mutate: vi.fn(),
      mutateAsync: mockStartMutateAsync,
      isPending: false,
    } as never)
    const user = (await import('@testing-library/user-event')).default.setup()

    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('Register Strategy')).toBeTruthy()
    })
    const registerButton = screen.getByText('Register Strategy')

    await user.click(registerButton)
    await waitFor(() => {
      expect(screen.getByText('Register Strategy Process')).toBeTruthy()
    })
    await waitFor(() => {
      const processNameInput = screen.getByPlaceholderText(
        'strategy_macd_custom'
      ) as HTMLInputElement

      expect(processNameInput.value).toBeTruthy()
    })
    const buttons = screen.getAllByRole('button', { name: /Register strategy/i })
    const submitButton =
      buttons.find(btn => btn.getAttribute('type') === 'submit') || buttons[buttons.length - 1]

    await user.click(submitButton)
    await waitFor(() => {
      expect(mockMutateAsync).toHaveBeenCalled()
    })
    await waitFor(() => {
      expect(mockStartMutateAsync).toHaveBeenCalled()
    })
  })
  it('handles strategy launch without startImmediately', async () => {
    const {
      useAvailableProcesses,
      useCreateProcessConfig,
      useStartProcessByName,
      useProcessSchema,
    } = await import('../../hooks/queries')

    vi.mocked(useAvailableProcesses).mockReturnValue({
      data: {
        payload: [
          {
            name: 'strategy_macd',
            class_path: 'snapper.strategy_macd',
            method: 'main',
            description: 'MACD Template',
            lifecycle: 'long_running',
            role: 'strategy',
            tags: [],
          },
        ],
      },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    vi.mocked(useProcessSchema).mockReturnValue({
      data: {
        payload: {
          default_args: [],
          default_parameters: { name: 'macd_default' },
          default_mode: 'thread',
        },
      },
      isLoading: false,
      error: null,
    } as never)
    const mockMutateAsync = vi.fn().mockResolvedValue({})

    vi.mocked(useCreateProcessConfig).mockReturnValue({
      mutateAsync: mockMutateAsync,
      isPending: false,
    } as never)
    const mockStartMutateAsync = vi.fn().mockResolvedValue({})

    vi.mocked(useStartProcessByName).mockReturnValue({
      mutate: vi.fn(),
      mutateAsync: mockStartMutateAsync,
      isPending: false,
    } as never)
    const user = (await import('@testing-library/user-event')).default.setup()

    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('Register Strategy')).toBeTruthy()
    })
    const registerButton = screen.getByText('Register Strategy')

    await user.click(registerButton)
    await waitFor(() => {
      expect(screen.getByText('Register Strategy Process')).toBeTruthy()
    })
    const startImmediatelyCheckbox = screen.getByRole('checkbox', {
      name: /Start immediately/i,
    })

    await user.click(startImmediatelyCheckbox)
    const submitButtons = screen.getAllByRole('button', { name: /Register strategy/i })
    const submitButton =
      submitButtons.find(btn => btn.getAttribute('type') === 'submit') ||
      submitButtons[submitButtons.length - 1]

    await user.click(submitButton)
    await waitFor(() => {
      expect(mockMutateAsync).toHaveBeenCalled()
    })
    expect(mockStartMutateAsync).not.toHaveBeenCalled()
  })
  it('handles strategy launch error', async () => {
    const { useAvailableProcesses, useCreateProcessConfig, useProcessSchema } =
      await import('../../hooks/queries')
    const toast = (await import('react-hot-toast')).default

    vi.mocked(useAvailableProcesses).mockReturnValue({
      data: {
        payload: [
          {
            name: 'strategy_macd',
            class_path: 'snapper.strategy_macd',
            method: 'main',
            description: 'MACD Template',
            lifecycle: 'long_running',
            role: 'strategy',
            tags: [],
          },
        ],
      },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    vi.mocked(useProcessSchema).mockReturnValue({
      data: {
        payload: {
          default_args: [],
          default_parameters: { name: 'macd_default' },
          default_mode: 'thread',
        },
      },
      isLoading: false,
      error: null,
    } as never)
    const mockMutateAsync = vi.fn().mockRejectedValue(new Error('Configuration already exists'))

    vi.mocked(useCreateProcessConfig).mockReturnValue({
      mutateAsync: mockMutateAsync,
      isPending: false,
    } as never)
    const user = (await import('@testing-library/user-event')).default.setup()

    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('Register Strategy')).toBeTruthy()
    })
    const registerButton = screen.getByText('Register Strategy')

    await user.click(registerButton)
    await waitFor(() => {
      expect(screen.getByText('Register Strategy Process')).toBeTruthy()
    })
    await waitFor(() => {
      const processNameInput = screen.getByPlaceholderText(
        'strategy_macd_custom'
      ) as HTMLInputElement

      expect(processNameInput.value).toBeTruthy()
    })
    const buttons = screen.getAllByRole('button', { name: /Register strategy/i })
    const submitButton =
      buttons.find(btn => btn.getAttribute('type') === 'submit') || buttons[buttons.length - 1]

    await user.click(submitButton)
    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        expect.stringContaining('Failed to register strategy')
      )
    })
  })
  it('handles strategy launch error for non-Error rejection', async () => {
    const { useAvailableProcesses, useCreateProcessConfig, useProcessSchema } =
      await import('../../hooks/queries')
    const toast = (await import('react-hot-toast')).default

    vi.mocked(useAvailableProcesses).mockReturnValue({
      data: {
        payload: [
          {
            name: 'strategy_macd',
            class_path: 'snapper.strategy_macd',
            method: 'main',
            description: 'MACD Template',
            lifecycle: 'long_running',
            role: 'strategy',
            tags: [],
          },
        ],
      },
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    vi.mocked(useProcessSchema).mockReturnValue({
      data: {
        payload: {
          default_args: [],
          default_parameters: { name: 'macd_default' },
          default_mode: 'thread',
        },
      },
      isLoading: false,
      error: null,
    } as never)
    const mockMutateAsync = vi.fn().mockRejectedValue('bad')

    vi.mocked(useCreateProcessConfig).mockReturnValue({
      mutateAsync: mockMutateAsync,
      isPending: false,
    } as never)
    const user = (await import('@testing-library/user-event')).default.setup()

    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('Register Strategy')).toBeTruthy()
    })
    const registerButton = screen.getByText('Register Strategy')

    await user.click(registerButton)
    await waitFor(() => {
      expect(screen.getByText('Register Strategy Process')).toBeTruthy()
    })
    const buttons = screen.getAllByRole('button', { name: /Register strategy/i })
    const submitButton =
      buttons.find(btn => btn.getAttribute('type') === 'submit') || buttons[buttons.length - 1]

    await user.click(submitButton)
    await waitFor(() => {
      expect(toast.error).toHaveBeenCalledWith(
        expect.stringContaining('Failed to register strategy: Unknown error')
      )
    })
  })
  it('handles stop strategy error - generic error', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_test', running: true })]
    const { useStrategies, useStopProcessByName } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    const mockMutate = vi.fn().mockImplementation((_data, options) => {
      options?.onError?.(new Error('Some unknown error'))
    })

    vi.mocked(useStopProcessByName).mockReturnValue({
      mutate: mockMutate,
      isPending: false,
    } as never)
    const user = (await import('@testing-library/user-event')).default.setup()

    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('TEST')).toBeTruthy()
    })
    const stopButtons = screen.getAllByRole('button', { name: /stop/i })

    await user.click(stopButtons[0])
    await waitFor(() => {
      expect(screen.getByText('Confirm')).toBeTruthy()
    })
    await user.click(screen.getByText('Confirm'))
    expect(mockMutate).toHaveBeenCalled()
  })
  it('handles start strategy error - generic error', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_test' })]
    const { useStrategies, useStartProcessByName } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    const mockMutate = vi.fn().mockImplementation((_data, options) => {
      options?.onError?.(new Error('Some unknown error'))
    })

    vi.mocked(useStartProcessByName).mockReturnValue({
      mutate: mockMutate,
      isPending: false,
    } as never)
    const user = (await import('@testing-library/user-event')).default.setup()

    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('TEST')).toBeTruthy()
    })
    const startButtons = screen.getAllByRole('button', { name: /start/i })

    await user.click(startButtons[0])
    await waitFor(() => {
      expect(screen.getByText('Confirm')).toBeTruthy()
    })
    await user.click(screen.getByText('Confirm'))
    expect(mockMutate).toHaveBeenCalled()
  })
  it('handles heartbeat message for warn status', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_test', running: true })]
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(mockWsClient.onMessage).toHaveBeenCalledWith('heartbeat', expect.any(Function))
    })

    if (storedHeartbeatCallback) {
      storedHeartbeatCallback(
        createHeartbeat('strategy_test', 'warning', 100, 1, {
          feed_health: {
            binance: { status: 'healthy', lag_ms: 10, heartbeat_age_ms: 100, healthy: true },
          },
          inputs: ['input1'],
          outputs: ['output1'],
        })
      )
    }
  })
  it('handles heartbeat message for error status', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_test', running: true })]
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(mockWsClient.onMessage).toHaveBeenCalledWith('heartbeat', expect.any(Function))
    })

    if (storedHeartbeatCallback) {
      storedHeartbeatCallback(createHeartbeat('strategy_test', 'error', 500, 2))
    }
  })
  it('handles heartbeat message for ok status', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_test', running: true })]
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(mockWsClient.onMessage).toHaveBeenCalledWith('heartbeat', expect.any(Function))
    })

    if (storedHeartbeatCallback) {
      storedHeartbeatCallback(createHeartbeat('strategy_test', 'healthy', 10, 3))
    }
  })
  it('defaults lag_ms to 0 when missing from heartbeat', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'test', running: true })]
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(mockWsClient.onMessage).toHaveBeenCalledWith('heartbeat', expect.any(Function))
    })

    if (storedHeartbeatCallback) {
      storedHeartbeatCallback(createHeartbeat('strategy_test', 'healthy', 0, 4))
    }

    await waitFor(() => {
      expect(screen.getByText('0ms')).toBeTruthy()
    })
  })
  it('ignores heartbeat messages for unknown strategies', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_known', running: true })]
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(mockWsClient.onMessage).toHaveBeenCalledWith('heartbeat', expect.any(Function))
    })

    if (storedHeartbeatCallback) {
      storedHeartbeatCallback(createHeartbeat('strategy_unknown', 'healthy', 10, 1))
    }
  })
  it('clears activeStrategyProcess when stopping process that is not running', async () => {
    let mockStrategies = [makeStrategyProcess({ name: 'strategy_test' })]
    const { useStrategies, useStartProcessByName, useStopProcessByName } =
      await import('../../hooks/queries')

    vi.mocked(useStrategies).mockImplementation(
      () =>
        ({
          data: makeListEnvelope('strategy_list', mockStrategies),
          isLoading: false,
          refetch: vi.fn(),
        }) as never
    )
    const mockStartMutate = vi.fn()

    vi.mocked(useStartProcessByName).mockReturnValue({
      mutate: mockStartMutate,
      isPending: false,
    } as never)
    let stopPending = false
    const mockStopMutate = vi.fn().mockImplementation((_data, options) => {
      options?.onError?.(new Error('Strategy is not running'))
    })

    vi.mocked(useStopProcessByName).mockImplementation(
      () =>
        ({
          mutate: mockStopMutate,
          isPending: stopPending,
        }) as never
    )
    const user = (await import('@testing-library/user-event')).default.setup()
    const queryClient = createQueryClient()
    const { rerender } = render(
      <QueryClientProvider client={queryClient}>
        <Strategies />
      </QueryClientProvider>
    )

    await waitFor(() => {
      expect(screen.getByText('TEST')).toBeTruthy()
    })
    const startButton = screen.getByLabelText('Start TEST strategy')

    await user.click(startButton)
    await waitFor(() => {
      expect(screen.getByText('Confirm')).toBeTruthy()
    })
    await user.click(screen.getByText('Confirm'))
    mockStrategies = [
      {
        ...mockStrategies[0],
        running: true,
      },
    ]
    rerender(
      <QueryClientProvider client={queryClient}>
        <Strategies />
      </QueryClientProvider>
    )
    const stopButton = await screen.findByLabelText('Stop TEST strategy')

    await user.click(stopButton)
    await waitFor(() => {
      expect(screen.getByText('Confirm')).toBeTruthy()
    })
    await user.click(screen.getByText('Confirm'))
    expect(mockStopMutate).toHaveBeenCalled()
    stopPending = true
    rerender(
      <QueryClientProvider client={queryClient}>
        <Strategies />
      </QueryClientProvider>
    )
    await waitFor(() => {
      expect(screen.queryByText('Stopping...')).toBeNull()
    })
  })
  it('handles wsClient being null', async () => {
    const { useWebSocketStore } = await import('../../stores/websocket')

    vi.mocked(useWebSocketStore).mockReturnValue({
      wsClient: null,
    } as never)
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_test', running: true })]
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('Strategy Management')).toBeTruthy()
    })
    expect(mockWsClient.subscribe).not.toHaveBeenCalled()
    vi.mocked(useWebSocketStore).mockReturnValue({
      wsClient: mockWsClient,
    } as never)
  })
  it('does not subscribe when no strategies configured', async () => {
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', []),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('No strategies configured')).toBeTruthy()
    })
    expect(mockWsClient.subscribe).not.toHaveBeenCalled()
  })
  it('handles heartbeat with warn status', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'test', running: true })]
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(mockWsClient.onMessage).toHaveBeenCalledWith('heartbeat', expect.any(Function))
    })

    if (storedHeartbeatCallback) {
      storedHeartbeatCallback(createHeartbeat('strategy_test', 'warning', 100, 1))
    }
  })
  it('handles heartbeat with error status', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'test', running: true })]
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(mockWsClient.onMessage).toHaveBeenCalledWith('heartbeat', expect.any(Function))
    })

    if (storedHeartbeatCallback) {
      storedHeartbeatCallback(
        createHeartbeat('strategy_test', 'error', 500, 1, {
          feed_health: {
            kraken: { status: 'healthy', lag_ms: 10, heartbeat_age_ms: 100, healthy: true },
          },
          inputs: ['feed.kraken.BTC-USD'],
          outputs: ['signal.macd'],
        })
      )
    }
  })
  it('handles heartbeat with warning status alternative', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'test', running: true })]
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(mockWsClient.onMessage).toHaveBeenCalledWith('heartbeat', expect.any(Function))
    })

    if (storedHeartbeatCallback) {
      storedHeartbeatCallback(createHeartbeat('strategy_test', 'warning', 150, 1))
    }
  })
  it('triggers connection callback and resubscribes', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_test', running: true })]
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(mockWsClient.onConnection).toHaveBeenCalled()
    })

    if (storedConnectionCallback) {
      storedConnectionCallback(true)
      expect(mockWsClient.subscribe).toHaveBeenCalledTimes(2)
    }
  })
  it('ignores heartbeat messages for non-strategy components', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_test', running: true })]
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(mockWsClient.onMessage).toHaveBeenCalledWith('heartbeat', expect.any(Function))
    })

    if (storedHeartbeatCallback) {
      storedHeartbeatCallback(createHeartbeat('executor_kraken', 'healthy', 10, 1))
    }
  })
  it('filters strategies by status filter', async () => {
    const mockStrategies = [
      makeStrategyProcess({ name: 'strategy_running', running: true }),
      makeStrategyProcess({ name: 'strategy_stopped' }),
    ]
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    const user = (await import('@testing-library/user-event')).default.setup()

    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('RUNNING')).toBeTruthy()
    })
    const select = screen.getByRole('combobox')

    await user.click(select)
    await user.click(screen.getByText('Running'))
    expect(screen.getByText('RUNNING')).toBeTruthy()
  })
  it('filters strategies by stopped status filter', async () => {
    const mockStrategies = [
      makeStrategyProcess({ name: 'strategy_running', running: true }),
      makeStrategyProcess({ name: 'strategy_stopped' }),
    ]
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    const user = (await import('@testing-library/user-event')).default.setup()

    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('RUNNING')).toBeTruthy()
      expect(screen.getByText('STOPPED')).toBeTruthy()
    })
    const select = screen.getByRole('combobox')

    await user.click(select)
    await user.click(screen.getByText('Stopped'))
    expect(screen.getByText('STOPPED')).toBeTruthy()
    expect(screen.queryByText('RUNNING')).toBeNull()
  })
  it('shows no match state and clears filters', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_test', running: true })]
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    const user = (await import('@testing-library/user-event')).default.setup()

    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('TEST')).toBeTruthy()
    })
    const searchInput = screen.getByPlaceholderText('Search strategies...')

    await user.type(searchInput, 'nonexistent')
    await waitFor(() => {
      expect(screen.getByText('No strategies match your filters')).toBeTruthy()
    })
    const clearButton = screen.getByText('Clear filters')

    await user.click(clearButton)
    await waitFor(() => {
      expect(screen.getByText('TEST')).toBeTruthy()
    })
  })
  it('cancels confirm dialog', async () => {
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_test' })]
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    const user = (await import('@testing-library/user-event')).default.setup()

    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('TEST')).toBeTruthy()
    })
    const startButtons = screen.getAllByRole('button', { name: /start/i })

    await user.click(startButtons[0])
    await waitFor(() => {
      expect(screen.getByText('Confirm')).toBeTruthy()
    })
    await user.click(screen.getByText('Cancel'))
    await waitFor(() => {
      expect(screen.queryByText('Confirm')).toBeNull()
    })
  })
  it('opens register modal from empty state button', async () => {
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', []),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    const user = (await import('@testing-library/user-event')).default.setup()

    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('No strategies configured')).toBeTruthy()
    })
    const registerButtons = screen.getAllByText('Register Strategy')

    await user.click(registerButtons[registerButtons.length - 1])
    await waitFor(() => {
      expect(screen.getByText('Register Strategy Process')).toBeTruthy()
    })
  })
  it('hides action buttons for viewer without manage permission', async () => {
    const { useAuth } = await import('../../stores/auth')

    vi.mocked(useAuth).mockReturnValue({
      hasPermission: () => false,
    } as never)
    const mockStrategies = [makeStrategyProcess({ name: 'strategy_test' })]
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', mockStrategies),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('TEST')).toBeTruthy()
    })
    expect(screen.queryByText('Register Strategy')).toBeNull()
    expect(screen.queryByRole('button', { name: /start/i })).toBeNull()
    expect(screen.queryByRole('button', { name: /stop/i })).toBeNull()
    expect(screen.getByText(/Contact an operator or admin to manage strategies/i)).toBeTruthy()
    vi.mocked(useAuth).mockReturnValue({
      hasPermission: () => true,
    } as never)
  })
  it('shows viewer empty state without register button', async () => {
    const { useAuth } = await import('../../stores/auth')

    vi.mocked(useAuth).mockReturnValue({
      hasPermission: () => false,
    } as never)
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', []),
      isLoading: false,
      refetch: vi.fn(),
    } as never)
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('No strategies configured')).toBeTruthy()
    })
    expect(screen.getByText('No strategies have been configured yet')).toBeTruthy()
    expect(screen.queryByText('Register Strategy')).toBeNull()
    vi.mocked(useAuth).mockReturnValue({
      hasPermission: () => true,
    } as never)
  })
  it('does not subscribe to heartbeats when time traveling', async () => {
    const { useAppStore } = await import('../../stores/app')
    const { useStrategies } = await import('../../hooks/queries')

    vi.mocked(useAppStore).mockImplementation(((
      selector: (s: Record<string, unknown>) => unknown
    ) => selector({ asOf: '2024-01-01T00:00:00Z', isTimeTraveling: true })) as never)
    vi.mocked(useStrategies).mockReturnValue({
      data: makeListEnvelope('strategy_list', [makeStrategyProcess({ name: 'test' })]),
      isLoading: false,
    } as never)
    mockWsClient.subscribe.mockClear()
    renderWithProviders(<Strategies />)
    await waitFor(() => {
      expect(screen.getByText('Strategy Management')).toBeTruthy()
    })

    expect(mockWsClient.subscribe).not.toHaveBeenCalled()
    vi.mocked(useAppStore).mockImplementation(((
      selector: (s: Record<string, unknown>) => unknown
    ) => selector({ asOf: null, isTimeTraveling: false })) as never)
  })
})
