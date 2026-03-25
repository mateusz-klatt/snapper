import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { StrategyLaunchModal } from './StrategyLaunchModal'
import type { AvailableProcess } from '../../types/api'

vi.mock('../../hooks/queries', () => ({
  useProcessSchema: vi.fn(() => ({
    data: null,
    isLoading: false,
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

describe('StrategyLaunchModal', () => {
  const mockOnClose = vi.fn()
  const mockOnSubmit = vi.fn()
  const mockTemplates: AvailableProcess[] = [
    {
      type: 'available_process' as const,
      sequence_id: 0,
      public_id: 'test-pid',
      timestamp: '2024-01-01T00:00:00Z',
      session_id: 'test-sid',
      name: 'strategy_macd',
      class_path: 'snapper.strategies.macd',
      method: 'main',
      description: 'MACD Strategy',
      lifecycle: 'long_running',
      role: 'strategy',
      tags: [],
      parameters_schema: null,
    },
  ]

  beforeEach(() => {
    vi.resetAllMocks()
  })
  it('renders modal when open', () => {
    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={mockTemplates}
        onSubmit={mockOnSubmit}
      />
    )
    expect(screen.getByText('Register Strategy Process')).toBeTruthy()
  })
  it('does not render when closed', () => {
    renderWithProviders(
      <StrategyLaunchModal
        open={false}
        onClose={mockOnClose}
        templates={[]}
        onSubmit={mockOnSubmit}
      />
    )
    expect(screen.queryByText('Register Strategy Process')).toBeFalsy()
  })
  it('shows empty state when no templates', () => {
    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={[]}
        onSubmit={mockOnSubmit}
      />
    )
    expect(screen.getByText(/No strategy templates registered/i)).toBeTruthy()
  })
  it('displays template dropdown with options', async () => {
    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={mockTemplates}
        onSubmit={mockOnSubmit}
      />
    )
    await waitFor(() => {
      expect(screen.getByText('Strategy template')).toBeTruthy()
    })
  })
  it('displays process name input', async () => {
    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={mockTemplates}
        onSubmit={mockOnSubmit}
      />
    )
    await waitFor(() => {
      expect(screen.getByText('Process name')).toBeTruthy()
    })
  })
  it('displays strategy instance name input', async () => {
    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={mockTemplates}
        onSubmit={mockOnSubmit}
      />
    )
    await waitFor(() => {
      expect(screen.getByText('Strategy instance name')).toBeTruthy()
    })
  })
  it('displays execution mode selector', async () => {
    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={mockTemplates}
        onSubmit={mockOnSubmit}
      />
    )
    await waitFor(() => {
      expect(screen.getByText('Execution mode')).toBeTruthy()
    })
  })
  it('displays autostart checkbox', async () => {
    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={mockTemplates}
        onSubmit={mockOnSubmit}
      />
    )
    await waitFor(() => {
      expect(screen.getByText('Autostart on server boot')).toBeTruthy()
    })
  })
  it('displays start immediately checkbox', async () => {
    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={mockTemplates}
        onSubmit={mockOnSubmit}
      />
    )
    await waitFor(() => {
      expect(screen.getByText('Start immediately after registration')).toBeTruthy()
    })
  })
  it('displays note textarea', async () => {
    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={mockTemplates}
        onSubmit={mockOnSubmit}
      />
    )
    await waitFor(() => {
      expect(screen.getByText('Note (optional)')).toBeTruthy()
    })
  })
  it('displays cancel and submit buttons', async () => {
    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={mockTemplates}
        onSubmit={mockOnSubmit}
      />
    )
    await waitFor(() => {
      expect(screen.getByText('Cancel')).toBeTruthy()
      expect(screen.getByText('Register strategy')).toBeTruthy()
    })
  })
  it('calls onClose when cancel is clicked', async () => {
    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={mockTemplates}
        onSubmit={mockOnSubmit}
      />
    )
    await waitFor(() => {
      expect(screen.getByText('Cancel')).toBeTruthy()
    })
    await userEvent.click(screen.getByText('Cancel'))
    expect(mockOnClose).toHaveBeenCalled()
  })
  it('shows loading state when schema is loading', async () => {
    const { useProcessSchema } = await import('../../hooks/queries')

    vi.mocked(useProcessSchema).mockReturnValue({
      data: null,
      isLoading: true,
      error: null,
    } as never)
    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={mockTemplates}
        onSubmit={mockOnSubmit}
      />
    )
    await waitFor(() => {
      expect(screen.getByText(/Loading template defaults/i)).toBeTruthy()
    })
  })
  it('shows error when schema fails to load', async () => {
    const { useProcessSchema } = await import('../../hooks/queries')

    vi.mocked(useProcessSchema).mockReturnValue({
      data: null,
      isLoading: false,
      error: new Error('Failed to load'),
    } as never)
    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={mockTemplates}
        onSubmit={mockOnSubmit}
      />
    )
    await waitFor(() => {
      expect(screen.getByText(/Unable to load template defaults/i)).toBeTruthy()
    })
  })
  it('disables submit button when submitting', async () => {
    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={mockTemplates}
        onSubmit={mockOnSubmit}
        isSubmitting={true}
      />
    )
    await waitFor(() => {
      const button = screen.getByText('Registering…')

      expect(button).toBeTruthy()
    })
  })
  it('shows close button in empty state', async () => {
    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={[]}
        onSubmit={mockOnSubmit}
      />
    )
    await waitFor(() => {
      expect(screen.getByText('Close')).toBeTruthy()
    })
  })
  it('submits form with correct data', async () => {
    const user = userEvent.setup()

    mockOnSubmit.mockResolvedValue(undefined)
    const { useProcessSchema } = await import('../../hooks/queries')

    vi.mocked(useProcessSchema).mockReturnValue({
      data: {
        payload: {
          default_args: [],
          default_parameters: { name: 'test_strategy' },
          default_mode: 'thread',
        },
      },
      isLoading: false,
      error: null,
    } as never)
    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={mockTemplates}
        onSubmit={mockOnSubmit}
      />
    )
    await waitFor(() => {
      expect(screen.getByText('Register strategy')).toBeTruthy()
    })
    const submitButton = screen.getByText('Register strategy')

    await user.click(submitButton)
    await waitFor(() => {
      expect(mockOnSubmit).toHaveBeenCalled()
    })
  })
  it('repopulates default names after clearing inputs', async () => {
    const user = userEvent.setup()

    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={mockTemplates}
        onSubmit={mockOnSubmit}
      />
    )
    const processNameInput = await screen.findByPlaceholderText('strategy_macd_custom')
    const strategyNameInput = screen.getByPlaceholderText('macd_custom')

    await waitFor(() => {
      expect(processNameInput).toHaveValue('strategy_macd_instance')
      expect(strategyNameInput).toHaveValue('macd')
    })
    await user.clear(processNameInput)
    await user.clear(strategyNameInput)
    await waitFor(() => {
      expect(processNameInput).toHaveValue('strategy_macd_instance')
      expect(strategyNameInput).toHaveValue('macd')
    })
  })
  it('logs error when submit fails', async () => {
    const user = userEvent.setup()
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

    mockOnSubmit.mockRejectedValue(new Error('Submit failed'))
    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={mockTemplates}
        onSubmit={mockOnSubmit}
      />
    )
    const processNameInput = await screen.findByPlaceholderText('strategy_macd_custom')

    await user.clear(processNameInput)
    await user.type(processNameInput, 'strategy_macd_custom')
    const strategyNameInput = screen.getByPlaceholderText('macd_custom')

    await user.clear(strategyNameInput)
    await user.type(strategyNameInput, 'macd_custom')
    const submitButton = screen.getByText('Register strategy')

    await user.click(submitButton)
    await waitFor(() => {
      expect(consoleSpy).toHaveBeenCalled()
    })
    consoleSpy.mockRestore()
  })
  it('allows changing template', async () => {
    const user = userEvent.setup()
    const multipleTemplates: AvailableProcess[] = [
      {
        type: 'available_process' as const,
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        name: 'strategy_macd',
        class_path: 'snapper.strategies.macd',
        method: 'main',
        description: 'MACD Strategy',
        lifecycle: 'long_running',
        role: 'strategy',
        tags: [],
        parameters_schema: null,
      },
      {
        type: 'available_process' as const,
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        name: 'strategy_rsi',
        class_path: 'snapper.strategies.rsi',
        method: 'main',
        description: 'RSI Strategy',
        lifecycle: 'long_running',
        role: 'strategy',
        tags: [],
        parameters_schema: null,
      },
    ]

    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={multipleTemplates}
        onSubmit={mockOnSubmit}
      />
    )
    await waitFor(() => {
      expect(screen.getAllByRole('combobox').length).toBeGreaterThan(0)
    })
    const selects = screen.getAllByRole('combobox')

    await user.selectOptions(selects[0], 'strategy_rsi')
    await waitFor(() => {
      expect(selects[0]).toHaveValue('strategy_rsi')
    })
  })
  it('uses empty parameters when default_parameters is not an object', async () => {
    const user = userEvent.setup()

    mockOnSubmit.mockResolvedValue(undefined)
    const { useProcessSchema } = await import('../../hooks/queries')

    vi.mocked(useProcessSchema).mockReturnValue({
      data: {
        payload: {
          default_args: [],
          default_parameters: 'invalid',
          default_mode: 'thread',
        },
      },
      isLoading: false,
      error: null,
    } as never)
    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={mockTemplates}
        onSubmit={mockOnSubmit}
      />
    )
    await waitFor(() => {
      expect(screen.getByText('Register strategy')).toBeTruthy()
    })
    const submitButton = screen.getByText('Register strategy')

    await user.click(submitButton)
    await waitFor(() => {
      expect(mockOnSubmit).toHaveBeenCalledWith(
        expect.objectContaining({
          parameters: expect.any(Object),
        })
      )
    })
  })
  it('allows toggling autostart checkbox', async () => {
    const user = userEvent.setup()

    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={mockTemplates}
        onSubmit={mockOnSubmit}
      />
    )
    const autostartCheckbox = screen.getByRole('checkbox', { name: /Autostart on server boot/i })

    expect(autostartCheckbox).not.toBeChecked()
    await user.click(autostartCheckbox)
    expect(autostartCheckbox).toBeChecked()
  })
  it('allows toggling start immediately checkbox', async () => {
    const user = userEvent.setup()

    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={mockTemplates}
        onSubmit={mockOnSubmit}
      />
    )
    const startCheckbox = screen.getByRole('checkbox', { name: /Start immediately/i })

    expect(startCheckbox).toBeChecked()
    await user.click(startCheckbox)
    expect(startCheckbox).not.toBeChecked()
  })
  it('allows entering note', async () => {
    const user = userEvent.setup()

    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={mockTemplates}
        onSubmit={mockOnSubmit}
      />
    )
    await waitFor(() => {
      expect(screen.getByPlaceholderText(/Describe purpose or parameters/i)).toBeTruthy()
    })
    const noteTextarea = screen.getByPlaceholderText(/Describe purpose or parameters/i)

    await user.type(noteTextarea, 'Test note for strategy')
    expect(noteTextarea).toHaveValue('Test note for strategy')
  })
  it('allows selecting execution mode', async () => {
    const user = userEvent.setup()

    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={mockTemplates}
        onSubmit={mockOnSubmit}
      />
    )
    await waitFor(() => {
      const selects = screen.getAllByRole('combobox')

      expect(selects.length).toBeGreaterThan(1)
    })
    const selects = screen.getAllByRole('combobox')
    const executionModeSelect = selects[1]

    await user.selectOptions(executionModeSelect, 'process')
    expect(executionModeSelect).toHaveValue('process')
  })
  it('uses default mode from schema', async () => {
    const { useProcessSchema } = await import('../../hooks/queries')

    vi.mocked(useProcessSchema).mockReturnValue({
      data: {
        payload: {
          default_args: [],
          default_parameters: {},
          default_mode: 'process',
        },
      },
      isLoading: false,
      error: null,
    } as never)
    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={mockTemplates}
        onSubmit={mockOnSubmit}
      />
    )
    await waitFor(() => {
      const selects = screen.getAllByRole('combobox')
      const executionModeSelect = selects[1]

      expect(executionModeSelect).toHaveValue('process')
    })
  })
  it('handles output transformation with prefix', async () => {
    const user = userEvent.setup()

    mockOnSubmit.mockResolvedValue(undefined)
    const { useProcessSchema } = await import('../../hooks/queries')

    vi.mocked(useProcessSchema).mockReturnValue({
      data: {
        payload: {
          default_args: [],
          default_parameters: { name: 'test', output: 'signals.original' },
          default_mode: 'thread',
        },
      },
      isLoading: false,
      error: null,
    } as never)
    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={mockTemplates}
        onSubmit={mockOnSubmit}
      />
    )
    await waitFor(() => {
      expect(screen.getByText('Register strategy')).toBeTruthy()
    })
    const submitButton = screen.getByText('Register strategy')

    await user.click(submitButton)
    await waitFor(() => {
      expect(mockOnSubmit).toHaveBeenCalledWith(
        expect.objectContaining({
          parameters: expect.objectContaining({
            output: expect.stringMatching(/^signals\./),
          }),
        })
      )
    })
  })
  it('handles output transformation without prefix', async () => {
    const user = userEvent.setup()

    mockOnSubmit.mockResolvedValue(undefined)
    const { useProcessSchema } = await import('../../hooks/queries')

    vi.mocked(useProcessSchema).mockReturnValue({
      data: {
        payload: {
          default_args: [],
          default_parameters: { name: 'test', output: 'signals' },
          default_mode: 'thread',
        },
      },
      isLoading: false,
      error: null,
    } as never)
    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={mockTemplates}
        onSubmit={mockOnSubmit}
      />
    )
    const submitButton = screen.getByText('Register strategy')

    await user.click(submitButton)
    await waitFor(() => {
      expect(mockOnSubmit).toHaveBeenCalledWith(
        expect.objectContaining({
          parameters: expect.objectContaining({
            output: 'test',
          }),
        })
      )
    })
  })
  it('shows fallback description when template lacks description', async () => {
    const templates: AvailableProcess[] = [
      {
        type: 'available_process' as const,
        sequence_id: 0,
        public_id: 'test-pid',
        timestamp: '2024-01-01T00:00:00Z',
        session_id: 'test-sid',
        name: 'strategy_simple',
        class_path: 'snapper.strategies.simple',
        method: 'main',
        description: '',
        lifecycle: 'long_running',
        role: 'strategy',
        tags: [],
        parameters_schema: null,
      },
    ]

    renderWithProviders(
      <StrategyLaunchModal
        open={true}
        onClose={mockOnClose}
        templates={templates}
        onSubmit={mockOnSubmit}
      />
    )
    await waitFor(() => {
      expect(screen.getByText('Strategy registered in backend')).toBeTruthy()
    })
  })
})
