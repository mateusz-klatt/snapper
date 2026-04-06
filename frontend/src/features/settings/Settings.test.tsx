import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import type { ReactNode } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { Settings } from './Settings'
import { apiClient } from '../../lib/apiClient'

vi.mock('../../components/ThemeSelect', () => ({
  ThemeSelect: ({
    id,
    value,
    onChange,
    options,
    placeholder,
    className,
    disabled,
  }: {
    id?: string
    value: string
    onChange: (v: string) => void
    options: readonly { value: string; label: string }[]
    placeholder?: string
    className?: string
    disabled?: boolean
  }) => (
    <select
      id={id}
      value={value}
      onChange={e => onChange(e.target.value)}
      className={className}
      disabled={disabled}
    >
      {placeholder && <option value=''>{placeholder}</option>}
      {options.map(opt => (
        <option key={opt.value} value={opt.value}>
          {opt.label}
        </option>
      ))}
    </select>
  ),
}))

vi.mock('../../lib/apiClient', () => ({
  apiClient: {
    getSettings: vi.fn(),
    getSettingCategories: vi.fn(),
    updateSetting: vi.fn(),
    removeSetting: vi.fn(),
  },
}))
vi.mock('../../stores/auth', () => ({
  useAuth: vi.fn(() => ({ isAuthenticated: true })),
}))

const createQueryClient = () =>
  new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  })

const renderSettings = (ui: ReactNode) => {
  const queryClient = createQueryClient()

  return render(<QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>)
}

describe('Settings', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })
  it('displays loading state initially', () => {
    vi.mocked(apiClient.getSettings).mockImplementation(() => new Promise(() => {}))
    vi.mocked(apiClient.getSettingCategories).mockResolvedValue([])
    renderSettings(<Settings />)
    const skeleton = document.querySelector('.animate-pulse')

    expect(skeleton).toBeTruthy()
  })
  it('loads and displays settings successfully', async () => {
    const mockSettings = [
      {
        key: 'api.base_url',
        value: 'https://api.example.com',
        category: 'api',
        description: 'Base API URL',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'tester',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['api', 'database'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('Settings')).toBeTruthy()
      expect(screen.getByText('api.base_url')).toBeTruthy()
    })
  })
  it('handles error loading settings', async () => {
    vi.mocked(apiClient.getSettings).mockRejectedValue(new Error('Network error'))
    vi.mocked(apiClient.getSettingCategories).mockResolvedValue([])
    renderSettings(<Settings />)
    await waitFor(
      () => {
        expect(screen.getByText(/Network error/i)).toBeTruthy()
      },
      { timeout: 2000 }
    )
  })
  it('filters settings by category', async () => {
    const mockSettings = [
      {
        key: 'api.url',
        value: 'http://api',
        category: 'api',
        description: 'API',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'tester',
      },
      {
        key: 'db.host',
        value: 'localhost',
        category: 'database',
        description: 'DB',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'tester',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['api', 'database'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('api.url')).toBeTruthy()
      expect(screen.getByText('db.host')).toBeTruthy()
    })
    const select = screen.getByRole('combobox')

    await userEvent.selectOptions(select, 'api')
    await waitFor(() => {
      expect(screen.getByText('api.url')).toBeTruthy()
      expect(screen.queryByText('db.host')).toBeFalsy()
    })
  })
  it('filters settings by search term', async () => {
    const mockSettings = [
      {
        key: 'api.base_url',
        value: 'http://api',
        category: 'api',
        description: 'API URL',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'tester',
      },
      {
        key: 'db.host',
        value: 'localhost',
        category: 'database',
        description: 'Database host',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'tester',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['api', 'database'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('api.base_url')).toBeTruthy()
    })
    const searchInput = screen.getByPlaceholderText(/Search settings/i)

    await userEvent.type(searchInput, 'api')
    await waitFor(() => {
      expect(screen.getByText('api.base_url')).toBeTruthy()
      expect(screen.queryByText('db.host')).toBeFalsy()
    })
  })
  it('handles setting update', async () => {
    const mockSettings = [
      {
        key: 'api.url',
        value: 'http://old',
        category: 'api',
        description: 'API',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'tester',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['api'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    vi.mocked(apiClient.updateSetting).mockResolvedValue({
      key: 'api.url',
      value: 'http://new',
      category: 'api',
      description: 'API',
      updated_at: '2024-01-02T00:00:00Z',
      updated_by: 'tester',
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('api.url')).toBeTruthy()
    })
    expect(screen.getByText('Settings')).toBeTruthy()
  })
  it('updates only the matching setting when multiple settings exist', async () => {
    const mockSettings = [
      {
        key: 'app.name',
        value: 'MyApp',
        category: 'general',
        description: 'Application name',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
      {
        key: 'app.mode',
        value: 'safe',
        category: 'general',
        description: 'Application mode',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['general'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    vi.mocked(apiClient.updateSetting).mockResolvedValue({
      key: 'app.name',
      value: 'NewAppName',
      category: 'general',
      description: 'Application name',
      updated_at: '2024-01-02T00:00:00Z',
      updated_by: 'admin',
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('app.name')).toBeTruthy()
      expect(screen.getByText('app.mode')).toBeTruthy()
    })
    const editButtons = screen.getAllByText('Edit')

    await userEvent.click(editButtons[0])
    const textarea = screen.getByPlaceholderText('Enter setting value...')

    await userEvent.clear(textarea)
    await userEvent.type(textarea, 'NewAppName')
    const saveButton = screen.getByText('Save')

    await userEvent.click(saveButton)
    await waitFor(() => {
      expect(apiClient.updateSetting).toHaveBeenCalledWith('app.name', {
        value: 'NewAppName',
        category: 'general',
        description: 'Application name',
      })
    })
    expect(screen.getByText('app.mode')).toBeTruthy()
  })
  it('passes isSaving to SettingItem during update', async () => {
    const mockSettings = [
      {
        key: 'busy.key',
        value: 'val',
        category: 'test',
        description: 'Busy setting',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'tester',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['test'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)

    let resolveUpdate: (v: never) => void = () => {}

    vi.mocked(apiClient.updateSetting).mockImplementation(
      () => new Promise(resolve => (resolveUpdate = resolve as (v: never) => void))
    )
    renderSettings(<Settings />)
    await waitFor(() => expect(screen.getByText('busy.key')).toBeTruthy())
    await userEvent.click(screen.getByText('Edit'))
    const textarea = screen.getByPlaceholderText('Enter setting value...')

    await userEvent.clear(textarea)
    await userEvent.type(textarea, 'new-value')
    await userEvent.click(screen.getByText('Save'))
    await waitFor(() => expect(screen.getByText('Saving...')).toBeTruthy())
    resolveUpdate({
      key: 'busy.key',
      value: 'val',
      category: 'test',
      description: 'Busy setting',
      updated_at: '2024-01-02T00:00:00Z',
      updated_by: 'tester',
    } as never)
    await waitFor(() => expect(screen.queryByText('Saving...')).toBeNull())
  })
  it('passes isSaving to SettingItem during delete', async () => {
    const mockSettings = [
      {
        key: 'del.busy',
        value: 'val',
        category: 'test',
        description: 'Delete busy',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'tester',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['test'])
    vi.mocked(apiClient.getSettings)
      .mockResolvedValueOnce({ payload: mockSettings, count: mockSettings.length } as never)
      .mockResolvedValue({ payload: [], count: 0 } as never)

    let resolveDelete: (v: never) => void = () => {}

    vi.mocked(apiClient.removeSetting).mockImplementation(
      () => new Promise(resolve => (resolveDelete = resolve as (v: never) => void))
    )
    renderSettings(<Settings />)
    await waitFor(() => expect(screen.getByText('del.busy')).toBeTruthy())
    await userEvent.click(screen.getByRole('button', { name: /delete/i }))
    await userEvent.click(screen.getByRole('button', { name: /Yes, Delete/i }))
    await waitFor(() => expect(screen.getByText('Deleting...')).toBeTruthy())
    resolveDelete({ message: 'deleted' } as never)
    await waitFor(() => expect(screen.queryByText('Deleting...')).toBeNull())
  })
  it('displays query error without dismiss button', async () => {
    vi.mocked(apiClient.getSettings).mockRejectedValue(new Error('Network error'))
    vi.mocked(apiClient.getSettingCategories).mockResolvedValue([])
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText(/Network error/i)).toBeTruthy()
    })
    expect(screen.queryByText('Dismiss')).toBeNull()
  })
  it('displays all categories option', async () => {
    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['api', 'database'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({ payload: [], count: 0 } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('All Categories')).toBeTruthy()
    })
  })
  it('shows no settings found message when filtered list is empty', async () => {
    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['api'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({ payload: [], count: 0 } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('No settings found matching your criteria.')).toBeTruthy()
    })
  })
  it('enters edit mode when Edit button is clicked', async () => {
    const mockSettings = [
      {
        key: 'app.name',
        value: 'MyApp',
        category: 'general',
        description: 'Application name',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['general'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('app.name')).toBeTruthy()
    })
    const editButton = screen.getByText('Edit')

    await userEvent.click(editButton)
    expect(screen.getByText('Cancel')).toBeTruthy()
    expect(screen.getByText('Save')).toBeTruthy()
  })
  it('cancels editing and resets value', async () => {
    const mockSettings = [
      {
        key: 'app.name',
        value: 'MyApp',
        category: 'general',
        description: 'Application name',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['general'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('app.name')).toBeTruthy()
    })
    const editButton = screen.getByText('Edit')

    await userEvent.click(editButton)
    const textarea = screen.getByPlaceholderText('Enter setting value...')

    await userEvent.clear(textarea)
    await userEvent.type(textarea, 'ChangedValue')
    const cancelButton = screen.getByText('Cancel')

    await userEvent.click(cancelButton)
    expect(screen.getByText('Edit')).toBeTruthy()
    expect(screen.queryByText('Cancel')).toBeFalsy()
  })
  it('saves updated value when Save is clicked', async () => {
    const mockSettings = [
      {
        key: 'app.name',
        value: 'MyApp',
        category: 'general',
        description: 'Application name',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['general'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    vi.mocked(apiClient.updateSetting).mockResolvedValue({
      key: 'app.name',
      value: 'NewAppName',
      category: 'general',
      description: 'Application name',
      updated_at: '2024-01-02T00:00:00Z',
      updated_by: 'admin',
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('app.name')).toBeTruthy()
    })
    const editButton = screen.getByText('Edit')

    await userEvent.click(editButton)
    const textarea = screen.getByPlaceholderText('Enter setting value...')

    await userEvent.clear(textarea)
    await userEvent.type(textarea, 'NewAppName')
    const saveButton = screen.getByText('Save')

    await userEvent.click(saveButton)
    await waitFor(() => {
      expect(apiClient.updateSetting).toHaveBeenCalledWith('app.name', {
        value: 'NewAppName',
        category: 'general',
        description: 'Application name',
      })
    })
  })
  it('displays sensitive key indicator for api_key settings', async () => {
    const mockSettings = [
      {
        key: 'exchange.api_key',
        value: 'secret123',
        category: 'auth',
        description: 'Exchange API key',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['auth'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('exchange.api_key')).toBeTruthy()
    })
    expect(screen.getByText('🔒 Sensitive')).toBeTruthy()
    expect(screen.getByText('••••••••')).toBeTruthy()
  })
  it('displays category badges with appropriate colors', async () => {
    const mockSettings = [
      {
        key: 'trading.enabled',
        value: 'true',
        category: 'trading',
        description: 'Trading enabled',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
      {
        key: 'auth.token',
        value: 'token',
        category: 'auth',
        description: 'Auth token',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['trading', 'auth'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('trading.enabled')).toBeTruthy()
    })
    expect(screen.getByText('trading')).toBeTruthy()
    expect(screen.getByText('auth')).toBeTruthy()
  })
  it('displays JSON badge for JSON values', async () => {
    const mockSettings = [
      {
        key: 'app.config',
        value: '{"enabled":true,"count":5}',
        category: 'general',
        description: 'App config',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['general'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('app.config')).toBeTruthy()
    })
    expect(screen.getByText('📋 JSON')).toBeTruthy()
  })
  it('renders boolean toggle for allow_short_selling=false', async () => {
    const mockSettings = [
      {
        key: 'allow_short_selling',
        value: 'false',
        category: 'trading',
        description: 'Allow shorts',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['trading'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('allow_short_selling')).toBeTruthy()
    })
    const toggle = screen.getByTestId('setting-toggle-allow_short_selling')

    expect(toggle).toBeInTheDocument()
    expect(toggle.getAttribute('aria-pressed')).toBe('false')
    expect(screen.getByText(/Allow the engine to open short positions/i)).toBeInTheDocument()
  })
  it('renders boolean toggle as ON when allow_short_selling=true', async () => {
    const mockSettings = [
      {
        key: 'allow_short_selling',
        value: 'true',
        category: 'trading',
        description: 'Allow shorts',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['trading'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('allow_short_selling')).toBeTruthy()
    })
    const toggle = screen.getByTestId('setting-toggle-allow_short_selling')

    expect(toggle.getAttribute('aria-pressed')).toBe('true')
  })
  it('flips boolean value when toggle is clicked', async () => {
    const user = userEvent.setup()
    const mockSettings = [
      {
        key: 'allow_short_selling',
        value: 'false',
        category: 'trading',
        description: 'Allow shorts',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['trading'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    vi.mocked(apiClient.updateSetting).mockResolvedValue({} as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('allow_short_selling')).toBeTruthy()
    })
    const toggle = screen.getByTestId('setting-toggle-allow_short_selling')

    await user.click(toggle)
    expect(apiClient.updateSetting).toHaveBeenCalledWith('allow_short_selling', {
      value: 'true',
      category: 'trading',
      description: 'Allow shorts',
    })
  })
  it('deletes a boolean setting via the toggle row delete button', async () => {
    const user = userEvent.setup()
    const mockSettings = [
      {
        key: 'allow_short_selling',
        value: 'false',
        category: 'trading',
        description: 'Allow shorts',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['trading'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    vi.mocked(apiClient.removeSetting).mockResolvedValue({} as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('allow_short_selling')).toBeTruthy()
    })
    const deleteButton = screen.getByTestId('setting-delete-allow_short_selling')

    await user.click(deleteButton)
    const confirmButton = screen.getByRole('button', { name: /Yes, Delete/i })

    await user.click(confirmButton)
    expect(apiClient.removeSetting).toHaveBeenCalledWith('allow_short_selling')
  })
  it('shows "Deleting..." text on the confirm button while delete is in flight', async () => {
    const user = userEvent.setup()
    const mockSettings = [
      {
        key: 'allow_short_selling',
        value: 'false',
        category: 'trading',
        description: 'Allow shorts',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['trading'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)

    let resolveDelete: () => void = () => {}

    vi.mocked(apiClient.removeSetting).mockImplementation(
      () =>
        new Promise(resolve => {
          resolveDelete = () => resolve({} as never)
        })
    )
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('allow_short_selling')).toBeTruthy()
    })
    const deleteButton = screen.getByTestId('setting-delete-allow_short_selling')

    await user.click(deleteButton)
    const confirmButton = screen.getByRole('button', { name: /Yes, Delete/i })

    await user.click(confirmButton)
    await waitFor(() => {
      expect(screen.getByRole('button', { name: /Deleting/i })).toBeInTheDocument()
    })
    resolveDelete()
  })
  it('disables the toggle while an update is in flight to prevent double-apply', async () => {
    const user = userEvent.setup()
    const mockSettings = [
      {
        key: 'allow_short_selling',
        value: 'false',
        category: 'trading',
        description: 'Allow shorts',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['trading'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)

    let resolveUpdate: () => void = () => {}

    vi.mocked(apiClient.updateSetting).mockImplementation(
      () =>
        new Promise(resolve => {
          resolveUpdate = () => resolve({} as never)
        })
    )
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('allow_short_selling')).toBeTruthy()
    })
    const toggle = screen.getByTestId('setting-toggle-allow_short_selling')

    await user.click(toggle)
    await waitFor(() => {
      expect((toggle as HTMLButtonElement).disabled).toBe(true)
    })
    await user.click(toggle)
    expect(apiClient.updateSetting).toHaveBeenCalledTimes(1)
    resolveUpdate()
  })
  it('cancels a boolean setting delete confirmation', async () => {
    const user = userEvent.setup()
    const mockSettings = [
      {
        key: 'allow_short_selling',
        value: 'true',
        category: 'trading',
        description: 'Allow shorts',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['trading'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('allow_short_selling')).toBeTruthy()
    })
    const deleteButton = screen.getByTestId('setting-delete-allow_short_selling')

    await user.click(deleteButton)
    const cancelButton = screen.getByRole('button', { name: /Cancel/i })

    await user.click(cancelButton)
    expect(apiClient.removeSetting).not.toHaveBeenCalled()
    expect(screen.getByTestId('setting-delete-allow_short_selling')).toBeInTheDocument()
  })
  it('handles update error gracefully', async () => {
    const mockSettings = [
      {
        key: 'app.name',
        value: 'MyApp',
        category: 'general',
        description: 'Application name',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['general'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    vi.mocked(apiClient.updateSetting).mockRejectedValue(new Error('Update failed'))
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('app.name')).toBeTruthy()
    })
    const editButton = screen.getByText('Edit')

    await userEvent.click(editButton)
    const textarea = screen.getByPlaceholderText('Enter setting value...')

    await userEvent.clear(textarea)
    await userEvent.type(textarea, 'NewValue')
    const saveButton = screen.getByText('Save')

    await userEvent.click(saveButton)
    await waitFor(() => {
      expect(screen.getByText('Update failed')).toBeTruthy()
    })
  })
  it('shows empty value placeholder', async () => {
    const mockSettings = [
      {
        key: 'app.empty',
        value: '',
        category: 'general',
        description: 'Empty setting',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['general'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('app.empty')).toBeTruthy()
    })
    expect(screen.getByText('(empty)')).toBeTruthy()
  })
  it('displays updated timestamp and user', async () => {
    const mockSettings = [
      {
        key: 'app.setting',
        value: 'value',
        category: 'general',
        description: 'A setting',
        updated_at: '2024-01-15T10:30:00Z',
        updated_by: 'testuser',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['general'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('app.setting')).toBeTruthy()
    })
    expect(screen.getByText(/testuser/)).toBeTruthy()
  })
  it('does not call updateSetting when value unchanged', async () => {
    const mockSettings = [
      {
        key: 'app.name',
        value: 'MyApp',
        category: 'general',
        description: 'Application name',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['general'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('app.name')).toBeTruthy()
    })
    const editButton = screen.getByText('Edit')

    await userEvent.click(editButton)
    const saveButton = screen.getByText('Save')

    await userEvent.click(saveButton)
    await waitFor(() => {
      expect(screen.getByText('Edit')).toBeTruthy()
    })
    expect(apiClient.updateSetting).not.toHaveBeenCalled()
  })
  it('does not call updateSetting when JSON value unchanged and non-sensitive', async () => {
    const jsonValue = { enabled: true }
    const jsonString = JSON.stringify(jsonValue, null, 2)
    const mockSettings = [
      {
        key: 'app.config',
        value: jsonString,
        category: 'general',
        description: 'App config',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['general'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('app.config')).toBeTruthy()
    })
    const editButton = screen.getByText('Edit')

    await userEvent.click(editButton)
    const saveButton = screen.getByText('Save')

    await userEvent.click(saveButton)
    await waitFor(() => {
      expect(screen.getByText('Edit')).toBeTruthy()
    })
    expect(apiClient.updateSetting).not.toHaveBeenCalled()
  })
  it('forces encryption save for sensitive unencrypted values', async () => {
    const mockSettings = [
      {
        key: 'auth.api_key',
        value: 'cleartext_secret',
        category: 'auth',
        description: 'API key',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['auth'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    vi.mocked(apiClient.updateSetting).mockResolvedValue({
      key: 'auth.api_key',
      value: 'gAAAAABencrypted_value',
      category: 'auth',
      description: 'API key',
      updated_at: '2024-01-02T00:00:00Z',
      updated_by: 'admin',
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('auth.api_key')).toBeTruthy()
    })
    const editButton = screen.getByText('Edit')

    await userEvent.click(editButton)
    const saveButton = screen.getByText('Save')

    await userEvent.click(saveButton)
    await waitFor(() => {
      expect(apiClient.updateSetting).toHaveBeenCalledWith('auth.api_key', {
        value: 'cleartext_secret',
        category: 'auth',
        description: 'API key',
      })
    })
  })
  it('forces encryption save for sensitive JSON values', async () => {
    const mockSettings = [
      {
        key: 'auth_secret_key',
        value: '{"token":"abc"}',
        category: 'auth',
        description: 'Secret config',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['auth'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    vi.mocked(apiClient.updateSetting).mockResolvedValue({
      key: 'auth_secret_key',
      value: 'gAAAAABencrypted_value',
      category: 'auth',
      description: 'Secret config',
      updated_at: '2024-01-02T00:00:00Z',
      updated_by: 'admin',
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('auth_secret_key')).toBeTruthy()
    })
    const editButton = screen.getByText('Edit')

    await userEvent.click(editButton)
    const saveButton = screen.getByText('Save')

    await userEvent.click(saveButton)
    await waitFor(() => {
      expect(apiClient.updateSetting).toHaveBeenCalledWith('auth_secret_key', {
        value: JSON.stringify({ token: 'abc' }, null, 2),
        category: 'auth',
        description: 'Secret config',
      })
    })
  })
  it('skips save when sensitive JSON value is already encrypted and unchanged', async () => {
    const encryptedValue = `gAAAAAB${'x'.repeat(40)}`
    const jsonString = JSON.stringify(encryptedValue)
    const mockSettings = [
      {
        key: 'auth_secret_key',
        value: jsonString,
        category: 'auth',
        description: 'Secret config',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['auth'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('auth_secret_key')).toBeTruthy()
    })
    const editButton = screen.getByText('Edit')

    await userEvent.click(editButton)
    const saveButton = screen.getByText('Save')

    await userEvent.click(saveButton)
    await waitFor(() => {
      expect(screen.getByText('Edit')).toBeTruthy()
    })
    expect(apiClient.updateSetting).not.toHaveBeenCalled()
  })
  it('handles non-Error exception during load', async () => {
    vi.mocked(apiClient.getSettings).mockRejectedValue('string error')
    vi.mocked(apiClient.getSettingCategories).mockResolvedValue([])
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('Failed to load settings')).toBeTruthy()
    })
  })
  it('handles non-Error exception during update', async () => {
    const mockSettings = [
      {
        key: 'app.name',
        value: 'MyApp',
        category: 'general',
        description: 'Application name',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['general'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    vi.mocked(apiClient.updateSetting).mockRejectedValue('string error')
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('app.name')).toBeTruthy()
    })
    const editButton = screen.getByText('Edit')

    await userEvent.click(editButton)
    const textarea = screen.getByPlaceholderText('Enter setting value...')

    await userEvent.clear(textarea)
    await userEvent.type(textarea, 'NewValue')
    const saveButton = screen.getByText('Save')

    await userEvent.click(saveButton)
    await waitFor(() => {
      expect(screen.getByText('Failed to update setting')).toBeTruthy()
    })
  })
  it('displays various category colors correctly', async () => {
    const mockSettings = [
      {
        key: 'risk.max_loss',
        value: '1000',
        category: 'risk',
        description: 'Max loss',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
      {
        key: 'zmq.port',
        value: '5555',
        category: 'zmq',
        description: 'ZMQ port',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
      {
        key: 'network.timeout',
        value: '30',
        category: 'network',
        description: 'Timeout',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
      {
        key: 'system.debug',
        value: 'false',
        category: 'system',
        description: 'Debug',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
      {
        key: 'other.setting',
        value: 'val',
        category: 'unknown',
        description: 'Unknown',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue([
      'risk',
      'zmq',
      'network',
      'system',
      'unknown',
    ])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('risk')).toBeTruthy()
      expect(screen.getByText('zmq')).toBeTruthy()
      expect(screen.getByText('network')).toBeTruthy()
      expect(screen.getByText('system')).toBeTruthy()
      expect(screen.getByText('unknown')).toBeTruthy()
    })
  })
  it('detects various sensitive key patterns', async () => {
    const mockSettings = [
      {
        key: 'db.password',
        value: 'pass',
        category: 'auth',
        description: 'Password',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
      {
        key: 'auth.api_secret',
        value: 'secret',
        category: 'auth',
        description: 'API secret',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
      {
        key: 'crypto.private_key',
        value: 'key',
        category: 'auth',
        description: 'Private key',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
      {
        key: 'aws.credential',
        value: 'cred',
        category: 'auth',
        description: 'Credential',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['auth'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('db.password')).toBeTruthy()
    })
    const sensitiveBadges = screen.getAllByText('🔒 Sensitive')

    expect(sensitiveBadges.length).toBe(4)
  })
  it('does not show sensitive badge for non-sensitive keys', async () => {
    const mockSettings = [
      {
        key: 'app.name',
        value: 'MyApp',
        category: 'general',
        description: 'App name',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['general'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('app.name')).toBeTruthy()
    })
    expect(screen.queryByText('🔒 Sensitive')).toBeFalsy()
  })
  it('shows setting without description', async () => {
    const mockSettings = [
      {
        key: 'app.nodesc',
        value: 'value',
        category: 'general',
        description: null,
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['general'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('app.nodesc')).toBeTruthy()
    })
    expect(screen.getByText('value')).toBeTruthy()
  })
  it('shows setting without updated_by', async () => {
    const mockSettings = [
      {
        key: 'app.nouser',
        value: 'value',
        category: 'general',
        description: 'Desc',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: null,
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['general'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('app.nouser')).toBeTruthy()
    })
    expect(screen.getByText('value')).toBeTruthy()
  })
  it('handles JSON setting edit and save', async () => {
    const mockSettings = [
      {
        key: 'config.json',
        value: '{"host": "localhost", "port": 8080}',
        category: 'config',
        description: 'JSON Config',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['config'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    vi.mocked(apiClient.updateSetting).mockResolvedValue({
      key: 'config.json',
      value: '{"host": "newhost", "port": 8080}',
      category: 'config',
      description: 'JSON Config',
      updated_at: '2024-01-02T00:00:00Z',
      updated_by: 'admin',
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('config.json')).toBeTruthy()
    })
    const editButton = screen.getByRole('button', { name: /Edit/i })

    await userEvent.click(editButton)
    const saveButton = screen.getByRole('button', { name: /Save/i })

    await userEvent.click(saveButton)
    await waitFor(() => {
      expect(screen.getByText('config.json')).toBeTruthy()
    })
  })
  it('handles JSON setting cancel', async () => {
    const mockSettings = [
      {
        key: 'config.json',
        value: '{"key": "value"}',
        category: 'config',
        description: 'JSON Config',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['config'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('config.json')).toBeTruthy()
    })
    const editButton = screen.getByRole('button', { name: /Edit/i })

    await userEvent.click(editButton)
    const cancelButton = screen.getByRole('button', { name: /Cancel/i })

    await userEvent.click(cancelButton)
    await waitFor(() => {
      expect(screen.getByRole('button', { name: /Edit/i })).toBeTruthy()
    })
  })
  it('handles sensitive JSON setting encryption on save', async () => {
    const mockSettings = [
      {
        key: 'api_key',
        value: '{"secret": "unencrypted"}',
        category: 'credentials',
        description: 'API credentials',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['credentials'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    vi.mocked(apiClient.updateSetting).mockResolvedValue({
      key: 'api_key',
      value: 'ENC:encrypted',
      category: 'credentials',
      description: 'API credentials',
      updated_at: '2024-01-02T00:00:00Z',
      updated_by: 'admin',
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('api_key')).toBeTruthy()
    })
    const editButton = screen.getByRole('button', { name: /Edit/i })

    await userEvent.click(editButton)
    const saveButton = screen.getByRole('button', { name: /Save/i })

    await userEvent.click(saveButton)
    await waitFor(() => {
      expect(apiClient.updateSetting).toHaveBeenCalled()
    })
  })
  describe('delete setting functionality', () => {
    it('shows delete confirmation when Delete button is clicked', async () => {
      const mockSettings = [
        {
          key: 'test.setting',
          value: 'test-value',
          category: 'test',
          description: 'Test setting',
          updated_at: '2024-01-01T00:00:00Z',
          updated_by: 'tester',
        },
      ]

      vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['test'])
      vi.mocked(apiClient.getSettings).mockResolvedValue({
        payload: mockSettings,
        count: mockSettings.length,
      } as never)
      renderSettings(<Settings />)
      await waitFor(() => {
        expect(screen.getByText('test.setting')).toBeTruthy()
      })
      const deleteButton = screen.getByRole('button', { name: /Delete/i })

      await userEvent.click(deleteButton)
      expect(screen.getByText('Delete this setting?')).toBeTruthy()
      expect(screen.getByRole('button', { name: /Yes, Delete/i })).toBeTruthy()
      expect(screen.getByRole('button', { name: /Cancel/i })).toBeTruthy()
    })
    it('cancels delete when Cancel button is clicked', async () => {
      const mockSettings = [
        {
          key: 'test.setting',
          value: 'test-value',
          category: 'test',
          description: 'Test setting',
          updated_at: '2024-01-01T00:00:00Z',
          updated_by: 'tester',
        },
      ]

      vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['test'])
      vi.mocked(apiClient.getSettings).mockResolvedValue({
        payload: mockSettings,
        count: mockSettings.length,
      } as never)
      renderSettings(<Settings />)
      await waitFor(() => {
        expect(screen.getByText('test.setting')).toBeTruthy()
      })
      const deleteButton = screen.getByRole('button', { name: /Delete/i })

      await userEvent.click(deleteButton)
      const cancelButton = screen.getByRole('button', { name: /Cancel/i })

      await userEvent.click(cancelButton)
      expect(screen.queryByText('Delete this setting?')).toBeFalsy()
      expect(screen.getByRole('button', { name: /Delete/i })).toBeTruthy()
    })
    it('deletes setting when confirmed', async () => {
      const mockSettings = [
        {
          key: 'test.setting',
          value: 'test-value',
          category: 'test',
          description: 'Test setting',
          updated_at: '2024-01-01T00:00:00Z',
          updated_by: 'tester',
        },
      ]

      vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['test'])
      vi.mocked(apiClient.getSettings)
        .mockResolvedValueOnce({ payload: mockSettings, count: mockSettings.length } as never)
        .mockResolvedValue({ payload: [], count: 0 } as never)
      vi.mocked(apiClient.removeSetting).mockResolvedValue({ payload: 'Setting deleted' })
      renderSettings(<Settings />)
      await waitFor(() => {
        expect(screen.getByText('test.setting')).toBeTruthy()
      })
      const deleteButton = screen.getByRole('button', { name: /Delete/i })

      await userEvent.click(deleteButton)
      const confirmButton = screen.getByRole('button', { name: /Yes, Delete/i })

      await userEvent.click(confirmButton)
      await waitFor(() => {
        expect(apiClient.removeSetting).toHaveBeenCalledWith('test.setting')
      })
      await waitFor(() => {
        expect(screen.queryByText('test.setting')).toBeFalsy()
      })
    })
    it('handles delete error gracefully', async () => {
      const mockSettings = [
        {
          key: 'test.setting',
          value: 'test-value',
          category: 'test',
          description: 'Test setting',
          updated_at: '2024-01-01T00:00:00Z',
          updated_by: 'tester',
        },
      ]

      vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['test'])
      vi.mocked(apiClient.getSettings).mockResolvedValue({
        payload: mockSettings,
        count: mockSettings.length,
      } as never)
      vi.mocked(apiClient.removeSetting).mockRejectedValue(new Error('Delete failed'))
      renderSettings(<Settings />)
      await waitFor(() => {
        expect(screen.getByText('test.setting')).toBeTruthy()
      })
      const deleteButton = screen.getByRole('button', { name: /Delete/i })

      await userEvent.click(deleteButton)
      const confirmButton = screen.getByRole('button', { name: /Yes, Delete/i })

      await userEvent.click(confirmButton)
      await waitFor(() => {
        expect(screen.getByText('Delete failed')).toBeTruthy()
      })
      expect(screen.getByText('test.setting')).toBeTruthy()
    })
    it('handles non-Error exception during delete', async () => {
      const mockSettings = [
        {
          key: 'test.setting',
          value: 'test-value',
          category: 'test',
          description: 'Test setting',
          updated_at: '2024-01-01T00:00:00Z',
          updated_by: 'tester',
        },
      ]

      vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['test'])
      vi.mocked(apiClient.getSettings).mockResolvedValue({
        payload: mockSettings,
        count: mockSettings.length,
      } as never)
      vi.mocked(apiClient.removeSetting).mockRejectedValue('unknown error')
      renderSettings(<Settings />)
      await waitFor(() => {
        expect(screen.getByText('test.setting')).toBeTruthy()
      })
      const deleteButton = screen.getByRole('button', { name: /Delete/i })

      await userEvent.click(deleteButton)
      const confirmButton = screen.getByRole('button', { name: /Yes, Delete/i })

      await userEvent.click(confirmButton)
      await waitFor(() => {
        expect(screen.getByText('Failed to delete setting')).toBeTruthy()
      })
      expect(screen.getByText('test.setting')).toBeTruthy()
    })
  })
  describe('Add Setting Modal', () => {
    it('opens Add Setting modal when clicking Add Setting button', async () => {
      vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['api'])
      vi.mocked(apiClient.getSettings).mockResolvedValue({ payload: [], count: 0 } as never)
      renderSettings(<Settings />)
      await waitFor(() => {
        expect(screen.getByText('Settings')).toBeTruthy()
      })
      const addButton = screen.getByRole('button', { name: /Add Setting/i })

      await userEvent.click(addButton)
      await waitFor(() => {
        expect(screen.getByText('Add New Setting')).toBeTruthy()
      })
    })
    it('creates new setting successfully', async () => {
      const newSetting = {
        key: 'new.setting',
        value: 'test value',
        category: 'api',
        description: 'Test description',
        updated_at: '2026-01-12T00:00:00Z',
        updated_by: 'tester',
      }

      vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['api'])
      vi.mocked(apiClient.getSettings)
        .mockResolvedValueOnce({ payload: [], count: 0 } as never)
        .mockResolvedValue({ payload: [newSetting], count: 1 } as never)
      vi.mocked(apiClient.updateSetting).mockResolvedValue(newSetting as never)
      renderSettings(<Settings />)
      await waitFor(() => {
        expect(screen.getByText('Settings')).toBeTruthy()
      })
      const addButton = screen.getByRole('button', { name: /Add Setting/i })

      await userEvent.click(addButton)
      await waitFor(() => {
        expect(screen.getByText('Add New Setting')).toBeTruthy()
      })
      await userEvent.type(
        screen.getByPlaceholderText(/e\.g\., walutomat\.api_key/i),
        'new.setting'
      )
      await userEvent.type(screen.getByPlaceholderText(/Setting value/i), 'test value')
      const comboboxes = screen.getAllByRole('combobox')

      await userEvent.selectOptions(comboboxes[1], 'api')
      await userEvent.type(screen.getByPlaceholderText(/Optional description/i), 'Test description')
      const createButton = screen.getByRole('button', { name: /Create Setting/i })

      await userEvent.click(createButton)
      await waitFor(() => {
        expect(apiClient.updateSetting).toHaveBeenCalledWith('new.setting', {
          value: 'test value',
          category: 'api',
          description: 'Test description',
        })
      })
      await waitFor(() => {
        expect(screen.queryByText('Add New Setting')).toBeNull()
      })
      await waitFor(() => {
        expect(screen.getByText('new.setting')).toBeTruthy()
      })
    })
    it('creates new setting with new category', async () => {
      const newSetting = {
        key: 'custom.setting',
        value: 'value',
        category: 'newcategory',
        description: '',
        updated_at: '2026-01-12T00:00:00Z',
        updated_by: 'tester',
      }

      vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['api'])
      vi.mocked(apiClient.getSettings).mockResolvedValue({ payload: [], count: 0 } as never)
      vi.mocked(apiClient.updateSetting).mockResolvedValue(newSetting as never)
      renderSettings(<Settings />)
      await waitFor(() => {
        expect(screen.getByText('Settings')).toBeTruthy()
      })
      await userEvent.click(screen.getByRole('button', { name: /Add Setting/i }))
      await waitFor(() => {
        expect(screen.getByText('Add New Setting')).toBeTruthy()
      })
      await userEvent.type(
        screen.getByPlaceholderText(/e\.g\., walutomat\.api_key/i),
        'custom.setting'
      )
      await userEvent.type(screen.getByPlaceholderText(/Setting value/i), 'value')
      await userEvent.type(
        screen.getByPlaceholderText(/Or enter new category name/i),
        'newcategory'
      )
      await userEvent.click(screen.getByRole('button', { name: /Create Setting/i }))
      await waitFor(() => {
        expect(apiClient.updateSetting).toHaveBeenCalledWith('custom.setting', {
          value: 'value',
          category: 'newcategory',
          description: '',
        })
      })
    })
    it('shows validation error when key is empty', async () => {
      vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['api'])
      vi.mocked(apiClient.getSettings).mockResolvedValue({ payload: [], count: 0 } as never)
      renderSettings(<Settings />)
      await waitFor(() => {
        expect(screen.getByText('Settings')).toBeTruthy()
      })
      await userEvent.click(screen.getByRole('button', { name: /Add Setting/i }))
      await waitFor(() => {
        expect(screen.getByText('Add New Setting')).toBeTruthy()
      })
      await userEvent.click(screen.getByRole('button', { name: /Create Setting/i }))
      await waitFor(() => {
        expect(screen.getByText('Key is required')).toBeTruthy()
      })
    })
    it('shows validation error when value is empty', async () => {
      vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['api'])
      vi.mocked(apiClient.getSettings).mockResolvedValue({ payload: [], count: 0 } as never)
      renderSettings(<Settings />)
      await waitFor(() => {
        expect(screen.getByText('Settings')).toBeTruthy()
      })
      await userEvent.click(screen.getByRole('button', { name: /Add Setting/i }))
      await waitFor(() => {
        expect(screen.getByText('Add New Setting')).toBeTruthy()
      })
      await userEvent.type(screen.getByPlaceholderText(/e\.g\., walutomat\.api_key/i), 'test.key')
      await userEvent.click(screen.getByRole('button', { name: /Create Setting/i }))
      await waitFor(() => {
        expect(screen.getByText('Value is required')).toBeTruthy()
      })
    })
    it('shows validation error when category is empty', async () => {
      vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['api'])
      vi.mocked(apiClient.getSettings).mockResolvedValue({ payload: [], count: 0 } as never)
      renderSettings(<Settings />)
      await waitFor(() => {
        expect(screen.getByText('Settings')).toBeTruthy()
      })
      await userEvent.click(screen.getByRole('button', { name: /Add Setting/i }))
      await waitFor(() => {
        expect(screen.getByText('Add New Setting')).toBeTruthy()
      })
      await userEvent.type(screen.getByPlaceholderText(/e\.g\., walutomat\.api_key/i), 'test.key')
      await userEvent.type(screen.getByPlaceholderText(/Setting value/i), 'value')
      await userEvent.click(screen.getByRole('button', { name: /Create Setting/i }))
      await waitFor(() => {
        expect(screen.getByText('Category is required')).toBeTruthy()
      })
    })
    it('shows error when setting already exists', async () => {
      const existingSettings = [
        {
          key: 'existing.key',
          value: 'value',
          category: 'api',
          description: '',
          updated_at: '2026-01-12T00:00:00Z',
          updated_by: 'tester',
        },
      ]

      vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['api'])
      vi.mocked(apiClient.getSettings).mockResolvedValue({
        payload: existingSettings,
        count: existingSettings.length,
      } as never)
      renderSettings(<Settings />)
      await waitFor(() => {
        expect(screen.getByText('existing.key')).toBeTruthy()
      })
      await userEvent.click(screen.getByRole('button', { name: /Add Setting/i }))
      await waitFor(() => {
        expect(screen.getByText('Add New Setting')).toBeTruthy()
      })
      await userEvent.type(
        screen.getByPlaceholderText(/e\.g\., walutomat\.api_key/i),
        'existing.key'
      )
      await userEvent.type(screen.getByPlaceholderText(/Setting value/i), 'new value')
      const comboboxes = screen.getAllByRole('combobox')

      await userEvent.selectOptions(comboboxes[1], 'api')
      await userEvent.click(screen.getByRole('button', { name: /Create Setting/i }))
      await waitFor(() => {
        expect(screen.getByText(/already exists/i)).toBeTruthy()
      })
    })
    it('closes modal when clicking Cancel', async () => {
      vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['api'])
      vi.mocked(apiClient.getSettings).mockResolvedValue({ payload: [], count: 0 } as never)
      renderSettings(<Settings />)
      await waitFor(() => {
        expect(screen.getByText('Settings')).toBeTruthy()
      })
      await userEvent.click(screen.getByRole('button', { name: /Add Setting/i }))
      await waitFor(() => {
        expect(screen.getByText('Add New Setting')).toBeTruthy()
      })
      await userEvent.click(screen.getByRole('button', { name: /Cancel/i }))
      await waitFor(() => {
        expect(screen.queryByText('Add New Setting')).toBeNull()
      })
    })
    it('handles API error when creating setting', async () => {
      vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['api'])
      vi.mocked(apiClient.getSettings).mockResolvedValue({ payload: [], count: 0 } as never)
      vi.mocked(apiClient.updateSetting).mockRejectedValue(new Error('Server error'))
      renderSettings(<Settings />)
      await waitFor(() => {
        expect(screen.getByText('Settings')).toBeTruthy()
      })
      await userEvent.click(screen.getByRole('button', { name: /Add Setting/i }))
      await waitFor(() => {
        expect(screen.getByText('Add New Setting')).toBeTruthy()
      })
      await userEvent.type(
        screen.getByPlaceholderText(/e\.g\., walutomat\.api_key/i),
        'new.setting'
      )
      await userEvent.type(screen.getByPlaceholderText(/Setting value/i), 'value')
      const comboboxes = screen.getAllByRole('combobox')

      await userEvent.selectOptions(comboboxes[1], 'api')
      await userEvent.click(screen.getByRole('button', { name: /Create Setting/i }))
      await waitFor(() => {
        expect(screen.getAllByText('Server error').length).toBeGreaterThan(0)
      })
      expect(screen.getByText('Add New Setting')).toBeTruthy()
    })
    it('resets form when reopening modal', async () => {
      vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['api'])
      vi.mocked(apiClient.getSettings).mockResolvedValue({ payload: [], count: 0 } as never)
      renderSettings(<Settings />)
      await waitFor(() => {
        expect(screen.getByText('Settings')).toBeTruthy()
      })
      await userEvent.click(screen.getByRole('button', { name: /Add Setting/i }))
      await waitFor(() => {
        expect(screen.getByText('Add New Setting')).toBeTruthy()
      })
      await userEvent.type(screen.getByPlaceholderText(/e\.g\., walutomat\.api_key/i), 'test.key')
      await userEvent.click(screen.getByRole('button', { name: /Cancel/i }))
      await userEvent.click(screen.getByRole('button', { name: /Add Setting/i }))
      await waitFor(() => {
        expect(screen.getByText('Add New Setting')).toBeTruthy()
      })
      const keyInput = screen.getByPlaceholderText(
        /e\.g\., walutomat\.api_key/i
      ) as HTMLInputElement

      expect(keyInput.value).toBe('')
    })
    it('clears new category when selecting existing category', async () => {
      vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['api', 'walutomat'])
      vi.mocked(apiClient.getSettings).mockResolvedValue({ payload: [], count: 0 } as never)
      renderSettings(<Settings />)
      await waitFor(() => {
        expect(screen.getByText('Settings')).toBeTruthy()
      })
      await userEvent.click(screen.getByRole('button', { name: /Add Setting/i }))
      await waitFor(() => {
        expect(screen.getByText('Add New Setting')).toBeTruthy()
      })
      const newCategoryInput = screen.getByPlaceholderText(
        /Or enter new category name/i
      ) as HTMLInputElement

      await userEvent.type(newCategoryInput, 'new-cat')
      expect(newCategoryInput.value).toBe('new-cat')
      const comboboxes = screen.getAllByRole('combobox')

      await userEvent.selectOptions(comboboxes[1], 'api')
      expect(newCategoryInput.value).toBe('')
    })
    it('does not clear new category when selecting empty option', async () => {
      vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['api', 'walutomat'])
      vi.mocked(apiClient.getSettings).mockResolvedValue({ payload: [], count: 0 } as never)
      renderSettings(<Settings />)
      await waitFor(() => {
        expect(screen.getByText('Settings')).toBeTruthy()
      })
      await userEvent.click(screen.getByRole('button', { name: /Add Setting/i }))
      await waitFor(() => {
        expect(screen.getByText('Add New Setting')).toBeTruthy()
      })
      const newCategoryInput = screen.getByPlaceholderText(
        /Or enter new category name/i
      ) as HTMLInputElement

      await userEvent.type(newCategoryInput, 'new-cat')
      const comboboxes = screen.getAllByRole('combobox')

      await userEvent.selectOptions(comboboxes[1], 'api')
      expect(newCategoryInput.value).toBe('')
      await userEvent.type(newCategoryInput, 'another-cat')
      expect(newCategoryInput.value).toBe('another-cat')
      await userEvent.selectOptions(comboboxes[1], '')
      expect(newCategoryInput.value).toBe('another-cat')
    })
    it('clears selected category when typing new category', async () => {
      vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['api', 'walutomat'])
      vi.mocked(apiClient.getSettings).mockResolvedValue({ payload: [], count: 0 } as never)
      renderSettings(<Settings />)
      await waitFor(() => {
        expect(screen.getByText('Settings')).toBeTruthy()
      })
      await userEvent.click(screen.getByRole('button', { name: /Add Setting/i }))
      await waitFor(() => {
        expect(screen.getByText('Add New Setting')).toBeTruthy()
      })
      const comboboxes = screen.getAllByRole('combobox')

      await userEvent.selectOptions(comboboxes[1], 'api')
      expect((comboboxes[1] as HTMLSelectElement).value).toBe('api')
      const newCategoryInput = screen.getByPlaceholderText(/Or enter new category name/i)

      await userEvent.type(newCategoryInput, 'x')
      expect((comboboxes[1] as HTMLSelectElement).value).toBe('')
    })
    it('does not clear selected category when new category is cleared via typing', async () => {
      vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['api', 'walutomat'])
      vi.mocked(apiClient.getSettings).mockResolvedValue({ payload: [], count: 0 } as never)
      renderSettings(<Settings />)
      await waitFor(() => {
        expect(screen.getByText('Settings')).toBeTruthy()
      })
      await userEvent.click(screen.getByRole('button', { name: /Add Setting/i }))
      await waitFor(() => {
        expect(screen.getByText('Add New Setting')).toBeTruthy()
      })
      const comboboxes = screen.getAllByRole('combobox')
      const newCategoryInput = screen.getByPlaceholderText(
        /Or enter new category name/i
      ) as HTMLInputElement

      await userEvent.selectOptions(comboboxes[1], 'api')
      expect((comboboxes[1] as HTMLSelectElement).value).toBe('api')
      await userEvent.type(newCategoryInput, 'x')
      expect(newCategoryInput.value).toBe('x')
      expect((comboboxes[1] as HTMLSelectElement).value).toBe('')
      await userEvent.clear(newCategoryInput)
      expect(newCategoryInput.value).toBe('')
      expect((comboboxes[1] as HTMLSelectElement).value).toBe('')
    })
    it('shows fallback error for non-Error exception when creating setting', async () => {
      vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['api'])
      vi.mocked(apiClient.getSettings).mockResolvedValue({ payload: [], count: 0 } as never)
      vi.mocked(apiClient.updateSetting).mockRejectedValue('String error')
      renderSettings(<Settings />)
      await waitFor(() => {
        expect(screen.getByText('Settings')).toBeTruthy()
      })
      await userEvent.click(screen.getByRole('button', { name: /Add Setting/i }))
      await waitFor(() => {
        expect(screen.getByText('Add New Setting')).toBeTruthy()
      })
      await userEvent.type(
        screen.getByPlaceholderText(/e\.g\., walutomat\.api_key/i),
        'new.setting'
      )
      await userEvent.type(screen.getByPlaceholderText(/Setting value/i), 'value')
      const comboboxes = screen.getAllByRole('combobox')

      await userEvent.selectOptions(comboboxes[1], 'api')
      await userEvent.click(screen.getByRole('button', { name: /Create Setting/i }))
      await waitFor(() => {
        expect(screen.getByText('Failed to create setting')).toBeTruthy()
      })
    })
  })
  it('toggles Show more and Show less for long JSON values', async () => {
    const longJson = JSON.stringify(
      { key1: 'value1', key2: 'value2', key3: 'value3', key4: 'value4' },
      null,
      2
    )
    const mockSettings = [
      {
        key: 'config.long_json',
        value: longJson,
        category: 'config',
        description: 'Long JSON config',
        updated_at: '2024-01-01T00:00:00Z',
        updated_by: 'admin',
      },
    ]

    vi.mocked(apiClient.getSettingCategories).mockResolvedValue(['config'])
    vi.mocked(apiClient.getSettings).mockResolvedValue({
      payload: mockSettings,
      count: mockSettings.length,
    } as never)
    renderSettings(<Settings />)
    await waitFor(() => {
      expect(screen.getByText('config.long_json')).toBeTruthy()
    })
    const showMoreButton = screen.getByText('Show more')

    await userEvent.click(showMoreButton)
    expect(screen.getByText('Show less')).toBeTruthy()
    await userEvent.click(screen.getByText('Show less'))
    expect(screen.getByText('Show more')).toBeTruthy()
  })
})
