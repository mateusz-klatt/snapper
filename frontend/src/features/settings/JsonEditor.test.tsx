import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor, fireEvent } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import type { ReactNode } from 'react'
import { JsonEditor } from './JsonEditor'

const renderWithMocks = (ui: ReactNode) => {
  return render(ui)
}

describe('JsonEditor', () => {
  const mockOnChange = vi.fn()

  beforeEach(() => {
    vi.clearAllMocks()
  })
  it('renders in form mode by default', () => {
    renderWithMocks(<JsonEditor value={{}} onChange={mockOnChange} />)
    expect(screen.getByText(/Form Editor/i)).toBeTruthy()
  })
  it('displays primitive values', () => {
    renderWithMocks(<JsonEditor value='test string' onChange={mockOnChange} />)
    expect(screen.getByDisplayValue('test string')).toBeTruthy()
  })
  it('displays object with nested properties', () => {
    const value = { name: 'test', age: 25 }

    renderWithMocks(<JsonEditor value={value} onChange={mockOnChange} />)
    expect(screen.getByDisplayValue('test')).toBeTruthy()
    expect(screen.getByDisplayValue('25')).toBeTruthy()
  })
  it('displays array items', () => {
    const value = ['item1', 'item2', 'item3']

    renderWithMocks(<JsonEditor value={value} onChange={mockOnChange} />)
    expect(screen.getByText(/Array \(3 items\)/i)).toBeTruthy()
    expect(screen.getByDisplayValue('item1')).toBeTruthy()
    expect(screen.getByDisplayValue('item2')).toBeTruthy()
  })
  it('switches to raw mode', async () => {
    renderWithMocks(<JsonEditor value={{ key: 'value' }} onChange={mockOnChange} />)
    const rawJsonButton = screen.getByRole('button', { name: /Raw JSON/i })

    await userEvent.click(rawJsonButton)
    await waitFor(() => {
      expect(screen.getByText(/Raw JSON Editor/i)).toBeTruthy()
    })
  })
  it('displays raw JSON in textarea', async () => {
    const value = { key: 'value', nested: { prop: 123 } }

    renderWithMocks(<JsonEditor value={value} onChange={mockOnChange} />)
    const rawJsonButton = screen.getByRole('button', { name: /Raw JSON/i })

    await userEvent.click(rawJsonButton)
    await waitFor(() => {
      const textarea = screen.getByRole('textbox') as HTMLTextAreaElement

      expect(textarea.value).toContain('"key"')
      expect(textarea.value).toContain('"value"')
    })
  })
  it('handles raw JSON changes', async () => {
    renderWithMocks(<JsonEditor value={{}} onChange={mockOnChange} />)
    const rawJsonButton = screen.getByRole('button', { name: /Raw JSON/i })

    await userEvent.click(rawJsonButton)
    await waitFor(() => {
      expect(screen.getByRole('textbox')).toBeTruthy()
    })
    const textarea = screen.getByRole('textbox')

    fireEvent.change(textarea, { target: { value: '{"newKey": "newValue"}' } })
    await waitFor(() => {
      expect(mockOnChange).toHaveBeenCalledWith({ newKey: 'newValue' })
    })
  })
  it('shows parse error for invalid JSON', async () => {
    renderWithMocks(<JsonEditor value={{}} onChange={mockOnChange} />)
    const rawJsonButton = screen.getByRole('button', { name: /Raw JSON/i })

    await userEvent.click(rawJsonButton)
    await waitFor(() => {
      expect(screen.getByRole('textbox')).toBeTruthy()
    })
    const textarea = screen.getByRole('textbox')

    fireEvent.change(textarea, { target: { value: '{invalid json}' } })
    await waitFor(() => {
      expect(screen.getByText(/Parse error/i)).toBeTruthy()
    })
  })
  it('imports raw JSON to form mode', async () => {
    renderWithMocks(<JsonEditor value={{}} onChange={mockOnChange} />)
    const rawJsonButton = screen.getByRole('button', { name: /Raw JSON/i })

    await userEvent.click(rawJsonButton)
    await waitFor(() => {
      expect(screen.getByRole('textbox')).toBeTruthy()
    })
    const textarea = screen.getByRole('textbox')

    fireEvent.change(textarea, { target: { value: '{"imported": true}' } })
    const importButton = screen.getByRole('button', { name: /Import to Form/i })

    await userEvent.click(importButton)
    await waitFor(() => {
      expect(screen.getByText(/Form Editor/i)).toBeTruthy()
    })
  })
  it('shows parse error when importing invalid JSON', async () => {
    renderWithMocks(<JsonEditor value={{}} onChange={mockOnChange} />)
    const rawJsonButton = screen.getByRole('button', { name: /Raw JSON/i })

    await userEvent.click(rawJsonButton)
    const textarea = await screen.findByRole('textbox')

    fireEvent.change(textarea, { target: { value: '{invalid json}' } })
    const importButton = screen.getByRole('button', { name: /Import to Form/i })

    await userEvent.click(importButton)
    await waitFor(() => {
      expect(screen.getByText(/Parse error/i)).toBeTruthy()
    })
  })
  it('ignores non-Error parse failures in raw editor', async () => {
    renderWithMocks(<JsonEditor value={{}} onChange={mockOnChange} />)
    const rawJsonButton = screen.getByRole('button', { name: /Raw JSON/i })

    await userEvent.click(rawJsonButton)
    const textarea = await screen.findByRole('textbox')
    const nonErrorObj = { code: 'PARSE_FAIL' }
    const parseSpy = vi.spyOn(JSON, 'parse').mockImplementation(() => {
      throw nonErrorObj
    })

    fireEvent.change(textarea, { target: { value: '{bad' } })
    expect(screen.queryByText(/Parse error/i)).toBeNull()
    parseSpy.mockRestore()
  })
  it('shows parse error when import fails after valid JSON', async () => {
    renderWithMocks(<JsonEditor value={{}} onChange={mockOnChange} />)
    const rawJsonButton = screen.getByRole('button', { name: /Raw JSON/i })

    await userEvent.click(rawJsonButton)
    const textarea = await screen.findByRole('textbox')
    const originalParse = JSON.parse
    const parseSpy = vi
      .spyOn(JSON, 'parse')
      .mockImplementationOnce((value: string) => originalParse(value))
      .mockImplementationOnce(() => {
        throw new Error('Import error')
      })
      .mockImplementation((value: string) => originalParse(value))

    fireEvent.change(textarea, { target: { value: '{"ok": true}' } })
    const importButton = screen.getByRole('button', { name: /Import to Form/i })

    await userEvent.click(importButton)
    await waitFor(() => {
      expect(screen.getByText(/Parse error/i)).toBeTruthy()
    })
    parseSpy.mockRestore()
  })
  it('ignores non-Error failures when importing raw JSON', async () => {
    renderWithMocks(<JsonEditor value={{}} onChange={mockOnChange} />)
    const rawJsonButton = screen.getByRole('button', { name: /Raw JSON/i })

    await userEvent.click(rawJsonButton)
    const textarea = await screen.findByRole('textbox')
    const originalParse = JSON.parse
    const parseSpy = vi
      .spyOn(JSON, 'parse')
      .mockImplementationOnce((value: string) => originalParse(value))
      .mockImplementationOnce(() => {
        throw { code: 'PARSE_FAIL' }
      })
      .mockImplementation((value: string) => originalParse(value))

    fireEvent.change(textarea, { target: { value: '{"ok": true}' } })
    const importButton = screen.getByRole('button', { name: /Import to Form/i })

    await userEvent.click(importButton)
    expect(screen.queryByText(/Parse error/i)).toBeNull()
    parseSpy.mockRestore()
  })
  it('switches back to form mode', async () => {
    renderWithMocks(<JsonEditor value={{ key: 'value' }} onChange={mockOnChange} />)
    const rawJsonButton = screen.getByRole('button', { name: /Raw JSON/i })

    await userEvent.click(rawJsonButton)
    await waitFor(() => {
      expect(screen.getByText(/Raw JSON Editor/i)).toBeTruthy()
    })
    const formButton = screen.getByRole('button', { name: /Switch to Form/i })

    await userEvent.click(formButton)
    await waitFor(() => {
      expect(screen.getByText(/Form Editor/i)).toBeTruthy()
    })
  })
  it('adds array item', async () => {
    const value = ['item1']

    renderWithMocks(<JsonEditor value={value} onChange={mockOnChange} />)
    const addButton = screen.getByRole('button', { name: /Add Item/i })

    await userEvent.click(addButton)
    expect(mockOnChange).toHaveBeenCalled()
    const call = mockOnChange.mock.calls[0][0]

    expect(call).toHaveLength(2)
  })
  it('adds default item for empty array', async () => {
    renderWithMocks(<JsonEditor value={[]} onChange={mockOnChange} />)
    const addButton = screen.getByRole('button', { name: /Add Item/i })

    await userEvent.click(addButton)
    expect(mockOnChange).toHaveBeenCalledWith([''])
  })
  it('removes array item', async () => {
    const value = ['item1', 'item2', 'item3']

    renderWithMocks(<JsonEditor value={value} onChange={mockOnChange} />)
    const removeButtons = screen.getAllByRole('button', { name: /Remove/i })

    await userEvent.click(removeButtons[0])
    await waitFor(() => {
      expect(mockOnChange).toHaveBeenCalledWith(['item2', 'item3'])
    })
  })
  it('edits primitive value', async () => {
    renderWithMocks(<JsonEditor value='initial' onChange={mockOnChange} />)
    const input = screen.getByDisplayValue('initial')

    await userEvent.clear(input)
    await userEvent.type(input, 'updated')
    expect(mockOnChange).toHaveBeenCalled()
  })
  it('renders in readonly mode', () => {
    renderWithMocks(<JsonEditor value={{ key: 'value' }} onChange={mockOnChange} readOnly={true} />)
    const input = screen.getByDisplayValue('value')

    expect(input).toHaveAttribute('readonly')
  })
  it('shows expand indicator for complex object values', () => {
    renderWithMocks(<JsonEditor value={{ nested: { foo: 'bar' } }} onChange={mockOnChange} />)
    expect(screen.getByText('▼')).toBeTruthy()
    expect(screen.getByText('nested')).toBeTruthy()
  })
  it('disables import button when parse error exists', async () => {
    renderWithMocks(<JsonEditor value={{}} onChange={mockOnChange} />)
    const rawJsonButton = screen.getByRole('button', { name: /Raw JSON/i })

    await userEvent.click(rawJsonButton)
    await waitFor(() => {
      expect(screen.getByRole('textbox')).toBeTruthy()
    })
    const textarea = screen.getByRole('textbox')

    fireEvent.change(textarea, { target: { value: '{invalid}' } })
    await waitFor(() => {
      const importButton = screen.getByRole('button', { name: /Import to Form/i })

      expect(importButton).toBeDisabled()
    })
  })
  it('handles boolean values', async () => {
    renderWithMocks(<JsonEditor value={{ enabled: true }} onChange={mockOnChange} />)
    const checkbox = screen.getByRole('checkbox')

    expect(checkbox).toBeChecked()
    await userEvent.click(checkbox)
    await waitFor(() => {
      expect(mockOnChange).toHaveBeenCalledWith({ enabled: false })
    })
  })
  it('handles number values', async () => {
    renderWithMocks(<JsonEditor value={{ count: 42 }} onChange={mockOnChange} />)
    const input = screen.getByDisplayValue('42')

    expect(input).toHaveAttribute('type', 'number')
    await userEvent.clear(input)
    await userEvent.type(input, '100')
    expect(mockOnChange).toHaveBeenCalled()
  })
  it('handles null values in objects', () => {
    renderWithMocks(<JsonEditor value={{ name: null }} onChange={mockOnChange} />)
    const input = screen.getByDisplayValue('')

    expect(input).toBeInTheDocument()
  })
  it('does not render expand arrow for primitive object values', () => {
    renderWithMocks(<JsonEditor value={{ name: 'value' }} onChange={mockOnChange} />)
    expect(screen.queryByText('▼')).toBeNull()
    expect(screen.queryByText('▶')).toBeNull()
  })
  it('imports valid raw JSON successfully', async () => {
    renderWithMocks(<JsonEditor value={{ old: 'value' }} onChange={mockOnChange} />)
    const rawJsonButton = screen.getByRole('button', { name: /Raw JSON/i })

    await userEvent.click(rawJsonButton)
    await waitFor(() => {
      expect(screen.getByRole('textbox')).toBeTruthy()
    })
    const textarea = screen.getByRole('textbox')

    fireEvent.change(textarea, { target: { value: '{"new": "value"}' } })
    const importButton = screen.getByRole('button', { name: /Import to Form/i })

    await userEvent.click(importButton)
    await waitFor(() => {
      expect(mockOnChange).toHaveBeenCalledWith({ new: 'value' })
    })
  })
  it('shows error when importing invalid raw JSON', async () => {
    renderWithMocks(<JsonEditor value={{ old: 'value' }} onChange={mockOnChange} />)
    const rawJsonButton = screen.getByRole('button', { name: /Raw JSON/i })

    await userEvent.click(rawJsonButton)
    await waitFor(() => {
      expect(screen.getByRole('textbox')).toBeTruthy()
    })
    const textarea = screen.getByRole('textbox')

    fireEvent.change(textarea, { target: { value: '{}' } })
    await waitFor(() => {
      expect(screen.queryByText(/Parse error/i)).toBeFalsy()
    })
    fireEvent.change(textarea, { target: { value: '{"invalid": }' } })
    const importButton = screen.getByRole('button', { name: /Import to Form/i })

    expect(importButton).toBeDisabled()
  })
  it('expands and collapses nested object fields', async () => {
    const value = {
      nested: {
        deep: 'value',
      },
    }

    renderWithMocks(<JsonEditor value={value} onChange={mockOnChange} />)
    const nestedToggle = screen.getByText('nested').closest('button')

    expect(nestedToggle).toBeTruthy()
    expect(screen.getByDisplayValue('value')).toBeTruthy()

    if (nestedToggle) {
      await userEvent.click(nestedToggle)
      await userEvent.click(nestedToggle)
    }

    expect(screen.getByText('▶')).toBeTruthy()
    expect(screen.getByText('nested')).toBeTruthy()
  })
  it('handles deeply nested objects', () => {
    const value = {
      level1: {
        level2: {
          level3: 'deep value',
        },
      },
    }

    renderWithMocks(<JsonEditor value={value} onChange={mockOnChange} />)
    expect(screen.getByText('level1')).toBeTruthy()
    expect(screen.getByText('level2')).toBeTruthy()
    expect(screen.getByDisplayValue('deep value')).toBeTruthy()
  })
  it('handles arrays inside objects', () => {
    const value = {
      items: ['a', 'b', 'c'],
    }

    renderWithMocks(<JsonEditor value={value} onChange={mockOnChange} />)
    expect(screen.getByText('items')).toBeTruthy()
    expect(screen.getByDisplayValue('a')).toBeTruthy()
    expect(screen.getByDisplayValue('b')).toBeTruthy()
    expect(screen.getByDisplayValue('c')).toBeTruthy()
  })
  it('renders readonly textarea in raw mode', async () => {
    renderWithMocks(<JsonEditor value={{ key: 'value' }} onChange={mockOnChange} readOnly={true} />)
    const rawJsonButton = screen.getByRole('button', { name: /Raw JSON/i })

    await userEvent.click(rawJsonButton)
    await waitFor(() => {
      const textarea = screen.getByRole('textbox')

      expect(textarea).toHaveAttribute('readonly')
    })
  })
  it('handles objects with numeric values', async () => {
    const value = { count: 100, rate: 0.5 }

    renderWithMocks(<JsonEditor value={value} onChange={mockOnChange} />)
    expect(screen.getByDisplayValue('100')).toBeTruthy()
    expect(screen.getByDisplayValue('0.5')).toBeTruthy()
  })
  it('disables import button in readonly mode', async () => {
    renderWithMocks(<JsonEditor value={{ key: 'value' }} onChange={mockOnChange} readOnly={true} />)
    const rawJsonButton = screen.getByRole('button', { name: /Raw JSON/i })

    await userEvent.click(rawJsonButton)
    await waitFor(() => {
      const importButton = screen.getByRole('button', { name: /Import to Form/i })

      expect(importButton).toBeDisabled()
    })
  })
  it('edits array item values', async () => {
    const value = ['item1', 'item2']

    renderWithMocks(<JsonEditor value={value} onChange={mockOnChange} />)
    const input = screen.getByDisplayValue('item1')

    await userEvent.clear(input)
    await userEvent.type(input, 'updated')
    await waitFor(() => {
      expect(mockOnChange).toHaveBeenCalled()
    })
  })
  it('adds items to array with different types', async () => {
    const value = [{ nested: 'object' }]

    renderWithMocks(<JsonEditor value={value} onChange={mockOnChange} />)
    const addButton = screen.getByRole('button', { name: /Add Item/i })

    await userEvent.click(addButton)
    await waitFor(() => {
      expect(mockOnChange).toHaveBeenCalled()
      const call = mockOnChange.mock.calls[0][0]

      expect(call).toHaveLength(2)
    })
  })
  it('adds items to array with boolean type', async () => {
    const value = [true, false]

    renderWithMocks(<JsonEditor value={value} onChange={mockOnChange} />)
    const addButton = screen.getByRole('button', { name: /Add Item/i })

    await userEvent.click(addButton)
    await waitFor(() => {
      expect(mockOnChange).toHaveBeenCalled()
      const call = mockOnChange.mock.calls[0][0]

      expect(call).toHaveLength(3)
      expect(call[2]).toBe(false)
    })
  })
  it('adds items to array with number type', async () => {
    const value = [1, 2, 3]

    renderWithMocks(<JsonEditor value={value} onChange={mockOnChange} />)
    const addButton = screen.getByRole('button', { name: /Add Item/i })

    await userEvent.click(addButton)
    await waitFor(() => {
      expect(mockOnChange).toHaveBeenCalled()
      const call = mockOnChange.mock.calls[0][0]

      expect(call).toHaveLength(4)
      expect(call[3]).toBe(0)
    })
  })
  it('adds items to array with nested array type', async () => {
    const value = [['nested']]

    renderWithMocks(<JsonEditor value={value} onChange={mockOnChange} />)
    const addButtons = screen.getAllByRole('button', { name: /Add Item/i })

    await userEvent.click(addButtons[0])
    await waitFor(() => {
      expect(mockOnChange).toHaveBeenCalled()
      const call = mockOnChange.mock.calls[0][0]

      expect(call).toHaveLength(2)
      expect(Array.isArray(call[1])).toBe(true)
    })
  })
})
