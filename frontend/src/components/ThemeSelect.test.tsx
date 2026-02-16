import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { ThemeSelect } from './ThemeSelect'

const COLOR_OPTIONS = [
  { value: 'red', label: 'Red' },
  { value: 'green', label: 'Green' },
  { value: 'blue', label: 'Blue' },
] as const

describe('ThemeSelect', () => {
  it('renders trigger with selected value', () => {
    render(<ThemeSelect value='red' onChange={() => {}} options={COLOR_OPTIONS} />)
    expect(screen.getByRole('combobox')).toHaveTextContent('Red')
  })

  it('renders placeholder when value is empty', () => {
    render(
      <ThemeSelect
        value=''
        onChange={() => {}}
        options={COLOR_OPTIONS}
        placeholder='Pick a color'
      />
    )
    expect(screen.getByRole('combobox')).toHaveTextContent('Pick a color')
  })

  it('passes id to trigger element', () => {
    render(
      <ThemeSelect id='color-select' value='red' onChange={() => {}} options={COLOR_OPTIONS} />
    )
    expect(screen.getByRole('combobox')).toHaveAttribute('id', 'color-select')
  })

  it('applies custom className to trigger', () => {
    render(
      <ThemeSelect value='red' onChange={() => {}} options={COLOR_OPTIONS} className='max-w-56' />
    )
    expect(screen.getByRole('combobox').className).toContain('max-w-56')
  })

  it('disables trigger when disabled prop is true', () => {
    render(<ThemeSelect value='red' onChange={() => {}} options={COLOR_OPTIONS} disabled />)
    expect(screen.getByRole('combobox')).toBeDisabled()
  })

  it('opens dropdown and shows options on click', async () => {
    const user = userEvent.setup()

    render(<ThemeSelect value='red' onChange={() => {}} options={COLOR_OPTIONS} />)
    await user.click(screen.getByRole('combobox'))
    expect(screen.getByRole('option', { name: 'Red' })).toBeInTheDocument()
    expect(screen.getByRole('option', { name: 'Green' })).toBeInTheDocument()
    expect(screen.getByRole('option', { name: 'Blue' })).toBeInTheDocument()
  })

  it('calls onChange with selected value', async () => {
    const handleChange = vi.fn()
    const user = userEvent.setup()

    render(<ThemeSelect value='red' onChange={handleChange} options={COLOR_OPTIONS} />)
    await user.click(screen.getByRole('combobox'))
    await user.click(screen.getByRole('option', { name: 'Green' }))
    expect(handleChange).toHaveBeenCalledWith('green')
  })

  it('renders with empty options list', () => {
    render(<ThemeSelect value='' onChange={() => {}} options={[]} placeholder='No items' />)
    expect(screen.getByRole('combobox')).toHaveTextContent('No items')
  })
})
