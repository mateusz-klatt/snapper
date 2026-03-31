import { describe, it, expect, beforeEach, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { TimeTravelPicker } from './TimeTravelPicker'
import { useAppStore } from '../stores/app'
import { apiClient } from '../lib/apiClient'

vi.mock('../lib/apiClient', () => ({
  apiClient: {
    setTimeTravelAsOf: vi.fn(),
  },
}))

describe('TimeTravelPicker', () => {
  beforeEach(() => {
    useAppStore.setState({ asOf: null, isTimeTraveling: false })
    vi.clearAllMocks()
  })

  it('renders datetime-local input', () => {
    render(<TimeTravelPicker />)
    expect(screen.getByTitle(/time travel/i)).toBeInTheDocument()
  })

  it('does not show clear button in live mode', () => {
    render(<TimeTravelPicker />)
    expect(screen.queryByLabelText(/exit time travel/i)).not.toBeInTheDocument()
  })

  it('shows clear button when time-traveling', () => {
    useAppStore.setState({ asOf: '2026-03-15T10:00:00Z', isTimeTraveling: true })
    render(<TimeTravelPicker />)
    expect(screen.getByLabelText(/exit time travel/i)).toBeInTheDocument()
  })

  it('sets asOf when date is selected', () => {
    render(<TimeTravelPicker />)
    const input = screen.getByTitle(/time travel/i)

    fireEvent.change(input, { target: { value: '2026-03-15T10:00' } })

    const state = useAppStore.getState()

    expect(state.isTimeTraveling).toBe(true)
    expect(state.asOf).toBeTruthy()
  })

  it('clears asOf when input is emptied', () => {
    useAppStore.setState({ asOf: '2026-03-15T10:00:00Z', isTimeTraveling: true })
    render(<TimeTravelPicker />)
    const input = screen.getByTitle(/time travel/i)

    fireEvent.change(input, { target: { value: '' } })

    expect(useAppStore.getState().isTimeTraveling).toBe(false)
    expect(useAppStore.getState().asOf).toBeNull()
  })

  it('clears asOf when clear button is clicked', () => {
    useAppStore.setState({ asOf: '2026-03-15T10:00:00Z', isTimeTraveling: true })
    render(<TimeTravelPicker />)
    fireEvent.click(screen.getByLabelText(/exit time travel/i))

    const state = useAppStore.getState()

    expect(state.isTimeTraveling).toBe(false)
    expect(state.asOf).toBeNull()
  })

  it('syncs asOf to apiClient synchronously via store action', () => {
    useAppStore.getState().setAsOf('2026-03-15T10:00:00Z')

    expect(apiClient.setTimeTravelAsOf).toHaveBeenCalledWith('2026-03-15T10:00:00Z')
  })

  it('syncs null to apiClient on clearAsOf', () => {
    useAppStore.getState().setAsOf('2026-03-15T10:00:00Z')
    vi.clearAllMocks()
    useAppStore.getState().clearAsOf()

    expect(apiClient.setTimeTravelAsOf).toHaveBeenCalledWith(null)
  })

  it('displays local time in input matching selected asOf', () => {
    useAppStore.setState({ asOf: '2026-03-15T10:00:00Z', isTimeTraveling: true })
    render(<TimeTravelPicker />)
    const input = screen.getByTitle(/time travel/i) as HTMLInputElement

    expect(input.value).toBeTruthy()

    const localDate = new Date('2026-03-15T10:00:00Z')
    const expected = `${localDate.getFullYear()}-${String(localDate.getMonth() + 1).padStart(2, '0')}-${String(localDate.getDate()).padStart(2, '0')}T${String(localDate.getHours()).padStart(2, '0')}:${String(localDate.getMinutes()).padStart(2, '0')}`

    expect(input.value).toBe(expected)
  })
})
