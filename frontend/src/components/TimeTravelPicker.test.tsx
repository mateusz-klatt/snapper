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

  it('clears asOf when clear button is clicked', () => {
    useAppStore.setState({ asOf: '2026-03-15T10:00:00Z', isTimeTraveling: true })
    render(<TimeTravelPicker />)
    fireEvent.click(screen.getByLabelText(/exit time travel/i))

    const state = useAppStore.getState()

    expect(state.isTimeTraveling).toBe(false)
    expect(state.asOf).toBeNull()
  })

  it('syncs asOf to apiClient via useEffect', () => {
    useAppStore.setState({ asOf: '2026-03-15T10:00:00Z', isTimeTraveling: true })
    render(<TimeTravelPicker />)

    expect(apiClient.setTimeTravelAsOf).toHaveBeenCalledWith('2026-03-15T10:00:00Z')
  })

  it('syncs null to apiClient when cleared', () => {
    useAppStore.setState({ asOf: null, isTimeTraveling: false })
    render(<TimeTravelPicker />)

    expect(apiClient.setTimeTravelAsOf).toHaveBeenCalledWith(null)
  })
})
