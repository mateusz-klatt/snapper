import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { LiveOnlyNotice } from './LiveOnlyNotice'

vi.mock('../stores/app', () => ({
  useAppStore: vi.fn((selector: (s: Record<string, unknown>) => unknown) =>
    selector({ isTimeTraveling: false })
  ),
}))

import { useAppStore } from '../stores/app'

describe('LiveOnlyNotice', () => {
  it('renders nothing when not time traveling', () => {
    const { container } = render(<LiveOnlyNotice />)

    expect(container.firstChild).toBeNull()
  })
  it('renders info banner when time traveling', () => {
    vi.mocked(useAppStore).mockImplementation(((
      selector: (s: Record<string, unknown>) => unknown
    ) => selector({ isTimeTraveling: true })) as never)
    render(<LiveOnlyNotice />)

    expect(
      screen.getByText('This tab shows current state — historical view is not available.')
    ).toBeInTheDocument()
    vi.mocked(useAppStore).mockImplementation(((
      selector: (s: Record<string, unknown>) => unknown
    ) => selector({ isTimeTraveling: false })) as never)
  })
})
