import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Admin } from './Admin'

vi.mock('./UserManagement/UserManagement', () => ({
  default: () => <div data-testid='user-management'>User Management</div>,
}))
describe('Admin', () => {
  it('renders admin page header', () => {
    render(<Admin />)
    expect(screen.getByText('Administration')).toBeInTheDocument()
    expect(screen.getByText(/Manage users and system configuration/i)).toBeInTheDocument()
  })
  it('renders user management component', () => {
    render(<Admin />)
    expect(screen.getByTestId('user-management')).toBeInTheDocument()
  })
  it('applies correct styling classes', () => {
    const { container } = render(<Admin />)
    const mainDiv = container.firstChild as HTMLElement

    expect(mainDiv).toHaveClass('space-y-6')
  })
  it('has proper heading hierarchy', () => {
    render(<Admin />)
    const heading = screen.getByRole('heading', { level: 1 })

    expect(heading).toHaveTextContent('Administration')
  })
})
