import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { StatusBadge, Card, Button, Badge, LoadingSpinner, ConnectionBar, MetricCard } from './ui'
import userEvent from '@testing-library/user-event'

describe('StatusBadge', () => {
  it('renders connected status', () => {
    render(<StatusBadge status='connected'>Connected</StatusBadge>)
    const badge = screen.getByText('Connected')

    expect(badge).toHaveClass('bg-green-100', 'text-green-800')
  })
  it('renders disconnected status', () => {
    render(<StatusBadge status='disconnected'>Disconnected</StatusBadge>)
    const badge = screen.getByText('Disconnected')

    expect(badge).toHaveClass('bg-red-100', 'text-red-800')
  })
  it('renders pending status', () => {
    render(<StatusBadge status='pending'>Pending</StatusBadge>)
    const badge = screen.getByText('Pending')

    expect(badge).toHaveClass('bg-yellow-100', 'text-yellow-800')
  })
  it('renders healthy status', () => {
    render(<StatusBadge status='healthy'>Healthy</StatusBadge>)
    const badge = screen.getByText('Healthy')

    expect(badge).toHaveClass('bg-green-100', 'text-green-800')
  })
  it('renders stale status', () => {
    render(<StatusBadge status='stale'>Stale</StatusBadge>)
    const badge = screen.getByText('Stale')

    expect(badge).toHaveClass('bg-gray-100', 'text-gray-800')
  })
  it('renders error status', () => {
    render(<StatusBadge status='error'>Error</StatusBadge>)
    const badge = screen.getByText('Error')

    expect(badge).toHaveClass('bg-red-100', 'text-red-800')
  })
  it('applies custom className', () => {
    render(
      <StatusBadge status='connected' className='custom-class'>
        Test
      </StatusBadge>
    )
    expect(screen.getByText('Test')).toHaveClass('custom-class')
  })
})
describe('Card', () => {
  it('renders title and children', () => {
    render(
      <Card title='Test Card'>
        <div>Card Content</div>
      </Card>
    )
    expect(screen.getByText('Test Card')).toBeInTheDocument()
    expect(screen.getByText('Card Content')).toBeInTheDocument()
  })
  it('renders actions when provided', () => {
    render(
      <Card title='Test Card' actions={<button>Action</button>}>
        <div>Content</div>
      </Card>
    )
    expect(screen.getByText('Action')).toBeInTheDocument()
  })
  it('applies custom className', () => {
    const { container } = render(
      <Card title='Test' className='custom-card'>
        <div>Content</div>
      </Card>
    )

    expect(container.firstChild).toHaveClass('custom-card')
  })
})
describe('Button', () => {
  it('renders primary variant by default', () => {
    render(<Button>Click me</Button>)
    expect(screen.getByText('Click me')).toHaveClass('btn-primary')
  })
  it('renders secondary variant', () => {
    render(<Button variant='secondary'>Click me</Button>)
    expect(screen.getByText('Click me')).toHaveClass('btn-secondary')
  })
  it('renders danger variant', () => {
    render(<Button variant='danger'>Click me</Button>)
    expect(screen.getByText('Click me')).toHaveClass('btn-danger')
  })
  it('renders small size', () => {
    render(<Button size='sm'>Small</Button>)
    expect(screen.getByText('Small')).toHaveClass('btn-sm')
  })
  it('renders large size', () => {
    render(<Button size='lg'>Large</Button>)
    const button = screen.getByText('Large')

    expect(button).toHaveClass('px-6', 'py-3', 'text-lg')
  })
  it('shows loading state', () => {
    render(<Button loading>Submit</Button>)
    expect(screen.getByText('Loading...')).toBeInTheDocument()
    expect(screen.queryByText('Submit')).not.toBeInTheDocument()
  })
  it('disables button when loading', () => {
    render(<Button loading>Submit</Button>)
    expect(screen.getByRole('button')).toBeDisabled()
  })
  it('disables button when disabled prop is true', () => {
    render(<Button disabled>Submit</Button>)
    expect(screen.getByRole('button')).toBeDisabled()
  })
  it('calls onClick handler', async () => {
    const user = userEvent.setup()
    const handleClick = vi.fn() as () => void

    render(<Button onClick={handleClick}>Click</Button>)
    await user.click(screen.getByText('Click'))
    expect(handleClick).toHaveBeenCalledTimes(1)
  })
  it('applies custom className', () => {
    render(<Button className='custom-btn'>Button</Button>)
    expect(screen.getByText('Button')).toHaveClass('custom-btn')
  })
})
describe('Badge', () => {
  it('renders default variant', () => {
    render(<Badge>Default</Badge>)
    expect(screen.getByText('Default')).toHaveClass('bg-primary-100', 'text-primary-800')
  })
  it('renders secondary variant', () => {
    render(<Badge variant='secondary'>Secondary</Badge>)
    expect(screen.getByText('Secondary')).toHaveClass('bg-gray-100', 'text-gray-800')
  })
  it('renders outline variant', () => {
    render(<Badge variant='outline'>Outline</Badge>)
    expect(screen.getByText('Outline')).toHaveClass('border', 'border-gray-300')
  })
  it('renders destructive variant', () => {
    render(<Badge variant='destructive'>Error</Badge>)
    expect(screen.getByText('Error')).toHaveClass('bg-red-100', 'text-red-800')
  })
  it('applies custom className', () => {
    render(<Badge className='custom-badge'>Badge</Badge>)
    expect(screen.getByText('Badge')).toHaveClass('custom-badge')
  })
})
describe('LoadingSpinner', () => {
  it('renders with default medium size', () => {
    const { container } = render(<LoadingSpinner />)
    const spinner = container.firstChild as HTMLElement

    expect(spinner).toHaveClass('w-6', 'h-6', 'animate-spin')
  })
  it('renders small size', () => {
    const { container } = render(<LoadingSpinner size='sm' />)
    const spinner = container.firstChild as HTMLElement

    expect(spinner).toHaveClass('w-4', 'h-4')
  })
  it('renders large size', () => {
    const { container } = render(<LoadingSpinner size='lg' />)
    const spinner = container.firstChild as HTMLElement

    expect(spinner).toHaveClass('w-8', 'h-8')
  })
  it('applies custom className', () => {
    const { container } = render(<LoadingSpinner className='custom-spinner' />)

    expect(container.firstChild).toHaveClass('custom-spinner')
  })
})
describe('ConnectionBar', () => {
  it('shows connected status', () => {
    render(<ConnectionBar isConnected={true} lag={10} subscribedTopicsCount={5} />)
    expect(screen.getByText(/connected/i)).toBeInTheDocument()
  })
  it('shows disconnected status', () => {
    render(<ConnectionBar isConnected={false} lag={0} subscribedTopicsCount={0} />)
    expect(screen.getByText(/disconnected/i)).toBeInTheDocument()
  })
  it('displays lag', () => {
    render(<ConnectionBar isConnected={true} lag={25} subscribedTopicsCount={5} />)
    expect(screen.getByText(/25ms/i)).toBeInTheDocument()
  })
  it('displays unknown lag for negative values', () => {
    render(<ConnectionBar isConnected={true} lag={-1} subscribedTopicsCount={5} />)
    expect(screen.getByText(/Unknown/i)).toBeInTheDocument()
  })
  it('displays subscribed topics count', () => {
    render(<ConnectionBar isConnected={true} lag={10} subscribedTopicsCount={8} />)
    expect(screen.getByText(/Topics: 8/i)).toBeInTheDocument()
  })
})
describe('MetricCard', () => {
  it('renders label and value', () => {
    render(<MetricCard label='Total Trades' value={150} />)
    expect(screen.getByText('Total Trades')).toBeInTheDocument()
    expect(screen.getByText('150')).toBeInTheDocument()
  })
  it('renders with suffix', () => {
    render(<MetricCard label='Price' value={1250} suffix='USD' />)
    expect(screen.getByText('USD')).toBeInTheDocument()
  })
  it('shows positive change', () => {
    render(<MetricCard label='Equity' value={10000} change={5.5} changeType='positive' />)
    expect(screen.getByText('+5.50%')).toBeInTheDocument()
    expect(screen.getByText('+5.50%')).toHaveClass('text-green-400')
  })
  it('shows negative change', () => {
    render(<MetricCard label='Equity' value={9500} change={-3.2} changeType='negative' />)
    expect(screen.getByText('-3.20%')).toBeInTheDocument()
    expect(screen.getByText('-3.20%')).toHaveClass('text-red-400')
  })
  it('shows neutral change', () => {
    render(<MetricCard label='Equity' value={10000} change={0} changeType='neutral' />)
    expect(screen.getByText('0.00%')).toBeInTheDocument()
    expect(screen.getByText('0.00%')).toHaveClass('text-dark-300')
  })
  it('handles string values', () => {
    render(<MetricCard label='Status' value='Active' />)
    expect(screen.getByText('Active')).toBeInTheDocument()
  })
})
