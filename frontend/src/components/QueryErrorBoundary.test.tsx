import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { QueryErrorFallback, QueryStateWrapper } from './QueryErrorBoundary'

describe('QueryErrorFallback', () => {
  it('renders error message', () => {
    const error = new Error('Failed to load data')
    const refetch = vi.fn()

    render(<QueryErrorFallback error={error} refetch={refetch} />)
    expect(screen.getByRole('heading', { level: 3 })).toHaveTextContent('Failed to load data')
    expect(screen.getByText('Failed to load data', { selector: 'p' })).toBeInTheDocument()
  })
  it('renders component name when provided', () => {
    const error = new Error('Error')
    const refetch = vi.fn()

    render(<QueryErrorFallback error={error} refetch={refetch} componentName='Users' />)
    expect(screen.getByText('Failed to load Users')).toBeInTheDocument()
  })
  it('shows network error message for fetch errors', () => {
    const error = new Error('Failed to fetch')
    const refetch = vi.fn()

    render(<QueryErrorFallback error={error} refetch={refetch} />)
    expect(
      screen.getByText(
        'Unable to connect to the server. Please check your connection and try again.'
      )
    ).toBeInTheDocument()
  })
  it('shows auth error message for 401 errors', () => {
    const error = new Error('401 Unauthorized')
    const refetch = vi.fn()

    render(<QueryErrorFallback error={error} refetch={refetch} />)
    expect(screen.getByText('Your session has expired. Please log in again.')).toBeInTheDocument()
  })
  it('shows not found message for 404 errors', () => {
    const error = new Error('404 Not found')
    const refetch = vi.fn()

    render(<QueryErrorFallback error={error} refetch={refetch} />)
    expect(screen.getByText('The requested resource was not found.')).toBeInTheDocument()
  })
  it('uses default message when error message is empty', () => {
    const error = { message: '', name: 'EmptyMessageError' } as Error
    const refetch = vi.fn()

    render(<QueryErrorFallback error={error} refetch={refetch} />)
    expect(screen.getByText('An unexpected error occurred while loading data.')).toBeInTheDocument()
  })
  it('calls refetch when retry button is clicked', () => {
    const error = new Error('Error')
    const refetch = vi.fn()

    render(<QueryErrorFallback error={error} refetch={refetch} />)
    fireEvent.click(screen.getByText('Retry'))
    expect(refetch).toHaveBeenCalledTimes(1)
  })
})
describe('QueryStateWrapper', () => {
  it('renders loading fallback when isLoading is true', () => {
    const { container } = render(
      <QueryStateWrapper
        isLoading={true}
        isError={false}
        error={null}
        data={undefined}
        refetch={vi.fn()}
      >
        {() => <div>Data loaded</div>}
      </QueryStateWrapper>
    )

    expect(container.querySelector('.animate-spin')).toBeInTheDocument()
  })
  it('renders custom loading fallback', () => {
    render(
      <QueryStateWrapper
        isLoading={true}
        isError={false}
        error={null}
        data={undefined}
        refetch={vi.fn()}
        loadingFallback={<div>Custom loading...</div>}
      >
        {() => <div>Data loaded</div>}
      </QueryStateWrapper>
    )
    expect(screen.getByText('Custom loading...')).toBeInTheDocument()
  })
  it('renders error fallback when isError is true', () => {
    const error = new Error('Load failed')

    render(
      <QueryStateWrapper
        isLoading={false}
        isError={true}
        error={error}
        data={undefined}
        refetch={vi.fn()}
        componentName='Items'
      >
        {() => <div>Data loaded</div>}
      </QueryStateWrapper>
    )
    expect(screen.getByText('Failed to load Items')).toBeInTheDocument()
    expect(screen.getByText('Load failed')).toBeInTheDocument()
  })
  it('renders empty message when data is undefined', () => {
    render(
      <QueryStateWrapper
        isLoading={false}
        isError={false}
        error={null}
        data={undefined}
        refetch={vi.fn()}
        emptyMessage='No items found'
      >
        {() => <div>Data loaded</div>}
      </QueryStateWrapper>
    )
    expect(screen.getByText('No items found')).toBeInTheDocument()
  })
  it('renders empty message when isEmpty returns true', () => {
    render(
      <QueryStateWrapper
        isLoading={false}
        isError={false}
        error={null}
        data={[]}
        refetch={vi.fn()}
        emptyMessage='List is empty'
        isEmpty={(data: unknown[]) => data.length === 0}
      >
        {() => <div>Data loaded</div>}
      </QueryStateWrapper>
    )
    expect(screen.getByText('List is empty')).toBeInTheDocument()
  })
  it('renders default empty message when isEmpty returns true without emptyMessage', () => {
    render(
      <QueryStateWrapper
        isLoading={false}
        isError={false}
        error={null}
        data={[1]}
        refetch={vi.fn()}
        isEmpty={(data: number[]) => data.length > 0}
      >
        {() => <div>Data loaded</div>}
      </QueryStateWrapper>
    )
    expect(screen.getByText('No data available')).toBeInTheDocument()
  })
  it('renders children with data when loaded successfully', () => {
    const data = { items: ['a', 'b', 'c'] }

    render(
      <QueryStateWrapper
        isLoading={false}
        isError={false}
        error={null}
        data={data}
        refetch={vi.fn()}
      >
        {(d: { items: string[] }) => <div>Items: {d.items.join(', ')}</div>}
      </QueryStateWrapper>
    )
    expect(screen.getByText('Items: a, b, c')).toBeInTheDocument()
  })
  it('renders default empty message when emptyMessage not provided', () => {
    render(
      <QueryStateWrapper
        isLoading={false}
        isError={false}
        error={null}
        data={null}
        refetch={vi.fn()}
      >
        {() => <div>Data loaded</div>}
      </QueryStateWrapper>
    )
    expect(screen.getByText('No data available')).toBeInTheDocument()
  })
})
