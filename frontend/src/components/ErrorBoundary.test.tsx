import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import ErrorBoundary, { ErrorFallback } from './ErrorBoundary'

const ThrowError = ({ shouldThrow, message }: { shouldThrow: boolean; message?: string }): null => {
  if (shouldThrow) {
    throw new Error(message ?? 'Test error')
  }

  return null
}

describe('ErrorFallback', () => {
  it('renders error message', () => {
    const error = new Error('Something went wrong')
    const resetError = vi.fn()

    render(<ErrorFallback error={error} resetError={resetError} />)
    expect(screen.getByRole('heading', { level: 3 })).toHaveTextContent('Something went wrong')
    expect(screen.getByText('Something went wrong', { selector: 'p' })).toBeInTheDocument()
  })
  it('renders component name when provided', () => {
    const error = new Error('Failed')
    const resetError = vi.fn()

    render(<ErrorFallback error={error} resetError={resetError} componentName='TestComponent' />)
    expect(screen.getByText('Error in TestComponent')).toBeInTheDocument()
  })
  it('calls resetError when try again button is clicked', () => {
    const error = new Error('Test error')
    const resetError = vi.fn()

    render(<ErrorFallback error={error} resetError={resetError} />)
    fireEvent.click(screen.getByText('Try again'))
    expect(resetError).toHaveBeenCalledTimes(1)
  })
  it('shows stack trace in dev mode', () => {
    const error = new Error('Test error')

    error.stack = 'Error: Test error\n    at TestComponent'
    const resetError = vi.fn()

    render(<ErrorFallback error={error} resetError={resetError} />)
    expect(screen.getByText('Stack trace')).toBeInTheDocument()
  })
})
describe('ErrorBoundary', () => {
  beforeEach(() => {
    vi.spyOn(console, 'error').mockImplementation(() => {})
  })
  it('renders children when no error', () => {
    render(
      <ErrorBoundary>
        <div>Child content</div>
      </ErrorBoundary>
    )
    expect(screen.getByText('Child content')).toBeInTheDocument()
  })
  it('renders fallback UI when child throws error', () => {
    render(
      <ErrorBoundary componentName='TestComponent'>
        <ThrowError shouldThrow={true} message='Component crashed' />
      </ErrorBoundary>
    )
    expect(screen.getByText('Error in TestComponent')).toBeInTheDocument()
    expect(screen.getByText('Component crashed')).toBeInTheDocument()
  })
  it('resets error state when try again is clicked', () => {
    let shouldThrow = true

    const TestComponent = () => {
      if (shouldThrow) {
        throw new Error('Test error')
      }

      return <div>Recovered content</div>
    }

    const { rerender } = render(
      <ErrorBoundary componentName='TestComponent' key='test'>
        <TestComponent />
      </ErrorBoundary>
    )

    expect(screen.getByText('Error in TestComponent')).toBeInTheDocument()
    shouldThrow = false
    fireEvent.click(screen.getByText('Try again'))
    rerender(
      <ErrorBoundary componentName='TestComponent' key='test'>
        <TestComponent />
      </ErrorBoundary>
    )
    expect(screen.getByText('Recovered content')).toBeInTheDocument()
  })
  it('calls onError callback when error occurs', () => {
    const onError = vi.fn()

    render(
      <ErrorBoundary onError={onError}>
        <ThrowError shouldThrow={true} message='Test error' />
      </ErrorBoundary>
    )
    expect(onError).toHaveBeenCalledWith(
      expect.objectContaining({ message: 'Test error' }),
      expect.objectContaining({ componentStack: expect.any(String) })
    )
  })
  it('renders custom fallback when provided as ReactNode', () => {
    render(
      <ErrorBoundary fallback={<div>Custom error fallback</div>}>
        <ThrowError shouldThrow={true} />
      </ErrorBoundary>
    )
    expect(screen.getByText('Custom error fallback')).toBeInTheDocument()
  })
  it('renders custom fallback function with error props', () => {
    const customFallback = ({ error, resetError }: { error: Error; resetError: () => void }) => (
      <div>
        <span>Custom: {error.message}</span>
        <button onClick={resetError}>Custom Reset</button>
      </div>
    )

    render(
      <ErrorBoundary fallback={customFallback}>
        <ThrowError shouldThrow={true} message='Custom error message' />
      </ErrorBoundary>
    )
    expect(screen.getByText('Custom: Custom error message')).toBeInTheDocument()
    expect(screen.getByText('Custom Reset')).toBeInTheDocument()
  })
  it('renders default title when componentName is not provided', () => {
    render(
      <ErrorBoundary>
        <ThrowError shouldThrow={true} />
      </ErrorBoundary>
    )
    expect(screen.getByText('Something went wrong')).toBeInTheDocument()
  })
})
