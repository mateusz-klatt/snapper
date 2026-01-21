import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, act } from '@testing-library/react'
import { createRef, type ErrorInfo } from 'react'
import AuthErrorBoundary from './AuthErrorBoundary'

const ThrowError = ({ error }: { error?: Error }) => {
  if (error) throw error

  return <div>No error</div>
}

describe('AuthErrorBoundary', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.spyOn(console, 'error').mockImplementation(() => {})
    delete (window as { authLogoutCallback?: () => void }).authLogoutCallback
  })
  it('renders children when no error', () => {
    render(
      <AuthErrorBoundary>
        <div>Test Content</div>
      </AuthErrorBoundary>
    )
    expect(screen.getByText('Test Content')).toBeInTheDocument()
  })
  it('catches authentication required error', () => {
    const error = new Error('Authentication required')

    render(
      <AuthErrorBoundary>
        <ThrowError error={error} />
      </AuthErrorBoundary>
    )
    expect(screen.queryByText('No error')).not.toBeInTheDocument()
  })
  it('catches access denied error', () => {
    const error = new Error('Access denied')

    render(
      <AuthErrorBoundary>
        <ThrowError error={error} />
      </AuthErrorBoundary>
    )
    expect(screen.queryByText('No error')).not.toBeInTheDocument()
  })
  it('catches 401 error', () => {
    const error = new Error('HTTP 401 Unauthorized')

    render(
      <AuthErrorBoundary>
        <ThrowError error={error} />
      </AuthErrorBoundary>
    )
    expect(screen.queryByText('No error')).not.toBeInTheDocument()
  })
  it('catches 403 error', () => {
    const error = new Error('HTTP 403 Forbidden')

    render(
      <AuthErrorBoundary>
        <ThrowError error={error} />
      </AuthErrorBoundary>
    )
    expect(screen.queryByText('No error')).not.toBeInTheDocument()
  })
  it('does not catch non-auth errors', () => {
    const error = new Error('Something else went wrong')

    expect(() => {
      render(
        <AuthErrorBoundary>
          <ThrowError error={error} />
        </AuthErrorBoundary>
      )
    }).toThrow()
  })
  it('logs error when caught', () => {
    const consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    const error = new Error('Authentication required')

    render(
      <AuthErrorBoundary>
        <ThrowError error={error} />
      </AuthErrorBoundary>
    )
    expect(consoleErrorSpy).toHaveBeenCalledWith(
      'AuthErrorBoundary caught an error:',
      error,
      expect.objectContaining({
        componentStack: expect.any(String),
      })
    )
    consoleErrorSpy.mockRestore()
  })
  it('calls logout callback when auth error occurs', async () => {
    vi.useFakeTimers()
    const logoutCallback = vi.fn()

    ;(window as { authLogoutCallback?: () => void }).authLogoutCallback = logoutCallback
    const error = new Error('Authentication required')

    render(
      <AuthErrorBoundary>
        <ThrowError error={error} />
      </AuthErrorBoundary>
    )
    vi.runAllTimers()
    expect(logoutCallback).toHaveBeenCalledTimes(1)
    vi.useRealTimers()
  })
  it('does not call logout callback if not defined', () => {
    vi.useFakeTimers()
    const error = new Error('Authentication required')

    expect(() => {
      render(
        <AuthErrorBoundary>
          <ThrowError error={error} />
        </AuthErrorBoundary>
      )
    }).not.toThrow()
    vi.runAllTimers()
    vi.useRealTimers()
  })
  it('renders null when auth error is caught', () => {
    const error = new Error('Authentication required')
    const { container } = render(
      <AuthErrorBoundary>
        <ThrowError error={error} />
      </AuthErrorBoundary>
    )

    expect(container.firstChild).toBeNull()
  })
  it('does not log when triggering logout', () => {
    vi.useFakeTimers()
    const consoleLogSpy = vi.spyOn(console, 'log').mockImplementation(() => {})
    const logoutCallback = vi.fn()

    ;(window as { authLogoutCallback?: () => void }).authLogoutCallback = logoutCallback
    const error = new Error('Authentication required')

    render(
      <AuthErrorBoundary>
        <ThrowError error={error} />
      </AuthErrorBoundary>
    )
    expect(consoleLogSpy).not.toHaveBeenCalled()
    consoleLogSpy.mockRestore()
    vi.useRealTimers()
  })
  it('only triggers logout once per error', () => {
    vi.useFakeTimers()
    const logoutCallback = vi.fn()

    ;(window as { authLogoutCallback?: () => void }).authLogoutCallback = logoutCallback
    const error = new Error('Authentication required')
    const { rerender } = render(
      <AuthErrorBoundary>
        <ThrowError error={error} />
      </AuthErrorBoundary>
    )

    vi.runAllTimers()
    rerender(
      <AuthErrorBoundary>
        <ThrowError error={error} />
      </AuthErrorBoundary>
    )
    vi.runAllTimers()
    expect(logoutCallback).toHaveBeenCalledTimes(1)
    vi.useRealTimers()
  })
  it('allows logout to trigger again after error is cleared', () => {
    vi.useFakeTimers()
    const logoutCallback = vi.fn()

    ;(window as { authLogoutCallback?: () => void }).authLogoutCallback = logoutCallback
    const error = new Error('Authentication required')
    const boundaryRef = createRef<AuthErrorBoundary>()
    const { rerender } = render(
      <AuthErrorBoundary ref={boundaryRef}>
        <ThrowError error={error} />
      </AuthErrorBoundary>
    )

    vi.runAllTimers()
    expect(logoutCallback).toHaveBeenCalledTimes(1)
    rerender(
      <AuthErrorBoundary ref={boundaryRef}>
        <ThrowError />
      </AuthErrorBoundary>
    )
    act(() => {
      boundaryRef.current?.setState({ hasError: false, error: null })
    })
    rerender(
      <AuthErrorBoundary ref={boundaryRef}>
        <ThrowError error={error} />
      </AuthErrorBoundary>
    )
    vi.runAllTimers()
    expect(logoutCallback).toHaveBeenCalledTimes(2)
    vi.useRealTimers()
  })
  it('does not trigger logout when error state is false', () => {
    const logoutCallback = vi.fn()

    ;(window as { authLogoutCallback?: () => void }).authLogoutCallback = logoutCallback
    const boundaryRef = createRef<AuthErrorBoundary>()

    render(
      <AuthErrorBoundary ref={boundaryRef}>
        <div>Safe</div>
      </AuthErrorBoundary>
    )
    act(() => {
      boundaryRef.current?.componentDidCatch(new Error('Non-auth error'), {
        componentStack: '',
      } as ErrorInfo)
    })
    expect(logoutCallback).not.toHaveBeenCalled()
  })
})
