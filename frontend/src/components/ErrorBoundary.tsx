import React, { Component, ReactNode } from 'react'

interface ErrorFallbackProps {
  error: Error
  resetError: () => void
  componentName?: string
}

export function ErrorFallback({
  error,
  resetError,
  componentName,
}: Readonly<ErrorFallbackProps>): ReactNode {
  const title = componentName ? `Error in ${componentName}` : 'Something went wrong'

  return (
    <div className='flex flex-col items-center justify-center p-6 bg-loss-500/10 border border-loss-500/30 rounded-lg min-h-[200px]'>
      <div className='text-loss-400 mb-2'>
        <svg
          className='w-12 h-12'
          fill='none'
          stroke='currentColor'
          viewBox='0 0 24 24'
          aria-hidden='true'
        >
          <path
            strokeLinecap='round'
            strokeLinejoin='round'
            strokeWidth={2}
            d='M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z'
          />
        </svg>
      </div>
      <h3 className='text-lg font-semibold text-loss-400 mb-2'>{title}</h3>
      <p className='text-sm text-muted-400 mb-4 text-center max-w-md'>{error.message}</p>
      {import.meta.env.DEV && (
        <details className='text-xs text-muted-500 mb-4 max-w-full overflow-auto'>
          <summary className='cursor-pointer hover:text-muted-400'>Stack trace</summary>
          <pre className='mt-2 p-2 bg-black/30 rounded text-left whitespace-pre-wrap break-all'>
            {error.stack}
          </pre>
        </details>
      )}
      <button
        onClick={resetError}
        className='px-4 py-2 bg-loss-600 hover:bg-loss-700 text-white rounded-md transition-colors text-sm font-medium'
      >
        Try again
      </button>
    </div>
  )
}

interface Props {
  children: ReactNode
  fallback?: ReactNode | ((props: ErrorFallbackProps) => ReactNode)
  componentName?: string
  onError?: (error: Error, errorInfo: React.ErrorInfo) => void
}
interface State {
  hasError: boolean
  error: Error | null
}

class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props)
    this.state = { hasError: false, error: null }
  }
  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error }
  }
  componentDidCatch(error: Error, errorInfo: React.ErrorInfo): void {
    this.props.onError?.(error, errorInfo)
  }
  resetError = (): void => {
    this.setState({ hasError: false, error: null })
  }
  render(): ReactNode {
    const { hasError, error } = this.state
    const { children, fallback, componentName } = this.props

    if (hasError && error) {
      if (typeof fallback === 'function') {
        return fallback({ error, resetError: this.resetError, componentName })
      }

      if (fallback) {
        return fallback
      }

      return (
        <ErrorFallback error={error} resetError={this.resetError} componentName={componentName} />
      )
    }

    return children
  }
}

export default ErrorBoundary
