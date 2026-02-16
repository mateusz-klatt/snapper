import type { ReactNode } from 'react'

interface QueryErrorFallbackProps {
  error: Error
  refetch: () => void
  componentName?: string
}

export function QueryErrorFallback({
  error,
  refetch,
  componentName,
}: Readonly<QueryErrorFallbackProps>): ReactNode {
  const title = componentName ? `Failed to load ${componentName}` : 'Failed to load data'
  const isNetworkError =
    error.message.includes('Network') ||
    error.message.includes('fetch') ||
    error.message.includes('Failed to fetch')
  const isAuthError = error.message.includes('401') || error.message.includes('Unauthorized')
  const isNotFound = error.message.includes('404') || error.message.includes('Not found')

  const getMessage = (): string => {
    if (isNetworkError) {
      return 'Unable to connect to the server. Please check your connection and try again.'
    }

    if (isAuthError) {
      return 'Your session has expired. Please log in again.'
    }

    if (isNotFound) {
      return 'The requested resource was not found.'
    }

    return error.message || 'An unexpected error occurred while loading data.'
  }

  return (
    <div className='flex flex-col items-center justify-center p-6 bg-warning-500/10 border border-warning-500/30 rounded-lg min-h-[150px]'>
      <div className='text-warning-400 mb-2'>
        <svg
          className='w-10 h-10'
          fill='none'
          stroke='currentColor'
          viewBox='0 0 24 24'
          aria-hidden='true'
        >
          <path
            strokeLinecap='round'
            strokeLinejoin='round'
            strokeWidth={2}
            d='M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z'
          />
        </svg>
      </div>
      <h3 className='text-base font-semibold text-warning-400 mb-1'>{title}</h3>
      <p className='text-sm text-muted-400 mb-4 text-center max-w-md'>{getMessage()}</p>
      <button
        onClick={() => refetch()}
        className='px-4 py-2 bg-warning-600 hover:bg-warning-700 text-white rounded-md transition-colors text-sm font-medium'
      >
        Retry
      </button>
    </div>
  )
}

interface QueryStateWrapperProps<T> {
  isLoading: boolean
  isError: boolean
  error: Error | null
  data: T | undefined
  refetch: () => void
  children: (data: T) => ReactNode
  loadingFallback?: ReactNode
  componentName?: string
  emptyMessage?: string
  isEmpty?: (data: T) => boolean
}

export function QueryStateWrapper<T>({
  isLoading,
  isError,
  error,
  data,
  refetch,
  children,
  loadingFallback,
  componentName,
  emptyMessage,
  isEmpty,
}: Readonly<QueryStateWrapperProps<T>>): ReactNode {
  if (isLoading) {
    return (
      loadingFallback || (
        <div className='flex items-center justify-center p-6 min-h-[150px]'>
          <div className='animate-spin rounded-full h-8 w-8 border-b-2 border-brand-500' />
        </div>
      )
    )
  }

  if (isError && error) {
    return <QueryErrorFallback error={error} refetch={refetch} componentName={componentName} />
  }

  if (data === undefined || data === null) {
    return (
      <div className='flex items-center justify-center p-6 text-muted-500 min-h-[150px]'>
        {emptyMessage || 'No data available'}
      </div>
    )
  }

  if (isEmpty?.(data)) {
    return (
      <div className='flex items-center justify-center p-6 text-muted-500 min-h-[150px]'>
        {emptyMessage || 'No data available'}
      </div>
    )
  }

  return children(data)
}
