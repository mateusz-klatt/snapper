import { QueryClient } from '@tanstack/react-query'
import { calculateRetryDelay } from './queryRetry'

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 1000 * 5,
      retry: 3,
      retryDelay: calculateRetryDelay,
    },
  },
})
