import React from 'react'
import ReactDOM from 'react-dom/client'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { Toaster } from 'react-hot-toast'
import AppWithAuth from './AppWithAuth'
import { calculateRetryDelay } from './lib/queryRetry'
import './index.css'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 1000 * 5,
      retry: 3,
      retryDelay: calculateRetryDelay,
    },
  },
})
const rootElement = document.getElementById('root')

if (!rootElement) throw new Error('Root element not found')
ReactDOM.createRoot(rootElement).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <AppWithAuth />
      <Toaster
        position='top-right'
        toastOptions={{
          duration: 4000,
          style: {
            background: '#1e293b',
            color: '#ffffff',
            border: '1px solid #475569',
          },
          success: {
            style: {
              border: '1px solid #10b981',
            },
          },
          error: {
            style: {
              border: '1px solid #ef4444',
            },
          },
        }}
      />
    </QueryClientProvider>
  </React.StrictMode>
)
