import React from 'react'
import ReactDOM from 'react-dom/client'
import { QueryClientProvider } from '@tanstack/react-query'
import { Toaster } from 'react-hot-toast'
import AppWithAuth from './AppWithAuth'
import { queryClient } from './lib/queryClient'
import './index.css'

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
            background: 'var(--color-dark-100)',
            color: 'var(--color-alpine-900)',
            border: '1px solid var(--color-dark-500)',
          },
          success: {
            style: {
              border: '1px solid var(--color-accent-400)',
            },
          },
          error: {
            style: {
              border: '1px solid var(--color-loss-400)',
            },
          },
        }}
      />
    </QueryClientProvider>
  </React.StrictMode>
)
