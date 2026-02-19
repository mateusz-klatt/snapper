import React from 'react'
import ErrorBoundary from './ErrorBoundary'
import ProtectedRoute from './auth/ProtectedRoute'
import { Overview } from '../features/overview/Overview'
import { MarketData } from '../features/market/MarketData'
import { Processes } from '../features/processes/Processes'
import { Strategies } from '../features/strategies/Strategies'
import { Orders } from '../features/orders/Orders'
import { Signals } from '../features/signals/Signals'
import { Health } from '../features/health/Health'
import { Admin } from '../features/admin/Admin'
import { Settings } from '../features/settings/Settings'

interface AppRoutesProps {
  activeTab: string
}

export function AppRoutes({ activeTab }: Readonly<AppRoutesProps>): React.ReactElement {
  switch (activeTab) {
    case 'market':
      return (
        <ErrorBoundary componentName='Market Data'>
          <ProtectedRoute resource='market'>
            <MarketData />
          </ProtectedRoute>
        </ErrorBoundary>
      )
    case 'processes':
      return (
        <ErrorBoundary componentName='Processes'>
          <ProtectedRoute resource='processes'>
            <Processes />
          </ProtectedRoute>
        </ErrorBoundary>
      )
    case 'strategies':
      return (
        <ErrorBoundary componentName='Strategies'>
          <ProtectedRoute resource='strategies'>
            <Strategies />
          </ProtectedRoute>
        </ErrorBoundary>
      )
    case 'orders':
      return (
        <ErrorBoundary componentName='Orders'>
          <ProtectedRoute resource='orders'>
            <Orders />
          </ProtectedRoute>
        </ErrorBoundary>
      )
    case 'signals':
      return (
        <ErrorBoundary componentName='Signals'>
          <ProtectedRoute resource='signals'>
            <Signals />
          </ProtectedRoute>
        </ErrorBoundary>
      )
    case 'health':
      return (
        <ErrorBoundary componentName='Health'>
          <ProtectedRoute resource='health'>
            <Health />
          </ProtectedRoute>
        </ErrorBoundary>
      )
    case 'admin':
      return (
        <ErrorBoundary componentName='Administration'>
          <ProtectedRoute resource='admin'>
            <Admin />
          </ProtectedRoute>
        </ErrorBoundary>
      )
    case 'settings':
      return (
        <ErrorBoundary componentName='Settings'>
          <ProtectedRoute resource='settings'>
            <Settings />
          </ProtectedRoute>
        </ErrorBoundary>
      )
    case 'overview':
    default:
      return (
        <ErrorBoundary componentName='Overview'>
          <ProtectedRoute resource='overview'>
            <Overview />
          </ProtectedRoute>
        </ErrorBoundary>
      )
  }
}
