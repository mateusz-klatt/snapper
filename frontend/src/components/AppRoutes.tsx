import React from 'react'
import ErrorBoundary from './ErrorBoundary'
import ProtectedRoute from './auth/ProtectedRoute'
import { Overview } from '../features/overview/Overview'
import { MarketData } from '../features/market/MarketData'
import { Processes } from '../features/processes/Processes'
import { Strategies } from '../features/strategies/Strategies'
import { Orders } from '../features/orders/Orders'
import { Positions } from '../features/positions/Positions'
import { Signals } from '../features/signals/Signals'
import { Backtests } from '../features/backtests/Backtests'
import { BacktestDetailPage } from '../features/backtests/BacktestDetailPage'
import { ComparePage } from '../features/backtests/ComparePage'
import { useHashSubpath } from '../hooks/useHashRouting'
import { Health } from '../features/health/Health'
import { Admin } from '../features/admin/Admin'
import { AIIntegration } from '../features/ai-integration/AIIntegration'
import { AiReviewInbox } from '../features/ai-reviews/AiReviewInbox'
import { Settings } from '../features/settings/Settings'

interface AppRoutesProps {
  activeTab: string
}

/**
 * Switches between the list view and the detail view based on
 * the hash sub-path (`#backtests/{run_public_id}`).
 */
function BacktestsRouter(): React.ReactElement {
  const subpath = useHashSubpath('backtests')

  if (subpath[0] === 'compare') {
    if (subpath.length === 2) {
      return <ComparePage comparisonPublicId={subpath[1]} />
    }

    return <Backtests />
  }

  if (subpath.length === 1) {
    return <BacktestDetailPage runPublicId={subpath[0]} />
  }

  return <Backtests />
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
    case 'positions':
      return (
        <ErrorBoundary componentName='Positions'>
          <ProtectedRoute resource='positions'>
            <Positions />
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
    case 'backtests':
      return (
        <ErrorBoundary componentName='Backtests'>
          <ProtectedRoute resource='backtests'>
            <BacktestsRouter />
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
    case 'ai-integration':
      return (
        <ErrorBoundary componentName='AI Integration'>
          <ProtectedRoute resource='ai-integration'>
            <AIIntegration />
          </ProtectedRoute>
        </ErrorBoundary>
      )
    case 'ai-reviews':
      return (
        <ErrorBoundary componentName='AI Reviews'>
          <ProtectedRoute resource='ai-reviews'>
            <AiReviewInbox />
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
