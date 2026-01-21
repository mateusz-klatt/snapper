import { useState, useEffect, useCallback } from 'react'

export type ValidTab =
  | 'overview'
  | 'market'
  | 'processes'
  | 'strategies'
  | 'orders'
  | 'signals'
  | 'health'
  | 'admin'
  | 'charts'
  | 'settings'
const VALID_TABS: ValidTab[] = [
  'overview',
  'market',
  'processes',
  'strategies',
  'orders',
  'signals',
  'health',
  'admin',
  'charts',
  'settings',
]

function useHashRouting<T extends string>(
  validRoutes: readonly T[],
  defaultRoute: T
): [T, (route: T) => void] {
  const getRouteFromHash = useCallback((): T => {
    const hash = window.location.hash.slice(1)

    return validRoutes.includes(hash as T) ? (hash as T) : defaultRoute
  }, [validRoutes, defaultRoute])
  const [currentRoute, setCurrentRoute] = useState<T>(getRouteFromHash)

  const navigateToRoute = (route: T) => {
    setCurrentRoute(route)
    window.location.hash = route
  }

  useEffect(() => {
    const handleHashChange = () => {
      setCurrentRoute(getRouteFromHash())
    }

    if (!window.location.hash && defaultRoute) {
      window.location.hash = defaultRoute
    }

    window.addEventListener('hashchange', handleHashChange)

    return () => window.removeEventListener('hashchange', handleHashChange)
  }, [defaultRoute, validRoutes, getRouteFromHash])

  return [currentRoute, navigateToRoute]
}

export function useTabRouting() {
  return useHashRouting(VALID_TABS, 'overview')
}
