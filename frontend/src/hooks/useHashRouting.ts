import { useState, useEffect, useCallback } from 'react'

export type ValidTab =
  | 'overview'
  | 'market'
  | 'processes'
  | 'strategies'
  | 'orders'
  | 'positions'
  | 'signals'
  | 'backtests'
  | 'health'
  | 'admin'
  | 'ai-integration'
  | 'ai-reviews'
  | 'settings'
const VALID_TABS: ValidTab[] = [
  'overview',
  'market',
  'processes',
  'strategies',
  'orders',
  'positions',
  'signals',
  'backtests',
  'health',
  'admin',
  'ai-integration',
  'ai-reviews',
  'settings',
]

function useHashRouting<T extends string>(
  validRoutes: readonly T[],
  defaultRoute: T
): [T, (route: T) => void] {
  const getRouteFromHash = useCallback((): T => {
    const hash = globalThis.location.hash.slice(1)
    // Match first segment before "/" so `#backtests/{uuid7}`
    // resolves to the "backtests" tab. Backwards-compatible because no
    // existing VALID_TABS identifier contains a slash.
    const firstSegment = hash.split('/')[0]

    return validRoutes.includes(firstSegment as T) ? (firstSegment as T) : defaultRoute
  }, [validRoutes, defaultRoute])
  const [currentRoute, setCurrentRoute] = useState<T>(getRouteFromHash)

  const navigateToRoute = (route: T) => {
    setCurrentRoute(route)
    globalThis.location.hash = route
  }

  useEffect(() => {
    const handleHashChange = () => {
      setCurrentRoute(getRouteFromHash())
    }

    if (!globalThis.location.hash && defaultRoute) {
      globalThis.location.hash = defaultRoute
    }

    globalThis.addEventListener('hashchange', handleHashChange)

    return () => globalThis.removeEventListener('hashchange', handleHashChange)
  }, [defaultRoute, validRoutes, getRouteFromHash])

  return [currentRoute, navigateToRoute]
}

export function useTabRouting() {
  return useHashRouting(VALID_TABS, 'overview')
}

/**
 * Parse the hash tail after `#<tab>/` into path segments.
 *
 * Returns `[]` when the current hash does not match the requested tab
 * (including when it is just `#<tab>` with no sub-path). Subscribes to
 * `hashchange` independently of `useHashRouting` so a detail view can
 * mount/unmount without coupling to the tab-level navigator.
 *
 * Example:
 *   `#backtests/01948f94-...` + `useHashSubpath("backtests")`
 *   → `["01948f94-..."]`
 */
export function useHashSubpath(tab: string): string[] {
  const compute = useCallback((): string[] => {
    const hash = globalThis.location.hash.slice(1)
    const segments = hash.split('/')

    if (segments[0] !== tab) return []

    return segments.slice(1).filter(Boolean)
  }, [tab])
  const [subpath, setSubpath] = useState<string[]>(compute)

  useEffect(() => {
    const handleHashChange = () => {
      setSubpath(compute())
    }

    globalThis.addEventListener('hashchange', handleHashChange)

    return () => globalThis.removeEventListener('hashchange', handleHashChange)
  }, [compute])

  return subpath
}
