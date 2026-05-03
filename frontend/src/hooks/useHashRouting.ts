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
    // Match first segment before "/" so `#backtests/{uuid7}` resolves to
    // the "backtests" tab. Strip "?query" before route matching so that
    // `#backtests?wallet=X` (scope-persistence params) also resolves.
    const firstSegment = hash.split('/')[0].split('?')[0]

    return validRoutes.includes(firstSegment as T) ? (firstSegment as T) : defaultRoute
  }, [validRoutes, defaultRoute])
  const [currentRoute, setCurrentRoute] = useState<T>(getRouteFromHash)

  const navigateToRoute = (route: T) => {
    setCurrentRoute(route)
    const queryIdx = globalThis.location.hash.indexOf('?')
    const queryString = queryIdx === -1 ? '' : globalThis.location.hash.slice(queryIdx)

    globalThis.location.hash = `${route}${queryString}`
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
 * The `?query` portion of the hash (used by `useScopePersistence` for
 * `?wallet=...&operator=...`) is stripped before segmentation — without
 * that strip, a hash like `#backtests/<uuid>?wallet=X` would yield
 * `["<uuid>?wallet=X"]` and contaminate any consumer that expects pure
 * subpath tokens (e.g. a backtest-detail route looking up by uuid).
 *
 * Example:
 *   `#backtests/01948f94-...` + `useHashSubpath("backtests")`
 *   → `["01948f94-..."]`
 *   `#backtests/01948f94-...?wallet=w-1` + `useHashSubpath("backtests")`
 *   → `["01948f94-..."]` (query stripped)
 */
export function useHashSubpath(tab: string): string[] {
  const compute = useCallback((): string[] => {
    const fullHash = globalThis.location.hash.slice(1)
    const queryIdx = fullHash.indexOf('?')
    const hash = queryIdx === -1 ? fullHash : fullHash.slice(0, queryIdx)
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
