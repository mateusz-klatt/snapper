import { useQuery } from '@tanstack/react-query'
import { apiClient } from '../../../lib/apiClient'
import { useAppStore } from '../../../stores/app'
import type { BacktestRunData } from '../../../types/api'

interface Options {
  enabled: boolean
}

const TERMINAL_STATUSES = ['completed', 'failed', 'cancelled'] as const

/**
 * "Show all runs" toggle data source for CompareLauncher.
 *
 * The list endpoint accepts only a single `status` value, so the
 * three terminal slices are fetched in parallel via Promise.all
 * inside one queryFn (single cache entry, single subscription, no
 * three-key explosion). The merged list is sorted by ``timestamp DESC``
 * client-side. Wallet-scoped via the cache key so the wallet-picker
 * change invalidates correctly.
 */
export function useAllTerminalRuns({ enabled }: Options) {
  const walletId = useAppStore(s => s.currentWalletPublicId)

  return useQuery({
    queryKey: ['backtests', 'all-terminal', walletId],
    queryFn: async (): Promise<BacktestRunData[]> => {
      const responses = await Promise.all(
        TERMINAL_STATUSES.map(status => apiClient.getBacktests(50, 0, undefined, status))
      )
      const merged: BacktestRunData[] = responses.flatMap(r => r.payload ?? [])

      return merged.sort(
        (a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime()
      )
    },
    enabled,
  })
}
