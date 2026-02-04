import { describe, it, expect } from 'vitest'
import {
  HEARTBEAT_STALE_THRESHOLD_MS,
  HEARTBEAT_PRUNE_INTERVAL_MS,
  SIGNAL_STRENGTH_STRONG,
  SIGNAL_STRENGTH_MEDIUM,
  SIGNAL_STRENGTH_WEAK,
} from './constants'

describe('constants', () => {
  it('defines heartbeat stale threshold as 10 seconds', () => {
    expect(HEARTBEAT_STALE_THRESHOLD_MS).toBe(10_000)
  })

  it('defines heartbeat prune interval as 5 seconds', () => {
    expect(HEARTBEAT_PRUNE_INTERVAL_MS).toBe(5_000)
  })

  it('defines strong signal strength threshold', () => {
    expect(SIGNAL_STRENGTH_STRONG).toBe(0.8)
  })

  it('defines medium signal strength threshold', () => {
    expect(SIGNAL_STRENGTH_MEDIUM).toBe(0.6)
  })

  it('defines weak signal strength threshold', () => {
    expect(SIGNAL_STRENGTH_WEAK).toBe(0.4)
  })
})
