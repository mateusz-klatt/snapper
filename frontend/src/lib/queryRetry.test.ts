import { describe, it, expect } from 'vitest'
import { calculateRetryDelay } from './queryRetry'

describe('calculateRetryDelay', () => {
  it('returns exponential backoff delay', () => {
    expect(calculateRetryDelay(0)).toBe(1000)
    expect(calculateRetryDelay(1)).toBe(2000)
    expect(calculateRetryDelay(2)).toBe(4000)
    expect(calculateRetryDelay(3)).toBe(8000)
  })
  it('caps delay at 30 seconds', () => {
    expect(calculateRetryDelay(5)).toBe(30000)
    expect(calculateRetryDelay(10)).toBe(30000)
  })
})
