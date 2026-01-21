import { describe, it, expect } from 'vitest'
import { noop } from './noop'

describe('noop', () => {
  it('is a function that returns undefined', () => {
    expect(typeof noop).toBe('function')
    expect(noop()).toBeUndefined()
  })
})
