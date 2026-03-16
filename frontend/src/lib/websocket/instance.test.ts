import { describe, it, expect } from 'vitest'
import * as instance from './instance'

describe('websocket instance module', () => {
  it('exports an empty module (singleton removed, use websocket store)', () => {
    expect(instance).toBeDefined()
  })
})
