import { describe, it, expect } from 'vitest'
import { wsClient } from './instance'

describe('wsClient instance', () => {
  it('exports a WebSocketClient instance', () => {
    expect(wsClient).toBeDefined()
    expect(typeof wsClient.connect).toBe('function')
    expect(typeof wsClient.disconnect).toBe('function')
  })
})
