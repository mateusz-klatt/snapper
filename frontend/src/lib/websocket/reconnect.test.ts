import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import {
  calculateReconnectDelay,
  shouldReconnect,
  createHeartbeatMessage,
  flushThrottledMessages,
  buildWebSocketUrl,
} from './reconnect'

describe('reconnect utilities', () => {
  describe('calculateReconnectDelay', () => {
    it('returns base interval for first attempt', () => {
      expect(calculateReconnectDelay(1, 1000)).toBe(1000)
    })
    it('applies exponential backoff', () => {
      expect(calculateReconnectDelay(2, 1000)).toBe(1500)
      expect(calculateReconnectDelay(3, 1000)).toBe(2250)
    })
    it('caps delay at maxDelay', () => {
      expect(calculateReconnectDelay(10, 1000, 5000)).toBe(5000)
    })
    it('uses default maxDelay of 30000', () => {
      expect(calculateReconnectDelay(20, 1000)).toBe(30000)
    })
  })
  describe('shouldReconnect', () => {
    it('returns true when not reconnecting and under max attempts', () => {
      expect(shouldReconnect(false, 0, 10)).toBe(true)
      expect(shouldReconnect(false, 5, 10)).toBe(true)
    })
    it('returns false when already reconnecting', () => {
      expect(shouldReconnect(true, 0, 10)).toBe(false)
    })
    it('returns false when at max attempts', () => {
      expect(shouldReconnect(false, 10, 10)).toBe(false)
    })
    it('returns false when over max attempts', () => {
      expect(shouldReconnect(false, 15, 10)).toBe(false)
    })
  })
  describe('createHeartbeatMessage', () => {
    it('returns ping message with correct type', () => {
      const message = createHeartbeatMessage()

      expect(message.type).toBe('ping')
    })
  })
  describe('flushThrottledMessages', () => {
    beforeEach(() => {
      vi.useFakeTimers()
    })
    it('delivers messages that exceed throttle interval', () => {
      const pendingMessages = new Map([['topic1', { data: 'test1' }]])
      const lastMessageTime = new Map([['topic1', Date.now() - 300]])
      const onDeliver = vi.fn()

      flushThrottledMessages(pendingMessages, lastMessageTime, 200, onDeliver)
      expect(onDeliver).toHaveBeenCalledWith({ data: 'test1' })
      expect(pendingMessages.size).toBe(0)
    })
    it('does not deliver messages within throttle interval', () => {
      const pendingMessages = new Map([['topic1', { data: 'test1' }]])
      const lastMessageTime = new Map([['topic1', Date.now() - 100]])
      const onDeliver = vi.fn()

      flushThrottledMessages(pendingMessages, lastMessageTime, 200, onDeliver)
      expect(onDeliver).not.toHaveBeenCalled()
      expect(pendingMessages.size).toBe(1)
    })
    it('handles topics without previous message time', () => {
      const pendingMessages = new Map([['topic1', { data: 'test1' }]])
      const lastMessageTime = new Map<string, number>()
      const onDeliver = vi.fn()

      flushThrottledMessages(pendingMessages, lastMessageTime, 200, onDeliver)
      expect(onDeliver).toHaveBeenCalledWith({ data: 'test1' })
    })
    it('updates lastMessageTime after delivery', () => {
      const now = Date.now()

      vi.setSystemTime(now)
      const pendingMessages = new Map([['topic1', { data: 'test1' }]])
      const lastMessageTime = new Map<string, number>()
      const onDeliver = vi.fn()

      flushThrottledMessages(pendingMessages, lastMessageTime, 200, onDeliver)
      expect(lastMessageTime.get('topic1')).toBe(now)
    })
    it('processes multiple topics', () => {
      const pendingMessages = new Map([
        ['topic1', { data: 'test1' }],
        ['topic2', { data: 'test2' }],
      ])
      const lastMessageTime = new Map<string, number>()
      const onDeliver = vi.fn()

      flushThrottledMessages(pendingMessages, lastMessageTime, 200, onDeliver)
      expect(onDeliver).toHaveBeenCalledTimes(2)
    })
  })
  describe('buildWebSocketUrl', () => {
    const originalLocation = globalThis.location

    beforeEach(() => {
      Object.defineProperty(globalThis, 'location', {
        value: {
          protocol: 'https:',
          host: 'example.com',
        },
        writable: true,
      })
    })
    afterEach(() => {
      Object.defineProperty(globalThis, 'location', {
        value: originalLocation,
        writable: true,
      })
    })
    it('builds wss URL for https', () => {
      Object.defineProperty(globalThis, 'location', {
        value: { protocol: 'https:', host: 'example.com' },
        writable: true,
      })
      expect(buildWebSocketUrl()).toBe('wss://example.com/api/ws')
    })
    it('builds ws URL for http', () => {
      Object.defineProperty(globalThis, 'location', {
        value: { protocol: 'http:', host: 'localhost:3000' },
        writable: true,
      })
      expect(buildWebSocketUrl()).toBe('ws://localhost:3000/api/ws')
    })
    it('uses custom endpoint', () => {
      Object.defineProperty(globalThis, 'location', {
        value: { protocol: 'https:', host: 'example.com' },
        writable: true,
      })
      expect(buildWebSocketUrl('/custom/ws')).toBe('wss://example.com/custom/ws')
    })
  })
})
