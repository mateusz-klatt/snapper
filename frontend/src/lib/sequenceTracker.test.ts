import { describe, it, expect, beforeEach } from 'vitest'
import { SequenceTracker, getTracker, resetTracker } from './sequenceTracker'

const STORAGE_KEY = 'snapper_sequence_tracker'

describe('SequenceTracker', () => {
  beforeEach(() => {
    sessionStorage.clear()
  })
  describe('construction', () => {
    it('creates tracker with sessionId', () => {
      const tracker = new SequenceTracker('test-session-id')

      expect(tracker.sessionId).toBe('test-session-id')
    })
    it('creates tracker with initial counters', () => {
      const counters = new Map([['control', 5]])
      const tracker = new SequenceTracker('test-session-id', counters)

      expect(tracker.nextSequence('control')).toBe(6)
    })
    it('starts counters at zero when none provided', () => {
      const tracker = new SequenceTracker('test-session-id')

      expect(tracker.nextSequence('control')).toBe(1)
    })
  })
  describe('nextSequence', () => {
    it('returns monotonically increasing values', () => {
      const tracker = new SequenceTracker('test-session-id')

      expect(tracker.nextSequence('control')).toBe(1)
      expect(tracker.nextSequence('control')).toBe(2)
      expect(tracker.nextSequence('control')).toBe(3)
    })
    it('tracks separate counters per table', () => {
      const tracker = new SequenceTracker('test-session-id')

      expect(tracker.nextSequence('control')).toBe(1)
      expect(tracker.nextSequence('telemetry')).toBe(1)
      expect(tracker.nextSequence('control')).toBe(2)
      expect(tracker.nextSequence('telemetry')).toBe(2)
    })
    it('persists to sessionStorage on each call', () => {
      const tracker = new SequenceTracker('sess-1')

      tracker.nextSequence('control')
      const stored = JSON.parse(sessionStorage.getItem(STORAGE_KEY) as string)

      expect(stored.sessionId).toBe('sess-1')
      expect(stored.counters.control).toBe(1)
    })
  })
  describe('persist', () => {
    it('writes state to sessionStorage', () => {
      const tracker = new SequenceTracker('sess-2')

      tracker.persist()
      const stored = JSON.parse(sessionStorage.getItem(STORAGE_KEY) as string)

      expect(stored.sessionId).toBe('sess-2')
      expect(stored.counters).toEqual({})
    })
  })
})

describe('getTracker', () => {
  beforeEach(() => {
    sessionStorage.clear()
    resetTracker()
  })
  it('returns a SequenceTracker instance', () => {
    const tracker = getTracker()

    expect(tracker).toBeInstanceOf(SequenceTracker)
    expect(tracker.sessionId).toBeDefined()
  })
  it('returns the same instance on repeated calls', () => {
    const t1 = getTracker()
    const t2 = getTracker()

    expect(t1).toBe(t2)
  })
  it('restores from sessionStorage', () => {
    const state = { sessionId: 'restored-id', counters: { control: 10 } }

    resetTracker()
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(state))

    const tracker = getTracker()

    expect(tracker.sessionId).toBe('restored-id')
    expect(tracker.nextSequence('control')).toBe(11)
  })
  it('creates fresh tracker when sessionStorage is empty', () => {
    const tracker = getTracker()

    expect(tracker.sessionId).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
    )
  })
  it('persists immediately on fresh creation', () => {
    getTracker()
    const stored = sessionStorage.getItem(STORAGE_KEY)

    expect(stored).not.toBeNull()
  })
})

describe('resetTracker', () => {
  beforeEach(() => {
    sessionStorage.clear()
  })
  it('clears sessionStorage', () => {
    const tracker = getTracker()

    tracker.nextSequence('control')
    resetTracker()
    expect(sessionStorage.getItem(STORAGE_KEY)).toBeNull()
  })
  it('creates new session on next getTracker call', () => {
    const t1 = getTracker()
    const oldId = t1.sessionId

    resetTracker()
    const t2 = getTracker()

    expect(t2.sessionId).not.toBe(oldId)
  })
})
