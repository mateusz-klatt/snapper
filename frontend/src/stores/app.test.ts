import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import { useAppStore } from './app'

describe('useAppStore', () => {
  beforeEach(() => {
    useAppStore.setState({
      isConnected: false,
      connectionLag: 0,
      subscribedTopics: [],
      lastUpdate: new Date().toISOString(),
      isDarkMode: true,
    })
    vi.clearAllMocks()
  })
  afterEach(() => {
    vi.restoreAllMocks()
  })
  describe('initial state', () => {
    it('has isConnected as false', () => {
      expect(useAppStore.getState().isConnected).toBe(false)
    })
    it('has connectionLag as 0', () => {
      expect(useAppStore.getState().connectionLag).toBe(0)
    })
    it('has empty subscribedTopics', () => {
      expect(useAppStore.getState().subscribedTopics).toEqual([])
    })
    it('has isDarkMode as true', () => {
      expect(useAppStore.getState().isDarkMode).toBe(true)
    })
    it('has lastUpdate as ISO string', () => {
      const lastUpdate = useAppStore.getState().lastUpdate

      expect(typeof lastUpdate).toBe('string')
      expect(lastUpdate).not.toBeNull()

      if (lastUpdate !== null) {
        expect(() => new Date(lastUpdate)).not.toThrow()
      }
    })
  })
  describe('setConnected', () => {
    it('sets isConnected to true', () => {
      useAppStore.getState().setConnected(true)
      expect(useAppStore.getState().isConnected).toBe(true)
    })
    it('sets isConnected to false', () => {
      useAppStore.getState().setConnected(true)
      useAppStore.getState().setConnected(false)
      expect(useAppStore.getState().isConnected).toBe(false)
    })
  })
  describe('setConnectionLag', () => {
    it('sets connectionLag directly', () => {
      useAppStore.getState().setConnectionLag(150)
      expect(useAppStore.getState().connectionLag).toBe(150)
    })
    it('overwrites previous value without smoothing', () => {
      useAppStore.getState().setConnectionLag(100)
      useAppStore.getState().setConnectionLag(200)
      expect(useAppStore.getState().connectionLag).toBe(200)
    })
  })
  describe('setSubscribedTopics', () => {
    it('replaces all subscribed topics', () => {
      useAppStore.setState({ subscribedTopics: ['old-topic'] })
      useAppStore.getState().setSubscribedTopics(['new-topic-1', 'new-topic-2'])
      expect(useAppStore.getState().subscribedTopics).toEqual(['new-topic-1', 'new-topic-2'])
    })
    it('can set empty topics array', () => {
      useAppStore.setState({ subscribedTopics: ['topic1', 'topic2'] })
      useAppStore.getState().setSubscribedTopics([])
      expect(useAppStore.getState().subscribedTopics).toEqual([])
    })
  })
  describe('updateLastUpdate', () => {
    it('updates lastUpdate to current time', () => {
      const before = new Date().toISOString()

      useAppStore.getState().updateLastUpdate()
      const lastUpdate = useAppStore.getState().lastUpdate
      const after = new Date().toISOString()

      expect(lastUpdate).not.toBeNull()

      if (lastUpdate !== null) {
        expect(lastUpdate >= before).toBe(true)
        expect(lastUpdate <= after).toBe(true)
      }
    })
  })
  describe('toggleDarkMode', () => {
    it('toggles isDarkMode from true to false', () => {
      useAppStore.setState({ isDarkMode: true })
      useAppStore.getState().toggleDarkMode()
      expect(useAppStore.getState().isDarkMode).toBe(false)
    })
    it('toggles isDarkMode from false to true', () => {
      useAppStore.setState({ isDarkMode: false })
      useAppStore.getState().toggleDarkMode()
      expect(useAppStore.getState().isDarkMode).toBe(true)
    })
    it('toggles multiple times', () => {
      useAppStore.setState({ isDarkMode: true })
      useAppStore.getState().toggleDarkMode()
      useAppStore.getState().toggleDarkMode()
      useAppStore.getState().toggleDarkMode()
      expect(useAppStore.getState().isDarkMode).toBe(false)
    })
    it('persists dark mode preference to localStorage', () => {
      useAppStore.setState({ isDarkMode: false })
      useAppStore.getState().toggleDarkMode()
      expect(localStorage.getItem('snapper-dark-mode')).toBe('true')
      useAppStore.getState().toggleDarkMode()
      expect(localStorage.getItem('snapper-dark-mode')).toBe('false')
    })
  })
  describe('loadDarkModePreference', () => {
    it('reads dark mode preference from localStorage on module load', async () => {
      localStorage.setItem('snapper-dark-mode', 'true')
      vi.resetModules()
      const { useAppStore: freshStore } = await import('./app')

      expect(freshStore.getState().isDarkMode).toBe(true)
      localStorage.removeItem('snapper-dark-mode')
    })
    it('defaults to false when localStorage has no value', async () => {
      localStorage.removeItem('snapper-dark-mode')
      vi.resetModules()
      const { useAppStore: freshStore } = await import('./app')

      expect(freshStore.getState().isDarkMode).toBe(false)
    })
  })
})
