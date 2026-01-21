import { describe, it, expect, beforeEach, vi, afterEach, Mock } from 'vitest'
import { useAppStore } from './app'

vi.mock('../lib/websocket/instance', () => ({
  wsClient: {
    subscribe: vi.fn(),
    unsubscribe: vi.fn(),
  },
}))
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
    it('sets connectionLag to positive value', () => {
      useAppStore.getState().setConnectionLag(150)
      expect(useAppStore.getState().connectionLag).toBe(150)
    })
    it('sets connectionLag to zero', () => {
      useAppStore.getState().setConnectionLag(100)
      useAppStore.getState().setConnectionLag(0)
      expect(useAppStore.getState().connectionLag).toBe(0)
    })
  })
  describe('addSubscribedTopic', () => {
    it('adds new topic to subscribedTopics', async () => {
      useAppStore.getState().addSubscribedTopic('market.kraken.BTC-USD.candles.1m')
      await vi.waitFor(() => {
        expect(useAppStore.getState().subscribedTopics).toContain(
          'market.kraken.BTC-USD.candles.1m'
        )
      })
    })
    it('does not add duplicate topic', async () => {
      useAppStore.getState().addSubscribedTopic('market.kraken.BTC-USD.candles.1m')
      useAppStore.getState().addSubscribedTopic('market.kraken.BTC-USD.candles.1m')
      await vi.waitFor(() => {
        const topics = useAppStore.getState().subscribedTopics

        expect(topics.filter(t => t === 'market.kraken.BTC-USD.candles.1m')).toHaveLength(1)
      })
    })
    it('calls wsClient.subscribe for new topic', async () => {
      const { wsClient } = await import('../lib/websocket/instance')

      useAppStore.getState().addSubscribedTopic('orders.')
      await vi.waitFor(() => {
        expect(wsClient.subscribe).toHaveBeenCalledWith(['orders.'])
      })
    })
    it('does not call wsClient.subscribe for duplicate topic', async () => {
      const { wsClient } = await import('../lib/websocket/instance')

      useAppStore.setState({ subscribedTopics: ['signals.'] })
      useAppStore.getState().addSubscribedTopic('signals.')
      await vi.waitFor(
        () => {
          expect((wsClient.subscribe as Mock).mock.calls.length).toBe(0)
        },
        { timeout: 100 }
      )
    })
  })
  describe('removeSubscribedTopic', () => {
    it('removes topic from subscribedTopics', async () => {
      useAppStore.setState({
        subscribedTopics: ['market.kraken.BTC-USD.candles.1m', 'orders.'],
      })
      useAppStore.getState().removeSubscribedTopic('market.kraken.BTC-USD.candles.1m')
      await vi.waitFor(() => {
        expect(useAppStore.getState().subscribedTopics).toEqual(['orders.'])
      })
    })
    it('calls wsClient.unsubscribe', async () => {
      const { wsClient } = await import('../lib/websocket/instance')

      useAppStore.setState({ subscribedTopics: ['executions.'] })
      useAppStore.getState().removeSubscribedTopic('executions.')
      await vi.waitFor(() => {
        expect(wsClient.unsubscribe).toHaveBeenCalledWith(['executions.'])
      })
    })
    it('handles removing non-existent topic', async () => {
      useAppStore.setState({ subscribedTopics: ['orders.'] })
      useAppStore.getState().removeSubscribedTopic('non-existent')
      await vi.waitFor(() => {
        expect(useAppStore.getState().subscribedTopics).toEqual(['orders.'])
      })
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
  })
})
