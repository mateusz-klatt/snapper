export function calculateReconnectDelay(
  attempt: number,
  baseInterval: number,
  maxDelay: number = 30000
): number {
  return Math.min(baseInterval * Math.pow(1.5, attempt - 1), maxDelay)
}

export function shouldReconnect(
  isReconnecting: boolean,
  reconnectAttempts: number,
  maxReconnectAttempts: number
): boolean {
  return !isReconnecting && reconnectAttempts < maxReconnectAttempts
}

export function createHeartbeatMessage(): { type: 'ping' } {
  return { type: 'ping' }
}

export function flushThrottledMessages<T>(
  pendingMessages: Map<string, T>,
  lastMessageTime: Map<string, number>,
  throttleInterval: number,
  onDeliver: (message: T) => void
): void {
  const now = Date.now()

  for (const [topic, message] of pendingMessages.entries()) {
    const lastTime = lastMessageTime.get(topic) || 0

    if (now - lastTime >= throttleInterval) {
      onDeliver(message)
      pendingMessages.delete(topic)
      lastMessageTime.set(topic, now)
    }
  }
}

export function buildWebSocketUrl(endpoint: string = '/api/ws'): string {
  const protocol = globalThis.location.protocol === 'https:' ? 'wss:' : 'ws:'
  const host = globalThis.location.host

  return `${protocol}//${host}${endpoint}`
}
