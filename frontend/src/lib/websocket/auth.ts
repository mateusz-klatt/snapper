import { apiClient } from '../apiClient'
import { consumeWsTicket } from '../wsTicketCache'
import { AuthControlMessageType, RefreshWsTokenResponse, AUTH_CONTROL_MESSAGES } from './types'

export function isAuthControlMessage(message: { type: string }): boolean {
  return AUTH_CONTROL_MESSAGES.has(message.type as AuthControlMessageType)
}

let wsTokenPromise: Promise<RefreshWsTokenResponse> | null = null

async function fetchWsToken(): Promise<RefreshWsTokenResponse> {
  wsTokenPromise ??= (async () => {
    const data = await apiClient.postJSON<RefreshWsTokenResponse>('/api/auth/refresh', undefined, {
      skipRetry: true,
    })

    if (!data || typeof data.ws_token !== 'string' || typeof data.ws_token_exp !== 'string') {
      throw new Error('Invalid ws_token response from refresh endpoint')
    }

    return data
  })()

  try {
    return await wsTokenPromise
  } finally {
    wsTokenPromise = null
  }
}

export async function getWsToken(): Promise<{ token: string; exp: number }> {
  const cachedTicket = consumeWsTicket()

  if (cachedTicket) {
    return { token: cachedTicket.token, exp: cachedTicket.exp }
  }

  const { ws_token, ws_token_exp } = await fetchWsToken()
  const expSeconds = Math.floor(new Date(ws_token_exp).getTime() / 1000)

  return { token: ws_token, exp: expSeconds }
}

const REAUTH_LEAD_TIME_MS = 45000

export function calculateReauthDelay(
  expirationSeconds: number,
  leadTimeMs: number = REAUTH_LEAD_TIME_MS
): number {
  const expirationMs = expirationSeconds * 1000
  const targetTimeMs = expirationMs - leadTimeMs
  const now = Date.now()
  const delta = targetTimeMs - now

  return delta <= 0 ? 0 : Math.max(delta, 5000)
}
