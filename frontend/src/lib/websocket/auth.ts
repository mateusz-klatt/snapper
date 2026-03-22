import { apiClient } from '../apiClient'
import { consumeWsTicket } from '../wsTicketCache'
import { AuthControlMessageType, RefreshWsTokenResponse, AUTH_CONTROL_MESSAGES } from './types'

export function isAuthControlMessage(message: { type: string }): boolean {
  return AUTH_CONTROL_MESSAGES.has(message.type as AuthControlMessageType)
}

let wsTokenPromise: Promise<RefreshWsTokenResponse> | null = null

async function fetchWsToken(): Promise<RefreshWsTokenResponse> {
  wsTokenPromise ??= (async () => {
    const envelope = await apiClient.postJSON<RefreshWsTokenResponse>(
      '/api/auth/refresh',
      undefined,
      {
        skipRetry: true,
      }
    )

    const data = envelope?.payload

    if (!data || typeof data.ws_token !== 'string' || typeof data.ws_token_exp !== 'string') {
      throw new Error('Invalid ws_token response from refresh endpoint')
    }

    return envelope
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

  const refreshResponse = await fetchWsToken()
  const { ws_token, ws_token_exp } = refreshResponse.payload
  const expSeconds = Math.floor(new Date(ws_token_exp).getTime() / 1000)

  return { token: ws_token, exp: expSeconds }
}
