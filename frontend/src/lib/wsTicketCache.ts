interface WsTicket {
  token: string
  exp: number
}
let cachedTicket: WsTicket | null = null

export function storeWsTicket(ticket: WsTicket | null): void {
  if (ticket && typeof ticket.token === 'string' && typeof ticket.exp === 'number') {
    cachedTicket = ticket
  } else {
    cachedTicket = null
  }
}

export function consumeWsTicket(now: number = Date.now()): WsTicket | null {
  if (!cachedTicket) {
    return null
  }

  const expiresAtMs = cachedTicket.exp * 1000

  if (!Number.isFinite(expiresAtMs) || expiresAtMs <= now) {
    cachedTicket = null

    return null
  }

  const ticket = cachedTicket

  cachedTicket = null

  return ticket
}
