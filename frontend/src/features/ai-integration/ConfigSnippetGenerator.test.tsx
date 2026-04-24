import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, act } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { ConfigSnippetGenerator } from './ConfigSnippetGenerator'
import { buildMcpConfigSnippet } from './buildMcpConfigSnippet'
import type { DelegateCreatedPayload } from '../../types/api'

const payload: DelegateCreatedPayload = {
  delegate: {
    public_id: 'd-1',
    username: 'ai-alpha',
    label: 'Alpha',
    created_by_user_public_id: 'u-1',
    created_at: '2026-04-21T00:00:00Z',
    is_active: true,
    caps: {
      max_open_orders: 10,
      max_daily_notional_usd: 1000,
      max_cancels_per_minute: null,
      max_order_quantity_per_instrument: null,
    },
    token_kind: 'rotating',
  },
  access_token: 'token-access',
  refresh_token: 'token-refresh',
  expires_in: 900,
  token_kind: 'rotating',
}

interface ParsedSnippet {
  mcpServers: {
    snapper: {
      command: string
      args: string[]
      env: {
        SNAPPER_BASE_URL: string
        SNAPPER_ACCESS_TOKEN: string
        SNAPPER_REFRESH_TOKEN?: string
      }
    }
  }
}

describe('buildMcpConfigSnippet', () => {
  it('builds JSON snippet with SNAPPER_BASE_URL derived from origin + /api/mcp', () => {
    const snippet = buildMcpConfigSnippet(payload, 'https://trader.snapper.dev')
    const parsed = JSON.parse(snippet) as ParsedSnippet

    expect(parsed.mcpServers.snapper.env.SNAPPER_BASE_URL).toBe(
      'https://trader.snapper.dev/api/mcp'
    )
    expect(parsed.mcpServers.snapper.env.SNAPPER_ACCESS_TOKEN).toBe('token-access')
    expect(parsed.mcpServers.snapper.env.SNAPPER_REFRESH_TOKEN).toBe('token-refresh')
    expect(parsed.mcpServers.snapper.command).toBe('npx')
    expect(parsed.mcpServers.snapper.args).toEqual(['-y', '@mateusz-klatt/snapper-mcp'])
  })

  it('rotating payload: snippet is strict JSON and env carries SNAPPER_REFRESH_TOKEN', () => {
    const snippet = buildMcpConfigSnippet(payload, 'https://trader.snapper.dev')
    const parsed = JSON.parse(snippet) as ParsedSnippet

    expect(parsed.mcpServers.snapper.env.SNAPPER_REFRESH_TOKEN).toBe('token-refresh')
  })

  it('long-lived PAT payload: snippet is strict JSON and env OMITS SNAPPER_REFRESH_TOKEN', () => {
    const patPayload: DelegateCreatedPayload = {
      ...payload,
      delegate: { ...payload.delegate, token_kind: 'long_lived' },
      refresh_token: null,
      token_kind: 'long_lived',
    }
    const snippet = buildMcpConfigSnippet(patPayload, 'https://trader.snapper.dev')
    const parsed = JSON.parse(snippet) as ParsedSnippet

    expect('SNAPPER_REFRESH_TOKEN' in parsed.mcpServers.snapper.env).toBe(false)
    expect(parsed.mcpServers.snapper.env.SNAPPER_ACCESS_TOKEN).toBe('token-access')
  })

  it('rotating payload with unexpectedly-null refresh_token still produces strict JSON (defensive)', () => {
    const weird: DelegateCreatedPayload = { ...payload, refresh_token: null }
    const snippet = buildMcpConfigSnippet(weird, 'https://trader.snapper.dev')

    expect(() => JSON.parse(snippet)).not.toThrow()
  })
})

describe('ConfigSnippetGenerator', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('renders warning banner and snippet textarea', () => {
    render(<ConfigSnippetGenerator payload={payload} />)
    expect(screen.getByRole('alert')).toHaveTextContent(/Save these credentials now/)
    const textarea = screen.getByLabelText(/\.mcp-config\.json/) as HTMLTextAreaElement

    expect(textarea.value).toContain('SNAPPER_ACCESS_TOKEN')
    expect(textarea.value).toContain('token-access')
    expect(textarea.value).toContain('/api/mcp')
  })

  it('copies snippet to clipboard and flips button label to Copied', async () => {
    const user = userEvent.setup()

    render(<ConfigSnippetGenerator payload={payload} />)
    const clipboardWrite = vi.spyOn(navigator.clipboard, 'writeText')
    const button = screen.getByRole('button', { name: /Copy config snippet to clipboard/ })

    await act(async () => {
      await user.click(button)
    })
    expect(clipboardWrite).toHaveBeenCalledWith(expect.stringContaining('token-access'))
    expect(screen.getByText('Copied')).toBeInTheDocument()
  })

  it('does not auto-copy on mount', () => {
    const clipboardWrite = vi.spyOn(navigator.clipboard, 'writeText')

    render(<ConfigSnippetGenerator payload={payload} />)
    expect(clipboardWrite).not.toHaveBeenCalled()
  })

  it('renders a PAT regeneration note OUTSIDE the textarea for long-lived payloads', () => {
    const patPayload: DelegateCreatedPayload = {
      ...payload,
      delegate: { ...payload.delegate, token_kind: 'long_lived' },
      refresh_token: null,
      token_kind: 'long_lived',
    }

    render(<ConfigSnippetGenerator payload={patPayload} />)
    const textarea = screen.getByLabelText(/\.mcp-config\.json/) as HTMLTextAreaElement

    expect(() => JSON.parse(textarea.value)).not.toThrow()
    expect(textarea.value).not.toMatch(/long-lived PAT/i)
    expect(screen.getByText(/long-lived PAT/i)).toBeInTheDocument()
    expect(screen.getByText(/SNAPPER_REFRESH_TOKEN/)).toBeInTheDocument()
  })

  it('does NOT render the PAT note for rotating payloads', () => {
    render(<ConfigSnippetGenerator payload={payload} />)
    expect(screen.queryByText(/long-lived PAT/i)).not.toBeInTheDocument()
  })

  it('reverts Copied label back to Copy to clipboard after 2s', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true })

    try {
      const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime })

      render(<ConfigSnippetGenerator payload={payload} />)
      const button = screen.getByRole('button', { name: /Copy config snippet to clipboard/ })

      await act(async () => {
        await user.click(button)
      })
      expect(screen.getByText('Copied')).toBeInTheDocument()
      await act(async () => {
        vi.advanceTimersByTime(2500)
      })
      expect(screen.queryByText('Copied')).not.toBeInTheDocument()
    } finally {
      vi.useRealTimers()
    }
  })
})
