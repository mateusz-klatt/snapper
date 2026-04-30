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
  },
  access_token: 'token-access',
  expires_in: 900,
}

interface ParsedSnippet {
  mcpServers: {
    snapper: {
      command: string
      args: string[]
      env: {
        SNAPPER_BASE_URL: string
        SNAPPER_ACCESS_TOKEN: string
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
    expect(parsed.mcpServers.snapper.command).toBe('npx')
    expect(parsed.mcpServers.snapper.args).toEqual(['-y', '@mateusz-klatt/snapper-mcp'])
  })

  it('snippet is strict JSON with the two expected environment keys', () => {
    const snippet = buildMcpConfigSnippet(payload, 'https://trader.snapper.dev')
    const parsed = JSON.parse(snippet) as ParsedSnippet

    expect(Object.keys(parsed.mcpServers.snapper.env).sort()).toEqual([
      'SNAPPER_ACCESS_TOKEN',
      'SNAPPER_BASE_URL',
    ])
  })
})

describe('ConfigSnippetGenerator', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('renders warning banner and snippet textarea', () => {
    render(<ConfigSnippetGenerator payload={payload} />)
    expect(screen.getByRole('alert')).toHaveTextContent(/Save these credentials now/)
    const textarea = screen.getByLabelText(/\.mcp\.json/) as HTMLTextAreaElement

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
