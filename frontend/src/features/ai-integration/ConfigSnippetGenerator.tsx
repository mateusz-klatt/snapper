import React, { useMemo, useState } from 'react'
import { AlertTriangle, Check, Clipboard } from 'lucide-react'
import { Button } from '../../components/ui'
import { buildMcpConfigSnippet } from './buildMcpConfigSnippet'
import type { DelegateCreatedPayload } from '../../types/api'

export function ConfigSnippetGenerator({
  payload,
}: Readonly<{ payload: DelegateCreatedPayload }>): React.ReactElement {
  const [copied, setCopied] = useState(false)
  const snippet = useMemo(
    () => buildMcpConfigSnippet(payload, globalThis.location.origin),
    [payload]
  )

  const handleCopy = async (): Promise<void> => {
    await navigator.clipboard.writeText(snippet)
    setCopied(true)
    globalThis.setTimeout(() => setCopied(false), 2000)
  }

  return (
    <div className='space-y-4'>
      <div
        className='flex items-start gap-3 p-4 rounded-lg bg-loss-50 border border-loss-200 text-loss-800'
        role='alert'
      >
        <AlertTriangle className='w-5 h-5 shrink-0 mt-0.5' />
        <div>
          <h3 className='font-semibold'>Save these credentials now</h3>
          <p className='text-sm'>
            They will not be shown again. Snapper does not store the raw tokens; once you close this
            wizard, the only copy will be in your MCP client config.
          </p>
        </div>
      </div>

      <div>
        <label
          htmlFor='mcp-config-snippet'
          className='block text-sm font-medium text-muted-700 mb-1'
        >
          <code>.mcp.json</code>
        </label>
        <textarea
          id='mcp-config-snippet'
          readOnly
          rows={14}
          value={snippet}
          className='w-full font-mono text-xs p-3 rounded-lg border border-dark-600 bg-dark-700 text-muted-100'
        />
        <p className='mt-2 text-xs text-muted-600'>
          Long-lived AI delegate token (~10 years). The same token authenticates both the proxy MCP
          server and the watch monitor. To rotate, deactivate this delegate and create a new one.
        </p>
      </div>

      <div className='flex items-center justify-between'>
        <p className='text-xs text-muted-500'>
          Base URL is derived from this browser&apos;s origin + <code>/api/mcp</code>.
        </p>
        <Button
          variant='primary'
          onClick={handleCopy}
          aria-label='Copy config snippet to clipboard'
        >
          {copied ? (
            <>
              <Check className='w-4 h-4 mr-1 inline' />
              Copied
            </>
          ) : (
            <>
              <Clipboard className='w-4 h-4 mr-1 inline' />
              Copy to clipboard
            </>
          )}
        </Button>
      </div>
    </div>
  )
}
