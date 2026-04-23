import type { DelegateCreatedPayload } from '../../types/api'

export function buildMcpConfigSnippet(payload: DelegateCreatedPayload, origin: string): string {
  return JSON.stringify(
    {
      mcpServers: {
        snapper: {
          command: 'npx',
          args: ['-y', 'snapper-mcp'],
          env: {
            SNAPPER_BASE_URL: `${origin}/api/mcp`,
            SNAPPER_ACCESS_TOKEN: payload.access_token,
            SNAPPER_REFRESH_TOKEN: payload.refresh_token,
          },
        },
      },
    },
    null,
    2
  )
}
