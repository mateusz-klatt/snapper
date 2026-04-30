import type { DelegateCreatedPayload } from '../../types/api'

interface SnapperEnvBlock {
  SNAPPER_BASE_URL: string
  SNAPPER_ACCESS_TOKEN: string
}

export function buildMcpConfigSnippet(payload: DelegateCreatedPayload, origin: string): string {
  const env: SnapperEnvBlock = {
    SNAPPER_BASE_URL: `${origin}/api/mcp`,
    SNAPPER_ACCESS_TOKEN: payload.access_token,
  }

  return JSON.stringify(
    {
      mcpServers: {
        snapper: {
          command: 'npx',
          args: ['-y', '@mateusz-klatt/snapper-mcp'],
          env,
        },
      },
    },
    null,
    2
  )
}
