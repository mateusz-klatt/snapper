export function isJsonString(str: string): boolean {
  try {
    JSON.parse(str)

    return true
  } catch {
    return false
  }
}

export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue }

export const SENSITIVE_PATTERNS = [
  'api_key',
  'api_secret',
  'password',
  'secret_key',
  'private_key',
  'credential',
]

export const isSensitive = (key: string): boolean => {
  const lowerKey = key.toLowerCase()

  return SENSITIVE_PATTERNS.some(pattern => lowerKey.includes(pattern))
}

export const isEncrypted = (value: string): boolean => {
  return value.length >= 40 && value.startsWith('gAAAAAB')
}

export const CATEGORY_COLORS: Record<string, string> = {
  trading: 'bg-accent-50 text-accent-700',
  auth: 'bg-loss-50 text-loss-700',
  risk: 'bg-warning-50 text-warning-700',
  zmq: 'bg-info-50 text-info-700',
  network: 'bg-purple-50 text-purple-700',
  system: 'bg-muted-100 text-muted-700',
}

export const getCategoryColor = (category: string): string =>
  CATEGORY_COLORS[category] || 'bg-muted-100 text-muted-600'

export const SENSITIVE_MASK = '••••••••'

export const getMaskedValue = (key: string, value: string): string => {
  if (isSensitive(key) && value) {
    return SENSITIVE_MASK
  }

  if (!value) {
    return '(empty)'
  }

  try {
    const parsed = JSON.parse(value)

    if (typeof parsed === 'object' && parsed !== null) {
      return JSON.stringify(parsed, null, 2)
    }
  } catch {
    /* not JSON — return as-is */
  }

  return value
}

export type JsonTokenType =
  | 'key'
  | 'string'
  | 'number'
  | 'boolean'
  | 'null'
  | 'punctuation'
  | 'whitespace'

interface JsonToken {
  type: JsonTokenType
  value: string
}

const JSON_TOKEN_SOURCE =
  '("(?:[^"\\\\]|\\\\.)*")(?=\\s*:)|("(?:[^"\\\\]|\\\\.)*")|(-?\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?)|\\b(true|false)\\b|\\b(null)\\b|([{}\\[\\]:,])|(\\s+)'

const TOKEN_GROUP_TYPES: JsonTokenType[] = [
  'key',
  'string',
  'number',
  'boolean',
  'null',
  'punctuation',
  'whitespace',
]

export function tokenizeJson(json: string): JsonToken[] {
  const tokens: JsonToken[] = []
  const pattern = new RegExp(JSON_TOKEN_SOURCE, 'g')
  let match: RegExpExecArray | null

  while ((match = pattern.exec(json)) !== null) {
    for (let i = 0; i < TOKEN_GROUP_TYPES.length; i++) {
      if (match[i + 1] !== undefined) {
        tokens.push({ type: TOKEN_GROUP_TYPES[i], value: match[i + 1] })
        break
      }
    }
  }

  return tokens
}
