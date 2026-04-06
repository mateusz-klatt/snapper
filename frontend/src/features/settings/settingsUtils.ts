export function isJsonString(str: string): boolean {
  try {
    JSON.parse(str)

    return true
  } catch {
    return false
  }
}

export function isBooleanString(value: string): boolean {
  const trimmed = value.trim().toLowerCase()

  return trimmed === 'true' || trimmed === 'false'
}

export function parseBooleanString(value: string): boolean {
  return value.trim().toLowerCase() === 'true'
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
  id: string
  type: JsonTokenType
  value: string
}

const TOKEN_PATTERNS: [JsonTokenType, RegExp][] = [
  ['key', /"(?:[^"\\]|\\.)*"(?=\s*:)/y],
  ['string', /"(?:[^"\\]|\\.)*"/y],
  ['number', /-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/y],
  ['boolean', /(?:true|false)\b/y],
  ['null', /null\b/y],
  ['punctuation', /[{}[\]:,]/y],
  ['whitespace', /\s+/y],
]

export function tokenizeJson(json: string): JsonToken[] {
  const tokens: JsonToken[] = []
  let pos = 0
  let id = 0

  while (pos < json.length) {
    let matched = false

    for (const [type, pattern] of TOKEN_PATTERNS) {
      pattern.lastIndex = pos
      const match = pattern.exec(json)

      if (match) {
        tokens.push({ id: `t${id++}`, type, value: match[0] })
        pos = pattern.lastIndex
        matched = true
        break
      }
    }

    if (!matched) {
      pos++
    }
  }

  return tokens
}
