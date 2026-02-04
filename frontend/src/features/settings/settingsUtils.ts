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
  trading: 'bg-green-900 text-green-200',
  auth: 'bg-red-900 text-red-200',
  risk: 'bg-yellow-900 text-yellow-200',
  zmq: 'bg-blue-900 text-blue-200',
  network: 'bg-purple-900 text-purple-200',
  system: 'bg-gray-900 text-gray-200',
}

export const getCategoryColor = (category: string): string =>
  CATEGORY_COLORS[category] || 'bg-dark-600 text-dark-200'

export const getMaskedValue = (key: string, value: string): string => {
  if (isSensitive(key) && value) {
    return '••••••••••••••••••••••••••••••••••••••••••••••••••'
  }

  return value || '(empty)'
}
