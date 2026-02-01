import type { ZodType } from 'zod'

class ApiValidationError extends Error {
  constructor(
    public readonly endpoint: string,
    public readonly zodError: unknown
  ) {
    super(`API response validation failed for ${endpoint}`)
    this.name = 'ApiValidationError'
  }
}

export function validateResponse<T>(data: unknown, schema: ZodType<T>, endpoint: string): T {
  const result = schema.safeParse(data)

  if (!result.success) {
    console.error(`API validation failed for ${endpoint}:`, result.error.issues)
    throw new ApiValidationError(endpoint, result.error)
  }

  return result.data
}
