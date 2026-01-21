import { describe, expect, it, vi } from 'vitest'
import { z } from 'zod'
import { validateResponse } from './api'

describe('validateResponse', () => {
  const TestSchema = z.object({
    id: z.number(),
    name: z.string(),
  })

  it('returns parsed data when validation succeeds', () => {
    const data = { id: 1, name: 'test' }
    const result = validateResponse(data, TestSchema, '/api/test')

    expect(result).toEqual(data)
  })
  it('throws ApiValidationError when validation fails', () => {
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    const data = { id: 'not-a-number', name: 123 }

    expect(() => validateResponse(data, TestSchema, '/api/test')).toThrow(
      'API response validation failed for /api/test'
    )
    expect(consoleSpy).toHaveBeenCalledWith(
      'API validation failed for /api/test:',
      expect.anything()
    )
    consoleSpy.mockRestore()
  })
  it('thrown error has correct name and properties', () => {
    vi.spyOn(console, 'error').mockImplementation(() => {})

    try {
      validateResponse({ invalid: true }, TestSchema, '/api/endpoint')
      expect.fail('Should have thrown')
    } catch (error) {
      expect((error as Error).name).toBe('ApiValidationError')
      expect((error as { endpoint: string }).endpoint).toBe('/api/endpoint')
      expect((error as { zodError: unknown }).zodError).toBeDefined()
    }

    vi.restoreAllMocks()
  })
})
