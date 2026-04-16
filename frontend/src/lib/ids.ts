/**
 * UUID7 format helpers — frontend mirror of `snapper.core.ids`.
 *
 * UUID7 is the canonical public-id format across Snapper. Used here
 * by hash-routing guards and backtest-subscription prefix builders
 * so a malformed URL fragment never becomes a WS subscription
 * prefix that the backend rejects.
 */

const UUID7_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i

/** Return true iff the value is a canonical-form UUID7 string. */
export function isUuid7(value: string): boolean {
  return UUID7_PATTERN.test(value)
}
