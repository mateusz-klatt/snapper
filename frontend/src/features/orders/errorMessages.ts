/**
 * User-facing error-code map for order-entry submit responses.
 *
 * Keyed on ``error_code`` strings raised by
 * ``snapper.server._capability_guard.require_tradable`` and the REST
 * submit handlers. Consumed by ``NewOrderModal`` + bracket/trailing-stop
 * dialogs when they branch on ``APIError.details.error_code``.
 *
 * Keys must stay in sync with the backend guard at
 * ``src/snapper/server/_capability_guard.py``. Unknown codes fall back
 * to ``APIError.message`` which already carries the ``reason`` from the
 * structured 422 detail.
 */
export const ORDER_ERROR_MESSAGES: Readonly<Record<string, string>> = {
  instrument_market_data_only:
    'This instrument is observation-only — place orders on an execution-capable instrument.',
  unknown_instrument: 'This instrument is not recognised. Reload the list and try again.',
  caps_violation:
    'This order would exceed your account safety caps. Review your risk settings and retry.',
}

/**
 * Extract the ``error_code`` from an ``APIError.details`` object, if present.
 *
 * Returns ``null`` when ``details`` is absent, not a structured object, or
 * lacks an ``error_code`` string — matches the non-guard 422 paths
 * (Pydantic validation errors, plain-string 500s) without crashing.
 */
export function extractErrorCode(details: unknown): string | null {
  if (details === null || typeof details !== 'object') {
    return null
  }

  const code = (details as { error_code?: unknown }).error_code

  return typeof code === 'string' ? code : null
}

/**
 * Map an ``APIError``'s ``details.error_code`` to a human-readable string.
 *
 * Returns ``null`` when the code is unknown so the caller can fall back to
 * ``APIError.message``.
 */
export function lookupOrderErrorMessage(details: unknown): string | null {
  const code = extractErrorCode(details)

  if (code === null) {
    return null
  }

  return ORDER_ERROR_MESSAGES[code] ?? null
}
