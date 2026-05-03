import { useEffect, useMemo, useRef } from 'react'
import { useAppStore } from '../stores/app'
import { useOperators, useWallets } from './queries'

const STORAGE_KEY_WALLET = 'snapper-current-wallet'
const STORAGE_KEY_OPERATOR = 'snapper-current-operator'
const SENTINEL_ALL = '__all__'

type ScopeIds = {
  readonly wallet: string | null
  readonly operator: string | null
}

type UrlScope = ScopeIds & {
  readonly hasParams: boolean
}

export function readUrlScope(): UrlScope {
  const hash = globalThis.location.hash
  const queryIdx = hash.indexOf('?')

  if (queryIdx === -1) {
    return { wallet: null, operator: null, hasParams: false }
  }

  const params = new URLSearchParams(hash.slice(queryIdx + 1))
  const walletParam = params.get('wallet')
  const operatorParam = params.get('operator')

  return {
    wallet: walletParam === SENTINEL_ALL ? null : walletParam,
    operator: operatorParam === SENTINEL_ALL ? null : operatorParam,
    hasParams: params.has('wallet') || params.has('operator'),
  }
}

export function readLocalStorageScope(): ScopeIds {
  try {
    const wallet = globalThis.localStorage.getItem(STORAGE_KEY_WALLET)
    const operator = globalThis.localStorage.getItem(STORAGE_KEY_OPERATOR)

    return {
      wallet: wallet === SENTINEL_ALL || wallet === null ? null : wallet,
      operator: operator === SENTINEL_ALL || operator === null ? null : operator,
    }
  } catch {
    return { wallet: null, operator: null }
  }
}

export function writeUrlScope(wallet: string | null, operator: string | null): void {
  const hash = globalThis.location.hash
  const queryIdx = hash.indexOf('?')
  const route = queryIdx === -1 ? hash.slice(1) : hash.slice(1, queryIdx)
  const params = new URLSearchParams()

  if (wallet) {
    params.set('wallet', wallet)
  }

  if (operator) {
    params.set('operator', operator)
  }

  const qs = params.toString()
  const next = qs ? `#${route}?${qs}` : `#${route}`

  if (globalThis.location.hash !== next) {
    globalThis.history.replaceState(null, '', next)
  }
}

export function writeLocalStorageScope(wallet: string | null, operator: string | null): void {
  try {
    if (wallet) {
      globalThis.localStorage.setItem(STORAGE_KEY_WALLET, wallet)
    } else {
      globalThis.localStorage.removeItem(STORAGE_KEY_WALLET)
    }

    if (operator) {
      globalThis.localStorage.setItem(STORAGE_KEY_OPERATOR, operator)
    } else {
      globalThis.localStorage.removeItem(STORAGE_KEY_OPERATOR)
    }
  } catch {
    // localStorage unavailable (private mode, quota) — silently ignore;
    // URL hash is still the primary source of truth.
  }
}

type IdList = ReadonlyArray<{ readonly public_id: string }>

function pickValidId(candidate: string | null, list: IdList): string | null {
  if (candidate === null) {
    return null
  }

  return list.some(item => item.public_id === candidate) ? candidate : null
}

function resolveInitialScope(
  url: UrlScope,
  storage: ScopeIds,
  wallets: IdList,
  operators: IdList
): ScopeIds {
  const operator =
    pickValidId(url.operator, operators) ?? pickValidId(storage.operator, operators) ?? null
  const walletFromUrl = pickValidId(url.wallet, wallets)
  const walletFromStorage = pickValidId(storage.wallet, wallets)
  const walletAutoPick =
    !url.hasParams && storage.wallet === null ? (wallets[0]?.public_id ?? null) : null
  const wallet = walletFromUrl ?? walletFromStorage ?? walletAutoPick

  return { wallet, operator }
}

const EMPTY_LIST: ReadonlyArray<{ readonly public_id: string }> = []

export function useScopePersistence(): void {
  const walletsResult = useWallets()
  const operatorsResult = useOperators()
  const wallets = useMemo(() => walletsResult.data?.payload ?? EMPTY_LIST, [walletsResult.data])
  const operators = useMemo(
    () => operatorsResult.data?.payload ?? EMPTY_LIST,
    [operatorsResult.data]
  )
  const currentWalletPublicId = useAppStore(s => s.currentWalletPublicId)
  const currentOperatorPublicId = useAppStore(s => s.currentOperatorPublicId)
  const setOperator = useAppStore(s => s.setCurrentOperatorPublicId)
  const selectWallet = useAppStore(s => s.selectWalletAndRefresh)
  const initializedRef = useRef(false)
  const lastWrittenRef = useRef<ScopeIds>({ wallet: null, operator: null })

  useEffect(() => {
    if (!initializedRef.current) {
      if (wallets.length === 0 && operators.length === 0) {
        return
      }

      const url = readUrlScope()
      const storage = readLocalStorageScope()
      const resolved = resolveInitialScope(url, storage, wallets, operators)

      initializedRef.current = true
      lastWrittenRef.current = { wallet: resolved.wallet, operator: resolved.operator }

      writeUrlScope(resolved.wallet, resolved.operator)
      writeLocalStorageScope(resolved.wallet, resolved.operator)

      if (resolved.operator !== null && resolved.operator !== currentOperatorPublicId) {
        setOperator(resolved.operator)
      }

      if (resolved.wallet !== currentWalletPublicId) {
        void selectWallet(resolved.wallet)
      }

      return
    }

    if (
      currentWalletPublicId !== lastWrittenRef.current.wallet ||
      currentOperatorPublicId !== lastWrittenRef.current.operator
    ) {
      lastWrittenRef.current = {
        wallet: currentWalletPublicId,
        operator: currentOperatorPublicId,
      }
      writeUrlScope(currentWalletPublicId, currentOperatorPublicId)
      writeLocalStorageScope(currentWalletPublicId, currentOperatorPublicId)
    }
  }, [
    wallets,
    operators,
    currentWalletPublicId,
    currentOperatorPublicId,
    setOperator,
    selectWallet,
  ])
}
