import type { FlagIconSpec } from '../types'

const flag = (country: string): FlagIconSpec => ({ kind: 'flag', country })

export const FIAT_FLAGS: Record<string, FlagIconSpec> = {
  USD: flag('us'),
  EUR: flag('eu'),
  GBP: flag('gb'),
  PLN: flag('pl'),
  JPY: flag('jp'),
  CHF: flag('ch'),
  AUD: flag('au'),
  CAD: flag('ca'),
  NZD: flag('nz'),
  CZK: flag('cz'),
  HUF: flag('hu'),
  SEK: flag('se'),
  NOK: flag('no'),
  DKK: flag('dk'),
  TRY: flag('tr'),
  ILS: flag('il'),
  CNY: flag('cn'),
  ZAR: flag('za'),
  HKD: flag('hk'),
  SGD: flag('sg'),
  MXN: flag('mx'),
  BRL: flag('br'),
  BGN: flag('bg'),
  RON: flag('ro'),
}
