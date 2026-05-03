import { describe, it, expect } from 'vitest'
import { render } from '@testing-library/react'
import { InstrumentIcon } from './InstrumentIcon'

describe('InstrumentIcon — smart-hybrid dispatcher', () => {
  describe('Tier 2: HIDE quote when USD (variable-quote crypto)', () => {
    it('renders single icon for BTC-USD (USD = default quote)', (): void => {
      const { container } = render(<InstrumentIcon symbol='BTC-USD' exchange='kraken' />)
      const imgs = container.querySelectorAll('img')

      expect(imgs.length).toBe(1)
      expect(imgs[0]?.getAttribute('src')).toContain('btc.svg')
    })

    it('renders single icon for ETH-USD', (): void => {
      const { container } = render(<InstrumentIcon symbol='ETH-USD' exchange='kraken' />)

      expect(container.querySelectorAll('img').length).toBe(1)
    })

    it('renders single icon for BTC-USD-PERP (perp = USD implicit)', (): void => {
      const { container } = render(
        <InstrumentIcon symbol='BTC-USD-PERP' exchange='kraken_futures' />
      )

      expect(container.querySelectorAll('img').length).toBe(1)
    })

    it('renders single icon for BTC-USD-260925 (dated future)', (): void => {
      const { container } = render(
        <InstrumentIcon symbol='BTC-USD-260925' exchange='kraken_futures' />
      )

      expect(container.querySelectorAll('img').length).toBe(1)
    })
  })

  describe('Tier 3: ALWAYS show both — non-USD quote on crypto', () => {
    it('renders dual icon for BTC-EUR', (): void => {
      const { container } = render(<InstrumentIcon symbol='BTC-EUR' exchange='kraken' />)
      const imgs = container.querySelectorAll('img')

      expect(imgs.length).toBe(2)
      expect(imgs[0]?.getAttribute('src')).toContain('btc.svg')
      expect(imgs[1]?.getAttribute('src')).toContain('eu.svg')
    })

    it('renders dual icon for BTC-USDT (stablecoin = always shown per PL tax law)', (): void => {
      const { container } = render(<InstrumentIcon symbol='BTC-USDT' exchange='kraken' />)
      const imgs = container.querySelectorAll('img')

      expect(imgs.length).toBe(2)
      expect(imgs[1]?.getAttribute('src')).toContain('usdt.svg')
    })

    it('renders dual icon for BTC-USDC (stablecoin)', (): void => {
      const { container } = render(<InstrumentIcon symbol='BTC-USDC' exchange='kraken' />)
      const imgs = container.querySelectorAll('img')

      expect(imgs.length).toBe(2)
      expect(imgs[1]?.getAttribute('src')).toContain('usdc.svg')
    })

    it('renders dual icon for ETH-BTC (cross pair)', (): void => {
      const { container } = render(<InstrumentIcon symbol='ETH-BTC' exchange='kraken' />)
      const imgs = container.querySelectorAll('img')

      expect(imgs.length).toBe(2)
    })
  })

  describe('Tier 3: forex always dual', () => {
    it('renders dual icon for EUR-USD (forex special-case despite USD quote)', (): void => {
      const { container } = render(<InstrumentIcon symbol='EUR-USD' exchange='walutomat' />)
      const imgs = container.querySelectorAll('img')

      expect(imgs.length).toBe(2)
      expect(imgs[0]?.getAttribute('src')).toContain('eu.svg')
      expect(imgs[1]?.getAttribute('src')).toContain('us.svg')
    })

    it('renders dual icon for USD-PLN', (): void => {
      const { container } = render(<InstrumentIcon symbol='USD-PLN' exchange='walutomat' />)
      const imgs = container.querySelectorAll('img')

      expect(imgs.length).toBe(2)
    })
  })

  describe('Tier 1: NO icon dimension (single asset, no quote concept)', () => {
    it('renders single lucide icon for AAPL (equity)', (): void => {
      const { container } = render(<InstrumentIcon symbol='AAPL' exchange='kraken' />)
      const imgs = container.querySelectorAll('img')

      expect(imgs.length).toBe(0)
      expect(container.querySelector('svg')).not.toBeNull()
    })

    it('renders single lucide icon for SPX (index)', (): void => {
      const { container } = render(<InstrumentIcon symbol='SPX' exchange='polygon' />)

      expect(container.querySelector('svg')).not.toBeNull()
      expect(container.querySelectorAll('img').length).toBe(0)
    })

    it('renders single lucide icon for US10Y (yield)', (): void => {
      const { container } = render(<InstrumentIcon symbol='US10Y' exchange='polygon' />)

      expect(container.querySelector('svg')).not.toBeNull()
    })

    it('renders single lucide icon for ESM6-CME (S&P future on kraken_equities)', (): void => {
      const { container } = render(<InstrumentIcon symbol='ESM6-CME' exchange='kraken_equities' />)

      expect(container.querySelector('svg')).not.toBeNull()
      expect(container.querySelectorAll('img').length).toBe(0)
    })

    it('renders gold-coloured gem for GCM6-COMEX (canonical underlying GOLD resolves to gold #FFD700)', (): void => {
      const { container } = render(
        <InstrumentIcon symbol='GCM6-COMEX' exchange='kraken_equities' />
      )
      const svg = container.querySelector('svg')

      expect(svg).not.toBeNull()
      expect(svg?.getAttribute('stroke')).toBe('#FFD700')
    })

    it('renders WTI-coloured droplet for CLM6-NYMEX (canonical underlying WTI)', (): void => {
      const { container } = render(
        <InstrumentIcon symbol='CLM6-NYMEX' exchange='kraken_equities' />
      )
      const svg = container.querySelector('svg')

      expect(svg?.getAttribute('stroke')).toBe('#475569')
    })

    it('renders single lucide icon for 10YK6-CBOT (yield future)', (): void => {
      const { container } = render(
        <InstrumentIcon symbol='10YK6-CBOT' exchange='kraken_equities' />
      )
      const svg = container.querySelector('svg')

      expect(svg?.getAttribute('stroke')).toBe('#059669')
    })
  })

  describe('CME forex futures (kraken_equities) render correct flag pair', () => {
    it('renders EU + US flags for 6EM6-CME (EURUSD pair)', (): void => {
      const { container } = render(<InstrumentIcon symbol='6EM6-CME' exchange='kraken_equities' />)
      const imgs = container.querySelectorAll('img')

      expect(imgs.length).toBe(2)
      expect(imgs[0]?.getAttribute('src')).toContain('eu.svg')
      expect(imgs[1]?.getAttribute('src')).toContain('us.svg')
    })

    it('renders US + CA flags for 6CM6-CME (USDCAD pair)', (): void => {
      const { container } = render(<InstrumentIcon symbol='6CM6-CME' exchange='kraken_equities' />)
      const imgs = container.querySelectorAll('img')

      expect(imgs[0]?.getAttribute('src')).toContain('us.svg')
      expect(imgs[1]?.getAttribute('src')).toContain('ca.svg')
    })

    it('renders US + JP flags for 6JM6-CME (USDJPY pair)', (): void => {
      const { container } = render(<InstrumentIcon symbol='6JM6-CME' exchange='kraken_equities' />)
      const imgs = container.querySelectorAll('img')

      expect(imgs[0]?.getAttribute('src')).toContain('us.svg')
      expect(imgs[1]?.getAttribute('src')).toContain('jp.svg')
    })
  })

  describe('size prop forwarding', () => {
    it('forwards size to single icon', (): void => {
      const { container } = render(<InstrumentIcon symbol='BTC-USD' exchange='kraken' size={40} />)
      const img = container.querySelector('img')

      expect(img?.getAttribute('width')).toBe('40')
    })

    it('forwards size to pair icon', (): void => {
      const { container } = render(
        <InstrumentIcon symbol='EUR-USD' exchange='walutomat' size={40} />
      )
      const wrap = container.firstElementChild as HTMLElement

      expect(wrap.style.height).toBe('40px')
    })
  })

  describe('borderColor forwarding', () => {
    it('forwards borderColor to PairIcon', (): void => {
      const { container } = render(
        <InstrumentIcon symbol='EUR-USD' exchange='walutomat' borderColor='red' />
      )
      const baseSpan = container.querySelector('span > span')

      expect((baseSpan as HTMLElement).style.border).toContain('red')
    })
  })

  describe('vendored-icon manifest fallback (air-gap friendly)', () => {
    it('renders textual badge for crypto missing from local manifest (e.g. SHIB)', (): void => {
      const { container } = render(<InstrumentIcon symbol='SHIB-EUR' exchange='kraken' />)
      const imgs = container.querySelectorAll('img')

      expect(imgs.length).toBe(1)
      const badges = container.querySelectorAll('span[role="img"]')

      expect(badges.length).toBeGreaterThan(0)
    })

    it('renders textual badge for both legs when both miss the manifest', (): void => {
      const { container } = render(<InstrumentIcon symbol='ARB-OP' exchange='kraken' />)

      expect(container.querySelectorAll('img').length).toBe(0)
    })
  })

  describe('unknown / fallback', () => {
    it('renders single icon for unknown asset class (kraken_equities exotic)', (): void => {
      const { container } = render(
        <InstrumentIcon symbol='XYZ-NOTHING' exchange='kraken_equities' />
      )

      expect(container.querySelector('svg')).not.toBeNull()
    })

    it('renders fallback for empty symbol when treated as equity', (): void => {
      const { container } = render(<InstrumentIcon symbol='UNKNOWNSTOCK' exchange='kraken' />)

      expect(container.querySelector('svg')).not.toBeNull()
    })
  })
})
