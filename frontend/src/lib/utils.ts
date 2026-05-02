export function getCookie(name: string): string {
  return (
    document.cookie
      .split('; ')
      .find(c => c.startsWith(name + '='))
      ?.split('=')[1] ?? ''
  )
}

export function formatNumber(value: number, options?: Intl.NumberFormatOptions): string {
  return value.toLocaleString('en-US', options)
}
