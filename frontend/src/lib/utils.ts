export function getCookie(name: string): string {
  return (
    document.cookie
      .split('; ')
      .find(c => c.startsWith(name + '='))
      ?.split('=')[1] ?? ''
  )
}
