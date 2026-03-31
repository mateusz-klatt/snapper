import { useAppStore } from '../stores/app'

export function useIsReadOnly(): boolean {
  return useAppStore(s => s.isTimeTraveling)
}
