import { useCallback } from 'react'
import { Clock, X } from 'lucide-react'
import { useAppStore } from '../stores/app'

export const TimeTravelPicker: React.FC = () => {
  const asOf = useAppStore(s => s.asOf)
  const isTimeTraveling = useAppStore(s => s.isTimeTraveling)
  const setAsOf = useAppStore(s => s.setAsOf)
  const clearAsOf = useAppStore(s => s.clearAsOf)

  const handleChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const value = e.target.value

      if (value) {
        setAsOf(new Date(value).toISOString())
      } else {
        clearAsOf()
      }
    },
    [setAsOf, clearAsOf]
  )

  const handleClear = useCallback(() => {
    clearAsOf()
  }, [clearAsOf])

  const toLocalDatetime = (iso: string): string => {
    const d = new Date(iso)
    const pad = (n: number) => String(n).padStart(2, '0')

    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`
  }

  const inputValue = asOf ? toLocalDatetime(asOf) : ''

  return (
    <div className='flex items-center gap-2'>
      <Clock size={14} className={isTimeTraveling ? 'text-brand-500' : 'text-muted-600'} />
      <input
        type='datetime-local'
        value={inputValue}
        onChange={handleChange}
        className='rounded-md border border-dark-600 bg-dark-800 px-2 py-1 text-xs text-alpine-900 focus:border-brand-500 focus:outline-none'
        title='Time travel: select a historical point in time'
      />
      {isTimeTraveling && (
        <button
          onClick={handleClear}
          className='rounded-md p-1 text-loss-600 hover:bg-dark-700'
          aria-label='Exit time travel mode'
          title='Return to live mode'
        >
          <X size={14} />
        </button>
      )}
    </div>
  )
}
