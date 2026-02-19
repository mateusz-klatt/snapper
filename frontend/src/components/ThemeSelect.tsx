import React from 'react'
import * as Select from '@radix-ui/react-select'
import { ChevronDownIcon } from 'lucide-react'
import { clsx } from 'clsx'

interface SelectOption {
  readonly value: string
  readonly label: string
}

interface ThemeSelectProps {
  readonly id?: string
  readonly value: string
  readonly onChange: (value: string) => void
  readonly options: readonly SelectOption[]
  readonly placeholder?: string
  readonly disabled?: boolean
  readonly className?: string
}

export const ThemeSelect: React.FC<Readonly<ThemeSelectProps>> = ({
  id,
  value,
  onChange,
  options,
  placeholder,
  disabled,
  className,
}) => (
  <Select.Root value={value || undefined} onValueChange={onChange} disabled={disabled}>
    <Select.Trigger
      id={id}
      className={clsx(
        'inline-flex cursor-pointer items-center justify-between rounded-xl border border-dark-600 bg-alpine-50 px-3 py-2 text-sm text-alpine-900',
        'hover:bg-dark-50 focus:border-brand-500 focus:outline-hidden focus:ring-2 focus:ring-brand-500',
        'disabled:cursor-not-allowed disabled:opacity-50',
        className
      )}
    >
      <Select.Value placeholder={placeholder} />
      <Select.Icon className='ml-2 text-muted-500'>
        <ChevronDownIcon size={14} />
      </Select.Icon>
    </Select.Trigger>
    <Select.Portal>
      <Select.Content
        className='z-50 overflow-hidden rounded-xl border border-dark-600 bg-alpine-50 shadow-lg'
        position='popper'
        sideOffset={4}
      >
        <Select.Viewport className='p-1'>
          {options.map(option => (
            <Select.Item
              key={option.value}
              value={option.value}
              className={clsx(
                'flex cursor-pointer select-none items-center rounded-lg px-3 py-2 text-sm text-alpine-900',
                'outline-none hover:bg-dark-700 focus:bg-dark-700 data-[highlighted]:bg-dark-700'
              )}
            >
              <Select.ItemText>{option.label}</Select.ItemText>
            </Select.Item>
          ))}
        </Select.Viewport>
      </Select.Content>
    </Select.Portal>
  </Select.Root>
)
