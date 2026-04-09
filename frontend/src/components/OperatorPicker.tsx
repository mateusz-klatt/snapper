import React from 'react'
import { ThemeSelect } from './ThemeSelect'
import { useAppStore } from '../stores/app'
import { useOperators } from '../hooks/queries'
import type { OperatorInfo } from '../types/api'

export const OperatorPicker: React.FC = () => {
  const currentId = useAppStore(s => s.currentOperatorPublicId)
  const setId = useAppStore(s => s.setCurrentOperatorPublicId)
  const { data } = useOperators()
  const operators: OperatorInfo[] = data?.payload ?? []
  const options = [
    { value: '__all__', label: 'All operators' },
    ...operators.map(op => ({ value: op.public_id, label: op.label })),
  ]

  return (
    <ThemeSelect
      id='operator-picker'
      value={currentId ?? '__all__'}
      onChange={v => setId(v === '__all__' ? null : v)}
      options={options}
      placeholder='Operator'
      className='w-40'
    />
  )
}
