import React from 'react'
import { ThemeSelect } from './ThemeSelect'
import { useAppStore } from '../stores/app'
import { useWallets } from '../hooks/queries'
import type { WalletInfo } from '../types/api'

export const WalletPicker: React.FC = () => {
  const currentId = useAppStore(s => s.currentWalletPublicId)
  const setId = useAppStore(s => s.setCurrentWalletPublicId)
  const { data } = useWallets()
  const wallets: WalletInfo[] = data?.payload ?? []
  const options = [
    { value: '__all__', label: 'All wallets' },
    ...wallets.map(w => ({
      value: w.public_id,
      label: `${w.label}${w.is_paper ? ' (paper)' : ''}`,
    })),
  ]

  return (
    <ThemeSelect
      id='wallet-picker'
      value={currentId ?? '__all__'}
      onChange={v => setId(v === '__all__' ? null : v)}
      options={options}
      placeholder='Wallet'
      className='w-44'
    />
  )
}
