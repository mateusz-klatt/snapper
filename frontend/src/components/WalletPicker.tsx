import React from 'react'
import { ThemeSelect } from './ThemeSelect'
import { useAppStore } from '../stores/app'
import { useWallets } from '../hooks/queries'
import type { WalletInfo } from '../types/api'

export const WalletPicker: React.FC = () => {
  const currentId = useAppStore(s => s.currentWalletPublicId)
  const selectWalletAndRefresh = useAppStore(s => s.selectWalletAndRefresh)
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
      onChange={v => {
        // Picker change mints a new JWT with the selected wallet
        // claim before swapping client scope, so REST + WS both
        // authorise against the same wallet.
        void selectWalletAndRefresh(v === '__all__' ? null : v)
      }}
      options={options}
      placeholder='Wallet'
      className='w-44'
    />
  )
}
