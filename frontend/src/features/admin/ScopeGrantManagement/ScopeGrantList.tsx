import React, { useState } from 'react'
import { Plus, ArrowRightLeft, Link2 } from 'lucide-react'
import { Button, Badge } from '../../../components/ui'
import { useWallets, useScopeGrants, useOperators } from '../../../hooks/queries'
import { ThemeSelect } from '../../../components/ThemeSelect'
import type { ScopeGrantInfo, WalletInfo, OperatorInfo } from '../../../types/api'

interface ScopeGrantListProps {
  onCreateGrant: () => void
  onHandover: (grant: ScopeGrantInfo) => void
  readOnly?: boolean
}

const ScopeGrantList: React.FC<Readonly<ScopeGrantListProps>> = ({
  onCreateGrant,
  onHandover,
  readOnly,
}) => {
  const [selectedWallet, setSelectedWallet] = useState<string>('')
  const { data: walletsData } = useWallets()
  const { data: operatorsData } = useOperators()
  const { data: grantsData, isLoading, error } = useScopeGrants(selectedWallet)

  const wallets: WalletInfo[] = walletsData?.payload ?? []
  const operators: OperatorInfo[] = operatorsData?.payload ?? []
  const grants: ScopeGrantInfo[] = grantsData?.payload ?? []

  const operatorLabel = (publicId: string): string => {
    const op = operators.find(o => o.public_id === publicId)

    return op?.label ?? publicId
  }

  const formatDate = (dateString: string): string => {
    return new Date(dateString).toLocaleDateString('en-US', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
    })
  }

  return (
    <div className='space-y-4'>
      <div className='flex flex-wrap items-center justify-between gap-2'>
        <div className='flex items-center space-x-4'>
          <h2 className='text-2xl font-bold text-alpine-900'>Scope Grants</h2>
          {selectedWallet && (
            <Badge variant='outline' className='text-sm'>
              {grants.length} grants
            </Badge>
          )}
        </div>
        <Button onClick={onCreateGrant} disabled={readOnly} className='flex items-center space-x-2'>
          <Plus className='w-4 h-4' />
          <span>Create Grant</span>
        </Button>
      </div>
      <div className='max-w-xs'>
        <label htmlFor='wallet-filter' className='block text-sm font-medium text-alpine-900 mb-1'>
          Select wallet
        </label>
        <ThemeSelect
          id='wallet-filter'
          value={selectedWallet}
          onChange={val => setSelectedWallet(val)}
          options={wallets.map(w => ({
            value: w.public_id,
            label: `${w.label}${w.is_paper ? ' (paper)' : ''}`,
          }))}
          placeholder='Choose a wallet...'
        />
      </div>
      {!selectedWallet && (
        <div className='text-center py-12'>
          <Link2 className='mx-auto h-12 w-12 text-muted-400' />
          <h3 className='mt-2 text-sm font-medium text-alpine-900'>Select a wallet</h3>
          <p className='mt-1 text-sm text-muted-500'>
            Choose a wallet above to view its scope grants.
          </p>
        </div>
      )}
      {selectedWallet && isLoading && (
        <div className='flex items-center justify-center p-8'>
          <div className='animate-spin rounded-full h-8 w-8 border-b-2 border-brand-600'></div>
        </div>
      )}
      {selectedWallet && error && (
        <div className='p-4 text-loss-600 bg-loss-50 rounded-lg'>
          Error loading grants: {error instanceof Error ? error.message : 'Unknown error'}
        </div>
      )}
      {selectedWallet && !isLoading && !error && grants.length === 0 && (
        <div className='text-center py-12'>
          <Link2 className='mx-auto h-12 w-12 text-muted-400' />
          <h3 className='mt-2 text-sm font-medium text-alpine-900'>No grants found</h3>
          <p className='mt-1 text-sm text-muted-500'>This wallet has no active scope grants.</p>
        </div>
      )}
      {selectedWallet && !isLoading && !error && grants.length > 0 && (
        <div className='bg-alpine-50 shadow-sm rounded-lg border border-dark-600 overflow-hidden'>
          <div className='overflow-x-auto'>
            <table className='min-w-full divide-y divide-dark-600'>
              <thead className='bg-dark-700'>
                <tr>
                  <th className='px-3 py-3 text-left text-xs font-medium text-muted-600 uppercase tracking-wider'>
                    Operator
                  </th>
                  <th className='px-3 py-3 text-left text-xs font-medium text-muted-600 uppercase tracking-wider'>
                    Scope
                  </th>
                  <th className='px-3 py-3 text-left text-xs font-medium text-muted-600 uppercase tracking-wider'>
                    Target
                  </th>
                  <th className='hidden xl:table-cell px-3 py-3 text-left text-xs font-medium text-muted-600 uppercase tracking-wider'>
                    Granted At
                  </th>
                  <th className='px-3 py-3 text-right text-xs font-medium text-muted-600 uppercase tracking-wider'>
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody className='bg-alpine-50 divide-y divide-dark-600'>
                {grants.map(grant => (
                  <tr key={grant.public_id} className='hover:bg-dark-700'>
                    <td className='px-3 py-4 whitespace-nowrap'>
                      <div className='text-sm font-medium text-alpine-900'>
                        {operatorLabel(grant.operator_public_id)}
                      </div>
                      <div className='text-xs text-muted-500'>{grant.operator_public_id}</div>
                    </td>
                    <td className='px-3 py-4 whitespace-nowrap'>
                      <span className='inline-flex items-center rounded-full border border-brand-200 bg-brand-100 px-2.5 py-0.5 text-xs font-medium text-brand-800'>
                        {grant.scope_kind}
                      </span>
                    </td>
                    <td className='px-3 py-4 whitespace-nowrap text-sm text-alpine-900'>
                      {grant.scope_kind === 'underlying'
                        ? grant.underlying_public_id
                        : grant.instrument_public_id}
                    </td>
                    <td className='hidden xl:table-cell px-3 py-4 whitespace-nowrap text-sm text-muted-500'>
                      {formatDate(grant.timestamp)}
                    </td>
                    <td className='px-3 py-4 whitespace-nowrap text-right text-sm font-medium'>
                      <Button
                        variant='secondary'
                        size='sm'
                        onClick={() => onHandover(grant)}
                        disabled={readOnly}
                        className='text-brand-600 hover:text-brand-900'
                      >
                        <ArrowRightLeft className='w-4 h-4' />
                        <span className='ml-1'>Handover</span>
                      </Button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}

export default ScopeGrantList
