import React, { useState } from 'react'
import { Save, X } from 'lucide-react'
import { toast } from 'react-hot-toast'
import { Button } from '../../../components/ui'
import { Modal } from '../../../components/ui/Modal'
import { ThemeSelect } from '../../../components/ThemeSelect'
import { useOperators, useWallets, useCreateScopeGrant } from '../../../hooks/queries'
import type { OperatorInfo, WalletInfo } from '../../../types/api'

interface ScopeGrantFormProps {
  open: boolean
  onClose: () => void
  readOnly?: boolean
}

const ScopeGrantForm: React.FC<Readonly<ScopeGrantFormProps>> = ({ open, onClose, readOnly }) => {
  const [operatorId, setOperatorId] = useState('')
  const [walletId, setWalletId] = useState('')
  const [scopeKind, setScopeKind] = useState<'underlying' | 'instrument'>('underlying')
  const [targetId, setTargetId] = useState('')
  const [note, setNote] = useState('')
  const [errors, setErrors] = useState<Record<string, string>>({})

  const { data: operatorsData } = useOperators()
  const { data: walletsData } = useWallets()
  const createMutation = useCreateScopeGrant()

  const operators: OperatorInfo[] = operatorsData?.payload ?? []
  const wallets: WalletInfo[] = walletsData?.payload ?? []

  const resetForm = () => {
    setOperatorId('')
    setWalletId('')
    setScopeKind('underlying')
    setTargetId('')
    setNote('')
    setErrors({})
  }

  const handleClose = () => {
    resetForm()
    onClose()
  }

  const validateForm = (): boolean => {
    const newErrors: Record<string, string> = {}

    if (!operatorId) newErrors.operator = 'Operator is required'
    if (!walletId) newErrors.wallet = 'Wallet is required'
    if (!targetId.trim()) newErrors.target = 'Target ID is required'
    setErrors(newErrors)

    return Object.keys(newErrors).length === 0
  }

  const handleSubmit = async (e: React.SubmitEvent<HTMLFormElement>) => {
    e.preventDefault()

    if (!validateForm()) return

    createMutation.mutate(
      {
        operator_public_id: operatorId,
        wallet_public_id: walletId,
        scope_kind: scopeKind,
        underlying_public_id: scopeKind === 'underlying' ? targetId.trim() : undefined,
        instrument_public_id: scopeKind === 'instrument' ? targetId.trim() : undefined,
        note: note.trim() || undefined,
      },
      {
        onSuccess: () => {
          toast.success('Scope grant created')
          handleClose()
        },
        onError: (err: Error) => {
          if (
            err.message.includes('409') ||
            err.message.toLowerCase().includes('conflict') ||
            err.message.toLowerCase().includes('already')
          ) {
            toast.error('Conflict: another operator already holds a grant on this scope')
          } else {
            toast.error(err.message || 'Error creating scope grant')
          }
        },
      }
    )
  }

  return (
    <Modal open={open} onClose={handleClose} title='Create Scope Grant' size='md'>
      <form onSubmit={handleSubmit} className='space-y-6'>
        <fieldset disabled={readOnly} className='space-y-6'>
          <div>
            <label htmlFor='sg-operator' className='block text-sm font-medium text-alpine-900 mb-2'>
              Operator
            </label>
            <ThemeSelect
              id='sg-operator'
              value={operatorId}
              onChange={setOperatorId}
              options={operators.map(o => ({ value: o.public_id, label: o.label }))}
              placeholder='Select operator...'
            />
            {errors.operator && <p className='mt-1 text-sm text-loss-600'>{errors.operator}</p>}
          </div>
          <div>
            <label htmlFor='sg-wallet' className='block text-sm font-medium text-alpine-900 mb-2'>
              Wallet
            </label>
            <ThemeSelect
              id='sg-wallet'
              value={walletId}
              onChange={setWalletId}
              options={wallets.map(w => ({
                value: w.public_id,
                label: `${w.label}${w.is_paper ? ' (paper)' : ''}`,
              }))}
              placeholder='Select wallet...'
            />
            {errors.wallet && <p className='mt-1 text-sm text-loss-600'>{errors.wallet}</p>}
          </div>
          <div>
            <label
              htmlFor='sg-scope-kind'
              className='block text-sm font-medium text-alpine-900 mb-2'
            >
              Scope Kind
            </label>
            <ThemeSelect
              id='sg-scope-kind'
              value={scopeKind}
              onChange={val => setScopeKind(val as 'underlying' | 'instrument')}
              options={[
                { value: 'underlying', label: 'Underlying' },
                { value: 'instrument', label: 'Instrument' },
              ]}
            />
          </div>
          <div>
            <label htmlFor='sg-target' className='block text-sm font-medium text-alpine-900 mb-2'>
              {scopeKind === 'underlying' ? 'Underlying Public ID' : 'Instrument Public ID'}
            </label>
            <input
              type='text'
              id='sg-target'
              value={targetId}
              onChange={e => {
                setTargetId(e.target.value)

                if (errors.target) setErrors(prev => ({ ...prev, target: '' }))
              }}
              className={`w-full rounded-md border bg-alpine-50 px-3 py-2 text-alpine-900 shadow-sm focus:outline-none focus:ring-2 focus:ring-brand-500 ${
                errors.target ? 'border-loss-500' : 'border-dark-600'
              }`}
              placeholder={scopeKind === 'underlying' ? 'e.g. BTC' : 'e.g. BTC-USD-PERP'}
            />
            {errors.target && <p className='mt-1 text-sm text-loss-600'>{errors.target}</p>}
          </div>
          <div>
            <label htmlFor='sg-note' className='block text-sm font-medium text-alpine-900 mb-2'>
              Note (optional)
            </label>
            <input
              type='text'
              id='sg-note'
              value={note}
              onChange={e => setNote(e.target.value)}
              className='w-full rounded-md border border-dark-600 bg-alpine-50 px-3 py-2 text-alpine-900 shadow-sm focus:outline-none focus:ring-2 focus:ring-brand-500'
              placeholder='Optional note'
            />
          </div>
        </fieldset>
        <div className='flex justify-end space-x-3 pt-4 border-t border-dark-600'>
          <Button
            type='button'
            variant='secondary'
            size='sm'
            onClick={handleClose}
            disabled={createMutation.isPending}
          >
            <X className='w-3.5 h-3.5' />
            Cancel
          </Button>
          <Button
            type='submit'
            variant='primary'
            size='sm'
            loading={createMutation.isPending}
            disabled={readOnly}
          >
            <Save className='w-3.5 h-3.5' />
            Create Grant
          </Button>
        </div>
      </form>
    </Modal>
  )
}

export default ScopeGrantForm
