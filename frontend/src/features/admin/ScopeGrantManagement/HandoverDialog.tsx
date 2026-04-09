import React, { useState } from 'react'
import { ArrowRightLeft, X } from 'lucide-react'
import { toast } from 'react-hot-toast'
import { Button } from '../../../components/ui'
import { Modal } from '../../../components/ui/Modal'
import { ThemeSelect } from '../../../components/ThemeSelect'
import { useOperators, useHandoverScopeGrant } from '../../../hooks/queries'
import type { ScopeGrantInfo, OperatorInfo } from '../../../types/api'

interface HandoverDialogProps {
  grant: ScopeGrantInfo | null
  open: boolean
  onClose: () => void
  readOnly?: boolean
}

const HandoverDialog: React.FC<Readonly<HandoverDialogProps>> = ({
  grant,
  open,
  onClose,
  readOnly,
}) => {
  const [toOperatorId, setToOperatorId] = useState('')
  const [reason, setReason] = useState('')
  const { data: operatorsData } = useOperators()
  const handoverMutation = useHandoverScopeGrant()

  const operators: OperatorInfo[] = operatorsData?.payload ?? []
  const availableOperators = operators.filter(o => o.public_id !== grant?.operator_public_id)

  const operatorLabel = (publicId: string): string => {
    const op = operators.find(o => o.public_id === publicId)

    return op?.label ?? publicId
  }

  const handleClose = () => {
    setToOperatorId('')
    setReason('')
    onClose()
  }

  const handleHandover = () => {
    if (!grant || !toOperatorId) return

    handoverMutation.mutate(
      {
        from_grant_public_id: grant.public_id,
        to_operator_public_id: toOperatorId,
        reason: reason.trim() || undefined,
      },
      {
        onSuccess: () => {
          toast.success('Scope grant handed over')
          handleClose()
        },
        onError: (err: Error) => {
          if (
            err.message.includes('409') ||
            err.message.toLowerCase().includes('conflict') ||
            err.message.toLowerCase().includes('already')
          ) {
            toast.error('Conflict: the destination operator already holds a conflicting grant')
          } else {
            toast.error(err.message || 'Error handing over scope grant')
          }
        },
      }
    )
  }

  const scopeTarget =
    grant?.scope_kind === 'underlying' ? grant.underlying_public_id : grant?.instrument_public_id

  return (
    <Modal open={open} onClose={handleClose} title='Handover Scope Grant' size='md'>
      <div className='space-y-6'>
        {grant && (
          <div className='bg-dark-700 rounded-lg p-4 space-y-2'>
            <div className='flex justify-between text-sm'>
              <span className='text-muted-500'>Current holder</span>
              <span className='text-alpine-900 font-medium'>
                {operatorLabel(grant.operator_public_id)}
              </span>
            </div>
            <div className='flex justify-between text-sm'>
              <span className='text-muted-500'>Scope</span>
              <span className='text-alpine-900'>
                {grant.scope_kind}: {scopeTarget}
              </span>
            </div>
            <div className='flex justify-between text-sm'>
              <span className='text-muted-500'>Wallet</span>
              <span className='text-alpine-900'>{grant.wallet_public_id}</span>
            </div>
          </div>
        )}
        <div>
          <label
            htmlFor='handover-operator'
            className='block text-sm font-medium text-alpine-900 mb-2'
          >
            Transfer to operator
          </label>
          <ThemeSelect
            id='handover-operator'
            value={toOperatorId}
            onChange={setToOperatorId}
            options={availableOperators.map(o => ({ value: o.public_id, label: o.label }))}
            placeholder='Select destination operator...'
          />
        </div>
        <div>
          <label
            htmlFor='handover-reason'
            className='block text-sm font-medium text-alpine-900 mb-2'
          >
            Reason (optional)
          </label>
          <input
            type='text'
            id='handover-reason'
            value={reason}
            onChange={e => setReason(e.target.value)}
            className='w-full rounded-md border border-dark-600 bg-alpine-50 px-3 py-2 text-alpine-900 shadow-sm focus:outline-none focus:ring-2 focus:ring-brand-500'
            placeholder='e.g. Shift change'
          />
        </div>
        <div className='flex justify-end space-x-3 pt-4 border-t border-dark-600'>
          <Button
            type='button'
            variant='secondary'
            size='sm'
            onClick={handleClose}
            disabled={handoverMutation.isPending}
          >
            <X className='w-3.5 h-3.5' />
            Cancel
          </Button>
          <Button
            type='button'
            variant='primary'
            size='sm'
            loading={handoverMutation.isPending}
            disabled={!toOperatorId || readOnly}
            onClick={handleHandover}
          >
            <ArrowRightLeft className='w-3.5 h-3.5' />
            Handover
          </Button>
        </div>
      </div>
    </Modal>
  )
}

export default HandoverDialog
