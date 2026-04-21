import React from 'react'
import { toast } from 'react-hot-toast'
import { ConfirmDialog } from '../../components/ui/ConfirmDialog'
import { useDeactivateAiDelegate } from '../../hooks/queries'
import type { DelegateRead } from '../../types/api'

export function RevokeConfirmDialog({
  delegate,
  open,
  onClose,
}: Readonly<{
  delegate: DelegateRead
  open: boolean
  onClose: () => void
}>): React.ReactElement {
  const deactivate = useDeactivateAiDelegate()

  const handleConfirm = async (): Promise<void> => {
    try {
      await deactivate.mutateAsync(delegate.public_id)
      toast.success('Delegate deactivated')
      onClose()
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Failed to deactivate delegate'

      toast.error(msg)
    }
  }

  return (
    <ConfirmDialog
      open={open}
      title={`Deactivate delegate "${delegate.label}"?`}
      message='This revokes the delegate tokens and stops its MCP subscriptions. This cannot be undone.'
      confirmText='Deactivate'
      cancelText='Cancel'
      variant='danger'
      onConfirm={handleConfirm}
      onCancel={onClose}
    />
  )
}
