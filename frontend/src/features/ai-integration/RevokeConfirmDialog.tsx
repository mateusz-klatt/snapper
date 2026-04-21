import React, { useEffect, useRef } from 'react'
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
  const mountedRef = useRef(true)

  useEffect(
    () => () => {
      mountedRef.current = false
    },
    []
  )

  const handleConfirm = async (): Promise<void> => {
    try {
      await deactivate.mutateAsync(delegate.public_id)

      if (!mountedRef.current) return
      toast.success('Delegate revoked')
      onClose()
    } catch (err) {
      if (!mountedRef.current) return
      const msg = err instanceof Error ? err.message : 'Failed to revoke delegate'

      toast.error(msg)
    }
  }

  return (
    <ConfirmDialog
      open={open}
      title={`Revoke delegate "${delegate.label}"?`}
      message='This revokes the delegate tokens and stops its MCP subscriptions. This cannot be undone.'
      confirmText='Revoke'
      cancelText='Cancel'
      variant='danger'
      onConfirm={handleConfirm}
      onCancel={onClose}
    />
  )
}
