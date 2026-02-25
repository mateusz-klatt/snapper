import React from 'react'
import { noop } from '../lib/noop'

type ConfirmDialogState = {
  open: boolean
  title: string
  message: string
  variant: 'default' | 'danger'
}

type OpenConfirmOptions = {
  title: string
  message: string
  onConfirm: () => void
  variant?: 'default' | 'danger'
}

type ConfirmDialogProps = {
  open: boolean
  title: string
  message: string
  variant: 'default' | 'danger'
  onConfirm: () => void
  onCancel: () => void
}

type UseConfirmDialog = {
  openConfirm: (options: OpenConfirmOptions) => void
  dialogProps: ConfirmDialogProps
}

const CLOSED: ConfirmDialogState = {
  open: false,
  title: '',
  message: '',
  variant: 'default',
}

export const useConfirmDialog = (): UseConfirmDialog => {
  const [state, setState] = React.useState<ConfirmDialogState>(CLOSED)
  const onConfirmRef = React.useRef<() => void>(noop)

  const close = React.useCallback(() => setState(prev => ({ ...prev, open: false })), [])

  const openConfirm = React.useCallback(
    ({ title, message, onConfirm, variant = 'default' }: OpenConfirmOptions) => {
      onConfirmRef.current = onConfirm
      setState({ open: true, title, message, variant })
    },
    []
  )

  const handleConfirm = React.useCallback(() => {
    onConfirmRef.current()
    close()
  }, [close])

  return {
    openConfirm,
    dialogProps: {
      open: state.open,
      title: state.title,
      message: state.message,
      variant: state.variant,
      onConfirm: handleConfirm,
      onCancel: close,
    },
  }
}
