import React, { useState } from 'react'
import { Save, X } from 'lucide-react'
import { toast } from 'react-hot-toast'
import { Button } from '../../../components/ui'
import { Modal } from '../../../components/ui/Modal'
import { ThemeSelect } from '../../../components/ThemeSelect'
import { useWallets, useCreateCredential } from '../../../hooks/queries'
import type { WalletInfo } from '../../../types/api'

const CREDENTIAL_TYPES = [
  { value: 'api_key_secret', label: 'API Key + Secret' },
  { value: 'rsa_pem', label: 'RSA PEM' },
  { value: 'oauth', label: 'OAuth' },
  { value: 'paper', label: 'Paper' },
]

const REQUIRED_FIELDS: Record<string, string[]> = {
  api_key_secret: ['api_key', 'api_secret'],
  rsa_pem: ['api_key', 'private_key_pem'],
  oauth: ['client_id', 'client_secret', 'refresh_token'],
  paper: ['initial_balance'],
}

const FIELD_LABELS: Record<string, string> = {
  api_key: 'API Key',
  api_secret: 'API Secret',
  private_key_pem: 'Private Key (PEM)',
  client_id: 'Client ID',
  client_secret: 'Client Secret',
  refresh_token: 'Refresh Token',
  initial_balance: 'Initial Balance',
}

interface CredentialFormProps {
  open: boolean
  onClose: () => void
  readOnly?: boolean
}

const CredentialForm: React.FC<Readonly<CredentialFormProps>> = ({ open, onClose, readOnly }) => {
  const [walletId, setWalletId] = useState('')
  const [exchange, setExchange] = useState('')
  const [credentialType, setCredentialType] = useState('api_key_secret')
  const [fields, setFields] = useState<Record<string, string>>({})
  const [label, setLabel] = useState('')
  const [errors, setErrors] = useState<Record<string, string>>({})

  const { data: walletsData } = useWallets()
  const createMutation = useCreateCredential()

  const wallets: WalletInfo[] = walletsData?.payload ?? []
  const requiredFields = REQUIRED_FIELDS[credentialType] ?? []

  const resetForm = () => {
    setWalletId('')
    setExchange('')
    setCredentialType('api_key_secret')
    setFields({})
    setLabel('')
    setErrors({})
  }

  const handleClose = () => {
    resetForm()
    onClose()
  }

  const handleTypeChange = (newType: string) => {
    setCredentialType(newType)
    setFields({})
    setErrors({})
  }

  const validateForm = (): boolean => {
    const newErrors: Record<string, string> = {}

    if (!walletId) newErrors.wallet = 'Wallet is required'
    if (!exchange.trim()) newErrors.exchange = 'Exchange is required'

    for (const field of requiredFields) {
      if (!fields[field]?.trim()) {
        newErrors[field] = `${FIELD_LABELS[field]} is required`
      }
    }

    setErrors(newErrors)

    return Object.keys(newErrors).length === 0
  }

  const handleSubmit = async (e: React.SubmitEvent<HTMLFormElement>) => {
    e.preventDefault()

    if (!validateForm()) return

    const payload: Record<string, string> = {}

    for (const field of requiredFields) {
      payload[field] = fields[field].trim()
    }

    createMutation.mutate(
      {
        walletPublicId: walletId,
        data: {
          exchange: exchange.trim(),
          credential_type: credentialType as 'api_key_secret' | 'rsa_pem' | 'oauth' | 'paper',
          credential_payload: payload,
          label: label.trim() || undefined,
        },
      },
      {
        onSuccess: () => {
          toast.success('Credential created')
          handleClose()
        },
        onError: (err: Error) => {
          if (err.message.includes('409') || err.message.toLowerCase().includes('already exists')) {
            toast.error('Conflict: this wallet already has an active credential for this exchange')
          } else {
            toast.error(err.message || 'Error creating credential')
          }
        },
      }
    )
  }

  return (
    <Modal open={open} onClose={handleClose} title='Add Credential' size='md'>
      <form onSubmit={handleSubmit} className='space-y-6'>
        <fieldset disabled={readOnly} className='space-y-6'>
          <div>
            <label htmlFor='cred-wallet' className='block text-sm font-medium text-alpine-900 mb-2'>
              Wallet
            </label>
            <ThemeSelect
              id='cred-wallet'
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
              htmlFor='cred-exchange'
              className='block text-sm font-medium text-alpine-900 mb-2'
            >
              Exchange
            </label>
            <input
              type='text'
              id='cred-exchange'
              value={exchange}
              onChange={e => {
                setExchange(e.target.value)

                if (errors.exchange) setErrors(prev => ({ ...prev, exchange: '' }))
              }}
              className={`w-full rounded-md border bg-alpine-50 px-3 py-2 text-alpine-900 shadow-sm focus:outline-none focus:ring-2 focus:ring-brand-500 ${
                errors.exchange ? 'border-loss-500' : 'border-dark-600'
              }`}
              placeholder='e.g. kraken'
            />
            {errors.exchange && <p className='mt-1 text-sm text-loss-600'>{errors.exchange}</p>}
          </div>
          <div>
            <label htmlFor='cred-type' className='block text-sm font-medium text-alpine-900 mb-2'>
              Credential Type
            </label>
            <ThemeSelect
              id='cred-type'
              value={credentialType}
              onChange={handleTypeChange}
              options={CREDENTIAL_TYPES}
            />
          </div>
          {requiredFields.map(field => (
            <div key={field}>
              <label
                htmlFor={`cred-field-${field}`}
                className='block text-sm font-medium text-alpine-900 mb-2'
              >
                {FIELD_LABELS[field]}
              </label>
              {field === 'private_key_pem' ? (
                <textarea
                  id={`cred-field-${field}`}
                  value={fields[field] ?? ''}
                  onChange={e => {
                    setFields(prev => ({ ...prev, [field]: e.target.value }))

                    if (errors[field]) setErrors(prev => ({ ...prev, [field]: '' }))
                  }}
                  rows={4}
                  className={`w-full rounded-md border bg-alpine-50 px-3 py-2 text-alpine-900 shadow-sm focus:outline-none focus:ring-2 focus:ring-brand-500 font-mono text-xs ${
                    errors[field] ? 'border-loss-500' : 'border-dark-600'
                  }`}
                  placeholder='-----BEGIN RSA PRIVATE KEY-----'
                />
              ) : (
                <input
                  type={
                    field.includes('secret') || field.includes('key') || field.includes('token')
                      ? 'password'
                      : 'text'
                  }
                  id={`cred-field-${field}`}
                  value={fields[field] ?? ''}
                  onChange={e => {
                    setFields(prev => ({ ...prev, [field]: e.target.value }))

                    if (errors[field]) setErrors(prev => ({ ...prev, [field]: '' }))
                  }}
                  className={`w-full rounded-md border bg-alpine-50 px-3 py-2 text-alpine-900 shadow-sm focus:outline-none focus:ring-2 focus:ring-brand-500 ${
                    errors[field] ? 'border-loss-500' : 'border-dark-600'
                  }`}
                />
              )}
              {errors[field] && <p className='mt-1 text-sm text-loss-600'>{errors[field]}</p>}
            </div>
          ))}
          <div>
            <label htmlFor='cred-label' className='block text-sm font-medium text-alpine-900 mb-2'>
              Label (optional)
            </label>
            <input
              type='text'
              id='cred-label'
              value={label}
              onChange={e => setLabel(e.target.value)}
              className='w-full rounded-md border border-dark-600 bg-alpine-50 px-3 py-2 text-alpine-900 shadow-sm focus:outline-none focus:ring-2 focus:ring-brand-500'
              placeholder='e.g. Main trading key'
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
            Add Credential
          </Button>
        </div>
      </form>
    </Modal>
  )
}

export default CredentialForm
