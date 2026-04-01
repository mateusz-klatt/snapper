import { useState } from 'react'
import { Modal } from '../../components/ui/Modal'
import { ThemeSelect } from '../../components/ThemeSelect'
import { useIsReadOnly } from '../../hooks/useIsReadOnly'

interface AddSettingModalProps {
  open: boolean
  onClose: () => void
  onSave: (key: string, value: string, category: string, description: string) => Promise<void>
  existingCategories: string[]
}

export const AddSettingModal = ({
  open,
  onClose,
  onSave,
  existingCategories,
}: AddSettingModalProps) => {
  const readOnly = useIsReadOnly()
  const [key, setKey] = useState('')
  const [value, setValue] = useState('')
  const [category, setCategory] = useState('')
  const [newCategory, setNewCategory] = useState('')
  const [description, setDescription] = useState('')
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const resetForm = () => {
    setKey('')
    setValue('')
    setCategory('')
    setNewCategory('')
    setDescription('')
    setError(null)
  }

  const handleClose = () => {
    resetForm()
    onClose()
  }

  const handleSave = async () => {
    if (!key.trim()) {
      setError('Key is required')

      return
    }

    if (!value.trim()) {
      setError('Value is required')

      return
    }

    const finalCategory = newCategory.trim() || category

    if (!finalCategory) {
      setError('Category is required')

      return
    }

    try {
      setSaving(true)
      setError(null)
      await onSave(key.trim(), value, finalCategory, description.trim())
      handleClose()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to create setting')
    } finally {
      setSaving(false)
    }
  }

  return (
    <Modal open={open} onClose={handleClose} title='Add New Setting' size='md'>
      <div className='space-y-4'>
        {error && (
          <div className='p-3 bg-loss-50 border border-loss-500 rounded-lg'>
            <p className='text-loss-700 text-sm'>{error}</p>
          </div>
        )}
        <div>
          <label htmlFor='setting-key' className='block text-sm font-medium text-muted-700 mb-1'>
            Key <span className='text-loss-400'>*</span>
          </label>
          <input
            id='setting-key'
            type='text'
            value={key}
            onChange={e => setKey(e.target.value)}
            placeholder='e.g., walutomat.api_key'
            className='w-full px-3 py-2 text-sm bg-alpine-50 border border-dark-600 rounded-lg text-alpine-900 placeholder-muted-400 focus:outline-none focus:border-brand-500'
          />
          <p className='mt-1 text-xs text-muted-500'>
            Use dot notation for nested settings (e.g., category.subcategory.name)
          </p>
        </div>
        <div>
          <label htmlFor='setting-value' className='block text-sm font-medium text-muted-700 mb-1'>
            Value <span className='text-loss-400'>*</span>
          </label>
          <textarea
            id='setting-value'
            value={value}
            onChange={e => setValue(e.target.value)}
            placeholder='Setting value'
            rows={3}
            className='w-full px-3 py-2 text-sm bg-alpine-50 border border-dark-600 rounded-lg text-alpine-900 placeholder-muted-400 focus:outline-none focus:border-brand-500 font-mono'
          />
        </div>
        <div>
          <label
            htmlFor='setting-category'
            className='block text-sm font-medium text-muted-700 mb-1'
          >
            Category <span className='text-loss-400'>*</span>
          </label>
          <div className='flex gap-2'>
            <ThemeSelect
              id='setting-category'
              value={category}
              onChange={val => {
                setCategory(val)
                if (val) setNewCategory('')
              }}
              options={existingCategories.map(cat => ({ value: cat, label: cat }))}
              placeholder='Select existing or create new'
              className='flex-1'
            />
          </div>
          <div className='mt-2'>
            <input
              type='text'
              value={newCategory}
              onChange={e => {
                setNewCategory(e.target.value)
                if (e.target.value) setCategory('')
              }}
              placeholder='Or enter new category name'
              className='w-full px-3 py-2 text-sm bg-alpine-50 border border-dark-600 rounded-lg text-alpine-900 placeholder-muted-400 focus:outline-none focus:border-brand-500'
            />
          </div>
        </div>
        <div>
          <label
            htmlFor='setting-description'
            className='block text-sm font-medium text-muted-700 mb-1'
          >
            Description
          </label>
          <textarea
            id='setting-description'
            value={description}
            onChange={e => setDescription(e.target.value)}
            placeholder='Optional description'
            rows={2}
            className='w-full px-3 py-2 text-sm bg-alpine-50 border border-dark-600 rounded-lg text-alpine-900 placeholder-muted-400 focus:outline-none focus:border-brand-500'
          />
        </div>
        <div className='flex justify-end gap-2 pt-2'>
          <button
            onClick={handleClose}
            className='px-4 py-2 text-sm bg-muted-100 hover:bg-muted-200 text-alpine-900 rounded-lg transition-colors'
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            disabled={saving || readOnly}
            className='px-4 py-2 text-sm bg-brand-600 hover:bg-brand-700 disabled:bg-brand-800 disabled:cursor-not-allowed text-white rounded-lg transition-colors'
          >
            {saving ? 'Creating...' : 'Create Setting'}
          </button>
        </div>
      </div>
    </Modal>
  )
}
