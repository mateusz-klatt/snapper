import React, { useState } from 'react'
import { Modal } from '../../components/ui/Modal'

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
          <div className='p-3 bg-red-900/50 border border-red-500 rounded-lg'>
            <p className='text-red-200 text-sm'>{error}</p>
          </div>
        )}
        <div>
          <label htmlFor='setting-key' className='block text-sm font-medium text-dark-200 mb-1'>
            Key <span className='text-red-400'>*</span>
          </label>
          <input
            id='setting-key'
            type='text'
            value={key}
            onChange={e => setKey(e.target.value)}
            placeholder='e.g., walutomat.api_key'
            className='w-full px-3 py-2 text-sm bg-dark-700 border border-dark-600 rounded-lg text-white placeholder-dark-400 focus:outline-none focus:border-blue-500'
          />
          <p className='mt-1 text-xs text-dark-400'>
            Use dot notation for nested settings (e.g., category.subcategory.name)
          </p>
        </div>
        <div>
          <label htmlFor='setting-value' className='block text-sm font-medium text-dark-200 mb-1'>
            Value <span className='text-red-400'>*</span>
          </label>
          <textarea
            id='setting-value'
            value={value}
            onChange={e => setValue(e.target.value)}
            placeholder='Setting value'
            rows={3}
            className='w-full px-3 py-2 text-sm bg-dark-700 border border-dark-600 rounded-lg text-white placeholder-dark-400 focus:outline-none focus:border-blue-500 font-mono'
          />
        </div>
        <div>
          <label
            htmlFor='setting-category'
            className='block text-sm font-medium text-dark-200 mb-1'
          >
            Category <span className='text-red-400'>*</span>
          </label>
          <div className='flex gap-2'>
            <select
              id='setting-category'
              value={category}
              onChange={e => {
                setCategory(e.target.value)
                if (e.target.value) setNewCategory('')
              }}
              className='flex-1 px-3 py-2 text-sm bg-dark-700 border border-dark-600 rounded-lg text-white focus:outline-none focus:border-blue-500'
            >
              <option value=''>Select existing or create new</option>
              {existingCategories.map(cat => (
                <option key={cat} value={cat}>
                  {cat}
                </option>
              ))}
            </select>
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
              className='w-full px-3 py-2 text-sm bg-dark-700 border border-dark-600 rounded-lg text-white placeholder-dark-400 focus:outline-none focus:border-blue-500'
            />
          </div>
        </div>
        <div>
          <label
            htmlFor='setting-description'
            className='block text-sm font-medium text-dark-200 mb-1'
          >
            Description
          </label>
          <textarea
            id='setting-description'
            value={description}
            onChange={e => setDescription(e.target.value)}
            placeholder='Optional description'
            rows={2}
            className='w-full px-3 py-2 text-sm bg-dark-700 border border-dark-600 rounded-lg text-white placeholder-dark-400 focus:outline-none focus:border-blue-500'
          />
        </div>
        <div className='flex justify-end gap-2 pt-2'>
          <button
            onClick={handleClose}
            className='px-4 py-2 text-sm bg-dark-600 hover:bg-dark-500 text-white rounded-lg transition-colors'
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            disabled={saving}
            className='px-4 py-2 text-sm bg-blue-600 hover:bg-blue-700 disabled:bg-blue-800 disabled:cursor-not-allowed text-white rounded-lg transition-colors'
          >
            {saving ? 'Creating...' : 'Create Setting'}
          </button>
        </div>
      </div>
    </Modal>
  )
}
