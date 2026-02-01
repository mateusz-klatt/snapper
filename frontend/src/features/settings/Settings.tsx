import React, { useState, useEffect } from 'react'
import { apiClient } from '../../lib/apiClient'
import type { SettingRead } from '../../types/api'
import { JsonEditor } from './JsonEditor'
import { Modal } from '../../components/ui/Modal'

function isJsonString(str: string): boolean {
  try {
    JSON.parse(str)

    return true
  } catch {
    return false
  }
}

type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue }

const SENSITIVE_PATTERNS = [
  'api_key',
  'api_secret',
  'password',
  'secret_key',
  'private_key',
  'credential',
]

const isSensitive = (key: string): boolean => {
  const lowerKey = key.toLowerCase()

  return SENSITIVE_PATTERNS.some(pattern => lowerKey.includes(pattern))
}

const isEncrypted = (value: string): boolean => {
  return value.length >= 40 && value.startsWith('gAAAAAB')
}

const CATEGORY_COLORS: Record<string, string> = {
  trading: 'bg-green-900 text-green-200',
  auth: 'bg-red-900 text-red-200',
  risk: 'bg-yellow-900 text-yellow-200',
  zmq: 'bg-blue-900 text-blue-200',
  network: 'bg-purple-900 text-purple-200',
  system: 'bg-gray-900 text-gray-200',
}

const getCategoryColor = (category: string): string =>
  CATEGORY_COLORS[category] || 'bg-dark-600 text-dark-200'

const getMaskedValue = (key: string, value: string): string => {
  if (isSensitive(key) && value) {
    return '••••••••••••••••••••••••••••••••••••••••••••••••••'
  }

  return value || '(empty)'
}

interface SaveCancelButtonsProps {
  readonly onSave: () => Promise<void>
  readonly onCancel: () => void
  readonly isSaving: boolean
}

const SaveCancelButtons: React.FC<SaveCancelButtonsProps> = ({ onSave, onCancel, isSaving }) => (
  <div className='flex gap-2'>
    <button
      onClick={onSave}
      disabled={isSaving}
      className='px-3 py-1 text-xs bg-blue-600 hover:bg-blue-700 disabled:bg-blue-800 disabled:cursor-not-allowed text-white rounded transition-colors'
    >
      {isSaving ? 'Saving...' : 'Save'}
    </button>
    <button
      onClick={onCancel}
      disabled={isSaving}
      className='px-3 py-1 text-xs bg-dark-600 hover:bg-dark-500 disabled:bg-dark-700 disabled:cursor-not-allowed text-white rounded transition-colors'
    >
      Cancel
    </button>
  </div>
)

interface EditingViewProps {
  readonly isJson: boolean
  readonly jsonValue: JsonValue | null
  readonly setJsonValue: (v: JsonValue | null) => void
  readonly localValue: string
  readonly setLocalValue: (v: string) => void
  readonly onJsonSave: () => Promise<void>
  readonly onSave: () => Promise<void>
  readonly onCancel: () => void
  readonly isSaving: boolean
}

const EditingView: React.FC<EditingViewProps> = ({
  isJson,
  jsonValue,
  setJsonValue,
  localValue,
  setLocalValue,
  onJsonSave,
  onSave,
  onCancel,
  isSaving,
}) => {
  if (isJson && jsonValue !== null) {
    return (
      <div className='space-y-2'>
        <JsonEditor
          value={jsonValue}
          onChange={setJsonValue}
          className='border border-dark-600 rounded-lg p-3 bg-dark-900'
        />
        <SaveCancelButtons onSave={onJsonSave} onCancel={onCancel} isSaving={isSaving} />
      </div>
    )
  }

  return (
    <div className='space-y-2'>
      <textarea
        value={localValue}
        onChange={e => setLocalValue(e.target.value)}
        className='w-full px-2 py-1.5 text-sm bg-dark-700 border border-dark-600 rounded text-white focus:outline-none focus:border-blue-500 resize-vertical min-h-[60px]'
        placeholder='Enter setting value...'
      />
      <SaveCancelButtons onSave={onSave} onCancel={onCancel} isSaving={isSaving} />
    </div>
  )
}

interface DisplayViewProps {
  readonly setting: SettingRead
  readonly showDeleteConfirm: boolean
  readonly setShowDeleteConfirm: (v: boolean) => void
  readonly setIsEditing: (v: boolean) => void
  readonly onDelete: (key: string) => Promise<void>
  readonly isSaving: boolean
}

const DisplayView: React.FC<DisplayViewProps> = ({
  setting,
  showDeleteConfirm,
  setShowDeleteConfirm,
  setIsEditing,
  onDelete,
  isSaving,
}) => (
  <div className='space-y-2'>
    <div className='bg-dark-700 border border-dark-600 rounded p-2'>
      <pre className='text-xs text-dark-100 whitespace-pre-wrap break-all'>
        {getMaskedValue(setting.key, setting.value)}
      </pre>
    </div>
    {showDeleteConfirm ? (
      <div className='flex items-center gap-2 p-2 bg-red-900/30 border border-red-700 rounded'>
        <span className='text-xs text-red-200'>Delete this setting?</span>
        <button
          onClick={async () => {
            await onDelete(setting.key)
            setShowDeleteConfirm(false)
          }}
          disabled={isSaving}
          className='px-2 py-1 text-xs bg-red-600 hover:bg-red-700 disabled:bg-red-800 disabled:cursor-not-allowed text-white rounded transition-colors'
        >
          {isSaving ? 'Deleting...' : 'Yes, Delete'}
        </button>
        <button
          onClick={() => setShowDeleteConfirm(false)}
          disabled={isSaving}
          className='px-2 py-1 text-xs bg-dark-600 hover:bg-dark-500 disabled:cursor-not-allowed text-white rounded transition-colors'
        >
          Cancel
        </button>
      </div>
    ) : (
      <div className='flex justify-between items-center'>
        <div className='flex gap-2'>
          <button
            onClick={() => setIsEditing(true)}
            className='px-3 py-1 text-xs bg-dark-600 hover:bg-dark-500 text-white rounded transition-colors'
          >
            Edit
          </button>
          <button
            onClick={() => setShowDeleteConfirm(true)}
            className='px-3 py-1 text-xs bg-red-900/50 hover:bg-red-800 text-red-200 hover:text-white rounded transition-colors'
          >
            Delete
          </button>
        </div>
        <div className='text-xs text-dark-400'>
          {new Date(setting.updated_at).toLocaleString()}
          {setting.updated_by && ` • ${setting.updated_by}`}
        </div>
      </div>
    )}
  </div>
)

export const Settings = () => {
  const [settings, setSettings] = useState<SettingRead[]>([])
  const [categories, setCategories] = useState<string[]>([])
  const [selectedCategory, setSelectedCategory] = useState<string>('all')
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [searchTerm, setSearchTerm] = useState('')
  const [showAddModal, setShowAddModal] = useState(false)

  useEffect(() => {
    loadSettings()
  }, [])

  const loadSettings = async () => {
    try {
      setLoading(true)
      setError(null)
      const [settingsResponse, categoriesResponse] = await Promise.all([
        apiClient.getSettings(),
        apiClient.getSettingCategories(),
      ])

      setSettings(settingsResponse)
      setCategories(['all', ...categoriesResponse])
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load settings')
    } finally {
      setLoading(false)
    }
  }

  const updateSetting = async (
    key: string,
    value: string,
    category: string,
    description?: string | null
  ) => {
    try {
      setSaving(key)
      setError(null)
      const response = await apiClient.updateSetting(key, {
        value,
        category,
        description,
      })

      setSettings(prev => prev.map(setting => (setting.key === key ? response : setting)))
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to update setting')
    } finally {
      setSaving(null)
    }
  }

  const createSetting = async (
    key: string,
    value: string,
    category: string,
    description: string
  ) => {
    if (settings.some(s => s.key === key)) {
      throw new Error(`Setting with key "${key}" already exists`)
    }

    const response = await apiClient.updateSetting(key, {
      value,
      category,
      description,
    })

    setSettings(prev => [...prev, response])
  }

  const deleteSetting = async (key: string) => {
    try {
      setSaving(key)
      setError(null)
      await apiClient.deleteSetting(key)
      setSettings(prev => prev.filter(setting => setting.key !== key))
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to delete setting')
    } finally {
      setSaving(null)
    }
  }

  const filteredSettings = settings.filter(setting => {
    const categoryMatch = selectedCategory === 'all' || setting.category === selectedCategory
    const searchMatch =
      searchTerm === '' ||
      setting.key.toLowerCase().includes(searchTerm.toLowerCase()) ||
      setting.description?.toLowerCase().includes(searchTerm.toLowerCase())

    return categoryMatch && searchMatch
  })

  if (loading) {
    return (
      <div className='p-8'>
        <div className='animate-pulse'>
          <div className='h-8 bg-dark-700 rounded mb-4 w-48'></div>
          <div className='space-y-4'>
            {Array.from({ length: 5 }, (_, i) => (
              <div key={`loading-skeleton-${i}`} className='h-16 bg-dark-700 rounded'></div>
            ))}
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className='flex flex-col h-full'>
      {}
      <div className='flex-shrink-0 p-4 border-b border-dark-600'>
        <div className='mb-4 flex justify-between items-start'>
          <div>
            <h1 className='text-2xl font-bold text-white mb-1'>Settings</h1>
            <p className='text-dark-300 text-sm'>Configure application settings and parameters</p>
          </div>
          <button
            onClick={() => setShowAddModal(true)}
            className='px-3 py-1.5 text-sm bg-blue-600 hover:bg-blue-700 text-white rounded-lg transition-colors flex items-center gap-1.5'
          >
            <svg className='w-4 h-4' fill='none' stroke='currentColor' viewBox='0 0 24 24'>
              <path
                strokeLinecap='round'
                strokeLinejoin='round'
                strokeWidth={2}
                d='M12 4v16m8-8H4'
              />
            </svg>
            Add Setting
          </button>
        </div>
        {error && (
          <div className='mb-4 p-3 bg-red-900/50 border border-red-500 rounded-lg'>
            <p className='text-red-200 text-sm'>{error}</p>
            <button
              onClick={() => setError(null)}
              className='mt-1 text-red-300 hover:text-red-100 underline text-xs'
            >
              Dismiss
            </button>
          </div>
        )}
        {}
        <div className='flex flex-col sm:flex-row gap-3'>
          <div className='flex-1'>
            <input
              type='text'
              placeholder='Search settings...'
              value={searchTerm}
              onChange={e => setSearchTerm(e.target.value)}
              className='w-full px-3 py-1.5 text-sm bg-dark-700 border border-dark-600 rounded-lg text-white placeholder-dark-300 focus:outline-none focus:border-blue-500'
            />
          </div>
          <div>
            <select
              value={selectedCategory}
              onChange={e => setSelectedCategory(e.target.value)}
              className='px-3 py-1.5 text-sm bg-dark-700 border border-dark-600 rounded-lg text-white focus:outline-none focus:border-blue-500'
            >
              {categories.map(category => (
                <option key={category} value={category}>
                  {category === 'all'
                    ? 'All Categories'
                    : category.charAt(0).toUpperCase() + category.slice(1)}
                </option>
              ))}
            </select>
          </div>
        </div>
      </div>
      {}
      <div className='flex-1 overflow-y-auto p-4'>
        <div className='grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-3 max-w-7xl mx-auto'>
          {filteredSettings.length === 0 ? (
            <div className='col-span-full text-center py-12 text-dark-400'>
              <p>No settings found matching your criteria.</p>
            </div>
          ) : (
            filteredSettings.map(setting => (
              <SettingItem
                key={setting.key}
                setting={setting}
                onUpdate={updateSetting}
                onDelete={deleteSetting}
                isSaving={saving === setting.key}
              />
            ))
          )}
        </div>
      </div>
      {}
      <AddSettingModal
        open={showAddModal}
        onClose={() => setShowAddModal(false)}
        onSave={createSetting}
        existingCategories={categories.filter(c => c !== 'all')}
      />
    </div>
  )
}

interface SettingItemProps {
  setting: SettingRead
  onUpdate: (
    key: string,
    value: string,
    category: string,
    description?: string | null
  ) => Promise<void>
  onDelete: (key: string) => Promise<void>
  isSaving: boolean
}

const SettingItem = ({ setting, onUpdate, onDelete, isSaving }: SettingItemProps) => {
  const [localValue, setLocalValue] = useState(setting.value)
  const [isEditing, setIsEditing] = useState(false)
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false)
  const isJson = isJsonString(setting.value)
  const [jsonValue, setJsonValue] = useState<JsonValue | null>(null)

  useEffect(() => {
    setLocalValue(setting.value)

    if (isJsonString(setting.value)) {
      setJsonValue(JSON.parse(setting.value))
    }
  }, [setting.value])

  const handleSave = async () => {
    const valueChanged = localValue !== setting.value
    const isSensitiveKey = isSensitive(setting.key)
    const isCurrentlyEncrypted = isEncrypted(localValue)

    if (valueChanged || (isSensitiveKey && !isCurrentlyEncrypted)) {
      await onUpdate(setting.key, localValue, setting.category, setting.description)
    }

    setIsEditing(false)
  }

  const handleJsonSave = async () => {
    const stringValue = JSON.stringify(jsonValue, null, 2)
    const valueChanged = stringValue !== setting.value
    const isSensitiveKey = isSensitive(setting.key)
    const encryptedValueCandidate = typeof jsonValue === 'string' ? jsonValue : stringValue
    const isCurrentlyEncrypted = isEncrypted(encryptedValueCandidate)

    if (valueChanged || (isSensitiveKey && !isCurrentlyEncrypted)) {
      await onUpdate(setting.key, stringValue, setting.category, setting.description)
    }

    setIsEditing(false)
  }

  const handleCancel = () => {
    setLocalValue(setting.value)

    if (isJsonString(setting.value)) {
      setJsonValue(JSON.parse(setting.value))
    }

    setIsEditing(false)
  }

  return (
    <div className='bg-dark-800 border border-dark-600 rounded-lg p-3 hover:border-dark-500 transition-colors'>
      <div className='flex items-start justify-between mb-2'>
        <div className='flex-1 min-w-0'>
          <div className='flex items-center gap-2 mb-1 flex-wrap'>
            <h3 className='text-sm font-semibold text-white'>{setting.key}</h3>
            <span
              className={`px-1.5 py-0.5 rounded text-xs font-medium ${getCategoryColor(setting.category)}`}
            >
              {setting.category}
            </span>
            {isJson && (
              <span className='px-1.5 py-0.5 rounded text-xs font-medium bg-purple-900 text-purple-200'>
                📋 JSON
              </span>
            )}
            {isSensitive(setting.key) && (
              <span className='px-1.5 py-0.5 rounded text-xs font-medium bg-orange-900 text-orange-200'>
                🔒 Sensitive
              </span>
            )}
          </div>
          {setting.description && <p className='text-dark-300 text-xs'>{setting.description}</p>}
        </div>
      </div>
      <div className='space-y-2'>
        {isEditing ? (
          <EditingView
            isJson={isJson}
            jsonValue={jsonValue}
            setJsonValue={setJsonValue}
            localValue={localValue}
            setLocalValue={setLocalValue}
            onJsonSave={handleJsonSave}
            onSave={handleSave}
            onCancel={handleCancel}
            isSaving={isSaving}
          />
        ) : (
          <DisplayView
            setting={setting}
            showDeleteConfirm={showDeleteConfirm}
            setShowDeleteConfirm={setShowDeleteConfirm}
            setIsEditing={setIsEditing}
            onDelete={onDelete}
            isSaving={isSaving}
          />
        )}
      </div>
    </div>
  )
}

interface AddSettingModalProps {
  open: boolean
  onClose: () => void
  onSave: (key: string, value: string, category: string, description: string) => Promise<void>
  existingCategories: string[]
}

const AddSettingModal = ({ open, onClose, onSave, existingCategories }: AddSettingModalProps) => {
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
