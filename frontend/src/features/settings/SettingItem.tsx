import React, { useState, useEffect } from 'react'
import type { SettingRead } from '../../types/api'
import { JsonEditor } from './JsonEditor'
import {
  isJsonString,
  isSensitive,
  isEncrypted,
  getCategoryColor,
  getMaskedValue,
  type JsonValue,
} from './settingsUtils'

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

export const SettingItem = ({ setting, onUpdate, onDelete, isSaving }: SettingItemProps) => {
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
