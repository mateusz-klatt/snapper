import React, { useState, useEffect } from 'react'
import type { SettingRead } from '../../types/api'
import { JsonEditor } from './JsonEditor'
import {
  isJsonString,
  isSensitive,
  isEncrypted,
  getCategoryColor,
  getMaskedValue,
  tokenizeJson,
  type JsonValue,
  type JsonTokenType,
} from './settingsUtils'

const JSON_TOKEN_COLORS: Record<JsonTokenType, string> = {
  key: 'text-brand-600',
  string: 'text-accent-600',
  number: 'text-info-400',
  boolean: 'text-warning-500',
  null: 'text-muted-500',
  punctuation: 'text-muted-600',
  whitespace: '',
}

interface JsonSyntaxHighlightProps {
  readonly value: string
}

const JsonSyntaxHighlight: React.FC<JsonSyntaxHighlightProps> = ({ value }) => {
  const formatted = (() => {
    try {
      const parsed = JSON.parse(value)

      if (typeof parsed === 'object' && parsed !== null) {
        return JSON.stringify(parsed, null, 2)
      }
    } catch {
      /* not valid JSON */
    }

    return value
  })()
  const tokens = tokenizeJson(formatted)

  return (
    <pre className='text-xs whitespace-pre-wrap break-all font-mono'>
      {tokens.map(token => (
        <span key={token.id} className={JSON_TOKEN_COLORS[token.type]}>
          {token.value}
        </span>
      ))}
    </pre>
  )
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
      className='px-3 py-1 text-xs bg-brand-600 hover:bg-brand-700 disabled:bg-brand-800 disabled:cursor-not-allowed text-white rounded transition-colors'
    >
      {isSaving ? 'Saving...' : 'Save'}
    </button>
    <button
      onClick={onCancel}
      disabled={isSaving}
      className='px-3 py-1 text-xs border border-dark-600 bg-alpine-50 hover:bg-muted-200 disabled:bg-muted-100 disabled:cursor-not-allowed text-alpine-900 rounded transition-colors'
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
          className='border border-dark-600 rounded-lg p-3 bg-dark-700'
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
        className='w-full px-2 py-1.5 text-sm bg-white border border-dark-600 rounded text-alpine-900 focus:outline-none focus:border-brand-500 resize-vertical min-h-[60px]'
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
    <div className='bg-white border border-dark-600 rounded p-2'>
      {isJsonString(setting.value) && !isSensitive(setting.key) ? (
        <JsonSyntaxHighlight value={setting.value} />
      ) : (
        <pre className='text-xs text-alpine-900 whitespace-pre-wrap break-all'>
          {getMaskedValue(setting.key, setting.value)}
        </pre>
      )}
    </div>
    {showDeleteConfirm ? (
      <div className='flex items-center gap-2 p-2 bg-loss-50 border border-loss-700 rounded'>
        <span className='text-xs text-loss-700'>Delete this setting?</span>
        <button
          onClick={async () => {
            await onDelete(setting.key)
            setShowDeleteConfirm(false)
          }}
          disabled={isSaving}
          className='px-2 py-1 text-xs bg-loss-600 hover:bg-loss-700 disabled:bg-loss-800 disabled:cursor-not-allowed text-white rounded transition-colors'
        >
          {isSaving ? 'Deleting...' : 'Yes, Delete'}
        </button>
        <button
          onClick={() => setShowDeleteConfirm(false)}
          disabled={isSaving}
          className='px-2 py-1 text-xs border border-dark-600 bg-alpine-50 hover:bg-muted-200 disabled:cursor-not-allowed text-alpine-900 rounded transition-colors'
        >
          Cancel
        </button>
      </div>
    ) : (
      <div className='flex justify-between items-center'>
        <div className='flex gap-2'>
          <button
            onClick={() => setIsEditing(true)}
            className='px-3 py-1 text-xs border border-dark-600 bg-alpine-50 hover:bg-muted-200 text-alpine-900 rounded transition-colors'
          >
            Edit
          </button>
          <button
            onClick={() => setShowDeleteConfirm(true)}
            className='px-3 py-1 text-xs bg-loss-50 hover:bg-loss-800 text-loss-700 hover:text-white rounded transition-colors'
          >
            Delete
          </button>
        </div>
        <div className='text-xs text-muted-500'>
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
    <div className='bg-alpine-50 border border-dark-600 rounded-2xl p-3 hover:border-muted-400 transition-colors'>
      <div className='flex items-start justify-between mb-2'>
        <div className='flex-1 min-w-0'>
          <div className='flex items-center gap-2 mb-1 flex-wrap'>
            <h3 className='text-sm font-semibold text-alpine-900'>{setting.key}</h3>
            <span
              className={`px-1.5 py-0.5 rounded text-xs font-medium ${getCategoryColor(setting.category)}`}
            >
              {setting.category}
            </span>
            {isJson && (
              <span className='px-1.5 py-0.5 rounded text-xs font-medium bg-purple-50 text-purple-700'>
                📋 JSON
              </span>
            )}
            {isSensitive(setting.key) && (
              <span className='px-1.5 py-0.5 rounded text-xs font-medium bg-warning-50 text-warning-700'>
                🔒 Sensitive
              </span>
            )}
          </div>
          {setting.description && <p className='text-muted-600 text-xs'>{setting.description}</p>}
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
