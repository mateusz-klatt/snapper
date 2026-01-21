import React, { useState, useEffect } from 'react'
import { XCircle, Edit3, Code, Plus, Trash2 } from 'lucide-react'

type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue }
interface JsonEditorProps {
  value: JsonValue
  onChange: (value: JsonValue) => void
  readOnly?: boolean
  className?: string
}

export const JsonEditor: React.FC<JsonEditorProps> = ({
  value,
  onChange,
  readOnly = false,
  className = '',
}) => {
  const [mode, setMode] = useState<'form' | 'raw'>('form')
  const [rawJson, setRawJson] = useState('')
  const [parseError, setParseError] = useState<string | null>(null)

  useEffect(() => {
    if (mode === 'raw') {
      setRawJson(JSON.stringify(value, null, 2))
    }
  }, [mode, value])

  const handleRawChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    const newValue = e.target.value

    setRawJson(newValue)

    try {
      const parsed = JSON.parse(newValue)

      setParseError(null)
      onChange(parsed)
    } catch (err) {
      if (err instanceof Error) {
        setParseError(err.message)
      }
    }
  }

  const handleImportRaw = () => {
    try {
      const parsed = JSON.parse(rawJson)

      setParseError(null)
      onChange(parsed)
      setMode('form')
    } catch (err) {
      if (err instanceof Error) {
        setParseError(err.message)
      }
    }
  }

  if (mode === 'raw') {
    return (
      <div className={className}>
        <div className='flex items-center justify-between mb-3'>
          <h3 className='text-sm font-medium text-dark-200 flex items-center gap-2'>
            <Code className='w-4 h-4' />
            Raw JSON Editor
          </h3>
          <div className='flex gap-2'>
            <button
              onClick={handleImportRaw}
              disabled={parseError !== null || readOnly}
              className='px-3 py-1 text-xs rounded bg-primary-600 text-white hover:bg-primary-700 disabled:opacity-50 disabled:cursor-not-allowed'
            >
              Import to Form
            </button>
            <button
              onClick={() => setMode('form')}
              className='px-3 py-1 text-xs rounded bg-dark-700 text-dark-200 hover:bg-dark-600'
            >
              Switch to Form
            </button>
          </div>
        </div>
        {parseError && (
          <div className='mb-3 p-2 rounded bg-red-500/10 border border-red-500/30 text-red-400 text-xs'>
            <div className='flex items-center gap-2'>
              <XCircle className='w-4 h-4' />
              <span>Parse error: {parseError}</span>
            </div>
          </div>
        )}
        <textarea
          value={rawJson}
          onChange={handleRawChange}
          readOnly={readOnly}
          className='w-full h-96 font-mono text-xs bg-dark-900 border border-dark-600 rounded p-3 text-dark-100 focus:outline-none focus:ring-2 focus:ring-primary-500'
          spellCheck={false}
        />
      </div>
    )
  }

  return (
    <div className={className}>
      <div className='flex items-center justify-between mb-3'>
        <h3 className='text-sm font-medium text-dark-200 flex items-center gap-2'>
          <Edit3 className='w-4 h-4' />
          Form Editor
        </h3>
        <button
          onClick={() => setMode('raw')}
          className='px-3 py-1 text-xs rounded bg-dark-700 text-dark-200 hover:bg-dark-600 flex items-center gap-1'
        >
          <Code className='w-3 h-3' />
          Raw JSON
        </button>
      </div>
      <JsonValueEditor value={value} onChange={onChange} readOnly={readOnly} path='' />
    </div>
  )
}

interface JsonValueEditorProps {
  value: JsonValue
  onChange: (value: JsonValue) => void
  readOnly?: boolean
  path: string
}

const JsonValueEditor: React.FC<JsonValueEditorProps> = ({ value, onChange, readOnly, path }) => {
  if (Array.isArray(value)) {
    return <ArrayEditor value={value} onChange={onChange} readOnly={readOnly} path={path} />
  }

  if (value !== null && typeof value === 'object') {
    return <ObjectEditor value={value} onChange={onChange} readOnly={readOnly} path={path} />
  }

  return <PrimitiveEditor value={value} onChange={onChange} readOnly={readOnly} path={path} />
}

const ArrayEditor: React.FC<JsonValueEditorProps> = ({ value, onChange, readOnly, path }) => {
  const arrayValue = value as JsonValue[]

  const handleItemChange = (index: number, newValue: JsonValue) => {
    const newArray = [...arrayValue]

    newArray[index] = newValue
    onChange(newArray)
  }

  const handleRemoveItem = (index: number) => {
    const newArray = arrayValue.filter((_, i) => i !== index)

    onChange(newArray)
  }

  const handleAddItem = () => {
    const defaultValue = arrayValue.length > 0 ? getDefaultValueForType(arrayValue[0]) : ''

    onChange([...arrayValue, defaultValue])
  }

  return (
    <div className='space-y-2'>
      <div className='flex items-center justify-between'>
        <span className='text-xs text-dark-400'>Array ({arrayValue.length} items)</span>
        {!readOnly && (
          <button
            onClick={handleAddItem}
            className='text-xs px-2 py-1 rounded bg-dark-700 text-dark-200 hover:bg-dark-600 flex items-center gap-1'
          >
            <Plus className='w-3 h-3' />
            Add Item
          </button>
        )}
      </div>
      <div className='space-y-2 pl-4 border-l-2 border-dark-700'>
        {arrayValue.map((item, index) => (
          <div key={index} className='flex items-start gap-2'>
            <div className='flex-1'>
              <div className='text-xs text-dark-500 mb-1'>[{index}]</div>
              <JsonValueEditor
                value={item}
                onChange={newValue => handleItemChange(index, newValue)}
                readOnly={readOnly}
                path={`${path}[${index}]`}
              />
            </div>
            {!readOnly && (
              <button
                onClick={() => handleRemoveItem(index)}
                className='mt-6 text-xs px-2 py-1 rounded bg-red-500/10 text-red-400 hover:bg-red-500/20 flex items-center gap-1'
              >
                <Trash2 className='w-3 h-3' />
                Remove
              </button>
            )}
          </div>
        ))}
      </div>
    </div>
  )
}

const ObjectEditor: React.FC<JsonValueEditorProps> = ({ value, onChange, readOnly, path }) => {
  const objectValue = value as { [key: string]: JsonValue }
  const [expanded, setExpanded] = useState<Record<string, boolean>>({})

  const handleFieldChange = (key: string, newValue: JsonValue) => {
    onChange({ ...objectValue, [key]: newValue })
  }

  const toggleExpanded = (key: string) => {
    setExpanded(prev => ({ ...prev, [key]: !prev[key] }))
  }

  return (
    <div className='space-y-2'>
      {Object.entries(objectValue).map(([key, val]) => {
        const isComplex = val !== null && (typeof val === 'object' || Array.isArray(val))
        const isExpanded = expanded[key] ?? true

        return (
          <div key={key} className='border border-dark-700 rounded p-2'>
            <div className='flex items-center justify-between mb-2'>
              <button
                onClick={() => toggleExpanded(key)}
                className='text-sm font-medium text-dark-200 hover:text-white flex items-center gap-2'
              >
                {isComplex && <span className='text-dark-500'>{isExpanded ? '▼' : '▶'}</span>}
                <span>{key}</span>
                <span className='text-xs text-dark-500'>
                  {Array.isArray(val)
                    ? `(array)`
                    : typeof val === 'object'
                      ? `(object)`
                      : `(${typeof val})`}
                </span>
              </button>
            </div>
            {(!isComplex || isExpanded) && (
              <div className={isComplex ? 'pl-4' : ''}>
                <JsonValueEditor
                  value={val}
                  onChange={newValue => handleFieldChange(key, newValue)}
                  readOnly={readOnly}
                  path={`${path}.${key}`}
                />
              </div>
            )}
          </div>
        )
      })}
    </div>
  )
}

const PrimitiveEditor: React.FC<JsonValueEditorProps> = ({ value, onChange, readOnly }) => {
  if (typeof value === 'boolean') {
    return (
      <label className='flex items-center gap-2 cursor-pointer'>
        <input
          type='checkbox'
          checked={value}
          onChange={e => onChange(e.target.checked)}
          disabled={readOnly}
          className='w-4 h-4 text-primary-500 bg-dark-700 border-dark-600 rounded focus:ring-primary-500'
        />
        <span className='text-sm text-dark-300'>{value ? 'true' : 'false'}</span>
      </label>
    )
  }

  if (typeof value === 'number') {
    return (
      <input
        type='number'
        value={value}
        onChange={e => onChange(Number(e.target.value))}
        readOnly={readOnly}
        className='w-full px-3 py-2 bg-dark-700 border border-dark-600 rounded text-white placeholder-dark-400 focus:outline-none focus:ring-2 focus:ring-primary-500'
      />
    )
  }

  const stringValue = typeof value === 'string' ? value : ''

  return (
    <input
      type='text'
      value={stringValue}
      onChange={e => onChange(e.target.value)}
      readOnly={readOnly}
      className='w-full px-3 py-2 bg-dark-700 border border-dark-600 rounded text-white placeholder-dark-400 focus:outline-none focus:ring-2 focus:ring-primary-500'
    />
  )
}

function getDefaultValueForType(sample: JsonValue): JsonValue {
  if (Array.isArray(sample)) return []
  if (sample !== null && typeof sample === 'object') return {}
  if (typeof sample === 'boolean') return false
  if (typeof sample === 'number') return 0

  return ''
}
