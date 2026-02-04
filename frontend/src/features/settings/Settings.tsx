import React, { useState, useEffect } from 'react'
import { apiClient } from '../../lib/apiClient'
import type { SettingRead } from '../../types/api'
import { SettingItem } from './SettingItem'
import { AddSettingModal } from './AddSettingModal'

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
