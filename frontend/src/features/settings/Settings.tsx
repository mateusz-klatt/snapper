import { useState } from 'react'
import { SettingItem } from './SettingItem'
import { AddSettingModal } from './AddSettingModal'
import { ThemeSelect } from '../../components/ThemeSelect'
import {
  useSettings,
  useSettingCategories,
  useUpdateSetting,
  useDeleteSetting,
} from '../../hooks/queries'
import { useIsReadOnly } from '../../hooks/useIsReadOnly'

export const Settings = () => {
  const readOnly = useIsReadOnly()
  const [selectedCategory, setSelectedCategory] = useState<string>('all')
  const [searchTerm, setSearchTerm] = useState('')
  const [showAddModal, setShowAddModal] = useState(false)

  const { data: settings = [], isLoading, error: queryError } = useSettings()
  const { data: rawCategories = [] } = useSettingCategories()
  const updateMutation = useUpdateSetting()
  const deleteMutation = useDeleteSetting()

  const categories = ['all', ...rawCategories]
  const displayError = queryError ?? updateMutation.error ?? deleteMutation.error
  const error = (() => {
    if (!displayError) return null
    if (displayError instanceof Error) return displayError.message
    if (updateMutation.error) return 'Failed to update setting'
    if (deleteMutation.error) return 'Failed to delete setting'

    return 'Failed to load settings'
  })()

  let savingKey: string | null = null

  if (updateMutation.isPending && updateMutation.variables) {
    savingKey = updateMutation.variables.key
  } else if (deleteMutation.isPending && deleteMutation.variables) {
    savingKey = deleteMutation.variables
  }

  const updateSetting = async (
    key: string,
    value: string,
    category: string,
    description?: string | null
  ) => {
    try {
      await updateMutation.mutateAsync({ key, data: { value, category, description } })
    } catch {
      /* error is captured in updateMutation.error */
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

    await updateMutation.mutateAsync({ key, data: { value, category, description } })
  }

  const deleteSetting = async (key: string) => {
    try {
      await deleteMutation.mutateAsync(key)
    } catch {
      /* error is captured in deleteMutation.error */
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

  if (isLoading) {
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
    <div className='space-y-0'>
      {}
      <div className='pb-4 border-b border-dark-600'>
        <div className='mb-4 flex justify-between items-start'>
          <div>
            <h1 className='text-2xl font-bold text-alpine-900 mb-1'>Settings</h1>
            <p className='text-muted-600 text-sm'>Configure application settings and parameters</p>
          </div>
          <button
            onClick={() => setShowAddModal(true)}
            disabled={readOnly}
            className='px-3 py-1.5 text-sm bg-brand-600 hover:bg-brand-700 text-white rounded-lg transition-colors flex items-center gap-1.5 disabled:opacity-50 disabled:cursor-not-allowed'
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
          <div className='mb-4 p-3 bg-loss-50 border border-loss-500 rounded-lg'>
            <p className='text-loss-700 text-sm'>{error}</p>
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
              className='input text-sm'
            />
          </div>
          <div>
            <ThemeSelect
              value={selectedCategory}
              onChange={setSelectedCategory}
              options={categories.map(category => ({
                value: category,
                label:
                  category === 'all'
                    ? 'All Categories'
                    : category.charAt(0).toUpperCase() + category.slice(1),
              }))}
            />
          </div>
        </div>
      </div>
      {}
      <div className='pt-4'>
        <div className='grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-3'>
          {filteredSettings.length === 0 ? (
            <div className='col-span-full text-center py-12 text-muted-500'>
              <p>No settings found matching your criteria.</p>
            </div>
          ) : (
            filteredSettings.map(setting => (
              <SettingItem
                key={setting.key}
                setting={setting}
                onUpdate={updateSetting}
                onDelete={deleteSetting}
                isSaving={savingKey === setting.key}
                readOnly={readOnly}
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
