import React, { useState, useEffect } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { Save, X, Eye, EyeOff } from 'lucide-react'
import { toast } from 'react-hot-toast'
import { Button } from '../../../components/ui'
import { Modal } from '../../../components/ui/Modal'
import { api } from '../../../lib/apiClient'
import type {
  UserProfile,
  CreateUserRequest,
  UpdateUserRequest,
  AdminResetPasswordRequest,
} from '../../../types/api'

interface UserFormProps {
  user?: UserProfile
  open: boolean
  onClose: () => void
}

const UserForm: React.FC<Readonly<UserFormProps>> = ({ user, open, onClose }) => {
  const [formData, setFormData] = useState({
    username: user?.username || '',
    password: '',
    email: user?.email || '',
    role: user?.role || ('viewer' as const),
    is_active: user?.is_active ?? true,
  })
  const [showPassword, setShowPassword] = useState(false)
  const [resetPassword, setResetPassword] = useState(false)
  const [errors, setErrors] = useState<Record<string, string>>({})

  useEffect(() => {
    if (user) {
      setFormData({
        username: user.username,
        password: '',
        email: user.email ?? '',
        role: user.role,
        is_active: user.is_active,
      })
    } else {
      setFormData({
        username: '',
        password: '',
        email: '',
        role: 'viewer',
        is_active: true,
      })
    }

    setErrors({})
    setResetPassword(false)
  }, [user])
  const queryClient = useQueryClient()
  const isEditing = !!user
  const createUserMutation = useMutation({
    mutationFn: async (data: CreateUserRequest) => {
      const response = await api('/auth/users', {
        method: 'POST',
        body: JSON.stringify(data),
      })

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`)
      }

      return await response.json()
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['users'] })
      toast.success('User has been created')
      onClose()
    },
    onError: (error: Error) => {
      toast.error(error.message || 'Error creating user')
    },
  })
  const updateUserMutation = useMutation({
    mutationFn: async (data: UpdateUserRequest) => {
      if (!user?.id) {
        throw new Error('User ID is required for update')
      }

      const response = await api(`/auth/users/${user.id}`, {
        method: 'PUT',
        body: JSON.stringify(data),
      })

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`)
      }

      return await response.json()
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['users'] })
      toast.success('User has been updated')
      onClose()
    },
    onError: (error: Error) => {
      toast.error(error.message || 'Error updating user')
    },
  })
  const adminResetPasswordMutation = useMutation({
    mutationFn: async (data: AdminResetPasswordRequest) => {
      if (!user?.id) {
        throw new Error('User ID is required for password reset')
      }

      const response = await api(`/auth/users/${user.id}/admin-reset-password`, {
        method: 'POST',
        body: JSON.stringify(data),
      })

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`)
      }

      return await response.json()
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['users'] })
      toast.success('User password has been reset')
      onClose()
    },
    onError: (error: Error) => {
      toast.error(error.message || 'Error resetting password')
    },
  })

  const validateForm = (): boolean => {
    const newErrors: Record<string, string> = {}

    if (!formData.username.trim()) {
      newErrors.username = 'Username is required'
    } else if (formData.username.trim().length < 3) {
      newErrors.username = 'Username must be at least 3 characters'
    }

    if (!formData.email.trim()) {
      newErrors.email = 'Email is required'
    } else if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(formData.email)) {
      newErrors.email = 'Invalid email format'
    }

    if (!isEditing && !formData.password) {
      newErrors.password = 'Password is required'
    } else if (!isEditing && formData.password.length < 8) {
      newErrors.password = 'Password must be at least 8 characters'
    } else if (isEditing && resetPassword && !formData.password) {
      newErrors.password = 'Password is required when resetting'
    } else if (isEditing && resetPassword && formData.password.length < 8) {
      newErrors.password = 'Password must be at least 8 characters'
    }

    setErrors(newErrors)

    return Object.keys(newErrors).length === 0
  }

  const handleSubmit = async (e: React.SubmitEvent<HTMLFormElement>) => {
    e.preventDefault()

    if (!validateForm()) {
      return
    }

    if (isEditing) {
      if (resetPassword) {
        adminResetPasswordMutation.mutate({
          new_password: formData.password,
        })
      } else {
        updateUserMutation.mutate({
          email: formData.email,
          role: formData.role,
          is_active: formData.is_active,
        })
      }
    } else {
      createUserMutation.mutate({
        username: formData.username,
        password: formData.password,
        email: formData.email,
        role: formData.role,
        is_active: formData.is_active,
      })
    }
  }

  const handleInputChange = (field: string, value: string | boolean) => {
    setFormData(prev => ({ ...prev, [field]: value }))

    if (errors[field]) {
      setErrors(prev => ({ ...prev, [field]: '' }))
    }
  }

  const isPending =
    createUserMutation.isPending ||
    updateUserMutation.isPending ||
    adminResetPasswordMutation.isPending

  return (
    <Modal open={open} onClose={onClose} title={isEditing ? 'Edit User' : 'Add User'} size='md'>
      <form onSubmit={handleSubmit} className='space-y-6'>
        {}
        <div>
          <label
            htmlFor='username'
            className='block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2'
          >
            Username
          </label>
          <input
            type='text'
            id='username'
            value={formData.username}
            onChange={e => handleInputChange('username', e.target.value)}
            disabled={isEditing}
            className={`w-full px-3 py-2 border rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white ${
              errors.username ? 'border-red-500' : 'border-gray-300 dark:border-gray-600'
            } ${isEditing ? 'bg-gray-100 dark:bg-gray-600 cursor-not-allowed' : ''}`}
            placeholder='Enter username'
          />
          {errors.username && (
            <p className='mt-1 text-sm text-red-600 dark:text-red-400'>{errors.username}</p>
          )}
        </div>
        {}
        {isEditing && (
          <div>
            <div className='flex items-center space-x-2 mb-3'>
              <input
                type='checkbox'
                id='resetPassword'
                checked={resetPassword}
                onChange={e => setResetPassword(e.target.checked)}
                className='rounded border-gray-300 text-blue-600 focus:ring-blue-500'
              />
              <label
                htmlFor='resetPassword'
                className='text-sm font-medium text-gray-700 dark:text-gray-300'
              >
                Reset user password
              </label>
            </div>
            {resetPassword && (
              <div>
                <label
                  htmlFor='password'
                  className='block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2'
                >
                  New Password
                </label>
                <div className='relative'>
                  <input
                    type={showPassword ? 'text' : 'password'}
                    id='password'
                    value={formData.password}
                    onChange={e => handleInputChange('password', e.target.value)}
                    className={`w-full px-3 py-2 pr-10 border rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white ${
                      errors.password ? 'border-red-500' : 'border-gray-300 dark:border-gray-600'
                    }`}
                    placeholder='Enter new password'
                  />
                  <button
                    type='button'
                    onClick={() => setShowPassword(!showPassword)}
                    className='absolute inset-y-0 right-0 flex items-center pr-3 text-gray-400 hover:text-gray-600'
                  >
                    {showPassword ? <EyeOff className='w-4 h-4' /> : <Eye className='w-4 h-4' />}
                  </button>
                </div>
                {errors.password && (
                  <p className='mt-1 text-sm text-red-600 dark:text-red-400'>{errors.password}</p>
                )}
              </div>
            )}
          </div>
        )}
        {}
        {!isEditing && (
          <div>
            <label
              htmlFor='password'
              className='block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2'
            >
              Password
            </label>
            <div className='relative'>
              <input
                type={showPassword ? 'text' : 'password'}
                id='password'
                value={formData.password}
                onChange={e => handleInputChange('password', e.target.value)}
                className={`w-full px-3 py-2 pr-10 border rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white ${
                  errors.password ? 'border-red-500' : 'border-gray-300 dark:border-gray-600'
                }`}
                placeholder='Enter password'
              />
              <button
                type='button'
                onClick={() => setShowPassword(!showPassword)}
                className='absolute inset-y-0 right-0 flex items-center pr-3 text-gray-400 hover:text-gray-600'
              >
                {showPassword ? <EyeOff className='w-4 h-4' /> : <Eye className='w-4 h-4' />}
              </button>
            </div>
            {errors.password && (
              <p className='mt-1 text-sm text-red-600 dark:text-red-400'>{errors.password}</p>
            )}
          </div>
        )}
        {}
        <div>
          <label
            htmlFor='email'
            className='block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2'
          >
            Email
          </label>
          <input
            type='email'
            id='email'
            value={formData.email}
            onChange={e => handleInputChange('email', e.target.value)}
            className={`w-full px-3 py-2 border rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:border-gray-600 dark:text-white ${
              errors.email ? 'border-red-500' : 'border-gray-300 dark:border-gray-600'
            }`}
            placeholder='Enter email'
          />
          {errors.email && (
            <p className='mt-1 text-sm text-red-600 dark:text-red-400'>{errors.email}</p>
          )}
        </div>
        {}
        <div>
          <label
            htmlFor='role'
            className='block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2'
          >
            Role
          </label>
          <select
            id='role'
            value={formData.role}
            onChange={e =>
              handleInputChange('role', e.target.value as 'admin' | 'operator' | 'viewer')
            }
            className='w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white'
          >
            <option value='viewer'>Viewer</option>
            <option value='operator'>Operator</option>
            <option value='admin'>Administrator</option>
          </select>
        </div>
        {}
        <div className='flex items-center'>
          <input
            type='checkbox'
            id='is_active'
            checked={formData.is_active}
            onChange={e => handleInputChange('is_active', e.target.checked)}
            className='h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 dark:border-gray-600 rounded'
          />
          <label
            htmlFor='is_active'
            className='ml-2 block text-sm text-gray-700 dark:text-gray-300'
          >
            Account active
          </label>
        </div>
        {}
        <div className='flex justify-end space-x-3 pt-6 border-t border-gray-200 dark:border-gray-700'>
          <Button type='button' variant='secondary' onClick={onClose} disabled={isPending}>
            <X className='w-4 h-4 mr-2' />
            Cancel
          </Button>
          <Button type='submit' variant='primary' loading={isPending}>
            <Save className='w-4 h-4 mr-2' />
            {isEditing ? 'Save Changes' : 'Create User'}
          </Button>
        </div>
      </form>
    </Modal>
  )
}

export default UserForm
