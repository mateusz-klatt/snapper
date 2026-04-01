import React, { useState, useEffect } from 'react'
import { Save, X, Eye, EyeOff } from 'lucide-react'
import { toast } from 'react-hot-toast'
import { Button } from '../../../components/ui'
import { Modal } from '../../../components/ui/Modal'
import { ThemeSelect } from '../../../components/ThemeSelect'
import { useCreateUser, useUpdateUser, useAdminResetPassword } from '../../../hooks/queries'
import type { UserProfile, UserRole } from '../../../types/api'

interface UserFormProps {
  user?: UserProfile
  open: boolean
  onClose: () => void
  readOnly?: boolean
}

const UserForm: React.FC<Readonly<UserFormProps>> = ({ user, open, onClose, readOnly }) => {
  const [formData, setFormData] = useState<{
    username: string
    password: string
    email: string
    role: UserRole
    is_active: boolean
  }>({
    username: user?.username || '',
    password: '',
    email: user?.email || '',
    role: user?.role || 'viewer',
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
  const isEditing = !!user
  const createMutation = useCreateUser()
  const updateMutation = useUpdateUser()
  const resetPasswordMutation = useAdminResetPassword()
  const createUserMutation = {
    mutate: (data: {
      username: string
      password: string
      email: string
      role: UserRole
      is_active: boolean
    }) => {
      createMutation.mutate(data, {
        onSuccess: () => {
          toast.success('User has been created')
          onClose()
        },
        onError: (err: Error) => toast.error(err.message || 'Error creating user'),
      })
    },
    isPending: createMutation.isPending,
  }
  const updateUserMutation = {
    mutate: (data: { email: string; role: UserRole; is_active: boolean }) => {
      updateMutation.mutate(
        { userId: formData.username, data },
        {
          onSuccess: () => {
            toast.success('User has been updated')
            onClose()
          },
          onError: (err: Error) => toast.error(err.message || 'Error updating user'),
        }
      )
    },
    isPending: updateMutation.isPending,
  }
  const adminResetPasswordMutation = {
    mutate: (data: { new_password: string }) => {
      resetPasswordMutation.mutate(
        { userId: formData.username, data },
        {
          onSuccess: () => {
            toast.success('User password has been reset')
            onClose()
          },
          onError: (err: Error) => toast.error(err.message || 'Error resetting password'),
        }
      )
    },
    isPending: resetPasswordMutation.isPending,
  }

  const validateForm = (): boolean => {
    const newErrors: Record<string, string> = {}

    if (!formData.username.trim()) {
      newErrors.username = 'Username is required'
    } else if (formData.username.trim().length < 3) {
      newErrors.username = 'Username must be at least 3 characters'
    }

    if (!formData.email.trim()) {
      newErrors.email = 'Email is required'
    } else if (!/^[^\s@]+@[^\s@.]+(?:\.[^\s@.]+)+$/.test(formData.email)) {
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
        <fieldset disabled={readOnly} className='space-y-6'>
          <div>
            <label htmlFor='username' className='block text-sm font-medium text-alpine-900 mb-2'>
              Username
            </label>
            <input
              type='text'
              id='username'
              value={formData.username}
              onChange={e => handleInputChange('username', e.target.value)}
              disabled={isEditing}
              className={`w-full rounded-md border bg-alpine-50 px-3 py-2 text-alpine-900 shadow-sm focus:outline-none focus:ring-2 focus:ring-brand-500  ${
                errors.username ? 'border-loss-500' : 'border-dark-600'
              } ${isEditing ? 'bg-muted-100 cursor-not-allowed bg-muted-100' : ''}`}
              placeholder='Enter username'
            />
            {errors.username && <p className='mt-1 text-sm text-loss-600 '>{errors.username}</p>}
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
                  className='rounded border-alpine-200 text-brand-600 focus:ring-brand-500'
                />
                <label htmlFor='resetPassword' className='text-sm font-medium text-alpine-900'>
                  Reset user password
                </label>
              </div>
              {resetPassword && (
                <div>
                  <label
                    htmlFor='password'
                    className='block text-sm font-medium text-alpine-900 mb-2'
                  >
                    New Password
                  </label>
                  <div className='relative'>
                    <input
                      type={showPassword ? 'text' : 'password'}
                      id='password'
                      value={formData.password}
                      onChange={e => handleInputChange('password', e.target.value)}
                      className={`w-full rounded-md border bg-alpine-50 px-3 py-2 pr-10 text-alpine-900 shadow-sm focus:outline-none focus:ring-2 focus:ring-brand-500  ${
                        errors.password ? 'border-loss-500' : 'border-dark-600'
                      }`}
                      placeholder='Enter new password'
                    />
                    <button
                      type='button'
                      onClick={() => setShowPassword(!showPassword)}
                      className='absolute inset-y-0 right-0 flex items-center pr-3 text-muted-400 hover:text-muted-600'
                    >
                      {showPassword ? <EyeOff className='w-4 h-4' /> : <Eye className='w-4 h-4' />}
                    </button>
                  </div>
                  {errors.password && (
                    <p className='mt-1 text-sm text-loss-600 '>{errors.password}</p>
                  )}
                </div>
              )}
            </div>
          )}
          {}
          {!isEditing && (
            <div>
              <label htmlFor='password' className='block text-sm font-medium text-alpine-900 mb-2'>
                Password
              </label>
              <div className='relative'>
                <input
                  type={showPassword ? 'text' : 'password'}
                  id='password'
                  value={formData.password}
                  onChange={e => handleInputChange('password', e.target.value)}
                  className={`w-full rounded-md border bg-alpine-50 px-3 py-2 pr-10 text-alpine-900 shadow-sm focus:outline-none focus:ring-2 focus:ring-brand-500  ${
                    errors.password ? 'border-loss-500' : 'border-dark-600'
                  }`}
                  placeholder='Enter password'
                />
                <button
                  type='button'
                  onClick={() => setShowPassword(!showPassword)}
                  className='absolute inset-y-0 right-0 flex items-center pr-3 text-muted-400 hover:text-muted-600'
                >
                  {showPassword ? <EyeOff className='w-4 h-4' /> : <Eye className='w-4 h-4' />}
                </button>
              </div>
              {errors.password && <p className='mt-1 text-sm text-loss-600 '>{errors.password}</p>}
            </div>
          )}
          {}
          <div>
            <label htmlFor='email' className='block text-sm font-medium text-alpine-900 mb-2'>
              Email
            </label>
            <input
              type='email'
              id='email'
              value={formData.email}
              onChange={e => handleInputChange('email', e.target.value)}
              className={`w-full rounded-md border bg-alpine-50 px-3 py-2 text-alpine-900 shadow-sm focus:outline-none focus:ring-2 focus:ring-brand-500  ${
                errors.email ? 'border-loss-500' : 'border-dark-600'
              }`}
              placeholder='Enter email'
            />
            {errors.email && <p className='mt-1 text-sm text-loss-600 '>{errors.email}</p>}
          </div>
          {}
          <div>
            <label htmlFor='role' className='block text-sm font-medium text-alpine-900 mb-2'>
              Role
            </label>
            <ThemeSelect
              id='role'
              value={formData.role}
              onChange={val => handleInputChange('role', val as 'admin' | 'operator' | 'viewer')}
              options={[
                { value: 'viewer', label: 'Viewer' },
                { value: 'operator', label: 'Operator' },
                { value: 'admin', label: 'Administrator' },
              ]}
            />
          </div>
          {}
          <div className='flex items-center'>
            <input
              type='checkbox'
              id='is_active'
              checked={formData.is_active}
              onChange={e => handleInputChange('is_active', e.target.checked)}
              className='h-4 w-4 text-brand-600 focus:ring-brand-500 border-dark-600 rounded'
            />
            <label htmlFor='is_active' className='ml-2 block text-sm text-alpine-900'>
              Account active
            </label>
          </div>
        </fieldset>
        <div className='flex justify-end space-x-3 pt-4 border-t border-dark-600'>
          <Button
            type='button'
            variant='secondary'
            size='sm'
            onClick={onClose}
            disabled={isPending}
          >
            <X className='w-3.5 h-3.5' />
            Cancel
          </Button>
          <Button type='submit' variant='primary' size='sm' loading={isPending} disabled={readOnly}>
            <Save className='w-3.5 h-3.5' />
            {isEditing ? 'Save Changes' : 'Create User'}
          </Button>
        </div>
      </form>
    </Modal>
  )
}

export default UserForm
