import React, { useState } from 'react'
import { useAuth } from '../../stores/auth'
import { apiClient } from '../../lib/apiClient'

interface UserProfileProps {
  className?: string
}

const UserProfile: React.FC<Readonly<UserProfileProps>> = ({ className = '' }) => {
  const [showDropdown, setShowDropdown] = useState(false)
  const [showPasswordForm, setShowPasswordForm] = useState(false)
  const [currentPassword, setCurrentPassword] = useState('')
  const [newPassword, setNewPassword] = useState('')
  const [confirmPassword, setConfirmPassword] = useState('')
  const [passwordError, setPasswordError] = useState('')
  const [passwordSuccess, setPasswordSuccess] = useState('')
  const [isChangingPassword, setIsChangingPassword] = useState(false)
  const { user, logout, isLoading } = useAuth()

  if (!user) return null

  const handleLogout = async () => {
    try {
      await logout()
    } catch (error) {
      console.error('Logout failed:', error)
    }
  }

  const resetPasswordForm = () => {
    setCurrentPassword('')
    setNewPassword('')
    setConfirmPassword('')
    setPasswordError('')
  }

  const handleChangePassword = async (e: React.SubmitEvent<HTMLFormElement>) => {
    e.preventDefault()
    setPasswordError('')
    setPasswordSuccess('')

    if (newPassword !== confirmPassword) {
      setPasswordError('New passwords do not match')

      return
    }

    if (newPassword.length < 8) {
      setPasswordError('Password must be at least 8 characters')

      return
    }

    setIsChangingPassword(true)

    try {
      await apiClient.changePassword(user.id, currentPassword, newPassword)
      setPasswordSuccess('Password changed successfully')
      resetPasswordForm()
      setTimeout(() => {
        setShowPasswordForm(false)
        setPasswordSuccess('')
      }, 2000)
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to change password'

      setPasswordError(message)
    } finally {
      setIsChangingPassword(false)
    }
  }

  const getRoleColor = (role: string) => {
    switch (role) {
      case 'admin':
        return 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200'
      case 'operator':
        return 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200'
      case 'viewer':
        return 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
      default:
        return 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-200'
    }
  }

  const getRoleIcon = (role: string) => {
    switch (role) {
      case 'admin':
        return '👑'
      case 'operator':
        return '🔧'
      case 'viewer':
        return '👁️'
      default:
        return '👤'
    }
  }

  return (
    <div className={`relative ${className}`}>
      <button
        onClick={() => setShowDropdown(!showDropdown)}
        className='flex items-center space-x-2 text-sm bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-lg px-3 py-2 hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors'
      >
        <div className='w-8 h-8 bg-gray-200 dark:bg-gray-600 rounded-full flex items-center justify-center'>
          <span className='text-lg'>{getRoleIcon(user.role)}</span>
        </div>
        <div className='hidden sm:block text-left'>
          <div className='font-medium text-gray-900 dark:text-white'>{user.username}</div>
          <div className={`text-xs px-2 py-0.5 rounded-full ${getRoleColor(user.role)}`}>
            {user.role.charAt(0).toUpperCase() + user.role.slice(1)}
          </div>
        </div>
        <svg
          className={`w-4 h-4 text-gray-500 transition-transform ${showDropdown ? 'rotate-180' : ''}`}
          fill='none'
          stroke='currentColor'
          viewBox='0 0 24 24'
        >
          <path strokeLinecap='round' strokeLinejoin='round' strokeWidth={2} d='M19 9l-7 7-7-7' />
        </svg>
      </button>
      {showDropdown && (
        <div className='absolute right-0 mt-2 w-56 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-lg z-50'>
          <div className='px-4 py-3 border-b border-gray-200 dark:border-gray-700'>
            <div className='flex items-center space-x-3'>
              <div className='w-10 h-10 bg-gray-200 dark:bg-gray-600 rounded-full flex items-center justify-center'>
                <span className='text-xl'>{getRoleIcon(user.role)}</span>
              </div>
              <div>
                <div className='font-medium text-gray-900 dark:text-white'>{user.username}</div>
                <div className='text-sm text-gray-500 dark:text-gray-400'>ID: {user.id}</div>
                <div
                  className={`text-xs px-2 py-0.5 rounded-full inline-block mt-1 ${getRoleColor(user.role)}`}
                >
                  {user.role.charAt(0).toUpperCase() + user.role.slice(1)}
                </div>
              </div>
            </div>
          </div>
          <div className='py-2'>
            <div className='px-4 py-2 text-sm text-gray-700 dark:text-gray-300'>
              <div className='font-medium mb-1'>Permissions:</div>
              <div className='text-xs space-y-1'>
                {user.role === 'admin' && (
                  <div className='text-red-600 dark:text-red-400'>• Full system administration</div>
                )}
                {(user.role === 'admin' || user.role === 'operator') && (
                  <>
                    <div className='text-blue-600 dark:text-blue-400'>• Trading operations</div>
                    <div className='text-blue-600 dark:text-blue-400'>• Strategy execution</div>
                  </>
                )}
                <div className='text-green-600 dark:text-green-400'>• Market data access</div>
              </div>
            </div>
            <div className='border-t border-gray-200 dark:border-gray-700 mt-2 pt-2'>
              <button
                onClick={() => {
                  setShowPasswordForm(true)
                  setShowDropdown(false)
                }}
                className='w-full text-left px-4 py-2 text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700'
              >
                Change password
              </button>
              <button
                onClick={handleLogout}
                disabled={isLoading}
                className='w-full text-left px-4 py-2 text-sm text-red-600 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20 disabled:opacity-50 disabled:cursor-not-allowed'
              >
                {isLoading ? (
                  <div className='flex items-center'>
                    <div className='animate-spin rounded-full h-3 w-3 border-b-2 border-red-600 mr-2'></div>
                    Signing out...
                  </div>
                ) : (
                  'Sign out'
                )}
              </button>
            </div>
          </div>
        </div>
      )}
      {}
      {showPasswordForm && (
        <div className='fixed inset-0 z-50 flex items-center justify-center bg-black/50'>
          <div className='bg-white dark:bg-gray-800 rounded-lg shadow-xl w-full max-w-md p-6'>
            <h2 className='text-lg font-semibold text-gray-900 dark:text-white mb-4'>
              Change Password
            </h2>
            {passwordError && (
              <div className='mb-4 p-3 bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400 rounded-lg text-sm'>
                {passwordError}
              </div>
            )}
            {passwordSuccess && (
              <div className='mb-4 p-3 bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400 rounded-lg text-sm'>
                {passwordSuccess}
              </div>
            )}
            <form onSubmit={handleChangePassword} className='space-y-4'>
              <div>
                <label
                  htmlFor='current-password'
                  className='block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1'
                >
                  Current Password
                </label>
                <input
                  id='current-password'
                  type='password'
                  value={currentPassword}
                  onChange={e => setCurrentPassword(e.target.value)}
                  required
                  className='w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white focus:ring-2 focus:ring-blue-500 focus:border-transparent'
                />
              </div>
              <div>
                <label
                  htmlFor='new-password'
                  className='block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1'
                >
                  New Password
                </label>
                <input
                  id='new-password'
                  type='password'
                  value={newPassword}
                  onChange={e => setNewPassword(e.target.value)}
                  required
                  minLength={8}
                  className='w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white focus:ring-2 focus:ring-blue-500 focus:border-transparent'
                />
              </div>
              <div>
                <label
                  htmlFor='confirm-password'
                  className='block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1'
                >
                  Confirm New Password
                </label>
                <input
                  id='confirm-password'
                  type='password'
                  value={confirmPassword}
                  onChange={e => setConfirmPassword(e.target.value)}
                  required
                  minLength={8}
                  className='w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white focus:ring-2 focus:ring-blue-500 focus:border-transparent'
                />
              </div>
              <div className='flex justify-end space-x-3 pt-2'>
                <button
                  type='button'
                  onClick={() => {
                    setShowPasswordForm(false)
                    resetPasswordForm()
                  }}
                  className='px-4 py-2 text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg transition-colors'
                >
                  Cancel
                </button>
                <button
                  type='submit'
                  disabled={isChangingPassword}
                  className='px-4 py-2 text-sm bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors'
                >
                  {isChangingPassword ? 'Changing...' : 'Change Password'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
      {}
      {showDropdown && (
        <button
          type='button'
          className='fixed inset-0 z-40 w-full h-full cursor-default bg-transparent border-none'
          onClick={() => setShowDropdown(false)}
          aria-label='Close dropdown'
        />
      )}
    </div>
  )
}

export default UserProfile
