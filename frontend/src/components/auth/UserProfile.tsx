import React, { useState } from 'react'
import { useAuth } from '../../stores/auth'
import { useChangePassword } from '../../hooks/queries'
import { useIsReadOnly } from '../../hooks/useIsReadOnly'
import { Modal } from '../ui/Modal'

interface UserProfileProps {
  className?: string
}

const UserProfile: React.FC<Readonly<UserProfileProps>> = ({ className = '' }) => {
  const readOnly = useIsReadOnly()
  const [showDropdown, setShowDropdown] = useState(false)
  const [showPasswordForm, setShowPasswordForm] = useState(false)
  const [showHelpDialog, setShowHelpDialog] = useState(false)
  const [currentPassword, setCurrentPassword] = useState('')
  const [newPassword, setNewPassword] = useState('')
  const [confirmPassword, setConfirmPassword] = useState('')
  const [passwordError, setPasswordError] = useState('')
  const [passwordSuccess, setPasswordSuccess] = useState('')
  const [isChangingPassword, setIsChangingPassword] = useState(false)
  const { user, logout, isLoading } = useAuth()
  const changePasswordMutation = useChangePassword()

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

    changePasswordMutation.mutate(
      { userId: user.username, currentPassword, newPassword },
      {
        onSuccess: () => {
          setPasswordSuccess('Password changed successfully')
          resetPasswordForm()
          setTimeout(() => {
            setShowPasswordForm(false)
            setPasswordSuccess('')
          }, 2000)
        },
        onError: (error: Error) => {
          setPasswordError(error.message || 'Failed to change password')
        },
        onSettled: () => {
          setIsChangingPassword(false)
        },
      }
    )
    setIsChangingPassword(true)
  }

  const getRoleColor = (role: string) => {
    switch (role) {
      case 'admin':
        return 'bg-loss-100 text-loss-800 border border-loss-200'
      case 'operator':
        return 'bg-brand-100 text-brand-800 border border-brand-200'
      case 'viewer':
        return 'bg-accent-100 text-accent-800 border border-accent-200'
      default:
        return 'bg-muted-200 text-muted-700 border border-muted-300'
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
        className='flex items-center space-x-2 text-sm bg-alpine-50 border border-dark-600 rounded-lg px-3 py-2 hover:bg-dark-700 transition-colors'
      >
        <div className='w-8 h-8 bg-muted-200 rounded-full flex items-center justify-center'>
          <span className='text-lg'>{getRoleIcon(user.role)}</span>
        </div>
        <div className='hidden sm:block text-left'>
          <div className='font-medium text-alpine-900'>{user.username}</div>
          <div className={`text-xs px-2 py-0.5 rounded-full ${getRoleColor(user.role)}`}>
            {user.role.charAt(0).toUpperCase() + user.role.slice(1)}
          </div>
        </div>
        <svg
          className={`w-4 h-4 text-muted-500 transition-transform ${showDropdown ? 'rotate-180' : ''}`}
          fill='none'
          stroke='currentColor'
          viewBox='0 0 24 24'
        >
          <path strokeLinecap='round' strokeLinejoin='round' strokeWidth={2} d='M19 9l-7 7-7-7' />
        </svg>
      </button>
      {showDropdown && (
        <div className='absolute right-0 mt-2 w-56 bg-alpine-50 border border-dark-600 rounded-lg shadow-lg z-50'>
          <div className='px-4 py-3 border-b border-dark-600'>
            <div className='flex items-center space-x-3'>
              <div className='w-10 h-10 bg-muted-200 rounded-full flex items-center justify-center'>
                <span className='text-xl'>{getRoleIcon(user.role)}</span>
              </div>
              <div>
                <div className='font-medium text-alpine-900'>{user.username}</div>
                <div
                  className={`text-xs px-2 py-0.5 rounded-full inline-block mt-1 ${getRoleColor(user.role)}`}
                >
                  {user.role.charAt(0).toUpperCase() + user.role.slice(1)}
                </div>
              </div>
            </div>
          </div>
          <div className='py-2'>
            <div className='px-4 py-2 text-sm text-muted-600'>
              <div className='font-medium mb-1'>Permissions:</div>
              <div className='text-xs space-y-1'>
                {user.role === 'admin' && (
                  <div className='text-loss-600'>• Full system administration</div>
                )}
                {(user.role === 'admin' || user.role === 'operator') && (
                  <>
                    <div className='text-brand-600'>• Trading operations</div>
                    <div className='text-brand-600'>• Strategy execution</div>
                  </>
                )}
                <div className='text-accent-600'>• Market data access</div>
              </div>
            </div>
            <div className='border-t border-dark-600 mt-2 pt-2'>
              <button
                onClick={() => {
                  setShowPasswordForm(true)
                  setShowDropdown(false)
                }}
                disabled={readOnly}
                className='w-full text-left px-4 py-2 text-sm text-muted-600 hover:bg-dark-700 disabled:opacity-50 disabled:cursor-not-allowed'
              >
                Change password
              </button>
              <button
                onClick={() => {
                  setShowHelpDialog(true)
                  setShowDropdown(false)
                }}
                className='w-full text-left px-4 py-2 text-sm text-muted-600 hover:bg-dark-700'
              >
                Help &amp; Documentation
              </button>
              <button
                onClick={handleLogout}
                disabled={isLoading}
                className='w-full text-left px-4 py-2 text-sm text-loss-600 hover:bg-loss-50 disabled:opacity-50 disabled:cursor-not-allowed'
              >
                {isLoading ? (
                  <div className='flex items-center'>
                    <div className='animate-spin rounded-full h-3 w-3 border-b-2 border-loss-600 mr-2'></div>
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
          <div className='bg-alpine-50 border border-dark-600 rounded-lg shadow-xl w-full max-w-md p-6'>
            <h2 className='text-lg font-semibold text-alpine-900 mb-4'>Change Password</h2>
            {passwordError && (
              <div className='mb-4 p-3 bg-loss-100 text-loss-700 rounded-lg text-sm'>
                {passwordError}
              </div>
            )}
            {passwordSuccess && (
              <div className='mb-4 p-3 bg-accent-100 text-accent-700 rounded-lg text-sm'>
                {passwordSuccess}
              </div>
            )}
            <form onSubmit={handleChangePassword} className='space-y-4'>
              <div>
                <label
                  htmlFor='current-password'
                  className='block text-sm font-medium text-muted-700 mb-1'
                >
                  Current Password
                </label>
                <input
                  id='current-password'
                  type='password'
                  value={currentPassword}
                  onChange={e => setCurrentPassword(e.target.value)}
                  required
                  className='w-full px-3 py-2 border border-dark-600 rounded-lg bg-dark-700 text-alpine-900 focus:ring-2 focus:ring-brand-500 focus:border-transparent'
                />
              </div>
              <div>
                <label
                  htmlFor='new-password'
                  className='block text-sm font-medium text-muted-700 mb-1'
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
                  className='w-full px-3 py-2 border border-dark-600 rounded-lg bg-dark-700 text-alpine-900 focus:ring-2 focus:ring-brand-500 focus:border-transparent'
                />
              </div>
              <div>
                <label
                  htmlFor='confirm-password'
                  className='block text-sm font-medium text-muted-700 mb-1'
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
                  className='w-full px-3 py-2 border border-dark-600 rounded-lg bg-dark-700 text-alpine-900 focus:ring-2 focus:ring-brand-500 focus:border-transparent'
                />
              </div>
              <div className='flex justify-end space-x-3 pt-2'>
                <button
                  type='button'
                  onClick={() => {
                    setShowPasswordForm(false)
                    resetPasswordForm()
                  }}
                  className='px-4 py-2 text-sm text-muted-600 hover:bg-dark-700 rounded-lg transition-colors'
                >
                  Cancel
                </button>
                <button
                  type='submit'
                  disabled={isChangingPassword || readOnly}
                  className='px-4 py-2 text-sm bg-brand-600 text-white rounded-lg hover:bg-brand-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors'
                >
                  {isChangingPassword ? 'Changing...' : 'Change Password'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
      {}
      <Modal
        open={showHelpDialog}
        onClose={() => setShowHelpDialog(false)}
        title='Help & Documentation'
      >
        <div className='space-y-4 text-sm'>
          <section>
            <h4 className='font-semibold text-alpine-900 mb-2'>Role Capabilities</h4>
            <div className='space-y-2 text-muted-600'>
              <div>
                <span className='font-medium text-loss-600'>Admin</span> — Full system access
                including user management, system settings, and all Operator permissions.
              </div>
              <div>
                <span className='font-medium text-brand-600'>Operator</span> — Trading operations,
                strategy execution, process management, and system health monitoring.
              </div>
              <div>
                <span className='font-medium text-accent-600'>Viewer</span> — Read-only access to
                Overview and Market Data.
              </div>
            </div>
          </section>
          <section>
            <h4 className='font-semibold text-alpine-900 mb-2'>Quick Reference</h4>
            <ul className='list-disc list-inside text-muted-600 space-y-1'>
              <li>Use the sidebar menu to navigate between sections</li>
              <li>Dark mode toggle is available in the header bar</li>
              <li>Export data to CSV from Orders and Signals pages</li>
              <li>Strategy start/stop actions require confirmation</li>
            </ul>
          </section>
          <section>
            <h4 className='font-semibold text-alpine-900 mb-2'>API Documentation</h4>
            <ul className='list-disc list-inside space-y-1 text-muted-600'>
              <li>
                <a
                  href='/docs'
                  target='_blank'
                  rel='noreferrer'
                  className='text-primary-500 hover:text-primary-400 underline'
                >
                  Swagger UI
                </a>{' '}
                — interactive API explorer
              </li>
              <li>
                <a
                  href='/redoc'
                  target='_blank'
                  rel='noreferrer'
                  className='text-primary-500 hover:text-primary-400 underline'
                >
                  ReDoc
                </a>{' '}
                — API reference documentation
              </li>
            </ul>
          </section>
        </div>
      </Modal>
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
