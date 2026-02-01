import React, { useState } from 'react'
import { useAuth } from '../../stores/auth'

interface LoginFormProps {
  onSuccess?: () => void
  className?: string
}

const LoginForm: React.FC<Readonly<LoginFormProps>> = ({ onSuccess, className = '' }) => {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const { login, isLoading, error, clearError } = useAuth()

  const handleSubmit = async (e: React.SubmitEvent<HTMLFormElement>) => {
    e.preventDefault()
    clearError()

    try {
      await login({ username, password, remember_me: false })
      onSuccess?.()
    } catch (error) {
      console.error('Login failed:', error)
    }
  }

  return (
    <div
      className={`max-w-md mx-auto bg-white dark:bg-gray-800 rounded-lg shadow-md p-6 ${className}`}
    >
      <div className='mb-6'>
        <h2 className='text-2xl font-bold text-gray-900 dark:text-white text-center'>
          Snapper Trading Login
        </h2>
        <p className='text-gray-600 dark:text-gray-300 text-center mt-2'>
          Sign in to access the trading dashboard
        </p>
      </div>
      {error && (
        <div className='mb-4 p-3 bg-red-100 border border-red-400 text-red-700 rounded-sm'>
          {error}
        </div>
      )}
      <form onSubmit={handleSubmit} className='space-y-4'>
        <div>
          <label
            htmlFor='username'
            className='block text-sm font-medium text-gray-700 dark:text-gray-300'
          >
            Username
          </label>
          <input
            id='username'
            type='text'
            value={username}
            onChange={e => setUsername(e.target.value)}
            required
            disabled={isLoading}
            className='mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-xs focus:outline-hidden focus:ring-blue-500 focus:border-blue-500 disabled:bg-gray-100 dark:bg-gray-700 dark:border-gray-600 dark:text-white'
            placeholder='Enter your username'
          />
        </div>
        <div>
          <label
            htmlFor='password'
            className='block text-sm font-medium text-gray-700 dark:text-gray-300'
          >
            Password
          </label>
          <input
            id='password'
            type='password'
            value={password}
            onChange={e => setPassword(e.target.value)}
            required
            disabled={isLoading}
            className='mt-1 block w-full px-3 py-2 border border-gray-300 rounded-md shadow-xs focus:outline-hidden focus:ring-blue-500 focus:border-blue-500 disabled:bg-gray-100 dark:bg-gray-700 dark:border-gray-600 dark:text-white'
            placeholder='Enter your password'
          />
        </div>
        <button
          type='submit'
          disabled={isLoading || !username || !password}
          className='w-full flex justify-center py-2 px-4 border border-transparent rounded-md shadow-xs text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 focus:outline-hidden focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:bg-gray-400 disabled:cursor-not-allowed'
        >
          {isLoading ? (
            <div className='flex items-center'>
              <div className='animate-spin rounded-full h-4 w-4 border-b-2 border-white mr-2'></div>
              Signing in...
            </div>
          ) : (
            'Sign In'
          )}
        </button>
      </form>
    </div>
  )
}

export default LoginForm
