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
      className={`mx-auto w-full max-w-md rounded-2xl border border-dark-600 bg-alpine-50 p-7 shadow-sm ${className}`}
    >
      <div className='mb-7'>
        <img src='/logo.png' alt='Snapper' className='mx-auto mb-4 h-14 w-14 rounded-2xl' />
        <h2 className='text-center text-2xl font-semibold text-alpine-900'>
          Snapper Trading Login
        </h2>
        <p className='mt-2 text-center text-sm text-muted-600'>
          Sign in to access the trading dashboard
        </p>
      </div>
      {error && (
        <div className='mb-4 rounded-lg border border-loss-200 bg-loss-50 p-3 text-sm text-loss-800'>
          {error}
        </div>
      )}
      <form onSubmit={handleSubmit} className='space-y-4'>
        <div>
          <label htmlFor='username' className='block text-sm font-medium text-muted-700'>
            Username
          </label>
          <input
            id='username'
            type='text'
            value={username}
            onChange={e => setUsername(e.target.value)}
            required
            disabled={isLoading}
            className='mt-1 block w-full rounded-xl border border-dark-600 bg-alpine-50 px-3 py-2 text-alpine-900 shadow-xs focus:border-brand-500 focus:outline-hidden focus:ring-2 focus:ring-brand-500 disabled:bg-dark-700'
            placeholder='Enter your username'
          />
        </div>
        <div>
          <label htmlFor='password' className='block text-sm font-medium text-muted-700'>
            Password
          </label>
          <input
            id='password'
            type='password'
            value={password}
            onChange={e => setPassword(e.target.value)}
            required
            disabled={isLoading}
            className='mt-1 block w-full rounded-xl border border-dark-600 bg-alpine-50 px-3 py-2 text-alpine-900 shadow-xs focus:border-brand-500 focus:outline-hidden focus:ring-2 focus:ring-brand-500 disabled:bg-dark-700'
            placeholder='Enter your password'
          />
        </div>
        <button
          type='submit'
          disabled={isLoading || !username || !password}
          className='flex w-full items-center justify-center rounded-xl border border-transparent bg-brand-600 px-4 py-2.5 text-sm font-medium text-white transition-all duration-200 hover:bg-brand-700 focus:outline-hidden focus:ring-2 focus:ring-brand-500 focus:ring-offset-2 disabled:cursor-not-allowed disabled:bg-muted-400'
        >
          {isLoading ? (
            <div className='flex items-center gap-2'>
              <div className='h-2.5 w-10 rounded-full bg-white/60 animate-pulse' />
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
