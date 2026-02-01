import React from 'react'
import { clsx } from 'clsx'

interface StatusBadgeProps {
  status: 'connected' | 'disconnected' | 'pending' | 'healthy' | 'stale' | 'error'
  children: React.ReactNode
  className?: string
}

export const StatusBadge: React.FC<Readonly<StatusBadgeProps>> = ({
  status,
  children,
  className,
}) => {
  const baseClasses = 'inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium'
  const statusClasses = {
    connected: 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300',
    healthy: 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300',
    disconnected: 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300',
    error: 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300',
    pending: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-300',
    stale: 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300',
  }

  return <span className={clsx(baseClasses, statusClasses[status], className)}>{children}</span>
}

interface CardProps {
  title: string
  children: React.ReactNode
  className?: string
  actions?: React.ReactNode
}

export const Card: React.FC<Readonly<CardProps>> = ({ title, children, className, actions }) => {
  return (
    <div className={clsx('panel', className)}>
      <div className='flex items-center justify-between mb-4'>
        <h3 className='text-lg font-semibold text-primary-400'>{title}</h3>
        {actions && <div className='flex gap-2'>{actions}</div>}
      </div>
      {children}
    </div>
  )
}

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: 'primary' | 'secondary' | 'danger'
  size?: 'sm' | 'md' | 'lg'
  loading?: boolean
  children: React.ReactNode
}

export const Button: React.FC<Readonly<ButtonProps>> = ({
  variant = 'primary',
  size = 'md',
  loading = false,
  children,
  className,
  disabled,
  ...props
}) => {
  const baseClasses =
    'btn focus:outline-hidden focus:ring-2 focus:ring-offset-2 focus:ring-offset-dark-900'
  const variantClasses = {
    primary: 'btn-primary',
    secondary: 'btn-secondary',
    danger: 'btn-danger',
  }
  const sizeClasses = {
    sm: 'btn-sm',
    md: '',
    lg: 'px-6 py-3 text-lg',
  }

  return (
    <button
      className={clsx(
        baseClasses,
        variantClasses[variant],
        sizeClasses[size],
        loading && 'opacity-50 cursor-not-allowed',
        className
      )}
      disabled={disabled || loading}
      {...props}
    >
      {loading ? (
        <div className='flex items-center gap-2'>
          <div className='w-4 h-4 border-2 border-current border-t-transparent rounded-full animate-spin' />
          Loading...
        </div>
      ) : (
        children
      )}
    </button>
  )
}

interface BadgeProps {
  variant?: 'default' | 'secondary' | 'outline' | 'destructive'
  className?: string
  children: React.ReactNode
}

export const Badge: React.FC<Readonly<BadgeProps>> = ({
  variant = 'default',
  className,
  children,
}) => {
  const baseClasses = 'inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium'
  const variantClasses = {
    default: 'bg-primary-100 text-primary-800 dark:bg-primary-900 dark:text-primary-300',
    secondary: 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300',
    outline: 'border border-gray-300 text-gray-700 dark:border-gray-600 dark:text-gray-300',
    destructive: 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300',
  }

  return <span className={clsx(baseClasses, variantClasses[variant], className)}>{children}</span>
}

interface LoadingSpinnerProps {
  size?: 'sm' | 'md' | 'lg'
  className?: string
}

export const LoadingSpinner: React.FC<Readonly<LoadingSpinnerProps>> = ({
  size = 'md',
  className,
}) => {
  const sizeClasses = {
    sm: 'w-4 h-4',
    md: 'w-6 h-6',
    lg: 'w-8 h-8',
  }

  return (
    <div
      className={clsx(
        'border-2 border-current border-t-transparent rounded-full animate-spin',
        sizeClasses[size],
        className
      )}
    />
  )
}

interface ConnectionBarProps {
  isConnected: boolean
  lag: number
  subscribedTopicsCount: number
}

export const ConnectionBar: React.FC<Readonly<ConnectionBarProps>> = ({
  isConnected,
  lag,
  subscribedTopicsCount,
}) => {
  return (
    <div
      className={clsx(
        'flex items-center justify-between px-4 py-2 text-sm border-b',
        isConnected
          ? 'bg-dark-800 border-dark-600 text-green-400'
          : 'bg-red-900 border-red-700 text-red-300'
      )}
    >
      <div className='flex items-center gap-4'>
        <div className='flex items-center gap-2'>
          <div
            className={clsx('w-2 h-2 rounded-full', isConnected ? 'bg-green-400' : 'bg-red-400')}
          />
          <span>{isConnected ? 'Connected' : 'Disconnected'}</span>
        </div>
        {isConnected && (
          <>
            <div className='text-dark-300'>Lag: {lag >= 0 ? `${lag}ms` : 'Unknown'}</div>
            <div className='text-dark-300'>Topics: {subscribedTopicsCount}</div>
          </>
        )}
      </div>
      <div className='text-xs text-dark-400'>Last update: {new Date().toLocaleTimeString()}</div>
    </div>
  )
}

interface MetricCardProps {
  label: string
  value: string | number
  change?: number
  changeType?: 'positive' | 'negative' | 'neutral'
  suffix?: string
}

export const MetricCard: React.FC<Readonly<MetricCardProps>> = ({
  label,
  value,
  change,
  changeType = 'neutral',
  suffix,
}) => {
  const changeColors = {
    positive: 'text-green-400',
    negative: 'text-red-400',
    neutral: 'text-dark-300',
  }

  return (
    <div className='bg-dark-800 border border-dark-700 rounded-lg p-4'>
      <div className='text-sm text-dark-300 mb-1'>{label}</div>
      <div className='flex items-baseline gap-2'>
        <span className='text-2xl font-bold text-white'>
          {value}
          {suffix && <span className='text-lg text-dark-300'>{suffix}</span>}
        </span>
        {change !== undefined && (
          <span className={clsx('text-sm', changeColors[changeType])}>
            {change > 0 ? '+' : ''}
            {change.toFixed(2)}%
          </span>
        )}
      </div>
    </div>
  )
}
