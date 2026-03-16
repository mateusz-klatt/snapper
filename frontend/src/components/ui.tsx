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
    connected: 'bg-accent-50 text-accent-800 border border-accent-200',
    healthy: 'bg-accent-50 text-accent-800 border border-accent-200',
    disconnected: 'bg-loss-50 text-loss-800 border border-loss-200',
    error: 'bg-loss-50 text-loss-800 border border-loss-200',
    pending: 'bg-warning-50 text-warning-800 border border-warning-200',
    stale: 'bg-dark-700 text-muted-700 border border-dark-600',
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
        <h3 className='text-lg font-semibold text-alpine-900'>{title}</h3>
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
    default: 'bg-brand-50 text-brand-700 border border-brand-200',
    secondary: 'bg-dark-700 text-muted-700 border border-dark-600',
    outline: 'border border-dark-600 text-muted-700',
    destructive: 'bg-loss-50 text-loss-700 border border-loss-200',
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

interface EmptyStateProps {
  icon: React.ReactNode
  title: string
  message?: React.ReactNode
}

export const EmptyState: React.FC<Readonly<EmptyStateProps>> = ({ icon, title, message }) => {
  return (
    <div className='py-8 text-center text-muted-500'>
      <div className='mx-auto mb-3 flex h-12 w-12 items-center justify-center rounded-full bg-dark-700'>
        {icon}
      </div>
      <p>{title}</p>
      {message && <p className='mt-1 text-sm'>{message}</p>}
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
    positive: 'text-gain-600',
    negative: 'text-loss-600',
    neutral: 'text-muted-600',
  }

  return (
    <div className='bg-alpine-50 border border-dark-600 rounded-2xl p-4'>
      <div className='text-sm text-muted-600 mb-1'>{label}</div>
      <div className='flex items-baseline gap-2'>
        <span className='text-2xl font-bold text-alpine-900'>
          {value}
          {suffix && <span className='text-lg text-muted-600'>{suffix}</span>}
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
