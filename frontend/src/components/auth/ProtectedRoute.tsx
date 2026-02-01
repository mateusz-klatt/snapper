import React from 'react'
import { useAuth } from '../../stores/auth'
import LoginForm from './LoginForm'

interface ProtectedRouteProps {
  children: React.ReactNode
  requiredRole?: 'viewer' | 'operator' | 'admin'
  requiredPermission?: string
  resource?: string
  fallback?: React.ReactNode
}

const ProtectedRoute: React.FC<Readonly<ProtectedRouteProps>> = ({
  children,
  requiredRole,
  requiredPermission,
  resource,
  fallback,
}) => {
  const { isAuthenticated, user, hasRole, hasPermission, canAccess } = useAuth()

  if (!isAuthenticated || !user) {
    if (fallback) {
      return <>{fallback}</>
    }

    return (
      <div className='min-h-screen flex items-center justify-center bg-gray-50 dark:bg-gray-900 px-4'>
        <LoginForm />
      </div>
    )
  }

  if (requiredRole && !hasRole(requiredRole)) {
    return (
      <div className='min-h-screen flex items-center justify-center bg-gray-50 dark:bg-gray-900 px-4'>
        <div className='text-center'>
          <div className='text-6xl mb-4'>🚫</div>
          <h1 className='text-2xl font-bold text-gray-900 dark:text-white mb-2'>Access Denied</h1>
          <p className='text-gray-600 dark:text-gray-400 mb-4'>
            You need {requiredRole} access or higher to view this resource.
          </p>
          <p className='text-sm text-gray-500 dark:text-gray-500'>
            Your current role: <span className='font-medium'>{user.role}</span>
          </p>
        </div>
      </div>
    )
  }

  if (requiredPermission && !hasPermission(requiredPermission)) {
    return (
      <div className='min-h-screen flex items-center justify-center bg-gray-50 dark:bg-gray-900 px-4'>
        <div className='text-center'>
          <div className='text-6xl mb-4'>🔒</div>
          <h1 className='text-2xl font-bold text-gray-900 dark:text-white mb-2'>
            Insufficient Permissions
          </h1>
          <p className='text-gray-600 dark:text-gray-400 mb-4'>
            You don&apos;t have the required permission: <code>{requiredPermission}</code>
          </p>
          <p className='text-sm text-gray-500 dark:text-gray-500'>
            Your current role: <span className='font-medium'>{user.role}</span>
          </p>
        </div>
      </div>
    )
  }

  if (resource && !canAccess(resource)) {
    return (
      <div className='min-h-screen flex items-center justify-center bg-gray-50 dark:bg-gray-900 px-4'>
        <div className='text-center'>
          <div className='text-6xl mb-4'>🚪</div>
          <h1 className='text-2xl font-bold text-gray-900 dark:text-white mb-2'>
            Resource Restricted
          </h1>
          <p className='text-gray-600 dark:text-gray-400 mb-4'>
            You don&apos;t have access to the <code>{resource}</code> resource.
          </p>
          <p className='text-sm text-gray-500 dark:text-gray-500'>
            Your current role: <span className='font-medium'>{user.role}</span>
          </p>
        </div>
      </div>
    )
  }

  return <>{children}</>
}

export default ProtectedRoute
