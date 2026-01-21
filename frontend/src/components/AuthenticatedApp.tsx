import React, { useEffect } from 'react'
import { useAuth } from '../stores/auth'
import ProtectedRoute from '../components/auth/ProtectedRoute'
import UserProfile from '../components/auth/UserProfile'

interface AuthenticatedAppProps {
  children: React.ReactNode
}

export const AuthenticatedApp: React.FC<AuthenticatedAppProps> = ({ children }) => {
  const { isAuthenticated, user, refreshToken } = useAuth()

  useEffect(() => {
    if (!isAuthenticated) return
    const interval = setInterval(
      async () => {
        try {
          await refreshToken()
        } catch {
          void 0
        }
      },
      14 * 60 * 1000
    )

    return () => clearInterval(interval)
  }, [isAuthenticated, refreshToken])

  return (
    <ProtectedRoute>
      <div className='h-screen flex flex-col bg-gray-50 dark:bg-gray-900'>
        {}
        <header className='flex-shrink-0 bg-white dark:bg-gray-800 shadow-xs'>
          <div className='max-w-7xl mx-auto px-4 sm:px-6 lg:px-8'>
            <div className='flex justify-between items-center h-16'>
              <div className='flex items-center'>
                <h1 className='text-xl font-semibold text-gray-900 dark:text-white'>
                  Snapper Trading Dashboard
                </h1>
                {user && (
                  <div className='ml-4 px-3 py-1 text-xs bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200 rounded-full'>
                    Connected as {user.role}
                  </div>
                )}
              </div>
              <UserProfile />
            </div>
          </div>
        </header>
        {}
        <main className='flex-1 overflow-hidden'>{children}</main>
      </div>
    </ProtectedRoute>
  )
}
