import React, { useEffect } from 'react'
import { useAuth } from '../stores/auth'
import ProtectedRoute from '../components/auth/ProtectedRoute'

interface AuthenticatedAppProps {
  children: React.ReactNode
}

export const AuthenticatedApp: React.FC<Readonly<AuthenticatedAppProps>> = ({ children }) => {
  const { isAuthenticated, refreshToken } = useAuth()

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

  return <ProtectedRoute>{children}</ProtectedRoute>
}
