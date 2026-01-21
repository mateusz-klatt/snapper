import { useEffect, useRef } from 'react'
import App from './App'
import { AuthenticatedApp } from './components/AuthenticatedApp'
import AuthErrorBoundary from './components/auth/AuthErrorBoundary'
import { useAuth } from './stores/auth'
import { apiClient } from './lib/apiClient'

function AppWithAuth() {
  const { isAuthenticated, refreshToken, silentLogout } = useAuth()
  const initialized = useRef(false)

  useEffect(() => {
    if (initialized.current) {
      return
    }

    const initializeAuth = async () => {
      initialized.current = true

      if (isAuthenticated) {
        return
      }

      const hasAuthCookies = apiClient.hasAuthCookies()

      if (!hasAuthCookies) {
        return
      }

      try {
        await refreshToken()
      } catch {
        silentLogout()
      }
    }

    initializeAuth()
  }, [isAuthenticated, refreshToken, silentLogout])

  return (
    <AuthErrorBoundary>
      <AuthenticatedApp>
        <App />
      </AuthenticatedApp>
    </AuthErrorBoundary>
  )
}

export default AppWithAuth
