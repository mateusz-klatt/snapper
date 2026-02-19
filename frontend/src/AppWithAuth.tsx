import { useEffect, useRef } from 'react'
import App from './App'
import { AuthenticatedApp } from './components/AuthenticatedApp'
import AuthErrorBoundary from './components/auth/AuthErrorBoundary'
import { useAuth } from './stores/auth'
import { useAppStore } from './stores/app'
import { apiClient } from './lib/apiClient'

function AppWithAuth() {
  const { isAuthenticated, refreshToken, silentLogout } = useAuth()
  const isDarkMode = useAppStore(s => s.isDarkMode)
  const initialized = useRef(false)

  useEffect(() => {
    if (isDarkMode) {
      document.documentElement.classList.add('dark')
    } else {
      document.documentElement.classList.remove('dark')
    }
  }, [isDarkMode])

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
