import React, { Component, ReactNode } from 'react'

interface Props {
  children: ReactNode
}
interface State {
  hasError: boolean
  error: Error | null
}

class AuthErrorBoundary extends Component<Props, State> {
  private hasTriggeredLogout = false
  constructor(props: Props) {
    super(props)
    this.state = { hasError: false, error: null }
  }
  componentDidUpdate(_prevProps: Props, prevState: State) {
    if (prevState.hasError && !this.state.hasError) {
      this.hasTriggeredLogout = false
    }
  }
  static getDerivedStateFromError(error: Error): State {
    const isAuthError =
      error.message.includes('Authentication required') ||
      error.message.includes('Access denied') ||
      error.message.includes('401') ||
      error.message.includes('403')

    return {
      hasError: isAuthError,
      error: isAuthError ? error : null,
    }
  }
  componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
    console.error('AuthErrorBoundary caught an error:', error, errorInfo)

    if (this.state.hasError && !this.hasTriggeredLogout) {
      this.hasTriggeredLogout = true
      const authCallback = (globalThis as { authLogoutCallback?: () => void }).authLogoutCallback

      if (authCallback) {
        setTimeout(() => {
          authCallback()
        }, 0)
      }
    }
  }
  render() {
    if (this.state.hasError) {
      return null
    }

    return this.props.children
  }
}

export default AuthErrorBoundary
