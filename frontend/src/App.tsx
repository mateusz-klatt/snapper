import { useState } from 'react'
import { useAuth } from './stores/auth'
import { useAppStore } from './stores/app'
import { useTabRouting, type ValidTab } from './hooks/useHashRouting'
import { useAppShell } from './hooks/useAppShell'
import { useScopePersistence } from './hooks/useScopePersistence'
import { AppRoutes } from './components/AppRoutes'
import { ALL_TABS } from './components/tabs'
import UserProfile from './components/auth/UserProfile'
import { TimeTravelPicker } from './components/TimeTravelPicker'
import { OperatorPicker } from './components/OperatorPicker'
import { WalletPicker } from './components/WalletPicker'
import { Menu, Wifi, WifiOff, X, Sun, Moon, Clock } from 'lucide-react'

function App() {
  const [activeTab, navigateToTab] = useTabRouting()
  const { canAccess } = useAuth()
  const { isConnected, connectionLag, subscribedTopicsCount } = useAppShell()

  useScopePersistence()
  const isDarkMode = useAppStore(s => s.isDarkMode)
  const toggleDarkMode = useAppStore(s => s.toggleDarkMode)
  const isTimeTraveling = useAppStore(s => s.isTimeTraveling)
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const tabs = ALL_TABS.filter(tab => canAccess(tab.id))
  const lagValue = connectionLag >= 0 ? `${connectionLag} ms` : 'Unknown'

  const handleNavigate = (tabId: ValidTab) => {
    navigateToTab(tabId)
    setSidebarOpen(false)
  }

  return (
    <div className='flex h-screen bg-dark-900 text-alpine-900'>
      {sidebarOpen && (
        <button
          type='button'
          className='fixed inset-0 z-40 bg-black/40 md:hidden appearance-none border-none cursor-default'
          onClick={() => setSidebarOpen(false)}
          onKeyDown={e => {
            if (e.key === 'Escape') setSidebarOpen(false)
          }}
          aria-label='Close sidebar'
        />
      )}
      <aside
        className={`fixed inset-y-0 left-0 z-50 flex w-72 shrink-0 flex-col border-r border-dark-600 bg-alpine-50 px-4 py-6 transition-transform duration-200 md:static md:translate-x-0 ${sidebarOpen ? 'translate-x-0' : '-translate-x-full'}`}
      >
        <div className='mb-4 flex shrink-0 items-center justify-between px-2 md:mb-8'>
          <div>
            <div className='text-xs font-semibold tracking-[0.16em] text-muted-500 uppercase'>
              Snapper
            </div>
            <h1 className='mt-1 text-xl font-semibold text-alpine-900 md:mt-2'>Trading Console</h1>
            <p className='mt-1 hidden text-sm text-muted-600 md:block'>Precision workstation</p>
          </div>
          <button
            className='rounded-lg p-1 text-muted-600 hover:bg-dark-700 md:hidden'
            onClick={() => setSidebarOpen(false)}
            aria-label='Close sidebar'
          >
            <X size={20} />
          </button>
        </div>
        <nav className='flex-1 space-y-1.5 overflow-y-auto pr-1'>
          {tabs.map(tab => {
            const Icon = tab.icon

            return (
              <button
                key={tab.id}
                onClick={() => handleNavigate(tab.id)}
                className={
                  activeTab === tab.id
                    ? 'flex w-full items-center gap-3 rounded-xl border border-brand-200 bg-brand-50 px-3 py-2.5 text-sm font-medium text-brand-700 transition-all duration-200'
                    : 'flex w-full items-center gap-3 rounded-xl border border-transparent px-3 py-2.5 text-sm font-medium text-muted-700 transition-all duration-200 hover:border-dark-600 hover:bg-dark-700 hover:text-alpine-900'
                }
              >
                <Icon
                  size={16}
                  className={activeTab === tab.id ? 'text-brand-600' : 'text-muted-600'}
                />
                <span>{tab.label}</span>
              </button>
            )
          })}
        </nav>
      </aside>
      <section className='flex min-w-0 flex-1 flex-col'>
        <header className='border-b border-dark-600 bg-alpine-50 px-4 py-4 sm:px-6'>
          <div className='flex items-center justify-between gap-3'>
            <button
              className='rounded-lg p-1 text-muted-600 hover:bg-dark-700 md:hidden'
              onClick={() => setSidebarOpen(true)}
              aria-label='Open sidebar'
            >
              <Menu size={20} />
            </button>
            <div className='ml-auto flex flex-wrap items-center justify-end gap-3'>
              <OperatorPicker />
              <WalletPicker />
              <div className='hidden md:flex'>
                <TimeTravelPicker />
              </div>
              {!isTimeTraveling && (
                <span
                  className='hidden text-sm tabular-nums text-muted-600 sm:inline'
                  title='Round-trip latency to the WebSocket server'
                >
                  Lag: {lagValue}
                </span>
              )}
              <span
                className='hidden text-sm tabular-nums text-muted-600 sm:inline'
                title='Number of subscribed WebSocket topics'
              >
                Topics: {subscribedTopicsCount}
              </span>
              <span
                className={
                  isConnected
                    ? 'inline-flex items-center gap-1.5 rounded-full border border-accent-200 bg-accent-50 px-2 py-1 text-xs font-semibold text-accent-800 sm:px-3'
                    : 'inline-flex items-center gap-1.5 rounded-full border border-loss-200 bg-loss-50 px-2 py-1 text-xs font-semibold text-loss-800 sm:px-3'
                }
                title={isConnected ? 'Connected to WebSocket' : 'Disconnected from WebSocket'}
              >
                {isConnected ? <Wifi size={13} /> : <WifiOff size={13} />}
                <span className='hidden sm:inline'>
                  {isConnected ? 'Connected' : 'Disconnected'}
                </span>
              </span>
              <button
                onClick={toggleDarkMode}
                className='rounded-lg p-2 text-muted-600 hover:bg-dark-700 transition-colors'
                aria-label={isDarkMode ? 'Switch to light mode' : 'Switch to dark mode'}
                title={isDarkMode ? 'Switch to light mode' : 'Switch to dark mode'}
              >
                {isDarkMode ? <Sun size={16} /> : <Moon size={16} />}
              </button>
              <UserProfile />
            </div>
          </div>
        </header>
        {isTimeTraveling &&
          activeTab !== 'processes' &&
          activeTab !== 'strategies' &&
          activeTab !== 'health' && (
            <div className='flex items-center gap-2 border-b border-brand-200 bg-brand-50 px-4 py-2 text-sm font-medium text-brand-800'>
              <Clock size={14} />
              <span>Time Travel Mode — viewing historical data (read-only)</span>
            </div>
          )}
        <main className='flex-1 overflow-y-auto p-4 sm:p-6'>
          <div className='mx-auto w-full max-w-screen-2xl'>
            <AppRoutes activeTab={activeTab} />
          </div>
        </main>
      </section>
    </div>
  )
}

export default App
