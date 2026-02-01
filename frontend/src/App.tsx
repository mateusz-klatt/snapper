import { useAuth } from './stores/auth'
import { useTabRouting } from './hooks/useHashRouting'
import { useAppShell } from './hooks/useAppShell'
import { ConnectionBar } from './components/ui'
import { AppRoutes } from './components/AppRoutes'
import { ALL_TABS } from './components/tabs'

function App() {
  const [activeTab, navigateToTab] = useTabRouting()
  const { canAccess } = useAuth()
  const { isConnected, connectionLag, subscribedTopicsCount } = useAppShell()
  const tabs = ALL_TABS.filter(tab => canAccess(tab.id))

  return (
    <div className='h-screen flex flex-col bg-dark-900 text-white'>
      {}
      <ConnectionBar
        isConnected={isConnected}
        lag={connectionLag}
        subscribedTopicsCount={subscribedTopicsCount}
      />
      {}
      <header className='bg-dark-800 border-b border-dark-700 px-6 py-4'>
        <div className='flex items-center justify-between'>
          <div className='flex items-center gap-4'>
            <h1 className='text-2xl font-bold text-primary-400'>Snapper Trading Console</h1>
            <div className='text-sm text-dark-300'>v0.1.0</div>
          </div>
          <div className='flex items-center gap-4'>
            <button className='text-dark-300 hover:text-white'>🌙</button>
            <div className='text-sm text-dark-300'>{new Date().toLocaleString()}</div>
          </div>
        </div>
      </header>
      {}
      <nav className='bg-dark-800 border-b border-dark-700 px-6'>
        <div className='flex space-x-8'>
          {tabs.map(tab => (
            <button
              key={tab.id}
              onClick={() => navigateToTab(tab.id)}
              className={`
                flex items-center gap-2 px-3 py-4 text-sm font-medium border-b-2 transition-colors
                ${
                  activeTab === tab.id
                    ? 'border-primary-400 text-primary-400'
                    : 'border-transparent text-dark-300 hover:text-white hover:border-dark-500'
                }
              `}
            >
              <span>{tab.icon}</span>
              {tab.label}
            </button>
          ))}
        </div>
      </nav>
      {}
      <main className='flex-1 overflow-hidden'>
        <div className='h-full p-6'>
          <AppRoutes activeTab={activeTab} />
        </div>
      </main>
    </div>
  )
}

export default App
