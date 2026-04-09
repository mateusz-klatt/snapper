import React, { useState } from 'react'
import { ChevronDown, ChevronUp, Shield, Users, Eye } from 'lucide-react'
import { RESOURCE_ACCESS } from '../../types/permissions.generated'
import { useIsReadOnly } from '../../hooks/useIsReadOnly'
import UserManagement from './UserManagement/UserManagement'
import ScopeGrantManagement from './ScopeGrantManagement/ScopeGrantManagement'
import CredentialManagement from './CredentialManagement/CredentialManagement'

const TAB_DISPLAY_NAMES: Record<string, string> = {
  overview: 'Overview',
  market: 'Market Data',
  processes: 'Processes',
  strategies: 'Strategies',
  orders: 'Orders & Fills',
  positions: 'Positions',
  signals: 'Signals',
  health: 'Health',
  admin: 'Administration',
  settings: 'Settings',
}

const ROLE_PERMISSIONS = Object.entries(TAB_DISPLAY_NAMES).map(([tabId, label]) => ({
  resource: label,
  viewer: RESOURCE_ACCESS[tabId].includes('viewer'),
  operator: RESOURCE_ACCESS[tabId].includes('operator'),
  admin: RESOURCE_ACCESS[tabId].includes('admin'),
}))

export const Admin: React.FC = () => {
  const readOnly = useIsReadOnly()
  const [showRoleInfo, setShowRoleInfo] = useState(false)

  return (
    <div className='space-y-6'>
      <div className='mb-6'>
        <h1 className='text-3xl font-bold text-alpine-900 mb-2'>Administration</h1>
        <p className='text-muted-600'>Manage users and system configuration</p>
      </div>
      {}
      <div className='panel'>
        <button
          onClick={() => setShowRoleInfo(prev => !prev)}
          className='flex w-full items-center justify-between text-left'
        >
          <h2 className='text-lg font-semibold text-alpine-900'>Role Permissions</h2>
          {showRoleInfo ? (
            <ChevronUp size={20} className='text-muted-500' />
          ) : (
            <ChevronDown size={20} className='text-muted-500' />
          )}
        </button>
        {showRoleInfo && (
          <div className='mt-4 space-y-4'>
            <div className='grid grid-cols-1 md:grid-cols-3 gap-4'>
              <div className='border border-dark-600 rounded-xl p-4'>
                <div className='flex items-center gap-2 mb-2'>
                  <Eye size={16} className='text-accent-600' />
                  <h3 className='font-medium text-alpine-900'>Viewer</h3>
                </div>
                <p className='text-xs text-muted-600'>
                  Read-only access to Overview and Market Data. Cannot manage processes, strategies,
                  or system settings.
                </p>
              </div>
              <div className='border border-dark-600 rounded-xl p-4'>
                <div className='flex items-center gap-2 mb-2'>
                  <Users size={16} className='text-brand-600' />
                  <h3 className='font-medium text-alpine-900'>Operator</h3>
                </div>
                <p className='text-xs text-muted-600'>
                  All Viewer permissions plus trading operations, strategy execution, process
                  management, and system health monitoring.
                </p>
              </div>
              <div className='border border-dark-600 rounded-xl p-4'>
                <div className='flex items-center gap-2 mb-2'>
                  <Shield size={16} className='text-loss-600' />
                  <h3 className='font-medium text-alpine-900'>Admin</h3>
                </div>
                <p className='text-xs text-muted-600'>
                  Full system access including user management, system settings, API key
                  configuration, and all Operator permissions.
                </p>
              </div>
            </div>
            <div className='overflow-x-auto'>
              <table className='min-w-full text-sm'>
                <thead>
                  <tr className='border-b border-dark-600'>
                    <th className='text-left py-2 px-3 text-xs font-medium text-muted-600 uppercase'>
                      Resource
                    </th>
                    <th className='text-center py-2 px-3 text-xs font-medium text-muted-600 uppercase'>
                      Viewer
                    </th>
                    <th className='text-center py-2 px-3 text-xs font-medium text-muted-600 uppercase'>
                      Operator
                    </th>
                    <th className='text-center py-2 px-3 text-xs font-medium text-muted-600 uppercase'>
                      Admin
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {ROLE_PERMISSIONS.map(row => (
                    <tr key={row.resource} className='border-b border-dark-600'>
                      <td className='py-2 px-3 text-alpine-900'>{row.resource}</td>
                      <td className='py-2 px-3 text-center'>
                        {row.viewer ? (
                          <span className='text-accent-600'>&#10003;</span>
                        ) : (
                          <span className='text-muted-400'>&#8212;</span>
                        )}
                      </td>
                      <td className='py-2 px-3 text-center'>
                        {row.operator ? (
                          <span className='text-accent-600'>&#10003;</span>
                        ) : (
                          <span className='text-muted-400'>&#8212;</span>
                        )}
                      </td>
                      <td className='py-2 px-3 text-center'>
                        <span className='text-accent-600'>&#10003;</span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
      <UserManagement readOnly={readOnly} />
      <ScopeGrantManagement readOnly={readOnly} />
      <CredentialManagement readOnly={readOnly} />
    </div>
  )
}
