import React from 'react'
import UserManagement from './UserManagement/UserManagement'

export const Admin: React.FC = () => {
  return (
    <div className='space-y-6'>
      <div className='mb-6'>
        <h1 className='text-3xl font-bold text-gray-900 dark:text-gray-100 mb-2'>Administration</h1>
        <p className='text-gray-600 dark:text-gray-400'>Manage users and system configuration</p>
      </div>
      <UserManagement />
    </div>
  )
}
