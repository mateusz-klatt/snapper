import React, { useState } from 'react'
import { Users } from 'lucide-react'
import UserList from './UserList'
import UserForm from './UserForm'
import { useAuthStore } from '../../../stores/auth'
import type { UserProfile } from '../../../types/api'

interface UserManagementProps {
  readOnly?: boolean
}

const UserManagement: React.FC<UserManagementProps> = ({ readOnly }) => {
  const [showUserForm, setShowUserForm] = useState(false)
  const [editingUser, setEditingUser] = useState<UserProfile | undefined>(undefined)
  const { hasPermission } = useAuthStore()

  if (!hasPermission('manage:users')) {
    return (
      <div className='flex flex-col items-center justify-center min-h-[60vh] text-center'>
        <Users className='w-16 h-16 text-muted-400 mb-4' />
        <h2 className='text-xl font-semibold text-alpine-900 mb-2'>Access Denied</h2>
        <p className='text-muted-600 max-w-md'>
          You don&apos;t have permission to manage users. Please contact your system administrator.
        </p>
      </div>
    )
  }

  const handleCreateUser = () => {
    setEditingUser(undefined)
    setShowUserForm(true)
  }

  const handleEditUser = (user: UserProfile) => {
    setEditingUser(user)
    setShowUserForm(true)
  }

  const handleCloseForm = () => {
    setShowUserForm(false)
    setEditingUser(undefined)
  }

  return (
    <div className='space-y-6'>
      <UserList onCreateUser={handleCreateUser} onEditUser={handleEditUser} readOnly={readOnly} />
      <UserForm
        key={editingUser?.username ?? 'new'}
        user={editingUser}
        open={showUserForm}
        onClose={handleCloseForm}
        readOnly={readOnly}
      />
    </div>
  )
}

export default UserManagement
