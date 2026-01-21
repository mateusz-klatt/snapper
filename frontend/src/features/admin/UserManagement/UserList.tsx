import React, { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Trash2, Edit, UserPlus, Eye, EyeOff, Shield, Users } from 'lucide-react'
import { toast } from 'react-hot-toast'
import { Button, Badge } from '../../../components/ui'
import { api } from '../../../lib/apiClient'
import type { UserProfile, UserListResponse } from '../../../types/api'

interface UserListProps {
  onCreateUser: () => void
  onEditUser: (user: UserProfile) => void
}

const UserList: React.FC<UserListProps> = ({ onCreateUser, onEditUser }) => {
  const [includeInactive, setIncludeInactive] = useState(false)
  const queryClient = useQueryClient()
  const {
    data: userListData,
    isLoading,
    error,
  } = useQuery<UserListResponse>({
    queryKey: ['users', includeInactive],
    queryFn: async () => {
      const response = await api(`/auth/users?include_inactive=${includeInactive}`)

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`)
      }

      return await response.json()
    },
  })
  const deleteUserMutation = useMutation({
    mutationFn: async (userId: string) => {
      const response = await api(`/auth/users/${userId}`, { method: 'DELETE' })

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`)
      }
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['users'] })
      toast.success('User has been deactivated')
    },
    onError: (error: Error) => {
      toast.error(error.message || 'Error deactivating user')
    },
  })

  const handleDeleteUser = (user: UserProfile) => {
    if (window.confirm(`Are you sure you want to deactivate user "${user.username}"?`)) {
      deleteUserMutation.mutate(user.id)
    }
  }

  const getRoleBadgeColor = (role: string): string => {
    switch (role) {
      case 'admin':
        return 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300'
      case 'operator':
        return 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300'
      case 'viewer':
        return 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300'
      default:
        return 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300'
    }
  }

  const getRoleIcon = (role: string) => {
    switch (role) {
      case 'admin':
        return <Shield className='w-3 h-3' />
      case 'operator':
        return <Users className='w-3 h-3' />
      case 'viewer':
        return <Eye className='w-3 h-3' />
      default:
        return null
    }
  }

  const formatDate = (dateString: string): string => {
    return new Date(dateString).toLocaleDateString('en-US', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
    })
  }

  if (isLoading) {
    return (
      <div className='flex items-center justify-center p-8'>
        <div className='animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600'></div>
      </div>
    )
  }

  if (error) {
    const errorMessage = error instanceof Error ? error.message : 'Unknown error'

    return (
      <div className='p-4 text-red-600 bg-red-50 dark:bg-red-900/20 rounded-lg'>
        Error loading users: {errorMessage}
      </div>
    )
  }

  const users = userListData?.users || []

  return (
    <div className='space-y-4'>
      {}
      <div className='flex items-center justify-between'>
        <div className='flex items-center space-x-4'>
          <h2 className='text-2xl font-bold text-gray-900 dark:text-gray-100'>User Management</h2>
          <Badge variant='outline' className='text-sm'>
            {userListData?.total_count || 0} users
          </Badge>
        </div>
        <div className='flex items-center space-x-2'>
          <Button
            variant='secondary'
            size='sm'
            onClick={() => setIncludeInactive(!includeInactive)}
            className='flex items-center space-x-2'
          >
            {includeInactive ? <EyeOff className='w-4 h-4' /> : <Eye className='w-4 h-4' />}
            <span>{includeInactive ? 'Hide inactive' : 'Show inactive'}</span>
          </Button>
          <Button onClick={onCreateUser} className='flex items-center space-x-2'>
            <UserPlus className='w-4 h-4' />
            <span>Add User</span>
          </Button>
        </div>
      </div>
      {}
      <div className='bg-white dark:bg-gray-800 shadow-sm rounded-lg overflow-hidden'>
        <div className='overflow-x-auto'>
          <table className='min-w-full divide-y divide-gray-200 dark:divide-gray-700'>
            <thead className='bg-gray-50 dark:bg-gray-700'>
              <tr>
                <th className='px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider'>
                  User
                </th>
                <th className='px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider'>
                  Role
                </th>
                <th className='px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider'>
                  Status
                </th>
                <th className='px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider'>
                  Last Login
                </th>
                <th className='px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider'>
                  Created At
                </th>
                <th className='px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider'>
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className='bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700'>
              {users.map(user => (
                <tr key={user.id} className='hover:bg-gray-50 dark:hover:bg-gray-700'>
                  <td className='px-6 py-4 whitespace-nowrap'>
                    <div>
                      <div className='text-sm font-medium text-gray-900 dark:text-gray-100'>
                        {user.username}
                      </div>
                      <div className='text-sm text-gray-500 dark:text-gray-400'>{user.email}</div>
                    </div>
                  </td>
                  <td className='px-6 py-4 whitespace-nowrap'>
                    <Badge
                      className={`inline-flex items-center space-x-1 ${getRoleBadgeColor(user.role)}`}
                    >
                      {getRoleIcon(user.role)}
                      <span className='capitalize'>{user.role}</span>
                    </Badge>
                  </td>
                  <td className='px-6 py-4 whitespace-nowrap'>
                    <Badge
                      variant={user.is_active ? 'default' : 'secondary'}
                      className={
                        user.is_active
                          ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300'
                          : 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300'
                      }
                    >
                      {user.is_active ? 'Active' : 'Inactive'}
                    </Badge>
                  </td>
                  <td className='px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-400'>
                    {user.last_login ? formatDate(user.last_login) : 'Never'}
                  </td>
                  <td className='px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-400'>
                    {user.created_at ? formatDate(user.created_at) : 'Unknown'}
                  </td>
                  <td className='px-6 py-4 whitespace-nowrap text-right text-sm font-medium space-x-2'>
                    <Button
                      variant='secondary'
                      size='sm'
                      onClick={() => onEditUser(user)}
                      className='text-blue-600 hover:text-blue-900'
                    >
                      <Edit className='w-4 h-4' />
                    </Button>
                    <Button
                      variant='danger'
                      size='sm'
                      onClick={() => handleDeleteUser(user)}
                      disabled={deleteUserMutation.isPending}
                      className='text-red-600 hover:text-red-900'
                    >
                      <Trash2 className='w-4 h-4' />
                    </Button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {users.length === 0 && (
          <div className='text-center py-12'>
            <Users className='mx-auto h-12 w-12 text-gray-400' />
            <h3 className='mt-2 text-sm font-medium text-gray-900 dark:text-gray-100'>
              No users found
            </h3>
            <p className='mt-1 text-sm text-gray-500 dark:text-gray-400'>
              {includeInactive ? 'No users found.' : 'No active users found.'}
            </p>
          </div>
        )}
      </div>
    </div>
  )
}

export default UserList
