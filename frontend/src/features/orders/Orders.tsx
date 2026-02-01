import React, { useState } from 'react'
import { useOrders, useExecutions } from '../../hooks/queries'
import type { OrderStatus, Fill } from '../../types/entities'
import { OrderCardSkeleton } from '../../components/Skeleton'
import { useWebSocketStore } from '../../stores/websocket'
import clsx from 'clsx'

const OrderCard: React.FC<{ order: OrderStatus }> = ({ order }) => {
  const getStatusColor = (status: string) => {
    switch (status.toLowerCase()) {
      case 'filled':
        return 'text-green-400 bg-green-900/20'
      case 'new':
      case 'open':
        return 'text-blue-400 bg-blue-900/20'
      case 'cancelled':
        return 'text-gray-400 bg-gray-900/20'
      case 'rejected':
      case 'error':
        return 'text-red-400 bg-red-900/20'
      case 'partially_filled':
        return 'text-yellow-400 bg-yellow-900/20'
      default:
        return 'text-gray-400 bg-gray-900/20'
    }
  }

  const getSideColor = (side: string) => {
    return side === 'buy' ? 'text-green-400' : 'text-red-400'
  }

  const formatPrice = (price: number | null | undefined) => {
    return price ? `$${price.toFixed(2)}` : 'Market'
  }

  return (
    <div className='bg-dark-800 border border-dark-700 rounded-lg p-4 hover:border-dark-600 transition-colors'>
      <div className='flex items-center justify-between mb-3'>
        <div className='flex items-center space-x-3'>
          <span className='font-medium text-white'>{order.instrument}</span>
          <span className={clsx('text-sm font-medium', order.side ? getSideColor(order.side) : '')}>
            {order.side?.toUpperCase() ?? 'N/A'}
          </span>
          <span className='text-sm text-dark-400'>{order.orderType}</span>
        </div>
        <span
          className={clsx(
            'px-2 py-1 text-xs font-medium rounded-full',
            getStatusColor(order.status)
          )}
        >
          {order.status}
        </span>
      </div>
      <div className='grid grid-cols-2 gap-4 text-sm'>
        <div>
          <div className='text-dark-400'>Quantity</div>
          <div className='text-white font-mono'>{order.size.toFixed(4)}</div>
        </div>
        <div>
          <div className='text-dark-400'>Price</div>
          <div className='text-white font-mono'>{formatPrice(order.price)}</div>
        </div>
        <div>
          <div className='text-dark-400'>Created</div>
          <div className='text-white text-xs'>
            {order.createdAt ? order.createdAt.toLocaleString() : 'N/A'}
          </div>
        </div>
        <div>
          <div className='text-dark-400'>Order ID</div>
          <div className='text-white text-xs font-mono'>{order.id}</div>
        </div>
      </div>
    </div>
  )
}

const ExecutionCard: React.FC<{ execution: Fill }> = ({ execution }) => {
  const totalCost = execution.price * execution.size
  const fees = execution.fee || 0

  return (
    <div className='bg-dark-800 border border-dark-700 rounded-lg p-4 hover:border-dark-600 transition-colors'>
      <div className='flex items-center justify-between mb-3'>
        <div className='flex items-center space-x-3'>
          <span className='font-medium text-white'>Order #{execution.orderId}</span>
          <span className='text-xs text-green-400 bg-green-900/20 px-2 py-1 rounded-full'>
            FILLED
          </span>
        </div>
        <div className='text-sm text-dark-400'>Exec ID {execution.id}</div>
      </div>
      <div className='grid grid-cols-3 gap-4 text-sm'>
        <div>
          <div className='text-dark-400'>Size</div>
          <div className='text-white font-mono'>{execution.size.toFixed(4)}</div>
        </div>
        <div>
          <div className='text-dark-400'>Price</div>
          <div className='text-white font-mono'>${execution.price.toFixed(2)}</div>
        </div>
        <div>
          <div className='text-dark-400'>Total</div>
          <div className='text-white font-mono'>${totalCost.toFixed(2)}</div>
        </div>
        <div className='col-span-2'>
          <div className='text-dark-400'>Executed</div>
          <div className='text-white text-xs'>
            {execution.executedAt?.toLocaleString() ?? 'N/A'}
          </div>
        </div>
        {fees > 0 && (
          <div>
            <div className='text-dark-400'>Fees</div>
            <div className='text-red-400 text-xs font-mono'>
              ${fees.toFixed(2)} {execution.feeAsset}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

export const Orders: React.FC = () => {
  const [activeTab, setActiveTab] = useState<'orders' | 'executions'>('orders')
  const [statusFilter, setStatusFilter] = useState<string>('all')
  const { data: orders = [], isLoading: ordersLoading } = useOrders({ limit: 50 })
  const { data: executions = [], isLoading: executionsLoading } = useExecutions({ limit: 50 })
  const filteredOrders = orders.filter(
    (order: OrderStatus) => statusFilter === 'all' || order.status.toLowerCase() === statusFilter
  )
  const statusOptions = [
    { value: 'all', label: 'All Orders' },
    { value: 'new', label: 'New' },
    { value: 'open', label: 'Open' },
    { value: 'partially_filled', label: 'Partially Filled' },
    { value: 'filled', label: 'Filled' },
    { value: 'cancelled', label: 'Cancelled' },
    { value: 'rejected', label: 'Rejected' },
  ]
  const { isConnected } = useWebSocketStore()

  return (
    <div className='p-4 space-y-6'>
      {}
      <div className='flex items-center justify-between'>
        <h2 className='text-xl font-bold text-white'>Orders & Executions</h2>
        <div className='flex items-center space-x-2 text-sm text-dark-400'>
          <div
            className={clsx(
              'w-2 h-2 rounded-full',
              isConnected ? 'bg-green-400 animate-pulse' : 'bg-red-400'
            )}
          ></div>
          <span>{isConnected ? 'Live updates via WebSocket' : 'WebSocket disconnected'}</span>
        </div>
      </div>
      {}
      <div className='flex space-x-1 bg-dark-800 p-1 rounded-lg'>
        <button
          onClick={() => setActiveTab('orders')}
          className={clsx(
            'flex-1 py-2 px-4 text-sm font-medium rounded-md transition-colors',
            activeTab === 'orders'
              ? 'bg-blue-600 text-white'
              : 'text-dark-300 hover:text-white hover:bg-dark-700'
          )}
        >
          Orders ({orders.length})
        </button>
        <button
          onClick={() => setActiveTab('executions')}
          className={clsx(
            'flex-1 py-2 px-4 text-sm font-medium rounded-md transition-colors',
            activeTab === 'executions'
              ? 'bg-blue-600 text-white'
              : 'text-dark-300 hover:text-white hover:bg-dark-700'
          )}
        >
          Executions ({executions.length})
        </button>
      </div>
      {}
      {activeTab === 'orders' && (
        <div className='flex items-center space-x-4'>
          <label htmlFor='status-filter' className='text-sm text-dark-400'>
            Filter by status:
          </label>
          <select
            id='status-filter'
            value={statusFilter}
            onChange={e => setStatusFilter(e.target.value)}
            className='px-3 py-1 bg-dark-800 border border-dark-600 rounded-sm text-white text-sm focus:outline-hidden focus:ring-2 focus:ring-blue-500'
          >
            {statusOptions.map(option => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </div>
      )}
      {}
      <div className='space-y-4'>
        {activeTab === 'orders' && (
          <>
            {ordersLoading && (
              <div className='space-y-3'>
                <OrderCardSkeleton />
                <OrderCardSkeleton />
                <OrderCardSkeleton />
                <OrderCardSkeleton />
              </div>
            )}
            {!ordersLoading && filteredOrders.length === 0 && (
              <div className='text-center py-8 text-dark-400'>
                <div className='w-12 h-12 bg-dark-700 rounded-full flex items-center justify-center mx-auto mb-3'>
                  <svg className='w-6 h-6' fill='none' stroke='currentColor' viewBox='0 0 24 24'>
                    <path
                      strokeLinecap='round'
                      strokeLinejoin='round'
                      strokeWidth={2}
                      d='M9 5H7a2 2 0 00-2 2v10a2 2 0 002 2h8a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2'
                    />
                  </svg>
                </div>
                <p>No orders found</p>
                <p className='text-sm mt-1'>
                  {statusFilter === 'all'
                    ? 'Start trading to see orders here'
                    : `No ${statusFilter} orders`}
                </p>
              </div>
            )}
            {!ordersLoading && filteredOrders.length > 0 && (
              <div className='grid gap-4'>
                {filteredOrders.map((order: OrderStatus) => (
                  <OrderCard key={order.id} order={order} />
                ))}
              </div>
            )}
          </>
        )}
        {activeTab === 'executions' && (
          <>
            {executionsLoading && (
              <div className='space-y-3'>
                <OrderCardSkeleton />
                <OrderCardSkeleton />
                <OrderCardSkeleton />
                <OrderCardSkeleton />
              </div>
            )}
            {!executionsLoading && executions.length === 0 && (
              <div className='text-center py-8 text-dark-400'>
                <div className='w-12 h-12 bg-dark-700 rounded-full flex items-center justify-center mx-auto mb-3'>
                  <svg className='w-6 h-6' fill='none' stroke='currentColor' viewBox='0 0 24 24'>
                    <path
                      strokeLinecap='round'
                      strokeLinejoin='round'
                      strokeWidth={2}
                      d='M13 10V3L4 14h7v7l9-11h-7z'
                    />
                  </svg>
                </div>
                <p>No executions found</p>
                <p className='text-sm mt-1'>Trade executions will appear here</p>
              </div>
            )}
            {!executionsLoading && executions.length > 0 && (
              <div className='grid gap-4'>
                {executions.map((execution: Fill) => (
                  <ExecutionCard key={execution.id} execution={execution} />
                ))}
              </div>
            )}
          </>
        )}
      </div>
    </div>
  )
}
