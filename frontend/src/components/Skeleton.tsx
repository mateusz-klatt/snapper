import React from 'react'
import { clsx } from 'clsx'

interface SkeletonProps {
  className?: string
  width?: string | number
  height?: string | number
  rounded?: 'none' | 'sm' | 'md' | 'lg' | 'full'
  animate?: boolean
  style?: React.CSSProperties
}

export const Skeleton: React.FC<Readonly<SkeletonProps>> = ({
  className,
  width,
  height,
  rounded = 'md',
  animate = true,
  style: styleProp,
}) => {
  const roundedClasses = {
    none: 'rounded-none',
    sm: 'rounded-sm',
    md: 'rounded-md',
    lg: 'rounded-lg',
    full: 'rounded-full',
  }
  const style: React.CSSProperties = { ...styleProp }

  if (width) style.width = typeof width === 'number' ? `${width}px` : width
  if (height) style.height = typeof height === 'number' ? `${height}px` : height

  return (
    <div
      data-testid='skeleton'
      className={clsx(
        'bg-dark-700',
        roundedClasses[rounded],
        animate && 'animate-pulse',
        className
      )}
      style={style}
    />
  )
}

interface SkeletonTextProps {
  lines?: number
  className?: string
  lineClassName?: string
  lastLineWidth?: string
}

export const SkeletonText: React.FC<Readonly<SkeletonTextProps>> = ({
  lines = 3,
  className,
  lineClassName,
  lastLineWidth = '75%',
}) => {
  return (
    <div className={clsx('space-y-2', className)} data-testid='skeleton-text'>
      {Array.from({ length: lines }, (_, i) => `text-line-${i}`).map((key, index) => (
        <Skeleton
          key={key}
          height={16}
          className={clsx(
            'w-full',
            index === lines - 1 && lastLineWidth !== '100%' && `w-[${lastLineWidth}]`,
            lineClassName
          )}
          style={index === lines - 1 ? { width: lastLineWidth } : undefined}
        />
      ))}
    </div>
  )
}

interface CardSkeletonProps {
  className?: string
  showTitle?: boolean
  contentLines?: number
}

export const CardSkeleton: React.FC<Readonly<CardSkeletonProps>> = ({
  className,
  showTitle = true,
  contentLines = 4,
}) => {
  return (
    <div className={clsx('panel', className)} data-testid='card-skeleton'>
      {showTitle && (
        <div className='flex items-center justify-between mb-4'>
          <Skeleton width={150} height={24} />
          <Skeleton width={80} height={32} rounded='lg' />
        </div>
      )}
      <div className='space-y-3'>
        {Array.from({ length: contentLines }, (_, i) => `card-line-${i}`).map(key => (
          <div key={key} className='flex items-center justify-between'>
            <Skeleton width={120} height={16} />
            <Skeleton width={80} height={16} />
          </div>
        ))}
      </div>
    </div>
  )
}

interface MetricCardSkeletonProps {
  className?: string
}

export const MetricCardSkeleton: React.FC<Readonly<MetricCardSkeletonProps>> = ({ className }) => {
  return (
    <div
      className={clsx('bg-dark-800 border border-dark-700 rounded-xl p-4', className)}
      data-testid='metric-card-skeleton'
    >
      <Skeleton width={100} height={14} className='mb-2' />
      <div className='flex items-baseline gap-2'>
        <Skeleton width={80} height={32} />
        <Skeleton width={50} height={16} />
      </div>
    </div>
  )
}

interface TableSkeletonProps {
  columns?: number
  rows?: number
  className?: string
  showHeader?: boolean
}

export const TableSkeleton: React.FC<Readonly<TableSkeletonProps>> = ({
  columns = 5,
  rows = 5,
  className,
  showHeader = true,
}) => {
  return (
    <div className={clsx('w-full', className)} data-testid='table-skeleton'>
      {showHeader && (
        <div className='flex gap-4 pb-3 border-b border-dark-700 mb-3'>
          {Array.from({ length: columns }, (_, i) => `table-header-${i}`).map(key => (
            <Skeleton key={key} height={16} className='flex-1' />
          ))}
        </div>
      )}
      <div className='space-y-3'>
        {Array.from({ length: rows }, (_, i) => `table-row-${i}`).map(rowKey => (
          <div key={rowKey} className='flex gap-4'>
            {Array.from({ length: columns }, (_, j) => `${rowKey}-col-${j}`).map(cellKey => (
              <Skeleton key={cellKey} height={20} className='flex-1' />
            ))}
          </div>
        ))}
      </div>
    </div>
  )
}

interface ChartSkeletonProps {
  className?: string
  height?: number | string
  showLegend?: boolean
}

export const ChartSkeleton: React.FC<Readonly<ChartSkeletonProps>> = ({
  className,
  height = 300,
  showLegend = true,
}) => {
  return (
    <div className={clsx('w-full', className)} data-testid='chart-skeleton'>
      {showLegend && (
        <div className='flex gap-4 mb-4'>
          <Skeleton width={60} height={16} />
          <Skeleton width={80} height={16} />
          <Skeleton width={70} height={16} />
        </div>
      )}
      <Skeleton height={height} className='w-full' rounded='lg' />
    </div>
  )
}

interface ProcessCardSkeletonProps {
  className?: string
}

export const ProcessCardSkeleton: React.FC<Readonly<ProcessCardSkeletonProps>> = ({
  className,
}) => {
  return (
    <div
      className={clsx('bg-dark-800 border border-dark-700 rounded-xl p-4', className)}
      data-testid='process-card-skeleton'
    >
      <div className='flex items-start justify-between mb-3'>
        <div className='flex-1'>
          <Skeleton width={140} height={20} className='mb-2' />
          <Skeleton width={200} height={14} />
        </div>
        <Skeleton width={70} height={24} rounded='full' />
      </div>
      <div className='flex gap-2 mt-4'>
        <Skeleton width={80} height={32} rounded='lg' />
        <Skeleton width={80} height={32} rounded='lg' />
      </div>
    </div>
  )
}

interface StrategyCardSkeletonProps {
  className?: string
}

export const StrategyCardSkeleton: React.FC<Readonly<StrategyCardSkeletonProps>> = ({
  className,
}) => {
  return (
    <div
      className={clsx('bg-dark-800 border border-dark-700 rounded-xl p-4', className)}
      data-testid='strategy-card-skeleton'
    >
      <div className='flex items-center justify-between mb-3'>
        <div className='flex items-center gap-3'>
          <Skeleton width={10} height={10} rounded='full' />
          <Skeleton width={150} height={20} />
        </div>
        <Skeleton width={60} height={24} rounded='full' />
      </div>
      <div className='space-y-2 mb-4'>
        <div className='flex justify-between'>
          <Skeleton width={80} height={14} />
          <Skeleton width={100} height={14} />
        </div>
        <div className='flex justify-between'>
          <Skeleton width={60} height={14} />
          <Skeleton width={80} height={14} />
        </div>
      </div>
      <div className='flex gap-2'>
        <Skeleton width={70} height={28} rounded='lg' />
        <Skeleton width={70} height={28} rounded='lg' />
      </div>
    </div>
  )
}

interface ListSkeletonProps {
  items?: number
  className?: string
  itemClassName?: string
}

export const ListSkeleton: React.FC<Readonly<ListSkeletonProps>> = ({
  items = 5,
  className,
  itemClassName,
}) => {
  return (
    <div className={clsx('space-y-3', className)} data-testid='list-skeleton'>
      {Array.from({ length: items }, (_, i) => `list-item-${i}`).map(key => (
        <div
          key={key}
          className={clsx('flex items-center gap-3 p-3 bg-dark-800 rounded-lg', itemClassName)}
        >
          <Skeleton width={40} height={40} rounded='full' />
          <div className='flex-1 space-y-2'>
            <Skeleton width='60%' height={16} />
            <Skeleton width='40%' height={12} />
          </div>
          <Skeleton width={60} height={24} rounded='lg' />
        </div>
      ))}
    </div>
  )
}

interface OverviewSkeletonProps {
  className?: string
}

export const OverviewSkeleton: React.FC<Readonly<OverviewSkeletonProps>> = ({ className }) => {
  return (
    <div className={clsx('space-y-6', className)} data-testid='overview-skeleton'>
      {}
      <div className='grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4'>
        <MetricCardSkeleton />
        <MetricCardSkeleton />
        <MetricCardSkeleton />
        <MetricCardSkeleton />
      </div>
      {}
      <div className='grid grid-cols-1 lg:grid-cols-2 gap-6'>
        <CardSkeleton contentLines={4} />
        <CardSkeleton contentLines={4} />
      </div>
      {}
      <div className='grid grid-cols-1 lg:grid-cols-2 gap-6'>
        <CardSkeleton contentLines={5} />
        <CardSkeleton contentLines={5} />
      </div>
    </div>
  )
}

interface ProcessesSkeletonProps {
  className?: string
}

export const ProcessesSkeleton: React.FC<Readonly<ProcessesSkeletonProps>> = ({ className }) => {
  return (
    <div className={clsx('space-y-6', className)} data-testid='processes-skeleton'>
      {}
      <div>
        <Skeleton width={150} height={24} className='mb-4' />
        <div className='grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4'>
          <ProcessCardSkeleton />
          <ProcessCardSkeleton />
          <ProcessCardSkeleton />
        </div>
      </div>
      {}
      <div>
        <Skeleton width={200} height={24} className='mb-4' />
        <div className='grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4'>
          <ProcessCardSkeleton />
          <ProcessCardSkeleton />
        </div>
      </div>
    </div>
  )
}

interface StrategiesSkeletonProps {
  className?: string
}

export const StrategiesSkeleton: React.FC<Readonly<StrategiesSkeletonProps>> = ({ className }) => {
  return (
    <div className={clsx('space-y-6', className)} data-testid='strategies-skeleton'>
      {}
      <div className='flex items-center justify-between'>
        <Skeleton width={180} height={28} />
        <Skeleton width={140} height={36} rounded='lg' />
      </div>
      {}
      <div className='grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4'>
        <StrategyCardSkeleton />
        <StrategyCardSkeleton />
        <StrategyCardSkeleton />
      </div>
    </div>
  )
}

interface SignalCardSkeletonProps {
  className?: string
}

export const SignalCardSkeleton: React.FC<Readonly<SignalCardSkeletonProps>> = ({ className }) => {
  return (
    <div
      className={clsx('bg-dark-800 border border-dark-700 rounded-xl p-4', className)}
      data-testid='signal-card-skeleton'
    >
      <div className='flex items-center justify-between mb-3'>
        <div className='flex items-center space-x-3'>
          <Skeleton width={80} height={18} />
          <Skeleton width={50} height={22} rounded='full' />
        </div>
        <Skeleton width={60} height={14} />
      </div>
      <div className='space-y-2'>
        <div className='flex justify-between'>
          <Skeleton width={60} height={14} />
          <Skeleton width={80} height={14} />
        </div>
        <Skeleton width='100%' height={14} />
      </div>
    </div>
  )
}

interface SignalsSkeletonProps {
  className?: string
}

export const SignalsSkeleton: React.FC<Readonly<SignalsSkeletonProps>> = ({ className }) => {
  return (
    <div className={clsx('p-4 space-y-6', className)} data-testid='signals-skeleton'>
      {}
      <div className='flex items-center justify-between'>
        <Skeleton width={150} height={28} />
        <div className='flex gap-2'>
          <Skeleton width={100} height={36} rounded='lg' />
          <Skeleton width={100} height={36} rounded='lg' />
        </div>
      </div>
      {}
      <div className='grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4'>
        <SignalCardSkeleton />
        <SignalCardSkeleton />
        <SignalCardSkeleton />
        <SignalCardSkeleton />
        <SignalCardSkeleton />
        <SignalCardSkeleton />
      </div>
    </div>
  )
}

interface OrderCardSkeletonProps {
  className?: string
}

export const OrderCardSkeleton: React.FC<Readonly<OrderCardSkeletonProps>> = ({ className }) => {
  return (
    <div
      className={clsx('bg-dark-800 border border-dark-700 rounded-xl p-4', className)}
      data-testid='order-card-skeleton'
    >
      <div className='flex items-center justify-between mb-3'>
        <div className='flex items-center space-x-3'>
          <Skeleton width={80} height={18} />
          <Skeleton width={40} height={16} />
          <Skeleton width={50} height={16} />
        </div>
        <Skeleton width={70} height={22} rounded='full' />
      </div>
      <div className='grid grid-cols-2 gap-2'>
        <div className='flex justify-between'>
          <Skeleton width={40} height={14} />
          <Skeleton width={60} height={14} />
        </div>
        <div className='flex justify-between'>
          <Skeleton width={40} height={14} />
          <Skeleton width={70} height={14} />
        </div>
      </div>
    </div>
  )
}

interface OrdersSkeletonProps {
  className?: string
}

export const OrdersSkeleton: React.FC<Readonly<OrdersSkeletonProps>> = ({ className }) => {
  return (
    <div className={clsx('p-4 space-y-6', className)} data-testid='orders-skeleton'>
      {}
      <div className='flex gap-4 border-b border-dark-700 pb-2'>
        <Skeleton width={80} height={32} />
        <Skeleton width={100} height={32} />
      </div>
      {}
      <div className='space-y-3'>
        <OrderCardSkeleton />
        <OrderCardSkeleton />
        <OrderCardSkeleton />
        <OrderCardSkeleton />
        <OrderCardSkeleton />
      </div>
    </div>
  )
}

interface HealthSkeletonProps {
  className?: string
}

export const HealthSkeleton: React.FC<Readonly<HealthSkeletonProps>> = ({ className }) => {
  return (
    <div className={clsx('p-6 space-y-6', className)} data-testid='health-skeleton'>
      {}
      <div className='flex items-center justify-between'>
        <Skeleton width={200} height={28} />
        <Skeleton width={120} height={32} rounded='lg' />
      </div>
      {}
      <div className='grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4'>
        <MetricCardSkeleton />
        <MetricCardSkeleton />
        <MetricCardSkeleton />
        <MetricCardSkeleton />
      </div>
      {}
      <div className='grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4'>
        <ProcessCardSkeleton />
        <ProcessCardSkeleton />
        <ProcessCardSkeleton />
      </div>
    </div>
  )
}

interface MarketDataSkeletonProps {
  className?: string
}

export const MarketDataSkeleton: React.FC<Readonly<MarketDataSkeletonProps>> = ({ className }) => {
  return (
    <div className={clsx('p-4 space-y-4', className)} data-testid='market-data-skeleton'>
      {}
      <div className='flex items-center gap-4'>
        <Skeleton width={150} height={36} rounded='lg' />
        <Skeleton width={120} height={36} rounded='lg' />
        <Skeleton width={100} height={36} rounded='lg' />
      </div>
      {}
      <div className='grid grid-cols-2 md:grid-cols-4 gap-4'>
        <CardSkeleton showTitle={true} contentLines={1} />
        <CardSkeleton showTitle={true} contentLines={1} />
        <CardSkeleton showTitle={true} contentLines={1} />
        <CardSkeleton showTitle={true} contentLines={1} />
      </div>
      {}
      <ChartSkeleton height={400} />
    </div>
  )
}
