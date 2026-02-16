import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import {
  Skeleton,
  SkeletonText,
  CardSkeleton,
  MetricCardSkeleton,
  TableSkeleton,
  ChartSkeleton,
  ProcessCardSkeleton,
  StrategyCardSkeleton,
  ListSkeleton,
  OverviewSkeleton,
  ProcessesSkeleton,
  StrategiesSkeleton,
  SignalCardSkeleton,
  SignalsSkeleton,
  OrderCardSkeleton,
  OrdersSkeleton,
  HealthSkeleton,
  MarketDataSkeleton,
} from './Skeleton'

describe('Skeleton', () => {
  it('renders with default props', () => {
    render(<Skeleton />)
    const skeleton = screen.getByTestId('skeleton')

    expect(skeleton).toBeInTheDocument()
    expect(skeleton).toHaveClass('bg-dark-700', 'animate-pulse', 'rounded-md')
  })
  it('renders with custom width and height', () => {
    render(<Skeleton width={100} height={50} />)
    const skeleton = screen.getByTestId('skeleton')

    expect(skeleton).toHaveStyle({ width: '100px', height: '50px' })
  })
  it('renders with string width and height', () => {
    render(<Skeleton width='50%' height='2rem' />)
    const skeleton = screen.getByTestId('skeleton')

    expect(skeleton).toHaveStyle({ width: '50%', height: '2rem' })
  })
  it('renders with different rounded values', () => {
    const { rerender } = render(<Skeleton rounded='none' />)

    expect(screen.getByTestId('skeleton')).toHaveClass('rounded-none')
    rerender(<Skeleton rounded='sm' />)
    expect(screen.getByTestId('skeleton')).toHaveClass('rounded-sm')
    rerender(<Skeleton rounded='lg' />)
    expect(screen.getByTestId('skeleton')).toHaveClass('rounded-lg')
    rerender(<Skeleton rounded='full' />)
    expect(screen.getByTestId('skeleton')).toHaveClass('rounded-full')
  })
  it('renders without animation when animate is false', () => {
    render(<Skeleton animate={false} />)
    const skeleton = screen.getByTestId('skeleton')

    expect(skeleton).not.toHaveClass('animate-pulse')
  })
  it('applies custom className', () => {
    render(<Skeleton className='custom-class' />)
    const skeleton = screen.getByTestId('skeleton')

    expect(skeleton).toHaveClass('custom-class')
  })
})
describe('SkeletonText', () => {
  it('renders default 3 lines', () => {
    render(<SkeletonText />)
    const container = screen.getByTestId('skeleton-text')
    const lines = container.querySelectorAll('[data-testid="skeleton"]')

    expect(lines).toHaveLength(3)
  })
  it('renders custom number of lines', () => {
    render(<SkeletonText lines={5} />)
    const container = screen.getByTestId('skeleton-text')
    const lines = container.querySelectorAll('[data-testid="skeleton"]')

    expect(lines).toHaveLength(5)
  })
  it('applies custom className', () => {
    render(<SkeletonText className='custom-text-skeleton' />)
    const container = screen.getByTestId('skeleton-text')

    expect(container).toHaveClass('custom-text-skeleton')
  })
})
describe('CardSkeleton', () => {
  it('renders with title by default', () => {
    render(<CardSkeleton />)
    const card = screen.getByTestId('card-skeleton')

    expect(card).toBeInTheDocument()
    expect(card).toHaveClass('panel')
  })
  it('renders without title when showTitle is false', () => {
    const { container } = render(<CardSkeleton showTitle={false} />)
    const flexRows = container.querySelectorAll('.flex.items-center.justify-between.mb-4')

    expect(flexRows).toHaveLength(0)
  })
  it('renders custom number of content lines', () => {
    render(<CardSkeleton contentLines={6} />)
    const card = screen.getByTestId('card-skeleton')
    const contentRows = card.querySelectorAll('.space-y-3 > div')

    expect(contentRows).toHaveLength(6)
  })
  it('applies custom className', () => {
    render(<CardSkeleton className='my-custom-card' />)
    const card = screen.getByTestId('card-skeleton')

    expect(card).toHaveClass('my-custom-card')
  })
})
describe('MetricCardSkeleton', () => {
  it('renders correctly', () => {
    render(<MetricCardSkeleton />)
    const card = screen.getByTestId('metric-card-skeleton')

    expect(card).toBeInTheDocument()
    expect(card).toHaveClass('bg-dark-800', 'border', 'border-dark-700', 'rounded-xl', 'p-4')
  })
  it('applies custom className', () => {
    render(<MetricCardSkeleton className='custom-metric' />)
    const card = screen.getByTestId('metric-card-skeleton')

    expect(card).toHaveClass('custom-metric')
  })
})
describe('TableSkeleton', () => {
  it('renders with default columns and rows', () => {
    render(<TableSkeleton />)
    const table = screen.getByTestId('table-skeleton')

    expect(table).toBeInTheDocument()
    const rows = table.querySelectorAll('.space-y-3 > div')

    expect(rows).toHaveLength(5)
  })
  it('renders custom number of columns and rows', () => {
    render(<TableSkeleton columns={3} rows={10} />)
    const table = screen.getByTestId('table-skeleton')
    const rows = table.querySelectorAll('.space-y-3 > div')

    expect(rows).toHaveLength(10)
    const firstRowCells = rows[0].querySelectorAll('[data-testid="skeleton"]')

    expect(firstRowCells).toHaveLength(3)
  })
  it('renders without header when showHeader is false', () => {
    const { container } = render(<TableSkeleton showHeader={false} />)
    const headerRow = container.querySelector('.border-b.border-dark-700')

    expect(headerRow).not.toBeInTheDocument()
  })
  it('applies custom className', () => {
    render(<TableSkeleton className='my-table' />)
    const table = screen.getByTestId('table-skeleton')

    expect(table).toHaveClass('my-table')
  })
})
describe('ChartSkeleton', () => {
  it('renders with legend by default', () => {
    render(<ChartSkeleton />)
    const chart = screen.getByTestId('chart-skeleton')

    expect(chart).toBeInTheDocument()
    const legendItems = chart.querySelectorAll('.flex.gap-4 [data-testid="skeleton"]')

    expect(legendItems.length).toBeGreaterThan(0)
  })
  it('renders without legend when showLegend is false', () => {
    const { container } = render(<ChartSkeleton showLegend={false} />)
    const legendRow = container.querySelector('.flex.gap-4.mb-4')

    expect(legendRow).not.toBeInTheDocument()
  })
  it('renders with custom height', () => {
    render(<ChartSkeleton height={400} />)
    const chart = screen.getByTestId('chart-skeleton')
    const mainSkeleton = chart.querySelector('.w-full[data-testid="skeleton"]')

    expect(mainSkeleton).toHaveStyle({ height: '400px' })
  })
  it('applies custom className', () => {
    render(<ChartSkeleton className='my-chart' />)
    const chart = screen.getByTestId('chart-skeleton')

    expect(chart).toHaveClass('my-chart')
  })
})
describe('ProcessCardSkeleton', () => {
  it('renders correctly', () => {
    render(<ProcessCardSkeleton />)
    const card = screen.getByTestId('process-card-skeleton')

    expect(card).toBeInTheDocument()
    expect(card).toHaveClass('bg-dark-800', 'border', 'border-dark-700', 'rounded-xl', 'p-4')
  })
  it('applies custom className', () => {
    render(<ProcessCardSkeleton className='custom-process' />)
    const card = screen.getByTestId('process-card-skeleton')

    expect(card).toHaveClass('custom-process')
  })
})
describe('StrategyCardSkeleton', () => {
  it('renders correctly', () => {
    render(<StrategyCardSkeleton />)
    const card = screen.getByTestId('strategy-card-skeleton')

    expect(card).toBeInTheDocument()
    expect(card).toHaveClass('bg-dark-800', 'border', 'border-dark-700', 'rounded-xl', 'p-4')
  })
  it('applies custom className', () => {
    render(<StrategyCardSkeleton className='custom-strategy' />)
    const card = screen.getByTestId('strategy-card-skeleton')

    expect(card).toHaveClass('custom-strategy')
  })
})
describe('ListSkeleton', () => {
  it('renders default 5 items', () => {
    render(<ListSkeleton />)
    const list = screen.getByTestId('list-skeleton')
    const items = list.children

    expect(items).toHaveLength(5)
  })
  it('renders custom number of items', () => {
    render(<ListSkeleton items={8} />)
    const list = screen.getByTestId('list-skeleton')
    const items = list.children

    expect(items).toHaveLength(8)
  })
  it('applies custom className', () => {
    render(<ListSkeleton className='my-list' />)
    const list = screen.getByTestId('list-skeleton')

    expect(list).toHaveClass('my-list')
  })
  it('applies itemClassName to each item', () => {
    render(<ListSkeleton items={3} itemClassName='custom-item' />)
    const list = screen.getByTestId('list-skeleton')
    const items = list.children

    Array.from(items).forEach(item => {
      expect(item).toHaveClass('custom-item')
    })
  })
})
describe('OverviewSkeleton', () => {
  it('renders all overview sections', () => {
    render(<OverviewSkeleton />)
    const overview = screen.getByTestId('overview-skeleton')

    expect(overview).toBeInTheDocument()
    const metricCards = screen.getAllByTestId('metric-card-skeleton')

    expect(metricCards).toHaveLength(4)
    const cardSkeletons = screen.getAllByTestId('card-skeleton')

    expect(cardSkeletons).toHaveLength(4)
  })
  it('applies custom className', () => {
    render(<OverviewSkeleton className='custom-overview' />)
    const overview = screen.getByTestId('overview-skeleton')

    expect(overview).toHaveClass('custom-overview')
  })
})
describe('ProcessesSkeleton', () => {
  it('renders all process sections', () => {
    render(<ProcessesSkeleton />)
    const processes = screen.getByTestId('processes-skeleton')

    expect(processes).toBeInTheDocument()
    const processCards = screen.getAllByTestId('process-card-skeleton')

    expect(processCards.length).toBeGreaterThan(0)
  })
  it('applies custom className', () => {
    render(<ProcessesSkeleton className='custom-processes' />)
    const processes = screen.getByTestId('processes-skeleton')

    expect(processes).toHaveClass('custom-processes')
  })
})
describe('StrategiesSkeleton', () => {
  it('renders strategy skeleton layout', () => {
    render(<StrategiesSkeleton />)
    const strategies = screen.getByTestId('strategies-skeleton')

    expect(strategies).toBeInTheDocument()
    const strategyCards = screen.getAllByTestId('strategy-card-skeleton')

    expect(strategyCards.length).toBeGreaterThan(0)
  })
  it('applies custom className', () => {
    render(<StrategiesSkeleton className='custom-strategies' />)
    const strategies = screen.getByTestId('strategies-skeleton')

    expect(strategies).toHaveClass('custom-strategies')
  })
})
describe('SignalCardSkeleton', () => {
  it('renders correctly', () => {
    render(<SignalCardSkeleton />)
    const card = screen.getByTestId('signal-card-skeleton')

    expect(card).toBeInTheDocument()
    expect(card).toHaveClass('bg-dark-800', 'border', 'border-dark-700', 'rounded-xl', 'p-4')
  })
  it('applies custom className', () => {
    render(<SignalCardSkeleton className='custom-signal' />)
    const card = screen.getByTestId('signal-card-skeleton')

    expect(card).toHaveClass('custom-signal')
  })
})
describe('SignalsSkeleton', () => {
  it('renders signal skeleton layout', () => {
    render(<SignalsSkeleton />)
    const signals = screen.getByTestId('signals-skeleton')

    expect(signals).toBeInTheDocument()
    const signalCards = screen.getAllByTestId('signal-card-skeleton')

    expect(signalCards.length).toBeGreaterThan(0)
  })
  it('applies custom className', () => {
    render(<SignalsSkeleton className='custom-signals' />)
    const signals = screen.getByTestId('signals-skeleton')

    expect(signals).toHaveClass('custom-signals')
  })
})
describe('OrderCardSkeleton', () => {
  it('renders correctly', () => {
    render(<OrderCardSkeleton />)
    const card = screen.getByTestId('order-card-skeleton')

    expect(card).toBeInTheDocument()
    expect(card).toHaveClass('bg-dark-800', 'border', 'border-dark-700', 'rounded-xl', 'p-4')
  })
  it('applies custom className', () => {
    render(<OrderCardSkeleton className='custom-order' />)
    const card = screen.getByTestId('order-card-skeleton')

    expect(card).toHaveClass('custom-order')
  })
})
describe('OrdersSkeleton', () => {
  it('renders orders skeleton layout', () => {
    render(<OrdersSkeleton />)
    const orders = screen.getByTestId('orders-skeleton')

    expect(orders).toBeInTheDocument()
    const orderCards = screen.getAllByTestId('order-card-skeleton')

    expect(orderCards.length).toBeGreaterThan(0)
  })
  it('applies custom className', () => {
    render(<OrdersSkeleton className='custom-orders' />)
    const orders = screen.getByTestId('orders-skeleton')

    expect(orders).toHaveClass('custom-orders')
  })
})
describe('HealthSkeleton', () => {
  it('renders health skeleton layout', () => {
    render(<HealthSkeleton />)
    const health = screen.getByTestId('health-skeleton')

    expect(health).toBeInTheDocument()
    const metricCards = screen.getAllByTestId('metric-card-skeleton')

    expect(metricCards.length).toBeGreaterThan(0)
    const processCards = screen.getAllByTestId('process-card-skeleton')

    expect(processCards.length).toBeGreaterThan(0)
  })
  it('applies custom className', () => {
    render(<HealthSkeleton className='custom-health' />)
    const health = screen.getByTestId('health-skeleton')

    expect(health).toHaveClass('custom-health')
  })
})
describe('MarketDataSkeleton', () => {
  it('renders market data skeleton layout', () => {
    render(<MarketDataSkeleton />)
    const marketData = screen.getByTestId('market-data-skeleton')

    expect(marketData).toBeInTheDocument()
    const chartSkeleton = screen.getByTestId('chart-skeleton')

    expect(chartSkeleton).toBeInTheDocument()
    const cardSkeletons = screen.getAllByTestId('card-skeleton')

    expect(cardSkeletons.length).toBeGreaterThan(0)
  })
  it('applies custom className', () => {
    render(<MarketDataSkeleton className='custom-market' />)
    const marketData = screen.getByTestId('market-data-skeleton')

    expect(marketData).toHaveClass('custom-market')
  })
})
