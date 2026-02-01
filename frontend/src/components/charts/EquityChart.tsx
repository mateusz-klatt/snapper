import React from 'react'
import Plot from 'react-plotly.js'
import type { PlotParams } from 'react-plotly.js'

interface EquityDataPoint {
  timestamp: string
  equity: number
}
interface EquityChartProps {
  data: EquityDataPoint[]
  title?: string
  height?: number
}

export const EquityChart: React.FC<Readonly<EquityChartProps>> = ({
  data,
  title = 'Equity Curve',
  height = 500,
}) => {
  const timestamps = data.map(d => d.timestamp)
  const equities = data.map(d => d.equity)
  const plotData: PlotParams['data'] = [
    {
      x: timestamps,
      y: equities,
      type: 'scatter',
      mode: 'lines',
      name: 'Equity',
      line: { color: '#2E86AB', width: 1.5 },
      hovertemplate: '<b>%{x}</b><br>Equity: $%{y:,.2f}<extra></extra>',
    },
  ]
  const layout: PlotParams['layout'] = {
    title: { text: title },
    xaxis: {
      title: { text: 'Date' },
      showgrid: true,
    },
    yaxis: {
      title: { text: 'Equity ($)' },
      showgrid: true,
    },
    hovermode: 'x unified',
    height: height,
    margin: { l: 60, r: 30, t: 60, b: 60 },
    shapes: [
      {
        type: 'line',
        x0: timestamps[0],
        x1: timestamps[timestamps.length - 1],
        y0: data[0]?.equity || 10000,
        y1: data[0]?.equity || 10000,
        line: {
          color: 'gray',
          width: 2,
          dash: 'dash',
        },
      },
    ],
    annotations: [
      {
        x: timestamps[Math.floor(timestamps.length * 0.95)],
        y: data[0]?.equity || 10000,
        text: 'Break-even',
        showarrow: false,
        xanchor: 'left',
        yanchor: 'bottom',
      },
    ],
  }
  const config: PlotParams['config'] = {
    displayModeBar: true,
    displaylogo: false,
    modeBarButtonsToRemove: ['lasso2d', 'select2d'],
    responsive: true,
  }

  return (
    <div className='equity-chart'>
      <Plot data={plotData} layout={layout} config={config} style={{ width: '100%' }} />
    </div>
  )
}
