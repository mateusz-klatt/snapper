import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

const { mockRender, mockCreateRoot } = vi.hoisted(() => {
  const mockRender = vi.fn()
  const mockCreateRoot = vi.fn(() => ({
    render: mockRender,
  }))

  return { mockRender, mockCreateRoot }
})

vi.mock('react-dom/client', () => ({
  default: {
    createRoot: mockCreateRoot,
  },
  createRoot: mockCreateRoot,
}))
describe('main', () => {
  let rootElement: HTMLDivElement | null = null

  beforeEach(() => {
    vi.resetModules()
    mockRender.mockClear()
    mockCreateRoot.mockClear()
    rootElement = document.createElement('div')
    rootElement.id = 'root'
    document.body.appendChild(rootElement)
  })
  afterEach(() => {
    if (rootElement) {
      rootElement.remove()
      rootElement = null
    }
  })
  it('renders the app when root element exists', async () => {
    await import('./main')
    expect(mockCreateRoot).toHaveBeenCalledWith(rootElement)
    expect(mockRender).toHaveBeenCalled()
  }, 10000)
  it('throws error when root element is not found', async () => {
    if (rootElement) {
      rootElement.remove()
      rootElement = null
    }

    await expect(import('./main')).rejects.toThrow('Root element not found')
  })
})
