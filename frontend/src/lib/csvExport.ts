const escapeCsvField = (value: string): string => {
  if (value.includes(',') || value.includes('"') || value.includes('\n')) {
    return `"${value.replaceAll('"', '""')}"`
  }

  return value
}

export const exportToCSV = (filename: string, headers: string[], rows: string[][]): void => {
  const headerLine = headers.map(escapeCsvField).join(',')
  const dataLines = rows.map(row => row.map(escapeCsvField).join(','))
  const csv = [headerLine, ...dataLines].join('\n')
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')

  link.href = url
  link.download = filename
  link.click()
  URL.revokeObjectURL(url)
}
