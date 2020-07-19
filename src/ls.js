import { scan } from 's3js'

import report from './report'

export default async function ls (url, options) {
  const { directory } = options
  if (directory && !url.endsWith('/')) url += '/'

  let totalCount = 0
  let totalSize = 0

  const fileStream = scan(url, {
    Delimiter: directory ? '/' : undefined
  })

  for await (const {
    Key,
    Prefix,
    Size,
    LastModified,
    StorageClass
  } of fileStream) {
    if (Key && Key.endsWith('/')) continue

    totalCount++
    totalSize += Size || 0

    const storageClass = STORAGE_CLASS[StorageClass] || '?'
    const key = Prefix || Key
    const mtime = LastModified
    const size = Size

    report('list.file', { ...options, key, mtime, size, storageClass })
  }
  report('list.file.totals', { ...options, totalSize, totalCount })
}

const STORAGE_CLASS = {
  STANDARD: 'S',
  STANDARD_IA: 'I',
  GLACIER: 'G',
  DEEP_ARCHIVE: 'D'
}
