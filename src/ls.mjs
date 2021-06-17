import { list } from './vfs.mjs'
import report from './report.mjs'

export default async function ls (url, options) {
  let totalCount = 0
  let totalSize = 0

  const lister = list(url)
  lister.on('files', files => {
    files.forEach(file => {
      totalCount++
      totalSize += file.size || 0
      file.storage = STORAGE_CLASS[file.storage] || 'F'
      report('list.file', { ...options, ...file })
    })
  })
  await lister.done
  report('list.file.totals', { ...options, totalSize, totalCount })
}

const STORAGE_CLASS = {
  STANDARD: 'S',
  STANDARD_IA: 'I',
  GLACIER: 'G',
  DEEP_ARCHIVE: 'D'
}
