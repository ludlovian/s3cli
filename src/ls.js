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

  for await (const { Key, Prefix, ETag, Size, LastModified } of fileStream) {
    if (Key && Key.endsWith('/')) continue

    totalCount++
    totalSize += Size || 0

    report('list.file', {
      ...options,
      key: Prefix || Key,
      md5: ETag ? ETag.replace(/"/g, '') : undefined,
      mtime: LastModified,
      size: Size
    })
  }
  report('list.file.totals', { ...options, totalSize, totalCount })
}
