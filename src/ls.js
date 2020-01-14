import { scan, stat, parseAddress } from 's3js'

import report from './report'

export default async function ls (url, options) {
  const { directory } = options
  if (directory && !url.endsWith('/')) url += '/'

  let totalCount = 0
  let totalSize = 0
  const { Bucket } = parseAddress(url)

  const fileStream = scan(url, {
    Delimiter: directory ? '/' : undefined
  })

  for await (const { Key, Prefix, ETag, Size, LastModified } of fileStream) {
    if (Key && Key.endsWith('/')) continue

    totalCount++
    totalSize += Size || 0

    let md5 = ETag ? ETag.replace(/"/g, '') : undefined
    if (md5 && Key && md5.includes('-')) {
      const stats = await stat(`s3://${Bucket}/${Key}`)
      md5 = stats.md5 || 'UNKNOWN'
    }

    report('list.file', {
      ...options,
      key: Prefix || Key,
      md5,
      mtime: LastModified,
      size: Size
    })
  }
  report('list.file.totals', { ...options, totalSize, totalCount })
}
