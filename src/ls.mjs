import log from 'logjs'

import File from './lib/file.mjs'
import { comma, fmtSize } from './util.mjs'
import { sql } from './db/index.mjs'

export default async function ls (url, opts) {
  const { long, rescan, human, total } = opts
  url = File.fromUrl(url, { resolve: true })

  if (rescan) await url.scan()

  let nTotalCount = 0
  let nTotalSize = 0

  const sql = url.isLocal ? listLocalFiles : listS3files
  for (const row of sql.all(url)) {
    const { path, mtime, size, storage } = row
    nTotalCount++
    nTotalSize += size
    let s = ''
    if (long) {
      s = (STORAGE[storage] || 'F') + '  '
      const sz = human ? fmtSize(size) : size.toString()
      s += sz.padStart(10) + '  '
      s += mtime + '  '
    }
    s += path
    log(s)
  }
  if (total) {
    const sz = human ? `${fmtSize(nTotalSize)}B` : `${comma(nTotalSize)} bytes`
    log(`\n${sz} in ${comma(nTotalCount)} file${nTotalCount > 1 ? 's' : ''}`)
  }
}

const STORAGE = {
  STANDARD: 'S',
  STANDARD_IA: 'I',
  GLACIER: 'G',
  DEEP_ARCHIVE: 'D'
}

const listLocalFiles = sql(`
  SELECT *
  FROM local_file_view
  WHERE path LIKE $path || '%'
  ORDER BY path
`)

const listS3files = sql(`
  SELECT *
  FROM s3_file_view
  WHERE bucket = $bucket
  AND   path LIKE $path || '%'
  ORDER BY bucket, path
`)
