import log from 'logjs'

import File from './lib/file.mjs'
import { comma, fmtSize } from './util.mjs'
import db from './db.mjs'

export default async function ls (url, opts) {
  db.open()
  const { long, rescan, human, total } = opts
  url = File.fromUrl(url, { resolve: true })

  const list = {
    local: db.getLocalFiles,
    s3: db.getS3Files,
    gdrive: db.getGdriveFiles
  }[url.type]

  if (rescan) await url.scan()

  let nTotalCount = 0
  let nTotalSize = 0

  for (const row of list(url)) {
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
