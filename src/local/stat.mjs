import { lstat } from 'fs/promises'

import hashFile from 'hash-stream/simple'
import log from 'logjs'

import { selectLocalHash, insertLocalFile } from '../db/sql.mjs'

export default async function stat (file) {
  const stats = await lstat(file.path)
  file.size = stats.size
  file.mtime = stats.mtime
  file.md5Hash = selectLocalHash.pluck().get(file)
  if (!file.md5Hash) {
    log.status('%s ... hashing', file.path)
    file.md5Hash = await hashFile(file.path)
    insertLocalFile(file)
  }
}
